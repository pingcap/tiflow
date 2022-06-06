// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/pkg/clock"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
)

type gcRunnerTestHelper struct {
	Runner *DefaultGCRunner
	Meta   pkgOrm.ResourceClient
	Clock  *clock.Mock

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	errCh  chan error

	gcRequestCh chan *resModel.ResourceMeta
}

func newGCRunnerTestHelper() *gcRunnerTestHelper {
	meta, err := pkgOrm.NewMockClient()
	if err != nil {
		panic(err)
	}
	return newGCRunnerTestHelperWithMeta(meta)
}

func newGCRunnerTestHelperWithMeta(meta pkgOrm.ResourceClient) *gcRunnerTestHelper {
	reqCh := make(chan *resModel.ResourceMeta, 16)
	mockHandler := func(ctx context.Context, meta *resModel.ResourceMeta) error {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case reqCh <- meta:
		}
		return nil
	}
	runner := NewGCRunner(meta, map[resModel.ResourceType]GCHandlerFunc{"local": mockHandler})
	clk := clock.NewMock()
	runner.clock = clk
	ctx, cancel := context.WithCancel(context.Background())

	return &gcRunnerTestHelper{
		Runner: runner,
		Meta:   meta,
		Clock:  clk,

		ctx:         ctx,
		cancel:      cancel,
		errCh:       make(chan error, 1),
		gcRequestCh: reqCh,
	}
}

func (h *gcRunnerTestHelper) Start() {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()

		h.errCh <- h.Runner.Run(h.ctx)
	}()
}

func (h *gcRunnerTestHelper) Close() {
	h.cancel()
	h.wg.Wait()
}

func (h *gcRunnerTestHelper) WaitGC(t *testing.T) (meta *resModel.ResourceMeta) {
	select {
	case <-time.After(2 * time.Second):
		t.FailNow()
	case meta = <-h.gcRequestCh:
	}
	return
}

// mockMetaClientErrOnce is a temporary solution for testing
// the retry logic of gcOnce().
// TODO make a more generic version of this struct, and
// do better error condition testing in other situations too.
type mockMetaClientErrOnce struct {
	pkgOrm.ResourceClient

	methodsAllReadyErred map[string]struct{}
}

func newMockMetaClientErrOnce() *mockMetaClientErrOnce {
	inner, err := pkgOrm.NewMockClient()
	if err != nil {
		panic(err)
	}

	return &mockMetaClientErrOnce{
		ResourceClient:       inner,
		methodsAllReadyErred: make(map[string]struct{}),
	}
}

func (c *mockMetaClientErrOnce) DeleteResource(ctx context.Context, resourceID string) (pkgOrm.Result, error) {
	if _, erred := c.methodsAllReadyErred["DeleteResource"]; !erred {
		c.methodsAllReadyErred["DeleteResource"] = struct{}{}
		return nil, errors.New("injected error")
	}

	return c.ResourceClient.DeleteResource(ctx, resourceID)
}

func (c *mockMetaClientErrOnce) GetOneResourceForGC(ctx context.Context) (*resModel.ResourceMeta, error) {
	if _, erred := c.methodsAllReadyErred["GetOneResourceForGC"]; !erred {
		c.methodsAllReadyErred["GetOneResourceForGC"] = struct{}{}
		return nil, errors.New("injected error")
	}

	return c.ResourceClient.GetOneResourceForGC(ctx)
}

func TestGCRunnerNotify(t *testing.T) {
	helper := newGCRunnerTestHelper()

	err := helper.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
		ID:        "/local/resource-1",
		Job:       "job-1",
		Worker:    "worker-1",
		Executor:  "executor-1",
		GCPending: true,
	})
	require.NoError(t, err)

	helper.Start()
	// Note that since we are not advancing the clock,
	// GC can only be triggered by calling Notify.
	helper.Runner.GCNotify()

	gcRes := helper.WaitGC(t)
	require.Equal(t, "/local/resource-1", gcRes.ID)

	helper.Close()
}

func TestGCRunnerUnsupportedResourceType(t *testing.T) {
	helper := newGCRunnerTestHelper()

	// Unsupported resources should be ignored by the GCRunner.
	err := helper.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
		ID:        "/unsupported/resource-1",
		Job:       "job-1",
		Worker:    "worker-1",
		Executor:  "executor-1",
		GCPending: true,
	})
	require.NoError(t, err)

	helper.Start()
	helper.Runner.GCNotify()

	// Assert that unsupported resources should not cause panic
	// and are NOT removed from meta.
	startTime := time.Now()
	for {
		if time.Since(startTime) > 1*time.Second {
			break
		}

		res, err := helper.Meta.GetResourceByID(
			context.Background(),
			"/unsupported/resource-1")
		require.NoError(t, err)
		require.True(t, res.GCPending)
	}

	helper.Close()
}

func TestGCRunnerTicker(t *testing.T) {
	helper := newGCRunnerTestHelper()

	err := helper.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
		ID:        "/local/resource-1",
		Job:       "job-1",
		Worker:    "worker-1",
		Executor:  "executor-1",
		GCPending: true,
	})
	require.NoError(t, err)

	helper.Start()
	time.Sleep(10 * time.Millisecond)
	helper.Clock.Add(10 * time.Second)
	helper.Clock.Add(10 * time.Second)

	gcRes := helper.WaitGC(t)
	require.Equal(t, "/local/resource-1", gcRes.ID)

	helper.Close()
}

func TestGCRunnerMultiple(t *testing.T) {
	helper := newGCRunnerTestHelper()

	const numResources = 1000
	for i := 0; i < numResources; i++ {
		err := helper.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
			ID:        fmt.Sprintf("/local/resource-%d", i),
			Job:       "job-1",
			Worker:    "worker-1",
			Executor:  "executor-1",
			GCPending: i%2 == 0, // marks half the resources as needing GC.
		})
		require.NoError(t, err)
	}

	helper.Start()

	alreadyGCedSet := make(map[resModel.ResourceID]struct{})
loop:
	for {
		select {
		case meta := <-helper.gcRequestCh:
			_, exists := alreadyGCedSet[meta.ID]
			require.False(t, exists)
			alreadyGCedSet[meta.ID] = struct{}{}

			if len(alreadyGCedSet) == 500 {
				break loop
			}
		default:
		}

		helper.Runner.GCNotify()
	}

	helper.Close()
}

func TestGCRunnerRetry(t *testing.T) {
	mockMeta := newMockMetaClientErrOnce()
	helper := newGCRunnerTestHelperWithMeta(mockMeta)

	err := helper.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
		ID:        "/local/resource-1",
		Job:       "job-1",
		Worker:    "worker-1",
		Executor:  "executor-1",
		GCPending: true,
	})
	require.NoError(t, err)

	helper.Start()

	// Note that since we are not advancing the clock,
	// GC can only be triggered by calling Notify.
	helper.Runner.GCNotify()

	gcRes := helper.WaitGC(t)
	require.Equal(t, "/local/resource-1", gcRes.ID)

	helper.Close()
}
