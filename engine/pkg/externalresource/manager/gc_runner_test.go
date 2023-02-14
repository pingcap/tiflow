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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/bucket"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
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
	gcExecutorCh := make(chan []*resModel.ResourceMeta, 16)
	runner := NewGCRunner(meta, nil, nil)
	runner.gcHandlers[resModel.ResourceTypeLocalFile] = &mockResourceController{gcRequestCh: reqCh}
	runner.gcHandlers[resModel.ResourceTypeS3] = &mockResourceController{
		gcRequestCh:  reqCh,
		gcExecutorCh: gcExecutorCh,
	}
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
		t.Fatal("timeout waiting for GC")
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

func (c *mockMetaClientErrOnce) DeleteResource(ctx context.Context, resourceKey pkgOrm.ResourceKey) (pkgOrm.Result, error) {
	if _, erred := c.methodsAllReadyErred["DeleteResource"]; !erred {
		c.methodsAllReadyErred["DeleteResource"] = struct{}{}
		return nil, errors.New("injected error")
	}

	return c.ResourceClient.DeleteResource(ctx, resourceKey)
}

func (c *mockMetaClientErrOnce) GetOneResourceForGC(ctx context.Context) (*resModel.ResourceMeta, error) {
	if _, erred := c.methodsAllReadyErred["GetOneResourceForGC"]; !erred {
		c.methodsAllReadyErred["GetOneResourceForGC"] = struct{}{}
		return nil, errors.New("injected error")
	}

	return c.ResourceClient.GetOneResourceForGC(ctx)
}

func (c *mockMetaClientErrOnce) DeleteResourcesByTypeAndExecutorIDs(ctx context.Context,
	resType resModel.ResourceType, executorID ...model.ExecutorID,
) (pkgOrm.Result, error) {
	if _, erred := c.methodsAllReadyErred["DeleteResourcesByTypeAndExecutorIDs"]; !erred {
		c.methodsAllReadyErred["DeleteResourcesByTypeAndExecutorIDs"] = struct{}{}
		return nil, errors.New("injected error")
	}

	return c.ResourceClient.DeleteResourcesByTypeAndExecutorIDs(ctx, resType, executorID...)
}

func (c *mockMetaClientErrOnce) QueryResourcesByExecutorIDs(ctx context.Context,
	executorID ...model.ExecutorID,
) ([]*resModel.ResourceMeta, error) {
	if _, erred := c.methodsAllReadyErred["QueryResourcesByExecutorIDs"]; !erred {
		c.methodsAllReadyErred["QueryResourcesByExecutorIDs"] = struct{}{}
		return nil, errors.New("injected error")
	}

	return c.ResourceClient.QueryResourcesByExecutorIDs(ctx, executorID...)
}

func TestGCRunnerNotify(t *testing.T) {
	t.Parallel()
	helper := newGCRunnerTestHelper()
	helper.Start()

	resources := []string{"/local/resource-1", "/s3/resource-1"}
	for _, res := range resources {
		err := helper.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
			ID:        res,
			Job:       "job-1",
			Worker:    "worker-1",
			Executor:  "executor-1",
			GCPending: true,
		})
		require.NoError(t, err)

		// Note that since we are not advancing the clock,
		// GC can only be triggered by calling Notify.
		helper.Runner.GCNotify()

		gcRes := helper.WaitGC(t)
		require.Equal(t, res, gcRes.ID)
	}

	helper.Close()
}

func TestGCRunnerUnsupportedResourceType(t *testing.T) {
	t.Parallel()
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
			pkgOrm.ResourceKey{
				JobID: "job-1",
				ID:    "/unsupported/resource-1",
			})
		require.NoError(t, err)
		require.True(t, res.GCPending)
	}

	helper.Close()
}

func TestGCRunnerTicker(t *testing.T) {
	t.Parallel()
	helper := newGCRunnerTestHelper()
	helper.Start()

	resources := []string{"/local/resource-1", "/s3/resource-1"}
	for _, res := range resources {
		err := helper.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
			ID:        res,
			Job:       "job-1",
			Worker:    "worker-1",
			Executor:  "executor-1",
			GCPending: true,
		})
		require.NoError(t, err)

		helper.Clock.Add(10 * time.Second)

		gcRes := helper.WaitGC(t)
		require.Equal(t, res, gcRes.ID)
	}

	helper.Close()
}

func TestGCRunnerMultiple(t *testing.T) {
	t.Parallel()
	helper := newGCRunnerTestHelper()

	resources := []string{"/local/resource", "/s3/resource"}
	const numResources = 1000
	for i := 0; i < numResources; i++ {
		err := helper.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
			ID:        fmt.Sprintf("%s-%d", resources[rand.Intn(2)], i),
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
	t.Parallel()
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

func TestGCExecutors(t *testing.T) {
	helper := newGCRunnerTestHelper()
	testGCExecutors(t, helper)
	helper.Close()
}

func TestGCExecutorsRetry(t *testing.T) {
	helper := newGCRunnerTestHelperWithMeta(newMockMetaClientErrOnce())
	testGCExecutors(t, helper)
	helper.Close()
}

func testGCExecutors(t *testing.T, helper *gcRunnerTestHelper) {
	gcExecutorsTimeout = 10 * time.Second
	gcExecutorsRateLimit = 200
	gcExecutorsMinIntervalMs = int64(10)
	gcExecutorsMaxIntervalMs = int64(100)

	checkAlive := func(ctx context.Context, executors ...model.ExecutorID) {
		for _, executor := range executors {
			res, err := helper.Meta.GetResourceByID(ctx, pkgOrm.ResourceKey{
				JobID: bucket.GetDummyJobID(executor),
				ID:    bucket.DummyResourceID,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
		}
	}
	checkOffline := func(ctx context.Context, executors ...model.ExecutorID) {
		metas, err := helper.Meta.QueryResourcesByExecutorIDs(ctx, executors...)
		require.NoError(t, err)
		for _, meta := range metas {
			tp, resName, err := resModel.ParseResourceID(meta.ID)
			require.NoError(t, err)
			require.Equal(t, resModel.ResourceTypeS3, tp)
			require.NotEqual(t, bucket.GetDummyResourceName(), resName)
		}
	}

	resources := []string{"/local/resource", "/s3/resource"}
	executors := []string{"executor-1", "executor-2", "executor-3", "executor-never-offline"}
	// generate mock meta
	for _, executor := range executors {
		err := helper.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
			ID:       bucket.DummyResourceID,
			Job:      bucket.GetDummyJobID(model.ExecutorID(executor)),
			Worker:   bucket.DummyWorkerID,
			Executor: model.ExecutorID(executor),
		})
		require.NoError(t, err)
	}
	const numResources = 1000
	for i := 0; i < numResources; i++ {
		workerID := rand.Intn(4)
		err := helper.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
			ID:        fmt.Sprintf("%s-%d", resources[rand.Intn(2)], i),
			Job:       "job-1",
			Worker:    fmt.Sprintf("worker-%d", workerID),
			Executor:  model.ExecutorID(executors[workerID]),
			GCPending: i%2 == 0, // marks half the resources as needing GC.
		})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	helper.Runner.GCExecutors(ctx, "executor-1", "executor-2")
	checkOffline(ctx, "executor-1", "executor-2")
	checkAlive(ctx, "executor-3", "executor-never-offline")

	helper.Runner.GCExecutors(ctx, "executor-3")
	checkOffline(ctx, "executor-3")
	checkAlive(ctx, "executor-never-offline")
}
