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
	"sync"
	"testing"
	"time"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/s3"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/stretchr/testify/require"
)

type gcTestHelper struct {
	ExecInfo *MockExecutorInfoProvider
	JobInfo  *MockJobStatusProvider
	GCRunner *MockGCRunner
	Meta     pkgOrm.Client
	Coord    *DefaultGCCoordinator

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	errCh  chan error
}

func newGCTestHelper() *gcTestHelper {
	execInfo := NewMockExecutorInfoProvider()
	jobInfo := NewMockJobStatusProvider()
	meta, err := pkgOrm.NewMockClient()
	if err != nil {
		panic(err)
	}
	gcRunner := NewMockGCRunner(meta)
	coord := NewGCCoordinator(execInfo, jobInfo, meta, gcRunner)

	ctx, cancel := context.WithCancel(context.Background())
	ret := &gcTestHelper{
		ExecInfo: execInfo,
		JobInfo:  jobInfo,
		Meta:     meta,
		GCRunner: gcRunner,
		Coord:    coord,

		ctx:    ctx,
		cancel: cancel,
		errCh:  make(chan error, 1),
	}

	return ret
}

func (h *gcTestHelper) Start() {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()

		h.errCh <- h.Coord.Run(h.ctx)
	}()
}

func (h *gcTestHelper) Close() {
	h.cancel()
	h.wg.Wait()
}

func (h *gcTestHelper) GetError() error {
	return <-h.errCh
}

func (h *gcTestHelper) IsGCPending(t *testing.T, resourceKey pkgOrm.ResourceKey) bool {
	meta, err := h.Meta.GetResourceByID(context.Background(), resourceKey)
	if err != nil {
		require.NoError(t, err)
	}

	return meta.GCPending
}

func (h *gcTestHelper) IsRemoved(t *testing.T, resourceKey pkgOrm.ResourceKey) bool {
	_, err := h.Meta.GetResourceByID(context.Background(), resourceKey)
	if pkgOrm.IsNotFoundError(err) {
		return true
	}
	require.NoError(t, err)
	return false
}

func (h *gcTestHelper) LoadDefaultMockData(t *testing.T) {
	h.ExecInfo.AddExecutor("executor-1", "addr-1:8080")
	h.ExecInfo.AddExecutor("executor-2", "addr-2:8080")
	h.ExecInfo.AddExecutor("executor-3", "addr-3:8080")

	h.JobInfo.SetJobStatus("job-1", frameModel.MasterStateInit)
	h.JobInfo.SetJobStatus("job-2", frameModel.MasterStateInit)
	h.JobInfo.SetJobStatus("job-3", frameModel.MasterStateInit)

	err := h.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
		ID:       "/local/resource-1",
		Job:      "job-1",
		Worker:   "worker-1",
		Executor: "executor-1",
	})
	require.NoError(t, err)

	err = h.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
		ID:       "/local/resource-2",
		Job:      "job-2",
		Worker:   "worker-2",
		Executor: "executor-2",
	})
	require.NoError(t, err)

	err = h.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
		ID:       "/local/resource-3",
		Job:      "job-3",
		Worker:   "worker-3",
		Executor: "executor-3",
	})
	require.NoError(t, err)

	executors := []string{"executor-1", "executor-2", "executor-3"}
	for _, executor := range executors {
		id := model.ExecutorID(executor)
		err = h.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
			ID:       s3.DummyResourceID,
			Job:      s3.GetDummyJobID(id),
			Worker:   s3.DummyWorkerID,
			Executor: id,
		})
		require.NoError(t, err)
	}
}

func TestGCCoordinatorRemoveExecutors(t *testing.T) {
	t.Parallel()
	helper := newGCTestHelper()
	helper.LoadDefaultMockData(t)
	helper.Start()

	require.False(t, helper.IsGCPending(t, pkgOrm.ResourceKey{JobID: "job-1", ID: "/local/resource-1"}))
	helper.ExecInfo.RemoveExecutor("executor-1")
	require.Eventually(t, func() bool {
		return helper.IsRemoved(t, pkgOrm.ResourceKey{JobID: "job-1", ID: "/local/resource-1"})
	}, 1*time.Second, 10*time.Millisecond)

	time.Sleep(20 * time.Millisecond)
	require.False(t, helper.IsGCPending(t, pkgOrm.ResourceKey{JobID: "job-2", ID: "/local/resource-2"}))
	helper.ExecInfo.RemoveExecutor("executor-2")
	require.Eventually(t, func() bool {
		return helper.IsRemoved(t, pkgOrm.ResourceKey{JobID: "job-2", ID: "/local/resource-2"})
	}, 1*time.Second, 10*time.Millisecond)

	time.Sleep(20 * time.Millisecond)
	require.False(t, helper.IsGCPending(t, pkgOrm.ResourceKey{JobID: "job-3", ID: "/local/resource-3"}))
	helper.ExecInfo.RemoveExecutor("executor-3")
	require.Eventually(t, func() bool {
		return helper.IsRemoved(t, pkgOrm.ResourceKey{JobID: "job-3", ID: "/local/resource-3"})
	}, 1*time.Second, 10*time.Millisecond)

	executors := []string{"executor-1", "executor-2", "executor-3"}
	for _, executor := range executors {
		require.Eventually(t, func() bool {
			return helper.IsRemoved(t, pkgOrm.ResourceKey{
				JobID: s3.GetDummyJobID(model.ExecutorID(executor)),
				ID:    s3.DummyResourceID,
			})
		}, 1*time.Second, 10*time.Millisecond)
	}
	helper.Close()
}

func TestGCCoordinatorRemoveJobs(t *testing.T) {
	t.Parallel()
	helper := newGCTestHelper()
	helper.LoadDefaultMockData(t)
	helper.Start()

	require.False(t, helper.IsGCPending(t, pkgOrm.ResourceKey{JobID: "job-1", ID: "/local/resource-1"}))
	helper.JobInfo.RemoveJob("job-1")
	helper.GCRunner.WaitNotify(t, 1*time.Second)
	require.Eventually(t, func() bool {
		return helper.IsGCPending(t, pkgOrm.ResourceKey{JobID: "job-1", ID: "/local/resource-1"})
	}, 1*time.Second, 10*time.Millisecond)

	time.Sleep(20 * time.Millisecond)
	require.False(t, helper.IsGCPending(t, pkgOrm.ResourceKey{JobID: "job-2", ID: "/local/resource-2"}))
	helper.JobInfo.RemoveJob("job-2")
	helper.GCRunner.WaitNotify(t, 1*time.Second)
	require.Eventually(t, func() bool {
		return helper.IsGCPending(t, pkgOrm.ResourceKey{JobID: "job-2", ID: "/local/resource-2"})
	}, 1*time.Second, 10*time.Millisecond)

	time.Sleep(20 * time.Millisecond)
	require.False(t, helper.IsGCPending(t, pkgOrm.ResourceKey{JobID: "job-3", ID: "/local/resource-3"}))
	helper.JobInfo.RemoveJob("job-3")
	helper.GCRunner.WaitNotify(t, 1*time.Second)
	require.Eventually(t, func() bool {
		return helper.IsGCPending(t, pkgOrm.ResourceKey{JobID: "job-3", ID: "/local/resource-3"})
	}, 1*time.Second, 10*time.Millisecond)

	helper.Close()
}

func TestGCCoordinatorRemoveJobAndExecutor(t *testing.T) {
	t.Parallel()
	helper := newGCTestHelper()
	helper.LoadDefaultMockData(t)
	helper.Start()

	require.False(t, helper.IsGCPending(t, pkgOrm.ResourceKey{JobID: "job-1", ID: "/local/resource-1"}))
	require.False(t, helper.IsGCPending(t, pkgOrm.ResourceKey{JobID: "job-2", ID: "/local/resource-2"}))

	helper.JobInfo.RemoveJob("job-1")
	helper.ExecInfo.RemoveExecutor("executor-2")

	helper.GCRunner.WaitNotify(t, 1*time.Second)
	require.Eventually(t, func() bool {
		return helper.IsGCPending(t, pkgOrm.ResourceKey{JobID: "job-1", ID: "/local/resource-1"})
	}, 1*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return helper.IsRemoved(t, pkgOrm.ResourceKey{JobID: "job-2", ID: "/local/resource-2"})
	}, 1*time.Second, 10*time.Millisecond)

	helper.Close()
}
