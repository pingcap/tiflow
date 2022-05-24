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

	"github.com/stretchr/testify/require"

	libModel "github.com/pingcap/tiflow/engine/lib/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
)

type gcTestHelper struct {
	ExecInfo *MockExecutorInfoProvider
	JobInfo  *MockJobStatusProvider
	Notifier *MockGCNotifier
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
	notifier := NewMockGCNotifier()
	meta, err := pkgOrm.NewMockClient()
	if err != nil {
		panic(err)
	}
	coord := NewGCCoordinator(execInfo, jobInfo, meta, notifier)

	ctx, cancel := context.WithCancel(context.Background())
	ret := &gcTestHelper{
		ExecInfo: execInfo,
		JobInfo:  jobInfo,
		Meta:     meta,
		Notifier: notifier,
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

func (h *gcTestHelper) IsGCPending(t *testing.T, id resModel.ResourceID) bool {
	meta, err := h.Meta.GetResourceByID(context.Background(), id)
	if err != nil {
		require.NoError(t, err)
	}

	return meta.GCPending
}

func (h *gcTestHelper) IsRemoved(t *testing.T, id resModel.ResourceID) bool {
	_, err := h.Meta.GetResourceByID(context.Background(), id)
	if pkgOrm.IsNotFoundError(err) {
		return true
	}
	require.NoError(t, err)
	return false
}

func (h *gcTestHelper) LoadDefaultMockData(t *testing.T) {
	h.ExecInfo.AddExecutor("executor-1")
	h.ExecInfo.AddExecutor("executor-2")
	h.ExecInfo.AddExecutor("executor-3")

	h.JobInfo.SetJobStatus("job-1", libModel.MasterStatusInit)
	h.JobInfo.SetJobStatus("job-2", libModel.MasterStatusInit)
	h.JobInfo.SetJobStatus("job-3", libModel.MasterStatusInit)

	err := h.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
		ID:       "resource-1",
		Job:      "job-1",
		Worker:   "worker-1",
		Executor: "executor-1",
	})
	require.NoError(t, err)

	err = h.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
		ID:       "resource-2",
		Job:      "job-2",
		Worker:   "worker-2",
		Executor: "executor-2",
	})
	require.NoError(t, err)

	err = h.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
		ID:       "resource-3",
		Job:      "job-3",
		Worker:   "worker-3",
		Executor: "executor-3",
	})
	require.NoError(t, err)
}

func TestGCCoordinatorRemoveExecutors(t *testing.T) {
	helper := newGCTestHelper()
	helper.LoadDefaultMockData(t)
	helper.Start()

	require.False(t, helper.IsGCPending(t, "resource-1"))
	helper.ExecInfo.RemoveExecutor("executor-1")
	require.Eventually(t, func() bool {
		return helper.IsRemoved(t, "resource-1")
	}, 1*time.Second, 10*time.Millisecond)

	time.Sleep(20 * time.Millisecond)
	require.False(t, helper.IsGCPending(t, "resource-2"))
	helper.ExecInfo.RemoveExecutor("executor-2")
	require.Eventually(t, func() bool {
		return helper.IsRemoved(t, "resource-2")
	}, 1*time.Second, 10*time.Millisecond)

	time.Sleep(20 * time.Millisecond)
	require.False(t, helper.IsGCPending(t, "resource-3"))
	helper.ExecInfo.RemoveExecutor("executor-3")
	require.Eventually(t, func() bool {
		return helper.IsRemoved(t, "resource-3")
	}, 1*time.Second, 10*time.Millisecond)

	helper.Close()
}

func TestGCCoordinatorRemoveJobs(t *testing.T) {
	helper := newGCTestHelper()
	helper.LoadDefaultMockData(t)
	helper.Start()

	require.False(t, helper.IsGCPending(t, "resource-1"))
	helper.JobInfo.RemoveJob("job-1")
	helper.Notifier.WaitNotify(t, 1*time.Second)
	require.Eventually(t, func() bool {
		return helper.IsGCPending(t, "resource-1")
	}, 1*time.Second, 10*time.Millisecond)

	time.Sleep(20 * time.Millisecond)
	require.False(t, helper.IsGCPending(t, "resource-2"))
	helper.JobInfo.RemoveJob("job-2")
	helper.Notifier.WaitNotify(t, 1*time.Second)
	require.Eventually(t, func() bool {
		return helper.IsGCPending(t, "resource-2")
	}, 1*time.Second, 10*time.Millisecond)

	time.Sleep(20 * time.Millisecond)
	require.False(t, helper.IsGCPending(t, "resource-3"))
	helper.JobInfo.RemoveJob("job-3")
	helper.Notifier.WaitNotify(t, 1*time.Second)
	require.Eventually(t, func() bool {
		return helper.IsGCPending(t, "resource-3")
	}, 1*time.Second, 10*time.Millisecond)

	helper.Close()
}
