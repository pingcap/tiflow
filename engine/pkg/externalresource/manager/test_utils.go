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
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

// MockExecutorInfoProvider implements ExecutorInfoProvider interface
type MockExecutorInfoProvider struct {
	mu          sync.RWMutex
	executorSet map[resModel.ExecutorID]string
	notifier    *notifier.Notifier[model.ExecutorStatusChange]
}

// NewMockExecutorInfoProvider creates a new MockExecutorInfoProvider instance
func NewMockExecutorInfoProvider() *MockExecutorInfoProvider {
	return &MockExecutorInfoProvider{
		executorSet: make(map[resModel.ExecutorID]string),
		notifier:    notifier.NewNotifier[model.ExecutorStatusChange](),
	}
}

// AddExecutor adds an executor to the mock.
func (p *MockExecutorInfoProvider) AddExecutor(executorID string, addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.executorSet[resModel.ExecutorID(executorID)] = addr
	p.notifier.Notify(model.ExecutorStatusChange{
		ID:   model.ExecutorID(executorID),
		Tp:   model.EventExecutorOnline,
		Addr: addr,
	})
}

// RemoveExecutor removes an executor from the mock.
func (p *MockExecutorInfoProvider) RemoveExecutor(executorID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	addr := p.executorSet[resModel.ExecutorID(executorID)]
	delete(p.executorSet, resModel.ExecutorID(executorID))
	p.notifier.Notify(model.ExecutorStatusChange{
		ID:   model.ExecutorID(executorID),
		Tp:   model.EventExecutorOffline,
		Addr: addr,
	})
}

// HasExecutor returns whether the mock contains the given executor.
func (p *MockExecutorInfoProvider) HasExecutor(executorID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	_, exists := p.executorSet[resModel.ExecutorID(executorID)]
	return exists
}

// ListExecutors lists all executors.
func (p *MockExecutorInfoProvider) ListExecutors() (ret []string) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for id := range p.executorSet {
		ret = append(ret, string(id))
	}
	return
}

// WatchExecutors implements ExecutorManager.WatchExecutors
func (p *MockExecutorInfoProvider) WatchExecutors(
	ctx context.Context,
) (map[model.ExecutorID]string, *notifier.Receiver[model.ExecutorStatusChange], error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	executors := make(map[model.ExecutorID]string, len(p.executorSet))
	for id, addr := range p.executorSet {
		executors[id] = addr
	}

	return executors, p.notifier.NewReceiver(), nil
}

// MockJobStatusProvider implements ExecutorManager interface
type MockJobStatusProvider struct {
	mu       sync.RWMutex
	jobInfos JobStatusesSnapshot
	notifier *notifier.Notifier[JobStatusChangeEvent]
}

// NewMockJobStatusProvider returns a new instance of MockJobStatusProvider.
func NewMockJobStatusProvider() *MockJobStatusProvider {
	return &MockJobStatusProvider{
		jobInfos: make(JobStatusesSnapshot),
		notifier: notifier.NewNotifier[JobStatusChangeEvent](),
	}
}

// SetJobStatus upserts the status of a given job.
func (jp *MockJobStatusProvider) SetJobStatus(jobID frameModel.MasterID, status JobStatus) {
	jp.mu.Lock()
	defer jp.mu.Unlock()

	jp.jobInfos[jobID] = status
}

// RemoveJob removes a job from the mock.
func (jp *MockJobStatusProvider) RemoveJob(jobID frameModel.MasterID) {
	jp.mu.Lock()
	defer jp.mu.Unlock()

	delete(jp.jobInfos, jobID)
	jp.notifier.Notify(JobStatusChangeEvent{
		EventType: JobRemovedEvent,
		JobID:     jobID,
	})
}

// GetJobStatuses implements the interface JobStatusProvider.
func (jp *MockJobStatusProvider) GetJobStatuses(ctx context.Context) (JobStatusesSnapshot, error) {
	jp.mu.RLock()
	defer jp.mu.RUnlock()

	return jp.jobInfos, nil
}

// WatchJobStatuses implements the interface JobStatusProvider.
func (jp *MockJobStatusProvider) WatchJobStatuses(
	ctx context.Context,
) (JobStatusesSnapshot, *notifier.Receiver[JobStatusChangeEvent], error) {
	jp.mu.Lock()
	defer jp.mu.Unlock()

	snapCopy := make(JobStatusesSnapshot, len(jp.jobInfos))
	for k, v := range jp.jobInfos {
		snapCopy[k] = v
	}

	return snapCopy, jp.notifier.NewReceiver(), nil
}

// MockGCRunner implements the interface GCRunner.
type MockGCRunner struct {
	GCRunner
	notifyCh chan struct{}
}

// NewMockGCRunner returns a new MockGCNotifier
func NewMockGCRunner(resClient pkgOrm.ResourceClient) *MockGCRunner {
	runner := NewGCRunner(resClient, nil, nil)
	runner.gcHandlers[resModel.ResourceTypeS3] = &mockResourceController{
		gcExecutorCh: make(chan []*resModel.ResourceMeta, 128),
	}
	return &MockGCRunner{
		GCRunner: runner,
		notifyCh: make(chan struct{}, 1),
	}
}

// GCNotify pushes a new notification to the internal channel so
// it can be waited on by WaitNotify().
func (n *MockGCRunner) GCNotify() {
	select {
	case n.notifyCh <- struct{}{}:
	default:
	}
}

// WaitNotify waits for a pending notification with timeout.
func (n *MockGCRunner) WaitNotify(t *testing.T, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		require.FailNow(t, "WaitNotify has timed out")
	case <-n.notifyCh:
	}
}

type mockResourceController struct {
	internal.ResourceController
	gcRequestCh  chan *resModel.ResourceMeta
	gcExecutorCh chan []*resModel.ResourceMeta
}

func (r *mockResourceController) GCSingleResource(
	ctx context.Context, res *resModel.ResourceMeta,
) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case r.gcRequestCh <- res:
	}
	return nil
}

func (r *mockResourceController) GCExecutor(
	ctx context.Context, resources []*resModel.ResourceMeta, executorID model.ExecutorID,
) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case r.gcExecutorCh <- resources:
	}
	return nil
}
