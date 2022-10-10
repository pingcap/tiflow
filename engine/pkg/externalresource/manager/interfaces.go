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

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
)

// ExecutorInfoProvider describes an object that maintains a list
// of all executors
type ExecutorInfoProvider interface {
	HasExecutor(executorID string) bool

	// WatchExecutors returns a snapshot of all online executors plus
	// a stream of events describing changes that happen to the executors
	// after the snapshot is taken.
	WatchExecutors(ctx context.Context) (
		snap map[model.ExecutorID]string, stream *notifier.Receiver[model.ExecutorStatusChange], err error,
	)
}

// JobStatus describes the a Job's status.
type JobStatus = frameModel.MasterState

// JobStatusesSnapshot describes the statuses of all jobs
// at some time point.
type JobStatusesSnapshot = map[frameModel.MasterID]JobStatus

// JobStatusChangeType describes the type of job status changes.
type JobStatusChangeType int32

const (
	// JobRemovedEvent means that a job has been removed.
	JobRemovedEvent = JobStatusChangeType(iota + 1)
	// TODO add more event types to support more features
)

// JobStatusChangeEvent is an event denoting a job status
// has changed.
type JobStatusChangeEvent struct {
	EventType JobStatusChangeType
	JobID     frameModel.MasterID
}

// JobStatusProvider describes an object that can be queried
// on the status of jobs.
type JobStatusProvider interface {
	// GetJobStatuses returns the status of all jobs that are
	// not deleted.
	GetJobStatuses(ctx context.Context) (JobStatusesSnapshot, error)

	// WatchJobStatuses listens on all job status changes followed by
	// a snapshot.
	WatchJobStatuses(
		ctx context.Context,
	) (JobStatusesSnapshot, *notifier.Receiver[JobStatusChangeEvent], error)
}

// GCCoordinator describes an object responsible for triggering
// file resource garbage collection.
type GCCoordinator interface {
	Run(ctx context.Context) error
	OnKeepAlive(resourceID resModel.ResourceID, workerID frameModel.WorkerID)
}

// GCRunner perform the actual GC operations.
type GCRunner interface {
	Run(ctx context.Context) error
	GCNotify()
	GCExecutors(context.Context, ...model.ExecutorID) error
}
