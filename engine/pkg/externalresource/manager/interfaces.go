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

	libModel "github.com/pingcap/tiflow/engine/lib/model"
)

// ExecutorInfoProvider describes an object that maintains a list
// of all executors
type ExecutorInfoProvider interface {
	HasExecutor(executorID string) bool
	ListExecutors() []string
}

// JobStatus describes the a Job's status.
type JobStatus = libModel.MasterStatusCode

// JobStatusProvider describes an object that can be queried
// on the status of jobs.
type JobStatusProvider interface {
	// GetJobStatuses returns the status of all jobs that are
	// not deleted.
	GetJobStatuses(ctx context.Context) (map[libModel.MasterID]JobStatus, error)
}
