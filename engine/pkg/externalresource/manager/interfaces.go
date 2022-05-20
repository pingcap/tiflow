package manager

import (
	"context"

	libModel "github.com/hanfei1991/microcosm/lib/model"
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
