package jobmaster

import (
	"context"

	"github.com/hanfei1991/microcosom/model"
)

// JobMaster maintains and manages the submitted job.
type JobMaster interface {
	// DispatchJob dispatches the jobs that a job master has generated.
	DispatchJob(ctx context.Context) error
	// OfflineExecutor notifies the offlined executor to all the job masters.
	OfflineExecutor(eid model.ExecutorID)
	// ID returns the current job id.
	ID() model.JobID
}
