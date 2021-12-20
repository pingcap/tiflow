package system

import (
	"context"

	"github.com/hanfei1991/microcosm/model"
)

// JobMaster maintains and manages the submitted job.
type JobMaster interface {
	// DispatchJob dispatches new tasks.
	DispatchTasks(ctx context.Context, tasks []*model.Task) error
	// Start the job master.
	Start(ctx context.Context) error
	// Stop the job master.
	Stop(ctx context.Context) error
	// OfflineExecutor notifies the offlined executor to all the job masters.
	OfflineExecutor(eid model.ExecutorID)
	// ID returns the current job id.
	ID() model.ID
}
