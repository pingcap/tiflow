package system

import (
	"context"

	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/metadata"
)

// JobMaster maintains and manages the submitted job.
type JobMaster interface {
	// DispatchJob dispatches new tasks.
	DispatchTasks(tasks ...*model.Task)
	// Start the job master.
	// TODO: the set of metaKV should happen when initializing.
	Start(ctx context.Context, metaKV metadata.MetaKV) (runtime.TaskRescUnit, error)
	// Stop the job master.
	Stop(ctx context.Context) error
	PauseTasks(tasks ...*model.Task) error
	PauseAllTasks() error
	// ID returns the current job id.
	ID() model.ID
}
