package registry

import (
	"github.com/hanfei1991/microcosm/lib"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
)

// WorkerFactory is an interface that should be implemented by the author of WorkerImpl's.
// It represents a constructor for a given type of worker.
type WorkerFactory interface {
	NewWorker(
		ctx *dcontext.Context, // We require a `dcontext` here to provide dependencies.
		workerID lib.WorkerID, // the globally unique workerID for this worker to be created.
		masterID lib.MasterID, // the masterID that this worker will report to.
	) (lib.Worker, error)
}

type ConstructorFuncFactory func(ctx *dcontext.Context, id lib.WorkerID, masterID lib.MasterID) lib.Worker

func (c ConstructorFuncFactory) NewWorker(ctx *dcontext.Context, workerID lib.WorkerID, masterID lib.MasterID) (lib.Worker, error) {
	return c(ctx, workerID, masterID), nil
}
