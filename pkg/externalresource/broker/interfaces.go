package broker

import (
	"context"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	resModel "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
)

// A Broker is created and maintained by the executor
// and provides file resources to the tasks.
type Broker interface {
	// OpenStorage creates a storage Handle for a worker.
	OpenStorage(
		ctx context.Context,
		workerID resModel.WorkerID,
		jobID resModel.JobID,
		resourcePath resModel.ResourceID,
	) (Handle, error)

	// OnWorkerClosed in called when a worker is closing.
	// The implementation should do necessary garbage collection
	// for the worker, especially local temporary files.
	OnWorkerClosed(
		ctx context.Context,
		workerID resModel.WorkerID,
		jobID resModel.JobID,
	)
}

// FileManager abstracts the operations on local resources that
// a Broker needs to perform.
type FileManager interface {
	CreateResource(
		creator libModel.WorkerID,
		resName resModel.ResourceName,
	) (*resModel.LocalFileResourceDescriptor, error)

	GetPersistedResource(
		creator libModel.WorkerID,
		resName resModel.ResourceName,
	) (*resModel.LocalFileResourceDescriptor, error)

	RemoveTemporaryFiles(creator libModel.WorkerID) error

	RemoveResource(
		creator libModel.WorkerID,
		resName resModel.ResourceName,
	) error

	SetPersisted(
		creator libModel.WorkerID,
		resName resModel.ResourceName,
	)
}
