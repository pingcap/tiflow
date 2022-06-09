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

package broker

import (
	"context"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
)

// A Broker is created and maintained by the executor
// and provides file resources to the tasks.
type Broker interface {
	pb.BrokerServiceServer

	// OpenStorage creates a storage Handle for a worker.
	OpenStorage(
		ctx context.Context,
		workerID resModel.WorkerID,
		jobID resModel.JobID,
		resourcePath resModel.ResourceID,
	) (Handle, error)

	// CheckResourceExists returns whether a given resource exists.
	CheckResourceExists(
		ctx context.Context,
		jobID resModel.JobID,
		resourceID resModel.ResourceID,
	) (bool, error)

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
	) (*LocalFileResourceDescriptor, error)

	GetPersistedResource(
		creator libModel.WorkerID,
		resName resModel.ResourceName,
	) (*LocalFileResourceDescriptor, error)

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
