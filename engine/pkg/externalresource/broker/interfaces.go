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
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
)

// A Broker is created and maintained by the executor
// and provides file resources to the tasks.
type Broker interface {
	pb.BrokerServiceServer

	// OpenStorage creates a storage Handle for a worker.
	OpenStorage(
		ctx context.Context,
		projectInfo tenant.ProjectInfo,
		workerID resModel.WorkerID,
		jobID resModel.JobID,
		resourcePath resModel.ResourceID,
		opts ...OpenStorageOption,
	) (Handle, error)

	// OnWorkerClosed is called when a worker is closing.
	// The implementation should do necessary garbage collection
	// for the worker, especially local temporary files.
	OnWorkerClosed(
		ctx context.Context,
		workerID resModel.WorkerID,
		jobID resModel.JobID,
	)

	IsS3StorageEnabled() bool

	Close()
}

type openStorageOptions struct {
	cleanBeforeOpen bool
}

// OpenStorageOption is an option for OpenStorage.
type OpenStorageOption func(*openStorageOptions)

// WithCleanBeforeOpen indicates that the storage should be cleaned before open.
func WithCleanBeforeOpen() OpenStorageOption {
	return func(opts *openStorageOptions) {
		opts.cleanBeforeOpen = true
	}
}
