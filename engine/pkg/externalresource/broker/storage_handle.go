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

	"github.com/pingcap/errors"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"

	"github.com/pingcap/tiflow/engine/pb"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
)

// Handle defines an interface for interact with framework
type Handle interface {
	ID() resModel.ResourceID
	BrExternalStorage() brStorage.ExternalStorage
	Persist(ctx context.Context) error
	Discard(ctx context.Context) error
}

// BrExternalStorageHandle contains a brStorage.ExternalStorage.
// It helps Dataflow Engine reuse the external storage facilities
// implemented in Br.
type BrExternalStorageHandle struct {
	id         resModel.ResourceID
	name       resModel.ResourceName
	jobID      resModel.JobID
	workerID   resModel.WorkerID
	executorID resModel.ExecutorID

	inner       brStorage.ExternalStorage
	client      *rpcutil.FailoverRPCClients[pb.ResourceManagerClient]
	fileManager FileManager
}

// ID implements Handle.ID
func (h *BrExternalStorageHandle) ID() resModel.ResourceID {
	return h.id
}

// BrExternalStorage implements Handle.BrExternalStorage
func (h *BrExternalStorageHandle) BrExternalStorage() brStorage.ExternalStorage {
	return h.inner
}

// Persist implements Handle.Persist
func (h *BrExternalStorageHandle) Persist(ctx context.Context) error {
	_, err := rpcutil.DoFailoverRPC(
		ctx,
		h.client,
		&pb.CreateResourceRequest{
			ResourceId:      h.id,
			CreatorExecutor: string(h.executorID),
			JobId:           h.jobID,
			CreatorWorkerId: h.workerID,
		},
		pb.ResourceManagerClient.CreateResource,
	)
	if err != nil {
		// The RPC could have succeeded on server's side.
		// We do not need to handle it for now, as the
		// dangling meta records will be cleaned up by
		// garbage collection eventually.
		return errors.Trace(err)
	}
	h.fileManager.SetPersisted(h.workerID, h.name)
	return nil
}

// Discard implements Handle.Discard
func (h *BrExternalStorageHandle) Discard(ctx context.Context) error {
	return nil
}
