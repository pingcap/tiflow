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
	"go.uber.org/atomic"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	derrors "github.com/pingcap/tiflow/engine/pkg/errors"
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

// LocalResourceHandle contains a brStorage.ExternalStorage.
// It helps Dataflow Engine reuse the external storage facilities
// implemented in Br.
type LocalResourceHandle struct {
	id         resModel.ResourceID
	jobID      resModel.JobID
	executorID resModel.ExecutorID
	desc       *LocalFileResourceDescriptor

	inner  brStorage.ExternalStorage
	client *rpcutil.FailoverRPCClients[pb.ResourceManagerClient]

	fileManager FileManager

	// isPersisted should be set to true if the
	// resource has been registered with the servermaster.
	isPersisted atomic.Bool
	isInvalid   atomic.Bool
}

func newLocalResourceHandle(
	resourceID resModel.ResourceID,
	jobID resModel.JobID,
	executorID resModel.ExecutorID,
	fm FileManager,
	desc *LocalFileResourceDescriptor,
	client ResourceManagerClient,
) (*LocalResourceHandle, error) {
	ls, err := newBrStorageForLocalFile(desc.AbsolutePath())
	if err != nil {
		return nil, err
	}

	return &LocalResourceHandle{
		id:         resourceID,
		jobID:      jobID,
		executorID: executorID,

		inner:  ls,
		client: client,
		desc:   desc,

		fileManager: fm,
	}, nil
}

// ID implements Handle.ID
func (h *LocalResourceHandle) ID() resModel.ResourceID {
	return h.id
}

// BrExternalStorage implements Handle.BrExternalStorage
func (h *LocalResourceHandle) BrExternalStorage() brStorage.ExternalStorage {
	return h.inner
}

// Persist implements Handle.Persist
func (h *LocalResourceHandle) Persist(ctx context.Context) error {
	if h.isInvalid.Load() {
		// Trying to persist invalid resource.
		return derrors.ErrInvalidResourceHandle.FastGenByArgs()
	}

	_, err := rpcutil.DoFailoverRPC(
		ctx,
		h.client,
		&pb.CreateResourceRequest{
			ResourceId:      h.id,
			CreatorExecutor: string(h.executorID),
			JobId:           h.jobID,
			CreatorWorkerId: h.desc.Creator,
		},
		pb.ResourceManagerClient.CreateResource,
	)
	if err != nil {
		// The RPC could have succeeded on server's side.
		// We do not need to handle it for now, as the
		// dangling meta records will be cleaned up by
		// garbage collection eventually.
		// TODO proper retrying.
		return errors.Trace(err)
	}
	// We only support local file resources, so fileManager is never nil.
	h.fileManager.SetPersisted(h.desc.Creator, h.desc.ResourceName)
	h.isPersisted.Store(true)
	return nil
}

// Discard implements Handle.Discard
func (h *LocalResourceHandle) Discard(ctx context.Context) error {
	if h.isInvalid.Load() {
		// Trying to discard invalid resource.
		return derrors.ErrInvalidResourceHandle.FastGenByArgs()
	}

	err := h.fileManager.RemoveResource(h.desc.Creator, h.desc.ResourceName)
	if err != nil {
		return err
	}

	if h.isPersisted.Load() {
		_, err := rpcutil.DoFailoverRPC(ctx,
			h.client,
			&pb.RemoveResourceRequest{
				ResourceId: h.id,
			},
			pb.ResourceManagerClient.RemoveResource)
		if err != nil {
			// TODO proper retrying.
			return errors.Trace(err)
		}
		h.isPersisted.Store(false)
	}

	h.isInvalid.Store(true)
	return nil
}
