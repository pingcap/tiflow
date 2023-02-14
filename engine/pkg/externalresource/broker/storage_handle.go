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

	"github.com/pingcap/log"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Handle defines an interface for interact with framework
type Handle interface {
	ID() resModel.ResourceID
	BrExternalStorage() brStorage.ExternalStorage
	Persist(ctx context.Context) error
	Discard(ctx context.Context) error
}

// ResourceHandle contains a brStorage.ExternalStorage.
// It helps Dataflow Engine reuse the external storage facilities
// implemented in Br.
type ResourceHandle struct {
	executorID resModel.ExecutorID
	jobID      resModel.JobID
	client     client.ResourceManagerClient

	fileManager internal.FileManager
	desc        internal.ResourceDescriptor
	inner       brStorage.ExternalStorage

	// isPersisted should be set to true if the
	// resource has been registered with the servermaster.
	isPersisted atomic.Bool
	isInvalid   atomic.Bool
}

func newResourceHandle(
	jobID resModel.JobID,
	executorID resModel.ExecutorID,
	fm internal.FileManager,
	desc internal.ResourceDescriptor,
	isPersisted bool,
	client client.ResourceManagerClient,
) (*ResourceHandle, error) {
	// TODO(maxshuang): check, may need a context for BrExternalStorage
	inner, err := desc.ExternalStorage(context.Background())
	if err != nil {
		return nil, err
	}

	h := &ResourceHandle{
		executorID: executorID,
		jobID:      jobID,
		client:     client,

		desc:        desc,
		fileManager: fm,
		inner:       inner,
	}

	if isPersisted {
		h.isPersisted.Store(true)
	}
	return h, nil
}

// ID implements Handle.ID
func (h *ResourceHandle) ID() resModel.ResourceID {
	return h.desc.ID()
}

// BrExternalStorage implements Handle.BrExternalStorage
func (h *ResourceHandle) BrExternalStorage() brStorage.ExternalStorage {
	return h.inner
}

// Persist implements Handle.Persist
func (h *ResourceHandle) Persist(ctx context.Context) error {
	if h.isInvalid.Load() {
		// Trying to persist invalid resource.
		return errors.ErrInvalidResourceHandle.FastGenByArgs()
	}

	creatorExecutor := h.desc.ResourceIdent().Executor
	if h.isPersisted.Load() {
		log.Warn("Trying to persist a resource that has already been persisted",
			zap.Any("resourceDesc", h.desc),
			zap.Any("creatorExecutor", string(creatorExecutor)),
			zap.String("currentExecutor", string(h.executorID)))
		return nil
	}

	if creatorExecutor != h.executorID {
		// in this case, the resource should have been persisted by the creator.
		log.Panic("Trying to persist a resource that is not created by current executor",
			zap.Any("resourceDesc", h.desc),
			zap.Any("creator", string(creatorExecutor)),
			zap.String("currentExecutor", string(h.executorID)))
	}

	err := h.client.CreateResource(ctx, &pb.CreateResourceRequest{
		ProjectInfo: &pb.ProjectInfo{
			TenantId:  h.desc.ResourceIdent().TenantID(),
			ProjectId: h.desc.ResourceIdent().ProjectID(),
		},
		ResourceId:      h.desc.ID(),
		CreatorExecutor: string(h.executorID),
		JobId:           h.jobID,
		CreatorWorkerId: h.desc.ResourceIdent().WorkerID,
	})
	if err != nil {
		// The RPC could have succeeded on server's side.
		// We do not need to handle it for now, as the
		// dangling meta records will be cleaned up by
		// garbage collection eventually.
		// TODO proper retrying.
		return errors.Trace(err)
	}
	err = h.fileManager.SetPersisted(ctx, h.desc.ResourceIdent())
	if err != nil {
		return errors.Trace(err)
	}
	h.isPersisted.Store(true)
	return nil
}

// Discard implements Handle.Discard
// Note that the current design does not allow multiple workers to hold
// persistent resources simultaneously.
func (h *ResourceHandle) Discard(ctx context.Context) error {
	if h.isInvalid.Load() {
		// Trying to discard invalid resource.
		return errors.ErrInvalidResourceHandle.FastGenByArgs()
	}

	err := h.fileManager.RemoveResource(ctx, h.desc.ResourceIdent())
	if err != nil {
		return err
	}

	if h.isPersisted.Load() {
		err := h.client.RemoveResource(ctx, &pb.RemoveResourceRequest{
			ResourceKey: &pb.ResourceKey{
				JobId:      h.jobID,
				ResourceId: h.desc.ID(),
			},
		})
		if err != nil {
			// TODO proper retrying.
			return errors.Trace(err)
		}
		h.isPersisted.Store(false)
	}

	h.isInvalid.Store(true)
	return nil
}
