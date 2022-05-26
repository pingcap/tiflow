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

package resourcetypes

import (
	"context"

	"github.com/pingcap/tiflow/engine/client"
	"github.com/pingcap/tiflow/engine/pb"
	derrors "github.com/pingcap/tiflow/engine/pkg/errors"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
)

// LocalFileResourceType defines operations specific to
// the local file type.
type LocalFileResourceType struct {
	// clientManager is used to communicate with executors.
	clientManager client.ClientsManager
}

// NewLocalFileResourceType creates a new LocalFileResourceType.
func NewLocalFileResourceType(clientManager client.ClientsManager) *LocalFileResourceType {
	return &LocalFileResourceType{clientManager: clientManager}
}

// GCHandler returns a closure to the invoker to perform GC.
func (r *LocalFileResourceType) GCHandler() func(context.Context, *resModel.ResourceMeta) error {
	return r.removeFilesOnExecutor
}

func (r *LocalFileResourceType) removeFilesOnExecutor(ctx context.Context, resource *resModel.ResourceMeta) error {
	cli := r.clientManager.ExecutorClient(resource.Executor)
	if cli == nil {
		// TODO we should retry here.
		// Ideally the retrying for unknown executors should reside in clientManager.
		// We will deal with that later.
		return derrors.ErrUnknownExecutorID.FastGenByArgs(resource.Executor)
	}

	return cli.RemoveLocalResource(ctx, &pb.RemoveLocalResourceRequest{
		ResourceId: resource.ID,
		CreatorId:  resource.Worker,
	})
}
