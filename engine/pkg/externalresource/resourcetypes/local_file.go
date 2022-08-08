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

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tiflow/engine/pkg/client"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
)

// LocalFileResourceController defines operations specific to
// the local file type.
type LocalFileResourceController struct {
	// clientManager is used to communicate with executors.
	clientGroup client.ExecutorGroup
}

// NewLocalFileResourceType creates a new LocalFileResourceController.
func NewLocalFileResourceType(clientGroup client.ExecutorGroup) *LocalFileResourceController {
	return &LocalFileResourceController{clientGroup: clientGroup}
}

// GCHandler returns a closure to the invoker to perform GC.
func (r *LocalFileResourceController) GCHandler() func(context.Context, *resModel.ResourceMeta) error {
	return r.removeFilesOnExecutor
}

func (r *LocalFileResourceController) removeFilesOnExecutor(ctx context.Context, resource *resModel.ResourceMeta) error {
	cli, err := r.clientGroup.GetExecutorClientB(ctx, resource.Executor)
	if err != nil {
		return perrors.Annotate(err, "removeFilesOnExecutor")
	}

	return cli.RemoveResource(ctx, resource.Worker, resource.ID)
}
