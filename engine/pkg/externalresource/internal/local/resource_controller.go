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

package local

import (
	"context"

	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

var _ internal.ResourceController = &resourceController{}

// resourceController defines operations specific to the local file type.
type resourceController struct {
	// clientManager is used to communicate with executors.
	clientGroup client.ExecutorGroup
}

// NewFileResourceController creates a new LocalFileResourceController.
func NewFileResourceController(clientGroup client.ExecutorGroup) *resourceController {
	return &resourceController{clientGroup: clientGroup}
}

// GCSingleResource remove a persisted resource on executor.
func (r *resourceController) GCSingleResource(ctx context.Context, res *resModel.ResourceMeta) error {
	return r.removeFilesOnExecutor(ctx, res)
}

func (r *resourceController) removeFilesOnExecutor(ctx context.Context, resource *resModel.ResourceMeta) error {
	cli, err := r.clientGroup.GetExecutorClientB(ctx, resource.Executor)
	if err != nil {
		return errors.Annotate(err, "removeFilesOnExecutor")
	}

	return cli.RemoveResource(ctx, resource.Worker, resource.ID)
}

// GCExecutor removes all temporary resources created by the offlined executors.
func (r *resourceController) GCExecutor(
	_ context.Context, _ []*resModel.ResourceMeta, _ model.ExecutorID,
) error {
	// There is no need to clean up temporary files, because no persistent volumes are
	// mounted to executor.
	return nil
}
