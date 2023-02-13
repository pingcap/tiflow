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

package bucket

import (
	"context"
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"go.uber.org/zap"
)

const defaultControllerID = "leader-controller"

var _ internal.ResourceController = &resourceController{}

// resourceController defines operations specific to the s3 file type.
type resourceController struct {
	// fm is used to operate s3 storage.
	fm *FileManager
}

// NewResourceController creates a new bucket resourceController.
func NewResourceController(config *resModel.Config) *resourceController {
	log.Debug("create bucket resource controller", zap.Any("config", config))
	fm := NewFileManagerWithConfig(defaultControllerID, config)
	return &resourceController{fm: fm}
}

// GCSingleResource returns a closure to the invoker to perform GC.
func (r *resourceController) GCSingleResource(ctx context.Context, res *resModel.ResourceMeta) error {
	tp, resName, err := resModel.ParseResourceID(res.ID)
	if err != nil {
		return err
	}
	if tp != resModel.ResourceTypeS3 && tp != resModel.ResourceTypeGCS {
		log.Panic("unexpected resource type", zap.Any("resource", res))
	}

	return r.fm.RemoveResource(ctx, internal.ResourceIdent{
		Name: resName,
		ResourceScope: internal.ResourceScope{
			Executor: res.Executor,
			WorkerID: res.Worker,
		},
	})
}

// GCExecutor cleanups all temporary resources created by the executor.
func (r *resourceController) GCExecutor(
	ctx context.Context, resources []*resModel.ResourceMeta, executorID model.ExecutorID,
) error {
	persistedResSet := make(map[string]struct{})
	for _, res := range resources {
		if res.Executor != executorID {
			log.Panic("unexpected resource which is persisted by other exeuctor",
				zap.Any("resource", res), zap.Any("expectedeExecutor", executorID))
		}
		tp, resName, err := resModel.ParseResourceID(res.ID)
		if err != nil {
			return err
		}
		if tp != resModel.ResourceTypeS3 && tp != resModel.ResourceTypeGCS {
			log.Panic("unexpected resource type", zap.Any("resource", res))
		}

		resPath := fmt.Sprintf("%s/%s", res.Worker, resName)
		persistedResSet[resPath] = struct{}{}
	}

	scope := internal.ResourceScope{
		Executor: executorID,
		WorkerID: "",
	}
	return r.fm.removeAllTemporaryFilesByMeta(ctx, scope, persistedResSet)
}
