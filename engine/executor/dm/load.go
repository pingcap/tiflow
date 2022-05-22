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

package dm

import (
	"context"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/engine/lib"
)

// LoadTask represents a load task
type LoadTask struct {
	BaseTask
}

// newLoadTask create a load task
func newLoadTask(baseDMTask BaseTask) lib.WorkerImpl {
	loadTask := &LoadTask{
		BaseTask: baseDMTask,
	}
	loadTask.BaseTask.Task = loadTask
	return loadTask
}

// onInit implements DMTask.onInit
func (t *LoadTask) onInit(ctx context.Context) error {
	return t.setupStorge(ctx)
}

// onFinished implements DMTask.onFinished
func (t *LoadTask) onFinished(ctx context.Context) error {
	return nil
}

// createUnitHolder implements DMTask.createUnitHolder
func (t *LoadTask) createUnitHolder(cfg *config.SubTaskConfig) *unitHolder {
	// `workerName` and `etcdClient` of `NewLightning` are not used in dataflow
	// scenario, we just use readable values here.
	workerName := "dataflow-worker"
	return newUnitHolder(lib.WorkerDMLoad, cfg.SourceID, loader.NewLightning(cfg, nil, workerName))
}
