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
	"github.com/pingcap/tiflow/engine/executor/dm/unit"
	"github.com/pingcap/tiflow/engine/lib"
)

// loadTask represents a load task
type loadTask struct {
	*baseTask
}

// newLoadTask creates a load task
func newLoadTask(baseTask *baseTask) lib.WorkerImpl {
	loadTask := &loadTask{
		baseTask: baseTask,
	}
	loadTask.baseTask.task = loadTask
	return loadTask
}

// onInit implements task.onInit
func (t *loadTask) onInit(ctx context.Context) error {
	return t.setupStorge(ctx)
}

// onFinished implements task.onFinished
func (t *loadTask) onFinished(ctx context.Context) error {
	return nil
}

// createUnitHolder implements task.createUnitHolder
func (t *loadTask) createUnitHolder(cfg *config.SubTaskConfig) unit.Holder {
	// `workerName` and `etcdClient` of `NewLightning` are not used in dataflow
	// scenario, we just use readable values here.
	workerName := "dataflow-worker"
	return unit.NewHolderImpl(lib.WorkerDMLoad, cfg.SourceID, loader.NewLightning(cfg, nil, workerName))
}
