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
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/syncer"
	"github.com/pingcap/tiflow/engine/executor/dm/unit"
	"github.com/pingcap/tiflow/engine/lib"
)

// syncTask represents a sync task
type syncTask struct {
	*baseTask
}

// newSyncTask create a sync task
func newSyncTask(baseTask *baseTask) lib.WorkerImpl {
	syncTask := &syncTask{
		baseTask: baseTask,
	}
	syncTask.baseTask.task = syncTask
	return syncTask
}

// onInit implements task.onInit
func (t *syncTask) onInit(ctx context.Context) error {
	if t.cfg.Mode == dmconfig.ModeAll {
		return t.setupStorge(ctx)
	}
	return nil
}

// onFinished implements task.onFinished
// Should not happened.
func (t *syncTask) onFinished(ctx context.Context) error {
	return nil
}

// createUnitHolder implements task.createUnitHolder
func (t *syncTask) createUnitHolder(cfg *config.SubTaskConfig) unit.Holder {
	return unit.NewHolderImpl(lib.WorkerDMSync, cfg.SourceID, syncer.NewSyncer(cfg, nil, nil))
}
