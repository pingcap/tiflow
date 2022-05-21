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
	"github.com/pingcap/tiflow/engine/lib"
)

// SyncTask represents a sync task
type SyncTask struct {
	BaseDMTask
}

// newSyncTask create a sync task
func newSyncTask(baseDMTask BaseDMTask) lib.WorkerImpl {
	syncTask := &SyncTask{
		BaseDMTask: baseDMTask,
	}
	syncTask.BaseDMTask.DMTask = syncTask
	return syncTask
}

// onInit implements DMTask.onInit
func (t *SyncTask) onInit(ctx context.Context) error {
	if t.cfg.Mode == dmconfig.ModeAll {
		return t.setupStorge(ctx)
	}
	return nil
}

// onFinished implements DMTask.onFinished
// Should not happened.
func (t *SyncTask) onFinished(ctx context.Context) error {
	return nil
}

// createUnitHolder implements DMTask.createUnitHolder
func (t *SyncTask) createUnitHolder(cfg *config.SubTaskConfig) *unitHolder {
	return newUnitHolder(lib.WorkerDMSync, cfg.SourceID, syncer.NewSyncer(cfg, nil, nil))
}
