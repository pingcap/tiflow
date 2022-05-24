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
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/engine/executor/dm/unit"
	"github.com/pingcap/tiflow/engine/lib"
)

// dumpTask represents a dump task
type dumpTask struct {
	*baseTask
}

// newDumpTask creates a dump task
func newDumpTask(baseTask *baseTask) lib.WorkerImpl {
	dumpTask := &dumpTask{
		baseTask: baseTask,
	}
	dumpTask.baseTask.task = dumpTask
	return dumpTask
}

// onInit implements task.onInit
func (t *dumpTask) onInit(ctx context.Context) error {
	return t.setupStorge(ctx)
}

// onFinished implements task.onFinished
func (t *dumpTask) onFinished(ctx context.Context) error {
	return t.persistStorge(ctx)
}

// createUnitHolder implements task.createUnitHolder
func (t *dumpTask) createUnitHolder(cfg *config.SubTaskConfig) unit.Holder {
	return unit.NewHolderImpl(lib.WorkerDMDump, cfg.SourceID, dumpling.NewDumpling(cfg))
}
