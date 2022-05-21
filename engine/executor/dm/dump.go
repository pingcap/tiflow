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
	"github.com/pingcap/tiflow/engine/lib"
)

// DumpTask represents a dump task
type DumpTask struct {
	BaseDMTask
}

// newDumpTask create a dump task
func newDumpTask(baseDMTask BaseDMTask) lib.WorkerImpl {
	dumpTask := &DumpTask{
		BaseDMTask: baseDMTask,
	}
	dumpTask.BaseDMTask.DMTask = dumpTask
	return dumpTask
}

// onInit implements DMTask.onInit
func (t *DumpTask) onInit(ctx context.Context) error {
	return t.setupStorge(ctx)
}

// onFinished implements DMTask.onFinished
func (t *DumpTask) onFinished(ctx context.Context) error {
	return t.persistStorge(ctx)
}

// createUnitHolder implements DMTask.createUnitHolder
func (t *DumpTask) createUnitHolder(cfg *config.SubTaskConfig) *unitHolder {
	return newUnitHolder(lib.WorkerDMDump, cfg.SourceID, dumpling.NewDumpling(cfg))
}
