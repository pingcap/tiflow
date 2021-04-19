// Copyright 2021 PingCAP, Inc.
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

package owner

import (
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// changeFeedState is part of the replication model that implements the control logic of a changeFeed
type changeFeedState struct {
	TableTasks map[model.TableID]*tableTask

	CheckpointTs uint64
	ResolvedTs   uint64

	barriers  *barriers
	scheduler scheduler
}

type tableTask struct {
	TableID      model.TableID
	CheckpointTs uint64
	ResolvedTs   uint64
}

type ddlResultAction = string

const (
	// AddTableAction denotes a request to start replicating a table.
	AddTableAction = ddlResultAction("add")
	// DropTableAction denotes a request to stop replicating a table.
	DropTableAction = ddlResultAction("drop")
)

type ddlResult struct {
	FinishTs uint64
	Actions  []tableAction
}

type tableAction struct {
	Action  ddlResultAction
	tableID model.TableID
}

func newChangeFeedState(initTableTasks map[model.TableID]*tableTask, ddlStartTs uint64, scheduler scheduler) *changeFeedState {
	cf := &changeFeedState{
		TableTasks: initTableTasks,
		barriers:   newBarriers(),
		scheduler:  scheduler,
	}
	cf.SetDDLResolvedTs(ddlStartTs)
	return cf
}

func (cf *changeFeedState) SyncTasks() {
	if cf.scheduler.IsReady() {
		cf.scheduler.PutTasks(cf.TableTasks)
	}
}

func (cf *changeFeedState) AddDDLBarrier(job *timodel.Job) {
	cf.barriers.Update(DDLJobBarrier, uint64(job.ID), job.BinlogInfo.FinishedTS-1)
}

func (cf *changeFeedState) BlockingByBarrier() (blocked bool, tp barrierType, index uint64, barrierTs model.Ts) {
	blocked = false
	tp, index, barrierTs = cf.barriers.Min()
	if barrierTs == cf.CheckpointTs {
		blocked = true
	}
	return
}

func (cf *changeFeedState) ClearBarrier(tp barrierType, index uint64) {
	cf.barriers.Remove(tp, index)
}

func (cf *changeFeedState) ApplyTableActions(actions []tableAction) {
	for _, tableAction := range actions {
		switch tableAction.Action {
		case AddTableAction:
			cf.TableTasks[tableAction.tableID] = &tableTask{
				TableID:      tableAction.tableID,
				CheckpointTs: cf.CheckpointTs,
				ResolvedTs:   cf.CheckpointTs,
			}
		case DropTableAction:
			if _, ok := cf.TableTasks[tableAction.tableID]; !ok {
				log.Panic("changeFeedState: Dropping unknown table", zap.Int64("table-id", tableAction.tableID))
			}

			delete(cf.TableTasks, tableAction.tableID)
		default:
			log.Panic("changeFeedState: unknown action", zap.String("action", tableAction.Action))
		}
	}
}

func (cf *changeFeedState) CalcResolvedTsAndCheckpointTs() {
	cf.ResolvedTs = cf.calcResolvedTs()
	cf.CheckpointTs = cf.calcCheckpointTs()
}

// TODO test-case: returned value is not zero
func (cf *changeFeedState) calcResolvedTs() uint64 {
	_, _, ts := cf.barriers.Min()
	resolvedTs := ts

	for _, table := range cf.TableTasks {
		if resolvedTs > table.ResolvedTs {
			resolvedTs = table.ResolvedTs
		}
	}

	if resolvedTs == 0 {
		log.Panic("Unexpected resolvedTs")
	}
	return resolvedTs
}

// TODO test-case: returned value is not zero
func (cf *changeFeedState) calcCheckpointTs() uint64 {
	_, _, ts := cf.barriers.Min()
	checkpointTs := ts

	for _, table := range cf.TableTasks {
		if checkpointTs > table.CheckpointTs {
			checkpointTs = table.CheckpointTs
		}
	}

	if checkpointTs == 0 {
		log.Panic("Unexpected checkpointTs", zap.Reflect("state", cf))
	}
	return checkpointTs
}

func (cf *changeFeedState) SetTableResolvedTs(tableID model.TableID, resolvedTs uint64) {
	tableTask, ok := cf.TableTasks[tableID]

	if !ok {
		return
	}

	tableTask.ResolvedTs = resolvedTs
}

func (cf *changeFeedState) SetTableCheckpointTs(tableID model.TableID, checkpointTs uint64) {
	tableTask, ok := cf.TableTasks[tableID]

	if !ok {
		return
	}

	if tableTask.CheckpointTs > checkpointTs {
		log.Panic("changeFeedState: table checkpoint regressed. Report a bug.",
			zap.Int64("tableID", tableID),
			zap.Uint64("oldCheckpointTs", tableTask.CheckpointTs),
			zap.Uint64("checkpointTs", checkpointTs))
	}

	tableTask.CheckpointTs = checkpointTs
}
