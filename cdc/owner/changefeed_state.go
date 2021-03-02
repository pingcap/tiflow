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
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// changeFeedState is part of the replication model that implements the control logic of a changeFeed
type changeFeedState struct {
	TableTasks    map[model.TableID]*tableTask
	DDLResolvedTs uint64
	Barriers      []*barrier

	scheduler scheduler
}

type tableTask struct {
	TableID      model.TableID
	CheckpointTs uint64
	ResolvedTs   uint64
}

type barrierType = int

const (
	// DDLBarrier denotes a replication barrier caused by a DDL.
	DDLBarrier = barrierType(iota)
	// SyncPointBarrier denotes a barrier for snapshot replication.
	// SyncPointBarrier
	// TODO support snapshot replication.
)

type barrier struct {
	BarrierType barrierType
	BarrierTs   uint64
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
	return &changeFeedState{
		TableTasks:    initTableTasks,
		DDLResolvedTs: ddlStartTs,
		scheduler:     scheduler,
	}
}

func (cf *changeFeedState) SyncTasks() {
	if cf.scheduler.IsReady() {
		cf.scheduler.PutTasks(cf.TableTasks)
	}
}

func (cf *changeFeedState) SetDDLResolvedTs(ddlResolvedTs uint64) {
	cf.DDLResolvedTs = ddlResolvedTs
}

func (cf *changeFeedState) AddDDLBarrier(barrierTs uint64) {
	if len(cf.Barriers) > 0 && barrierTs < cf.Barriers[len(cf.Barriers)-1].BarrierTs {
		log.Panic("changeFeedState: DDLBarrier too small",
			zap.Uint64("last-barrier-ts", cf.Barriers[len(cf.Barriers)-1].BarrierTs),
			zap.Uint64("new-barrier-ts", barrierTs))
	}

	if barrierTs < cf.DDLResolvedTs {
		log.Panic("changeFeedState: DDLBarrier too small",
			zap.Uint64("cur-ddl-resolved-ts", cf.DDLResolvedTs),
			zap.Uint64("new-barrier-ts", barrierTs))
	}

	cf.Barriers = append(cf.Barriers, &barrier{
		BarrierType: DDLBarrier,
		BarrierTs:   barrierTs,
	})
}

func (cf *changeFeedState) ShouldRunDDL() *barrier {
	if len(cf.Barriers) > 0 {
		if cf.Barriers[0].BarrierTs == cf.CheckpointTs()+1 &&
			cf.Barriers[0].BarrierType == DDLBarrier {

			return cf.Barriers[0]
		}
	}

	return nil
}

func (cf *changeFeedState) MarkDDLDone(result ddlResult) {
	if cf.CheckpointTs() != result.FinishTs-1 {
		log.Panic("changeFeedState: Unexpected checkpoint when DDL is done",
			zap.Uint64("cur-checkpoint-ts", cf.CheckpointTs()),
			zap.Reflect("ddl-result", result))
	}

	if len(cf.Barriers) == 0 ||
		cf.Barriers[0].BarrierType != DDLBarrier ||
		cf.Barriers[0].BarrierTs != result.FinishTs {

		log.Panic("changeFeedState: no DDL barrier found",
			zap.Reflect("barriers", cf.Barriers),
			zap.Reflect("ddl-result", result))
	}

	cf.Barriers = cf.Barriers[1:]

	for _, tableAction := range result.Actions {
		switch tableAction.Action {
		case AddTableAction:
			cf.TableTasks[tableAction.tableID] = &tableTask{
				TableID:      tableAction.tableID,
				CheckpointTs: cf.CheckpointTs(),
				ResolvedTs:   cf.CheckpointTs(),
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

// TODO test-case: returned value is not zero
func (cf *changeFeedState) ResolvedTs() uint64 {
	resolvedTs := cf.DDLResolvedTs

	for _, table := range cf.TableTasks {
		if resolvedTs > table.ResolvedTs {
			resolvedTs = table.ResolvedTs
		}
	}

	if len(cf.Barriers) > 0 && resolvedTs > cf.Barriers[0].BarrierTs-1 {
		resolvedTs = cf.Barriers[0].BarrierTs - 1
	}

	if resolvedTs == 0 {
		log.Panic("Unexpected resolvedTs")
	}
	return resolvedTs
}

// TODO test-case: returned value is not zero
func (cf *changeFeedState) CheckpointTs() uint64 {
	checkpointTs := cf.DDLResolvedTs

	for _, table := range cf.TableTasks {
		if checkpointTs > table.CheckpointTs {
			checkpointTs = table.CheckpointTs
		}
	}

	if len(cf.Barriers) > 0 && checkpointTs > cf.Barriers[0].BarrierTs-1 {
		checkpointTs = cf.Barriers[0].BarrierTs - 1
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
