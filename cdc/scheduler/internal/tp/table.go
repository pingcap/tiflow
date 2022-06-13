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

package tp

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"go.uber.org/zap"
)

// table is a state machine that manage the table's state,
// also tracking its progress by utilize the `TableExecutor`
type table struct {
	id model.TableID

	status   schedulepb.TableStatus
	executor internal.TableExecutor

	task *dispatchTableTask
}

func newTable(tableID model.TableID, executor internal.TableExecutor) *table {
	return &table{
		id:       tableID,
		executor: executor,
		status:   schedulepb.TableStatus{TableID: tableID},
		task:     nil,
	}
}

// refresh the table' status, return true if the table state changed,
// should be called before any table operations.
func (t *table) refresh() bool {
	oldState := t.status.State

	meta := t.executor.GetTableMeta(t.id)
	newState := tableStatus2PB(meta.State)

	t.status.State = newState
	t.status.Checkpoint.CheckpointTs = meta.CheckpointTs
	t.status.Checkpoint.ResolvedTs = meta.ResolvedTs

	return oldState != newState
}

func newAddTableResponseMessage(status schedulepb.TableStatus) *schedulepb.Message {
	return &schedulepb.Message{
		MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_AddTable{
				AddTable: &schedulepb.AddTableResponse{
					Status:     &status,
					Checkpoint: status.Checkpoint,
				},
			},
		},
	}
}

func newRemoveTableResponseMessage(status schedulepb.TableStatus) *schedulepb.Message {
	message := &schedulepb.Message{
		MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_RemoveTable{
				RemoveTable: &schedulepb.RemoveTableResponse{
					Status:     &status,
					Checkpoint: status.Checkpoint,
				},
			},
		},
	}

	return message
}

func (t *table) handleRemoveTableTask(ctx context.Context) *schedulepb.Message {
	status := t.status
	stateChanged := true
	for stateChanged {
		switch status.State {
		case schedulepb.TableStateAbsent:
			log.Warn("tpscheduler: remove table, but table is absent", zap.Any("table", t))
			t.task = nil
			return newRemoveTableResponseMessage(status)
		case schedulepb.TableStateStopping, // stopping now is useless
			schedulepb.TableStateStopped:
			// release table resource, and get the latest checkpoint
			// this will let the table become `absent`
			checkpointTs, done := t.executor.IsRemoveTableFinished(ctx, t.id)
			_ = t.refresh()
			if !done {
				// actually, this should never be hit, since we know that table is stopped.
				status.State = schedulepb.TableStateStopping
				return newRemoveTableResponseMessage(status)
			}
			t.task = nil
			status.State = schedulepb.TableStateStopped
			status.Checkpoint.CheckpointTs = checkpointTs
			return newRemoveTableResponseMessage(status)
		case schedulepb.TableStatePreparing,
			schedulepb.TableStatePrepared,
			schedulepb.TableStateReplicating:
			done := t.executor.RemoveTable(ctx, t.task.TableID)
			stateChanged = t.refresh()
			status = t.status
			if !done {
				status.State = schedulepb.TableStateStopping
				return newRemoveTableResponseMessage(status)
			}
		default:
			log.Panic("tpscheduler: unknown table state", zap.Any("table", t))
		}
	}
	return nil
}

func (t *table) handleAddTableTask(ctx context.Context) (result *schedulepb.Message, err error) {
	status := t.status
	stateChanged := true
	for stateChanged {
		switch status.State {
		case schedulepb.TableStateAbsent:
			done, err := t.executor.AddTable(ctx, t.task.TableID, t.task.StartTs, t.task.IsPrepare)
			if err != nil || !done {
				log.Info("tpscheduler: agent add table failed",
					zap.Any("task", t.task),
					zap.Error(err))
				return newAddTableResponseMessage(status), errors.Trace(err)
			}
			stateChanged = t.refresh()
			status = t.status
		case schedulepb.TableStateReplicating:
			log.Info("tpscheduler: table is replicating", zap.Any("table", t))
			t.task = nil
			return newAddTableResponseMessage(t.status), nil
		case schedulepb.TableStatePrepared:
			if t.task.IsPrepare {
				// `prepared` is a stable state, if the task was to prepare the table.
				log.Info("tpscheduler: table is prepared", zap.Any("table", t))
				t.task = nil
				return newAddTableResponseMessage(t.status), nil
			}

			if t.task.status == dispatchTableTaskReceived {
				done, err := t.executor.AddTable(ctx, t.task.TableID, t.task.StartTs, false)
				if err != nil || !done {
					log.Info("tpscheduler: agent add table failed",
						zap.Any("task", t.task),
						zap.Error(err))
					return newAddTableResponseMessage(status), errors.Trace(err)
				}
				t.task.status = dispatchTableTaskProcessed
			}

			done := t.executor.IsAddTableFinished(ctx, t.task.TableID, false)
			stateChanged = t.refresh()
			status = t.status
			if !done {
				return newAddTableResponseMessage(status), nil
			}
		case schedulepb.TableStatePreparing:
			// `preparing` is not stable state and would last a long time,
			// it's no need to return such a state, to make the coordinator become burdensome.
			done := t.executor.IsAddTableFinished(ctx, t.task.TableID, t.task.IsPrepare)
			if !done {
				return nil, nil
			}
			log.Info("tpscheduler: add table finished", zap.Any("table", t))
			stateChanged = t.refresh()
			status = t.status
		case schedulepb.TableStateStopping,
			schedulepb.TableStateStopped:
			log.Warn("tpscheduler: ignore add table", zap.Any("table", t))
			t.task = nil
			return newAddTableResponseMessage(status), nil
		default:
			log.Panic("tpscheduler: unknown table state", zap.Any("table", t))
		}
	}

	return nil, nil
}

func (t *table) injectDispatchTableTask(task *dispatchTableTask) {
	if t.id != task.TableID {
		log.Panic("tpscheduler: tableID not match",
			zap.Int64("tableID", t.id),
			zap.Int64("task.TableID", task.TableID))
	}
	if t.task == nil {
		log.Info("tpscheduler: table found new task",
			zap.Any("table", t), zap.Any("task", task))
		t.task = task
		return
	}
	log.Warn("tpscheduler: table inject dispatch table task ignored,"+
		"since there is one not finished yet",
		zap.Any("table", t),
		zap.Any("nowTask", t.task),
		zap.Any("ignoredTask", task))
}

func (t *table) poll(ctx context.Context) (*schedulepb.Message, error) {
	if t.task == nil {
		return nil, nil
	}
	_ = t.refresh()
	if t.task.IsRemove {
		return t.handleRemoveTableTask(ctx), nil
	}
	return t.handleAddTableTask(ctx)
}

type tableManager struct {
	tables   map[model.TableID]*table
	executor internal.TableExecutor
}

func newTableManager(executor internal.TableExecutor) *tableManager {
	tm := &tableManager{
		tables:   make(map[model.TableID]*table),
		executor: executor,
	}
	tm.collectAllTables()
	return tm
}

func (tm *tableManager) getAllTables() map[model.TableID]*table {
	tm.collectAllTables()
	return tm.tables
}

func (tm *tableManager) collectAllTables() {
	allTables := tm.executor.GetAllCurrentTables()
	for _, tableID := range allTables {
		table, ok := tm.tables[tableID]
		if !ok {
			table = newTable(tableID, tm.executor)
			tm.tables[tableID] = table
		}
		_ = table.refresh()
	}
}

// register the target table, and return it.
func (tm *tableManager) register(tableID model.TableID) *table {
	table, ok := tm.tables[tableID]
	if !ok {
		table = newTable(tableID, tm.executor)
		tm.tables[tableID] = table
	}
	_ = table.refresh()
	return table
}

func (tm *tableManager) getTable(tableID model.TableID) (*table, bool) {
	table, ok := tm.tables[tableID]
	if ok {
		_ = table.refresh()
		return table, true
	}

	meta := tm.executor.GetTableMeta(tableID)
	state := tableStatus2PB(meta.State)

	if state != schedulepb.TableStateAbsent {
		log.Panic("tpscheduler: tableManager found table not tracked",
			zap.Int64("tableID", tableID), zap.Any("meta", meta))

	}
	return nil, false
}

func (tm *tableManager) dropTable(tableID model.TableID) {
	table, ok := tm.tables[tableID]
	if !ok {
		log.Warn("tpscheduler: tableManager drop table not found",
			zap.Int64("tableID", tableID))
		return
	}
	_ = table.refresh()
	if table.status.State != schedulepb.TableStateAbsent {
		log.Panic("tpscheduler: tableManager drop table undesired",
			zap.Any("table", table))
	}

	log.Debug("tpscheduler: tableManager drop table", zap.Any("table", table))
	delete(tm.tables, tableID)
}

func (tm *tableManager) newTableStatus(tableID model.TableID) schedulepb.TableStatus {
	meta := tm.executor.GetTableMeta(tableID)
	state := tableStatus2PB(meta.State)
	return schedulepb.TableStatus{
		TableID: meta.TableID,
		State:   state,
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: meta.CheckpointTs,
			ResolvedTs:   meta.ResolvedTs,
		},
	}
}

func tableStatus2PB(state pipeline.TableState) schedulepb.TableState {
	switch state {
	case pipeline.TableStatePreparing:
		return schedulepb.TableStatePreparing
	case pipeline.TableStatePrepared:
		return schedulepb.TableStatePrepared
	case pipeline.TableStateReplicating:
		return schedulepb.TableStateReplicating
	case pipeline.TableStateStopping:
		return schedulepb.TableStateStopping
	case pipeline.TableStateStopped:
		return schedulepb.TableStateStopped
	case pipeline.TableStateAbsent:
		return schedulepb.TableStateAbsent
	default:
	}
	return schedulepb.TableStateUnknown
}
