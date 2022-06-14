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

	state    schedulepb.TableState
	executor internal.TableExecutor

	task *dispatchTableTask
}

func newTable(tableID model.TableID, executor internal.TableExecutor) *table {
	return &table{
		id:       tableID,
		state:    schedulepb.TableStateAbsent, // use `absent` as the default state.
		executor: executor,
		task:     nil,
	}
}

// getAndUpdateTableState get the table' state, return true if the table state changed
func (t *table) getAndUpdateTableState() (schedulepb.TableState, bool) {
	oldState := t.state

	meta := t.executor.GetTableMeta(t.id)
	state := tableStatus2PB(meta.State)
	t.state = state

	if oldState != state {
		log.Info("tpscheduler: table state changed",
			zap.Any("oldState", oldState), zap.Any("state", state))
		return t.state, true

	}
	return t.state, false
}

func (t *table) getTableStatus() schedulepb.TableStatus {
	meta := t.executor.GetTableMeta(t.id)
	state := tableStatus2PB(meta.State)

	return schedulepb.TableStatus{
		TableID: t.id,
		State:   state,
		Checkpoint: schedulepb.Checkpoint{
			CheckpointTs: meta.CheckpointTs,
			ResolvedTs:   meta.ResolvedTs,
		},
	}
}

func newAddTableResponseMessage(status schedulepb.TableStatus, reject bool) *schedulepb.Message {
	return &schedulepb.Message{
		MsgType: schedulepb.MsgDispatchTableResponse,
		DispatchTableResponse: &schedulepb.DispatchTableResponse{
			Response: &schedulepb.DispatchTableResponse_AddTable{
				AddTable: &schedulepb.AddTableResponse{
					Status:     &status,
					Checkpoint: status.Checkpoint,
					Reject:     reject,
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
	state, changed := t.getAndUpdateTableState()
	changed = true
	for changed {
		switch state {
		case schedulepb.TableStateAbsent:
			log.Warn("tpscheduler: remove table, but table is absent",
				zap.Int64("tableID", t.id))
			t.task = nil
			return newRemoveTableResponseMessage(t.getTableStatus())
		case schedulepb.TableStateStopping, // stopping now is useless
			schedulepb.TableStateStopped:
			// release table resource, and get the latest checkpoint
			// this will let the table become `absent`
			checkpointTs, done := t.executor.IsRemoveTableFinished(ctx, t.id)
			if !done {
				// actually, this should never be hit, since we know that table is stopped.
				status := t.getTableStatus()
				status.State = schedulepb.TableStateStopping
				return newRemoveTableResponseMessage(status)
			}
			t.task = nil
			status := t.getTableStatus()
			status.State = schedulepb.TableStateStopped
			status.Checkpoint.CheckpointTs = checkpointTs
			return newRemoveTableResponseMessage(status)
		case schedulepb.TableStatePreparing,
			schedulepb.TableStatePrepared,
			schedulepb.TableStateReplicating:
			done := t.executor.RemoveTable(ctx, t.task.TableID)
			if !done {
				status := t.getTableStatus()
				status.State = schedulepb.TableStateStopping
				return newRemoveTableResponseMessage(status)
			}
			state, changed = t.getAndUpdateTableState()
		default:
			log.Panic("tpscheduler: unknown table state",
				zap.Int64("tableID", t.id), zap.Any("state", state))
		}
	}
	return nil
}

func (t *table) handleAddTableTask(ctx context.Context, stopping bool) (result *schedulepb.Message, err error) {
	if stopping {
		log.Info("tpscheduler: reject add table, since agent stopping",
			zap.Int64("tableID", t.id), zap.Any("task", t.task))
		t.task = nil
		return newAddTableResponseMessage(t.getTableStatus(), true), nil
	}

	state, changed := t.getAndUpdateTableState()
	changed = true
	for changed {
		switch state {
		case schedulepb.TableStateAbsent:
			done, err := t.executor.AddTable(ctx, t.task.TableID, t.task.StartTs, t.task.IsPrepare)
			if err != nil || !done {
				log.Info("tpscheduler: agent add table failed",
					zap.Int64("tableID", t.id),
					zap.Any("task", t.task),
					zap.Error(err))
				status := t.getTableStatus()
				return newAddTableResponseMessage(status, false), errors.Trace(err)
			}
			state, changed = t.getAndUpdateTableState()
		case schedulepb.TableStateReplicating:
			log.Info("tpscheduler: table is replicating",
				zap.Int64("tableID", t.id), zap.Any("state", state))
			t.task = nil
			status := t.getTableStatus()
			return newAddTableResponseMessage(status, false), nil
		case schedulepb.TableStatePrepared:
			if t.task.IsPrepare {
				// `prepared` is a stable state, if the task was to prepare the table.
				log.Info("tpscheduler: table is prepared",
					zap.Int64("tableID", t.id), zap.Any("state", state))
				t.task = nil
				return newAddTableResponseMessage(t.getTableStatus(), false), nil
			}

			if t.task.status == dispatchTableTaskReceived {
				done, err := t.executor.AddTable(ctx, t.task.TableID, t.task.StartTs, false)
				if err != nil || !done {
					log.Info("tpscheduler: agent add table failed",
						zap.Int64("tableID", t.id), zap.Any("state", state),
						zap.Error(err))
					status := t.getTableStatus()
					return newAddTableResponseMessage(status, false), errors.Trace(err)
				}
				t.task.status = dispatchTableTaskProcessed
			}

			done := t.executor.IsAddTableFinished(ctx, t.task.TableID, false)
			if !done {
				return newAddTableResponseMessage(t.getTableStatus(), false), nil
			}
			state, changed = t.getAndUpdateTableState()
		case schedulepb.TableStatePreparing:
			// `preparing` is not stable state and would last a long time,
			// it's no need to return such a state, to make the coordinator become burdensome.
			done := t.executor.IsAddTableFinished(ctx, t.task.TableID, t.task.IsPrepare)
			if !done {
				return nil, nil
			}
			state, changed = t.getAndUpdateTableState()
			log.Info("tpscheduler: add table finished",
				zap.Int64("tableID", t.id), zap.Any("state", state))
		case schedulepb.TableStateStopping,
			schedulepb.TableStateStopped:
			log.Warn("tpscheduler: ignore add table", zap.Int64("tableID", t.id))
			t.task = nil
			return newAddTableResponseMessage(t.getTableStatus(), false), nil
		default:
			log.Panic("tpscheduler: unknown table state", zap.Int64("tableID", t.id))
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
			zap.Int64("tableID", t.id),
			zap.Any("task", task))
		t.task = task
		return
	}
	log.Debug("tpscheduler: table inject dispatch table task ignored,"+
		"since there is one not finished yet",
		zap.Int64("tableID", t.id),
		zap.Any("nowTask", t.task),
		zap.Any("ignoredTask", task))
}

func (t *table) poll(ctx context.Context, stopping bool) (*schedulepb.Message, error) {
	if t.task == nil {
		return nil, nil
	}
	if t.task.IsRemove {
		return t.handleRemoveTableTask(ctx), nil
	}
	return t.handleAddTableTask(ctx, stopping)
}

type tableManager struct {
	tables   map[model.TableID]*table
	executor internal.TableExecutor
}

func newTableManager(executor internal.TableExecutor) *tableManager {
	return &tableManager{
		tables:   make(map[model.TableID]*table),
		executor: executor,
	}
}

func (tm *tableManager) poll(ctx context.Context, stopping bool) ([]*schedulepb.Message, error) {
	result := make([]*schedulepb.Message, 0)
	for tableID, table := range tm.tables {
		message, err := table.poll(ctx, stopping)
		if err != nil {
			return result, errors.Trace(err)
		}

		state, _ := table.getAndUpdateTableState()
		if state == schedulepb.TableStateAbsent {
			tm.dropTable(tableID)
		}

		if message == nil {
			continue
		}
		result = append(result, message)
	}
	return result, nil
}

func (tm *tableManager) getAllTables() map[model.TableID]*table {
	return tm.tables
}

// addTable add the target table, and return it.
func (tm *tableManager) addTable(tableID model.TableID) *table {
	table, ok := tm.tables[tableID]
	if !ok {
		table = newTable(tableID, tm.executor)
		tm.tables[tableID] = table
	}
	return table
}

func (tm *tableManager) getTable(tableID model.TableID) (*table, bool) {
	table, ok := tm.tables[tableID]
	if ok {
		return table, true
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
	state, _ := table.getAndUpdateTableState()
	if state != schedulepb.TableStateAbsent {
		log.Panic("tpscheduler: tableManager drop table undesired",
			zap.Int64("tableID", tableID),
			zap.Any("state", table.state))
	}

	log.Debug("tpscheduler: tableManager drop table", zap.Any("tableID", tableID))
	delete(tm.tables, tableID)
}

func (tm *tableManager) getTableStatus(tableID model.TableID) schedulepb.TableStatus {
	table, ok := tm.getTable(tableID)
	if ok {
		return table.getTableStatus()
	}

	return schedulepb.TableStatus{
		TableID: tableID,
		State:   schedulepb.TableStateAbsent,
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
