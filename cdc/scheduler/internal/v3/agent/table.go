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

package agent

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"go.uber.org/zap"
)

// table is a state machine that manage the table's state,
// also tracking its progress by utilize the `TableExecutor`
type table struct {
	changefeedID model.ChangeFeedID
	id           model.TableID

	state    tablepb.TableState
	executor internal.TableExecutor

	task *dispatchTableTask
}

func newTable(
	changefeed model.ChangeFeedID, tableID model.TableID, executor internal.TableExecutor,
) *table {
	return &table{
		changefeedID: changefeed,
		id:           tableID,
		state:        tablepb.TableStateAbsent, // use `absent` as the default state.
		executor:     executor,
		task:         nil,
	}
}

// getAndUpdateTableState get the table' state, return true if the table state changed
func (t *table) getAndUpdateTableState() (tablepb.TableState, bool) {
	oldState := t.state

	meta := t.executor.GetTableStatus(t.id, false)
	t.state = meta.State

	if oldState != t.state {
		log.Debug("schedulerv3: table state changed",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.Int64("tableID", t.id),
			zap.Stringer("oldState", oldState),
			zap.Stringer("state", t.state))
		return t.state, true
	}
	return t.state, false
}

func (t *table) getTableStatus(collectStat bool) tablepb.TableStatus {
	return t.executor.GetTableStatus(t.id, collectStat)
}

func newAddTableResponseMessage(status tablepb.TableStatus) *schedulepb.Message {
	if status.Checkpoint.ResolvedTs < status.Checkpoint.CheckpointTs {
		log.Warn("schedulerv3: resolved ts should not less than checkpoint ts",
			zap.Any("tableStatus", status),
			zap.Any("checkpoint", status.Checkpoint.CheckpointTs),
			zap.Any("resolved", status.Checkpoint.ResolvedTs))
	}
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

func newRemoveTableResponseMessage(status tablepb.TableStatus) *schedulepb.Message {
	if status.Checkpoint.ResolvedTs < status.Checkpoint.CheckpointTs {
		// TODO: resolvedTs should not be zero, but we have to handle it for now.
		if status.Checkpoint.ResolvedTs == 0 {
			// Advance resolved ts to checkpoint ts if table is removed.
			status.Checkpoint.ResolvedTs = status.Checkpoint.CheckpointTs
		} else {
			log.Warn("schedulerv3: resolved ts should not less than checkpoint ts",
				zap.Any("tableStatus", status),
				zap.Any("checkpoint", status.Checkpoint.CheckpointTs),
				zap.Any("resolved", status.Checkpoint.ResolvedTs))
		}
	}
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

func (t *table) handleRemoveTableTask() *schedulepb.Message {
	state, _ := t.getAndUpdateTableState()
	changed := true
	for changed {
		switch state {
		case tablepb.TableStateAbsent:
			log.Warn("schedulerv3: remove table, but table is absent",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.id))
			t.task = nil
			return newRemoveTableResponseMessage(t.getTableStatus(false))
		case tablepb.TableStateStopping, // stopping now is useless
			tablepb.TableStateStopped:
			// release table resource, and get the latest checkpoint
			// this will let the table become `absent`
			checkpointTs, done := t.executor.IsRemoveTableFinished(t.id)
			if !done {
				// actually, this should never be hit, since we know that table is stopped.
				status := t.getTableStatus(false)
				status.State = tablepb.TableStateStopping
				return newRemoveTableResponseMessage(status)
			}
			t.task = nil
			status := t.getTableStatus(false)
			status.State = tablepb.TableStateStopped
			status.Checkpoint.CheckpointTs = checkpointTs
			return newRemoveTableResponseMessage(status)
		case tablepb.TableStatePreparing,
			tablepb.TableStatePrepared,
			tablepb.TableStateReplicating:
			done := t.executor.RemoveTable(t.task.TableID)
			if !done {
				status := t.getTableStatus(false)
				status.State = tablepb.TableStateStopping
				return newRemoveTableResponseMessage(status)
			}
			state, changed = t.getAndUpdateTableState()
		default:
			log.Panic("schedulerv3: unknown table state",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.id), zap.Stringer("state", state))
		}
	}
	return nil
}

func (t *table) handleAddTableTask(ctx context.Context) (result *schedulepb.Message, err error) {
	state, _ := t.getAndUpdateTableState()
	changed := true
	for changed {
		switch state {
		case tablepb.TableStateAbsent:
			done, err := t.executor.AddTable(ctx, t.task.TableID, t.task.Checkpoint, t.task.IsPrepare)
			if err != nil || !done {
				log.Warn("schedulerv3: agent add table failed",
					zap.String("namespace", t.changefeedID.Namespace),
					zap.String("changefeed", t.changefeedID.ID),
					zap.Int64("tableID", t.id), zap.Any("task", t.task),
					zap.Error(err))
				status := t.getTableStatus(false)
				return newAddTableResponseMessage(status), errors.Trace(err)
			}
			state, changed = t.getAndUpdateTableState()
		case tablepb.TableStateReplicating:
			log.Info("schedulerv3: table is replicating",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.id), zap.Stringer("state", state))
			t.task = nil
			status := t.getTableStatus(false)
			return newAddTableResponseMessage(status), nil
		case tablepb.TableStatePrepared:
			if t.task.IsPrepare {
				// `prepared` is a stable state, if the task was to prepare the table.
				log.Info("schedulerv3: table is prepared",
					zap.String("namespace", t.changefeedID.Namespace),
					zap.String("changefeed", t.changefeedID.ID),
					zap.Int64("tableID", t.id), zap.Stringer("state", state))
				t.task = nil
				return newAddTableResponseMessage(t.getTableStatus(false)), nil
			}

			if t.task.status == dispatchTableTaskReceived {
				done, err := t.executor.AddTable(ctx, t.task.TableID, t.task.Checkpoint, false)
				if err != nil || !done {
					log.Warn("schedulerv3: agent add table failed",
						zap.String("namespace", t.changefeedID.Namespace),
						zap.String("changefeed", t.changefeedID.ID),
						zap.Int64("tableID", t.id), zap.Stringer("state", state),
						zap.Error(err))
					status := t.getTableStatus(false)
					return newAddTableResponseMessage(status), errors.Trace(err)
				}
				t.task.status = dispatchTableTaskProcessed
			}

			done := t.executor.IsAddTableFinished(t.task.TableID, false)
			if !done {
				return newAddTableResponseMessage(t.getTableStatus(false)), nil
			}
			state, changed = t.getAndUpdateTableState()
		case tablepb.TableStatePreparing:
			// `preparing` is not stable state and would last a long time,
			// it's no need to return such a state, to make the coordinator become burdensome.
			done := t.executor.IsAddTableFinished(t.task.TableID, t.task.IsPrepare)
			if !done {
				return nil, nil
			}
			state, changed = t.getAndUpdateTableState()
			log.Info("schedulerv3: add table finished",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.id), zap.Stringer("state", state))
		case tablepb.TableStateStopping,
			tablepb.TableStateStopped:
			log.Warn("schedulerv3: ignore add table",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.id))
			t.task = nil
			return newAddTableResponseMessage(t.getTableStatus(false)), nil
		default:
			log.Panic("schedulerv3: unknown table state",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.id))
		}
	}

	return nil, nil
}

func (t *table) injectDispatchTableTask(task *dispatchTableTask) {
	if t.id != task.TableID {
		log.Panic("schedulerv3: tableID not match",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.Int64("tableID", t.id),
			zap.Int64("task.TableID", task.TableID))
	}
	if t.task == nil {
		log.Info("schedulerv3: table found new task",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.Int64("tableID", t.id),
			zap.Any("task", task))
		t.task = task
		return
	}
	log.Debug("schedulerv3: table inject dispatch table task ignored,"+
		"since there is one not finished yet",
		zap.String("namespace", t.changefeedID.Namespace),
		zap.String("changefeed", t.changefeedID.ID),
		zap.Int64("tableID", t.id),
		zap.Any("nowTask", t.task),
		zap.Any("ignoredTask", task))
}

func (t *table) poll(ctx context.Context) (*schedulepb.Message, error) {
	if t.task == nil {
		return nil, nil
	}
	if t.task.IsRemove {
		return t.handleRemoveTableTask(), nil
	}
	return t.handleAddTableTask(ctx)
}

type tableManager struct {
	tables   map[model.TableID]*table
	executor internal.TableExecutor

	changefeedID model.ChangeFeedID
}

func newTableManager(
	changefeed model.ChangeFeedID, executor internal.TableExecutor,
) *tableManager {
	return &tableManager{
		tables:       make(map[model.TableID]*table),
		executor:     executor,
		changefeedID: changefeed,
	}
}

func (tm *tableManager) poll(ctx context.Context) ([]*schedulepb.Message, error) {
	result := make([]*schedulepb.Message, 0)
	for tableID, table := range tm.tables {
		message, err := table.poll(ctx)
		if err != nil {
			return result, errors.Trace(err)
		}

		state, _ := table.getAndUpdateTableState()
		if state == tablepb.TableStateAbsent {
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
		table = newTable(tm.changefeedID, tableID, tm.executor)
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
		log.Warn("schedulerv3: tableManager drop table not found",
			zap.String("namespace", tm.changefeedID.Namespace),
			zap.String("changefeed", tm.changefeedID.ID),
			zap.Int64("tableID", tableID))
		return
	}
	state, _ := table.getAndUpdateTableState()
	if state != tablepb.TableStateAbsent {
		log.Panic("schedulerv3: tableManager drop table undesired",
			zap.String("namespace", tm.changefeedID.Namespace),
			zap.String("changefeed", tm.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.Stringer("state", table.state))
	}

	log.Debug("schedulerv3: tableManager drop table",
		zap.String("namespace", tm.changefeedID.Namespace),
		zap.String("changefeed", tm.changefeedID.ID),
		zap.Int64("tableID", tableID))
	delete(tm.tables, tableID)
}

func (tm *tableManager) getTableStatus(
	tableID model.TableID, collectStat bool,
) tablepb.TableStatus {
	table, ok := tm.getTable(tableID)
	if ok {
		return table.getTableStatus(collectStat)
	}

	return tablepb.TableStatus{
		TableID: tableID,
		State:   tablepb.TableStateAbsent,
	}
}
