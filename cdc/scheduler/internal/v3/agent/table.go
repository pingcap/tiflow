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
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/schedulepb"
	"go.uber.org/zap"
)

// table is a state machine that manage the table's state,
// also tracking its progress by utilize the `TableExecutor`
type table struct {
	changefeedID model.ChangeFeedID
	id           model.TableID

	state    schedulepb.TableState
	executor internal.TableExecutor

	task *dispatchTableTask
}

func newTable(
	changefeed model.ChangeFeedID, tableID model.TableID, executor internal.TableExecutor,
) *table {
	return &table{
		changefeedID: changefeed,
		id:           tableID,
		state:        schedulepb.TableStateAbsent, // use `absent` as the default state.
		executor:     executor,
		task:         nil,
	}
}

// getAndUpdateTableState get the table' state, return true if the table state changed
func (t *table) getAndUpdateTableState() (schedulepb.TableState, bool) {
	oldState := t.state

	meta := t.executor.GetTableMeta(t.id)
	state := tableStatus2PB(meta.State)
	t.state = state

	if oldState != state {
		log.Debug("schedulerv3: table state changed",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.Int64("tableID", t.id),
			zap.Stringer("oldState", oldState),
			zap.Stringer("state", state))
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

func (t *table) handleRemoveTableTask() *schedulepb.Message {
	state, _ := t.getAndUpdateTableState()
	changed := true
	for changed {
		switch state {
		case schedulepb.TableStateAbsent:
			log.Warn("schedulerv3: remove table, but table is absent",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.id))
			t.task = nil
			return newRemoveTableResponseMessage(t.getTableStatus())
		case schedulepb.TableStateStopping, // stopping now is useless
			schedulepb.TableStateStopped:
			// release table resource, and get the latest checkpoint
			// this will let the table become `absent`
			checkpointTs, done := t.executor.IsRemoveTableFinished(t.id)
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
			done := t.executor.RemoveTable(t.task.TableID)
			if !done {
				status := t.getTableStatus()
				status.State = schedulepb.TableStateStopping
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
		case schedulepb.TableStateAbsent:
			done, err := t.executor.AddTable(ctx, t.task.TableID, t.task.StartTs, t.task.IsPrepare)
			if err != nil || !done {
				log.Warn("schedulerv3: agent add table failed",
					zap.String("namespace", t.changefeedID.Namespace),
					zap.String("changefeed", t.changefeedID.ID),
					zap.Int64("tableID", t.id), zap.Any("task", t.task),
					zap.Error(err))
				status := t.getTableStatus()
				return newAddTableResponseMessage(status), errors.Trace(err)
			}
			state, changed = t.getAndUpdateTableState()
		case schedulepb.TableStateReplicating:
			log.Info("schedulerv3: table is replicating",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.id), zap.Stringer("state", state))
			t.task = nil
			status := t.getTableStatus()
			return newAddTableResponseMessage(status), nil
		case schedulepb.TableStatePrepared:
			if t.task.IsPrepare {
				// `prepared` is a stable state, if the task was to prepare the table.
				log.Info("schedulerv3: table is prepared",
					zap.String("namespace", t.changefeedID.Namespace),
					zap.String("changefeed", t.changefeedID.ID),
					zap.Int64("tableID", t.id), zap.Stringer("state", state))
				t.task = nil
				return newAddTableResponseMessage(t.getTableStatus()), nil
			}

			if t.task.status == dispatchTableTaskReceived {
				done, err := t.executor.AddTable(ctx, t.task.TableID, t.task.StartTs, false)
				if err != nil || !done {
					log.Warn("schedulerv3: agent add table failed",
						zap.String("namespace", t.changefeedID.Namespace),
						zap.String("changefeed", t.changefeedID.ID),
						zap.Int64("tableID", t.id), zap.Stringer("state", state),
						zap.Error(err))
					status := t.getTableStatus()
					return newAddTableResponseMessage(status), errors.Trace(err)
				}
				t.task.status = dispatchTableTaskProcessed
			}

			done := t.executor.IsAddTableFinished(t.task.TableID, false)
			if !done {
				return newAddTableResponseMessage(t.getTableStatus()), nil
			}
			state, changed = t.getAndUpdateTableState()
		case schedulepb.TableStatePreparing:
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
		case schedulepb.TableStateStopping,
			schedulepb.TableStateStopped:
			log.Warn("schedulerv3: ignore add table",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.id))
			t.task = nil
			return newAddTableResponseMessage(t.getTableStatus()), nil
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
	if state != schedulepb.TableStateAbsent {
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
