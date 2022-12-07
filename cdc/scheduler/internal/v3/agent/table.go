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
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

// table is a state machine that manage the table's state,
// also tracking its progress by utilize the `TableExecutor`
type table struct {
	changefeedID model.ChangeFeedID
	span         tablepb.Span

	state    tablepb.TableState
	executor internal.TableExecutor

	task *dispatchTableTask
}

func newTable(
	changefeed model.ChangeFeedID, span tablepb.Span, executor internal.TableExecutor,
) *table {
	return &table{
		changefeedID: changefeed,
		span:         span,
		state:        tablepb.TableStateAbsent, // use `absent` as the default state.
		executor:     executor,
		task:         nil,
	}
}

// getAndUpdateTableState get the table' state, return true if the table state changed
func (t *table) getAndUpdateTableState() (tablepb.TableState, bool) {
	oldState := t.state

	meta := t.executor.GetTableStatus(t.span)
	t.state = meta.State

	if oldState != t.state {
		log.Debug("schedulerv3: table state changed",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.Int64("tableID", t.span.TableID),
			zap.Stringer("oldState", oldState),
			zap.Stringer("state", t.state))
		return t.state, true
	}
	return t.state, false
}

func (t *table) getTableStatus() tablepb.TableStatus {
	return t.executor.GetTableStatus(t.span)
}

func newAddTableResponseMessage(status tablepb.TableStatus) *schedulepb.Message {
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
				zap.Int64("tableID", t.span.TableID))
			t.task = nil
			return newRemoveTableResponseMessage(t.getTableStatus())
		case tablepb.TableStateStopping, // stopping now is useless
			tablepb.TableStateStopped:
			// release table resource, and get the latest checkpoint
			// this will let the table become `absent`
			checkpointTs, done := t.executor.IsRemoveTableFinished(t.span)
			if !done {
				// actually, this should never be hit, since we know that table is stopped.
				status := t.getTableStatus()
				status.State = tablepb.TableStateStopping
				return newRemoveTableResponseMessage(status)
			}
			t.task = nil
			status := t.getTableStatus()
			status.State = tablepb.TableStateStopped
			status.Checkpoint.CheckpointTs = checkpointTs
			return newRemoveTableResponseMessage(status)
		case tablepb.TableStatePreparing,
			tablepb.TableStatePrepared,
			tablepb.TableStateReplicating:
			done := t.executor.RemoveTable(t.task.Span)
			if !done {
				status := t.getTableStatus()
				status.State = tablepb.TableStateStopping
				return newRemoveTableResponseMessage(status)
			}
			state, changed = t.getAndUpdateTableState()
		default:
			log.Panic("schedulerv3: unknown table state",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.span.TableID), zap.Stringer("state", state))
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
			done, err := t.executor.AddTable(ctx, t.task.Span, t.task.StartTs, t.task.IsPrepare)
			if err != nil || !done {
				log.Warn("schedulerv3: agent add table failed",
					zap.String("namespace", t.changefeedID.Namespace),
					zap.String("changefeed", t.changefeedID.ID),
					zap.Int64("tableID", t.span.TableID), zap.Any("task", t.task),
					zap.Error(err))
				status := t.getTableStatus()
				return newAddTableResponseMessage(status), errors.Trace(err)
			}
			state, changed = t.getAndUpdateTableState()
		case tablepb.TableStateReplicating:
			log.Info("schedulerv3: table is replicating",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.span.TableID), zap.Stringer("state", state))
			t.task = nil
			status := t.getTableStatus()
			return newAddTableResponseMessage(status), nil
		case tablepb.TableStatePrepared:
			if t.task.IsPrepare {
				// `prepared` is a stable state, if the task was to prepare the table.
				log.Info("schedulerv3: table is prepared",
					zap.String("namespace", t.changefeedID.Namespace),
					zap.String("changefeed", t.changefeedID.ID),
					zap.Int64("tableID", t.span.TableID), zap.Stringer("state", state))
				t.task = nil
				return newAddTableResponseMessage(t.getTableStatus()), nil
			}

			if t.task.status == dispatchTableTaskReceived {
				done, err := t.executor.AddTable(ctx, t.task.Span, t.task.StartTs, false)
				if err != nil || !done {
					log.Warn("schedulerv3: agent add table failed",
						zap.String("namespace", t.changefeedID.Namespace),
						zap.String("changefeed", t.changefeedID.ID),
						zap.Int64("tableID", t.span.TableID), zap.Stringer("state", state),
						zap.Error(err))
					status := t.getTableStatus()
					return newAddTableResponseMessage(status), errors.Trace(err)
				}
				t.task.status = dispatchTableTaskProcessed
			}

			done := t.executor.IsAddTableFinished(t.task.Span, false)
			if !done {
				return newAddTableResponseMessage(t.getTableStatus()), nil
			}
			state, changed = t.getAndUpdateTableState()
		case tablepb.TableStatePreparing:
			// `preparing` is not stable state and would last a long time,
			// it's no need to return such a state, to make the coordinator become burdensome.
			done := t.executor.IsAddTableFinished(t.task.Span, t.task.IsPrepare)
			if !done {
				return nil, nil
			}
			state, changed = t.getAndUpdateTableState()
			log.Info("schedulerv3: add table finished",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.span.TableID), zap.Stringer("state", state))
		case tablepb.TableStateStopping,
			tablepb.TableStateStopped:
			log.Warn("schedulerv3: ignore add table",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.span.TableID))
			t.task = nil
			return newAddTableResponseMessage(t.getTableStatus()), nil
		default:
			log.Panic("schedulerv3: unknown table state",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Int64("tableID", t.span.TableID))
		}
	}

	return nil, nil
}

func (t *table) injectDispatchTableTask(task *dispatchTableTask) {
	if !t.span.Eq(&task.Span) {
		log.Panic("schedulerv3: tableID not match",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.Int64("tableID", t.span.TableID),
			zap.Stringer("task.TableID", &task.Span))
	}
	if t.task == nil {
		log.Info("schedulerv3: table found new task",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.Int64("tableID", t.span.TableID),
			zap.Any("task", task))
		t.task = task
		return
	}
	log.Debug("schedulerv3: table inject dispatch table task ignored,"+
		"since there is one not finished yet",
		zap.String("namespace", t.changefeedID.Namespace),
		zap.String("changefeed", t.changefeedID.ID),
		zap.Int64("tableID", t.span.TableID),
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
	tables   *spanz.Map[*table]
	executor internal.TableExecutor

	changefeedID model.ChangeFeedID
}

func newTableManager(
	changefeed model.ChangeFeedID, executor internal.TableExecutor,
) *tableManager {
	return &tableManager{
		tables:       spanz.NewMap[*table](),
		executor:     executor,
		changefeedID: changefeed,
	}
}

func (tm *tableManager) poll(ctx context.Context) ([]*schedulepb.Message, error) {
	result := make([]*schedulepb.Message, 0)
	var err error
	toBeDropped := []tablepb.Span{}
	tm.tables.Ascend(func(span tablepb.Span, table *table) bool {
		message, err1 := table.poll(ctx)
		if err != nil {
			err = errors.Trace(err1)
			return false
		}

		state, _ := table.getAndUpdateTableState()
		if state == tablepb.TableStateAbsent {
			toBeDropped = append(toBeDropped, span)
		}

		if message == nil {
			return true
		}
		result = append(result, message)
		return true
	})
	for _, span := range toBeDropped {
		tm.dropTable(span)
	}
	return result, err
}

func (tm *tableManager) getAllTables() *spanz.Map[*table] {
	return tm.tables
}

// addTable add the target table, and return it.
func (tm *tableManager) addTable(span tablepb.Span) *table {
	table, ok := tm.tables.Get(span)
	if !ok {
		table = newTable(tm.changefeedID, span, tm.executor)
		tm.tables.ReplaceOrInsert(span, table)
	}
	return table
}

func (tm *tableManager) getTable(span tablepb.Span) (*table, bool) {
	table, ok := tm.tables.Get(span)
	if ok {
		return table, true
	}
	return nil, false
}

func (tm *tableManager) dropTable(span tablepb.Span) {
	table, ok := tm.tables.Get(span)
	if !ok {
		log.Warn("schedulerv3: tableManager drop table not found",
			zap.String("namespace", tm.changefeedID.Namespace),
			zap.String("changefeed", tm.changefeedID.ID),
			zap.Stringer("span", &span))
		return
	}
	state, _ := table.getAndUpdateTableState()
	if state != tablepb.TableStateAbsent {
		log.Panic("schedulerv3: tableManager drop table undesired",
			zap.String("namespace", tm.changefeedID.Namespace),
			zap.String("changefeed", tm.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Stringer("state", table.state))
	}

	log.Debug("schedulerv3: tableManager drop table",
		zap.String("namespace", tm.changefeedID.Namespace),
		zap.String("changefeed", tm.changefeedID.ID),
		zap.Stringer("span", &span))
	tm.tables.Delete(span)
}

func (tm *tableManager) getTableStatus(span tablepb.Span) tablepb.TableStatus {
	table, ok := tm.getTable(span)
	if ok {
		return table.getTableStatus()
	}

	return tablepb.TableStatus{
		Span:  span,
		State: tablepb.TableStateAbsent,
	}
}
