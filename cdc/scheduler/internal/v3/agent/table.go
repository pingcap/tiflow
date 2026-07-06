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

// tableSpan is a state machine that manage the tableSpan's state,
// also tracking its progress by utilize the `TableExecutor`
type tableSpan struct {
	changefeedID model.ChangeFeedID
	span         tablepb.Span

	state    tablepb.TableState
	executor internal.TableExecutor

	task *dispatchTableTask
}

func newTableSpan(
	changefeed model.ChangeFeedID, span tablepb.Span, executor internal.TableExecutor,
) *tableSpan {
	return &tableSpan{
		changefeedID: changefeed,
		span:         span,
		state:        tablepb.TableStateAbsent, // use `absent` as the default state.
		executor:     executor,
		task:         nil,
	}
}

// getAndUpdateTableSpanState get the table span' state, return true if the table state changed
func (t *tableSpan) getAndUpdateTableSpanState() (tablepb.TableState, bool) {
	oldState := t.state

	meta := t.executor.GetTableSpanStatus(t.span, false)
	t.state = meta.State

	if oldState != t.state {
		log.Debug("schedulerv3: table state changed",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.Any("tableSpan", t.span),
			zap.Stringer("oldState", oldState),
			zap.Stringer("state", t.state))
		return t.state, true
	}
	return t.state, false
}

func (t *tableSpan) getTableSpanStatus(collectStat bool) tablepb.TableStatus {
	return t.executor.GetTableSpanStatus(t.span, collectStat)
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

func (t *tableSpan) handleRemoveTableTask() *schedulepb.Message {
	state, _ := t.getAndUpdateTableSpanState()
	changed := true
	for changed {
		switch state {
		case tablepb.TableStateAbsent:
			log.Warn("schedulerv3: remove table, but table is absent",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Any("tableSpan", t.span))
			t.task = nil
			return newRemoveTableResponseMessage(t.getTableSpanStatus(false))
		case tablepb.TableStateStopping, // stopping now is useless
			tablepb.TableStateStopped:
			// release table resource, and get the latest checkpoint
			// this will let the table span become `absent`
			checkpointTs, done := t.executor.IsRemoveTableSpanFinished(t.span)
			if !done {
				// actually, this should never be hit, since we know that table is stopped.
				status := t.getTableSpanStatus(false)
				status.State = tablepb.TableStateStopping
				return newRemoveTableResponseMessage(status)
			}
			t.task = nil
			status := t.getTableSpanStatus(false)
			status.State = tablepb.TableStateStopped
			status.Checkpoint.CheckpointTs = checkpointTs
			return newRemoveTableResponseMessage(status)
		case tablepb.TableStatePreparing,
			tablepb.TableStatePrepared,
			tablepb.TableStateReplicating:
			done := t.executor.RemoveTableSpan(t.task.Span)
			if !done {
				status := t.getTableSpanStatus(false)
				status.State = tablepb.TableStateStopping
				return newRemoveTableResponseMessage(status)
			}
			state, changed = t.getAndUpdateTableSpanState()
		default:
			log.Panic("schedulerv3: unknown table state",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Any("tableSpan", t.span), zap.Stringer("state", state))
		}
	}
	return nil
}

func (t *tableSpan) handleAddTableTask(ctx context.Context) (result *schedulepb.Message, err error) {
	state, _ := t.getAndUpdateTableSpanState()
	changed := true
	for changed {
		switch state {
		case tablepb.TableStateAbsent:
			done, err := t.executor.AddTableSpan(ctx, t.task.Span, t.task.Checkpoint, t.task.IsPrepare)
			if err != nil || !done {
				log.Warn("schedulerv3: agent add table failed",
					zap.String("namespace", t.changefeedID.Namespace),
					zap.String("changefeed", t.changefeedID.ID),
					zap.Any("tableSpan", t.span), zap.Any("task", t.task),
					zap.Error(err))
				status := t.getTableSpanStatus(false)
				return newAddTableResponseMessage(status), errors.Trace(err)
			}
			state, changed = t.getAndUpdateTableSpanState()
		case tablepb.TableStateReplicating:
			log.Info("schedulerv3: table is replicating",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Any("tableSpan", t.span), zap.Stringer("state", state))
			t.task = nil
			status := t.getTableSpanStatus(false)
			return newAddTableResponseMessage(status), nil
		case tablepb.TableStatePrepared:
			if t.task.IsPrepare {
				// `prepared` is a stable state, if the task was to prepare the table.
				log.Info("schedulerv3: table is prepared",
					zap.String("namespace", t.changefeedID.Namespace),
					zap.String("changefeed", t.changefeedID.ID),
					zap.Any("tableSpan", t.span), zap.Stringer("state", state))
				t.task = nil
				return newAddTableResponseMessage(t.getTableSpanStatus(false)), nil
			}

			if t.task.status == dispatchTableTaskReceived {
				done, err := t.executor.AddTableSpan(ctx, t.task.Span, t.task.Checkpoint, false)
				if err != nil || !done {
					log.Warn("schedulerv3: agent add table failed",
						zap.String("namespace", t.changefeedID.Namespace),
						zap.String("changefeed", t.changefeedID.ID),
						zap.Any("tableSpan", t.span), zap.Stringer("state", state),
						zap.Error(err))
					status := t.getTableSpanStatus(false)
					return newAddTableResponseMessage(status), errors.Trace(err)
				}
				t.task.status = dispatchTableTaskProcessed
			}

			done := t.executor.IsAddTableSpanFinished(t.task.Span, false)
			if !done {
				return newAddTableResponseMessage(t.getTableSpanStatus(false)), nil
			}
			state, changed = t.getAndUpdateTableSpanState()
		case tablepb.TableStatePreparing:
			// `preparing` is not stable state and would last a long time,
			// it's no need to return such a state, to make the coordinator become burdensome.
			done := t.executor.IsAddTableSpanFinished(t.task.Span, t.task.IsPrepare)
			if !done {
				return nil, nil
			}
			state, changed = t.getAndUpdateTableSpanState()
			log.Info("schedulerv3: add table finished",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Any("tableSpan", t.span), zap.Stringer("state", state))
		case tablepb.TableStateStopping,
			tablepb.TableStateStopped:
			log.Warn("schedulerv3: ignore add table",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Any("tableSpan", t.span))
			t.task = nil
			return newAddTableResponseMessage(t.getTableSpanStatus(false)), nil
		default:
			log.Panic("schedulerv3: unknown table state",
				zap.String("namespace", t.changefeedID.Namespace),
				zap.String("changefeed", t.changefeedID.ID),
				zap.Any("tableSpan", t.span))
		}
	}

	return nil, nil
}

func (t *tableSpan) injectDispatchTableTask(task *dispatchTableTask) {
	if !t.span.Eq(&task.Span) {
		log.Panic("schedulerv3: tableID not match",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.Any("tableSpan", t.span),
			zap.Stringer("task.TableID", &task.Span))
	}
	if t.task == nil {
		log.Info("schedulerv3: table found new task",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.Any("tableSpan", t.span),
			zap.Any("task", task))
		t.task = task
		return
	}
	log.Warn("schedulerv3: table inject dispatch table task ignored,"+
		"since there is one not finished yet",
		zap.String("namespace", t.changefeedID.Namespace),
		zap.String("changefeed", t.changefeedID.ID),
		zap.Any("tableSpan", t.span),
		zap.Any("nowTask", t.task),
		zap.Any("ignoredTask", task))
}

func (t *tableSpan) poll(ctx context.Context) (*schedulepb.Message, error) {
	if t.task == nil {
		return nil, nil
	}
	if t.task.IsRemove {
		return t.handleRemoveTableTask(), nil
	}
	return t.handleAddTableTask(ctx)
}

type tableSpanManager struct {
	tables   *spanz.BtreeMap[*tableSpan]
	executor internal.TableExecutor

	changefeedID model.ChangeFeedID
}

func newTableSpanManager(
	changefeed model.ChangeFeedID, executor internal.TableExecutor,
) *tableSpanManager {
	return &tableSpanManager{
		tables:       spanz.NewBtreeMap[*tableSpan](),
		executor:     executor,
		changefeedID: changefeed,
	}
}

func (tm *tableSpanManager) poll(ctx context.Context) ([]*schedulepb.Message, error) {
	result := make([]*schedulepb.Message, 0)
	var err error
	var toBeDropped []tablepb.Span
	tm.tables.Ascend(func(span tablepb.Span, table *tableSpan) bool {
		message, err1 := table.poll(ctx)
		if err != nil {
			err = errors.Trace(err1)
			return false
		}

		state, _ := table.getAndUpdateTableSpanState()
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
		tm.dropTableSpan(span)
	}
	return result, err
}

func (tm *tableSpanManager) getAllTableSpans() *spanz.BtreeMap[*tableSpan] {
	return tm.tables
}

// addTableSpan add the target table span, and return it.
func (tm *tableSpanManager) addTableSpan(span tablepb.Span) *tableSpan {
	table, ok := tm.tables.Get(span)
	if !ok {
		table = newTableSpan(tm.changefeedID, span, tm.executor)
		tm.tables.ReplaceOrInsert(span, table)
	}
	return table
}

func (tm *tableSpanManager) getTableSpan(span tablepb.Span) (*tableSpan, bool) {
	table, ok := tm.tables.Get(span)
	if ok {
		return table, true
	}
	return nil, false
}

func (tm *tableSpanManager) dropTableSpan(span tablepb.Span) {
	table, ok := tm.tables.Get(span)
	if !ok {
		log.Warn("schedulerv3: tableManager drop table not found",
			zap.String("namespace", tm.changefeedID.Namespace),
			zap.String("changefeed", tm.changefeedID.ID),
			zap.String("span", span.String()))
		return
	}
	state, _ := table.getAndUpdateTableSpanState()
	if state != tablepb.TableStateAbsent {
		log.Panic("schedulerv3: tableManager drop table undesired",
			zap.String("namespace", tm.changefeedID.Namespace),
			zap.String("changefeed", tm.changefeedID.ID),
			zap.String("span", span.String()),
			zap.Stringer("state", table.state))
	}

	log.Debug("schedulerv3: tableManager drop table",
		zap.String("namespace", tm.changefeedID.Namespace),
		zap.String("changefeed", tm.changefeedID.ID),
		zap.String("span", span.String()))
	tm.tables.Delete(span)
}

func (tm *tableSpanManager) getTableSpanStatus(
	span tablepb.Span, collectStat bool,
) tablepb.TableStatus {
	table, ok := tm.getTableSpan(span)
	if ok {
		return table.getTableSpanStatus(collectStat)
	}

	return tablepb.TableStatus{
		Span:  span,
		State: tablepb.TableStateAbsent,
	}
}
