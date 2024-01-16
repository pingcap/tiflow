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

package replication

import (
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	checkpointCannotProceed = internal.CheckpointCannotProceed

	defaultSlowTableHeapSize  = 4
	logSlowTablesLagThreshold = 30 * time.Second
	logSlowTablesInterval     = 1 * time.Minute
	logMissingTableInterval   = 30 * time.Second
)

// Callback is invoked when something is done.
type Callback func()

// BurstBalance for changefeed set up or unplanned TiCDC node failure.
// TiCDC needs to balance interrupted tables as soon as possible.
type BurstBalance struct {
	AddTables    []AddTable
	RemoveTables []RemoveTable
	MoveTables   []MoveTable
}

func (b BurstBalance) String() string {
	if len(b.AddTables) != 0 {
		return fmt.Sprintf("BurstBalance, add tables: %v", b.AddTables)
	}
	if len(b.RemoveTables) != 0 {
		return fmt.Sprintf("BurstBalance, remove tables: %v", b.RemoveTables)
	}
	if len(b.MoveTables) != 0 {
		return fmt.Sprintf("BurstBalance, move tables: %v", b.MoveTables)
	}
	return "BurstBalance, no tables"
}

// MoveTable is a schedule task for moving a table.
type MoveTable struct {
	Span        tablepb.Span
	DestCapture model.CaptureID
}

func (t MoveTable) String() string {
	return fmt.Sprintf("MoveTable, span: %s, dest: %s",
		t.Span.String(), t.DestCapture)
}

// AddTable is a schedule task for adding a table.
type AddTable struct {
	Span         tablepb.Span
	CaptureID    model.CaptureID
	CheckpointTs model.Ts
}

func (t AddTable) String() string {
	return fmt.Sprintf("AddTable, span: %s, capture: %s, checkpointTs: %d",
		t.Span.String(), t.CaptureID, t.CheckpointTs)
}

// RemoveTable is a schedule task for removing a table.
type RemoveTable struct {
	Span      tablepb.Span
	CaptureID model.CaptureID
}

func (t RemoveTable) String() string {
	return fmt.Sprintf("RemoveTable, span: %s, capture: %s",
		t.Span.String(), t.CaptureID)
}

// ScheduleTask is a schedule task that wraps add/move/remove table tasks.
type ScheduleTask struct { //nolint:revive
	MoveTable    *MoveTable
	AddTable     *AddTable
	RemoveTable  *RemoveTable
	BurstBalance *BurstBalance

	Accept Callback
}

// Name returns the name of a schedule task.
func (s *ScheduleTask) Name() string {
	if s.MoveTable != nil {
		return "moveTable"
	} else if s.AddTable != nil {
		return "addTable"
	} else if s.RemoveTable != nil {
		return "removeTable"
	} else if s.BurstBalance != nil {
		return "burstBalance"
	}
	return "unknown"
}

func (s *ScheduleTask) String() string {
	if s.MoveTable != nil {
		return s.MoveTable.String()
	}
	if s.AddTable != nil {
		return s.AddTable.String()
	}
	if s.RemoveTable != nil {
		return s.RemoveTable.String()
	}
	if s.BurstBalance != nil {
		return s.BurstBalance.String()
	}
	return ""
}

// Manager manages replications and running scheduling tasks.
type Manager struct { //nolint:revive
	spans *spanz.BtreeMap[*ReplicationSet]

	runningTasks       *spanz.BtreeMap[*ScheduleTask]
	maxTaskConcurrency int

	changefeedID           model.ChangeFeedID
	slowestPuller          tablepb.Span
	slowestSink            tablepb.Span
	acceptAddTableTask     int
	acceptRemoveTableTask  int
	acceptMoveTableTask    int
	acceptBurstBalanceTask int

	slowTableHeap         SetHeap
	lastLogSlowTablesTime time.Time
	lastMissTableID       tablepb.TableID
	lastLogMissTime       time.Time
}

// NewReplicationManager returns a new replication manager.
func NewReplicationManager(
	maxTaskConcurrency int, changefeedID model.ChangeFeedID,
) *Manager {
	// degreeReadHeavy is a degree optimized for read heavy map, many Ascend.
	// There may be a large number of tables.
	const degreeReadHeavy = 256
	return &Manager{
		spans:              spanz.NewBtreeMapWithDegree[*ReplicationSet](degreeReadHeavy),
		runningTasks:       spanz.NewBtreeMap[*ScheduleTask](),
		maxTaskConcurrency: maxTaskConcurrency,
		changefeedID:       changefeedID,
	}
}

// HandleCaptureChanges handles capture changes.
func (r *Manager) HandleCaptureChanges(
	init map[model.CaptureID][]tablepb.TableStatus,
	removed map[model.CaptureID][]tablepb.TableStatus,
	checkpointTs model.Ts,
) ([]*schedulepb.Message, error) {
	if init != nil {
		if r.spans.Len() != 0 {
			log.Panic("schedulerv3: init again",
				zap.String("namespace", r.changefeedID.Namespace),
				zap.String("changefeed", r.changefeedID.ID),
				zap.Any("init", init), zap.Any("tablesCount", r.spans.Len()))
		}
		spanStatusMap := spanz.NewBtreeMap[map[model.CaptureID]*tablepb.TableStatus]()
		for captureID, spans := range init {
			for i := range spans {
				table := spans[i]
				if _, ok := spanStatusMap.Get(table.Span); !ok {
					spanStatusMap.ReplaceOrInsert(
						table.Span, map[model.CaptureID]*tablepb.TableStatus{})
				}
				spanStatusMap.GetV(table.Span)[captureID] = &table
			}
		}
		var err error
		spanStatusMap.Ascend(func(span tablepb.Span, status map[string]*tablepb.TableStatus) bool {
			table, err1 := NewReplicationSet(span, checkpointTs, status, r.changefeedID)
			if err1 != nil {
				err = errors.Trace(err1)
				return false
			}
			r.spans.ReplaceOrInsert(table.Span, table)
			return true
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	sentMsgs := make([]*schedulepb.Message, 0)
	if removed != nil {
		var err error
		r.spans.Ascend(func(span tablepb.Span, table *ReplicationSet) bool {
			for captureID := range removed {
				msgs, affected, err1 := table.handleCaptureShutdown(captureID)
				if err != nil {
					err = errors.Trace(err1)
					return false
				}
				sentMsgs = append(sentMsgs, msgs...)
				if affected {
					// Cleanup its running task.
					r.runningTasks.Delete(table.Span)
				}
			}
			return true
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return sentMsgs, nil
}

// HandleMessage handles messages sent by other captures.
func (r *Manager) HandleMessage(
	msgs []*schedulepb.Message,
) ([]*schedulepb.Message, error) {
	sentMsgs := make([]*schedulepb.Message, 0, len(msgs))
	for i := range msgs {
		msg := msgs[i]
		switch msg.MsgType {
		case schedulepb.MsgDispatchTableResponse:
			msgs, err := r.handleMessageDispatchTableResponse(msg.From, msg.DispatchTableResponse)
			if err != nil {
				return nil, errors.Trace(err)
			}
			sentMsgs = append(sentMsgs, msgs...)
		case schedulepb.MsgHeartbeatResponse:
			msgs, err := r.handleMessageHeartbeatResponse(msg.From, msg.HeartbeatResponse)
			if err != nil {
				return nil, errors.Trace(err)
			}
			sentMsgs = append(sentMsgs, msgs...)
		default:
			log.Warn("schedulerv3: ignore message",
				zap.String("namespace", r.changefeedID.Namespace),
				zap.String("changefeed", r.changefeedID.ID),
				zap.Stringer("type", msg.MsgType), zap.Any("message", msg))
		}
	}
	return sentMsgs, nil
}

func (r *Manager) handleMessageHeartbeatResponse(
	from model.CaptureID, msg *schedulepb.HeartbeatResponse,
) ([]*schedulepb.Message, error) {
	sentMsgs := make([]*schedulepb.Message, 0)
	for _, status := range msg.Tables {
		table, ok := r.spans.Get(status.Span)
		if !ok {
			log.Info("schedulerv3: ignore table status no table found",
				zap.String("namespace", r.changefeedID.Namespace),
				zap.String("changefeed", r.changefeedID.ID),
				zap.Any("message", status))
			continue
		}
		msgs, err := table.handleTableStatus(from, &status)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if table.hasRemoved() {
			log.Info("schedulerv3: table has removed",
				zap.String("namespace", r.changefeedID.Namespace),
				zap.String("changefeed", r.changefeedID.ID),
				zap.Int64("tableID", status.Span.TableID))
			r.spans.Delete(status.Span)
		}
		sentMsgs = append(sentMsgs, msgs...)
	}
	return sentMsgs, nil
}

func (r *Manager) handleMessageDispatchTableResponse(
	from model.CaptureID, msg *schedulepb.DispatchTableResponse,
) ([]*schedulepb.Message, error) {
	var status *tablepb.TableStatus
	switch resp := msg.Response.(type) {
	case *schedulepb.DispatchTableResponse_AddTable:
		status = resp.AddTable.Status
	case *schedulepb.DispatchTableResponse_RemoveTable:
		status = resp.RemoveTable.Status
	default:
		log.Warn("schedulerv3: ignore unknown dispatch table response",
			zap.String("namespace", r.changefeedID.Namespace),
			zap.String("changefeed", r.changefeedID.ID),
			zap.Any("message", msg))
		return nil, nil
	}

	table, ok := r.spans.Get(status.Span)
	if !ok {
		log.Info("schedulerv3: ignore table status no table found",
			zap.String("namespace", r.changefeedID.Namespace),
			zap.String("changefeed", r.changefeedID.ID),
			zap.Any("message", status))
		return nil, nil
	}
	msgs, err := table.handleTableStatus(from, status)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if table.hasRemoved() {
		log.Info("schedulerv3: table has removed",
			zap.String("namespace", r.changefeedID.Namespace),
			zap.String("changefeed", r.changefeedID.ID),
			zap.Int64("tableID", status.Span.TableID))
		r.spans.Delete(status.Span)
	}
	return msgs, nil
}

// HandleTasks handles schedule tasks.
func (r *Manager) HandleTasks(
	tasks []*ScheduleTask,
) ([]*schedulepb.Message, error) {
	// Check if a running task is finished.
	var toBeDeleted []tablepb.Span
	r.runningTasks.Ascend(func(span tablepb.Span, task *ScheduleTask) bool {
		if table, ok := r.spans.Get(span); ok {
			// If table is back to Replicating or Removed,
			// the running task is finished.
			if table.State == ReplicationSetStateReplicating || table.hasRemoved() {
				toBeDeleted = append(toBeDeleted, span)
			}
		} else {
			// No table found, remove the task
			toBeDeleted = append(toBeDeleted, span)
		}
		return true
	})
	for _, span := range toBeDeleted {
		r.runningTasks.Delete(span)
	}

	sentMsgs := make([]*schedulepb.Message, 0)
	for _, task := range tasks {
		// Burst balance does not affect by maxTaskConcurrency.
		if task.BurstBalance != nil {
			msgs, err := r.handleBurstBalanceTasks(task.BurstBalance)
			if err != nil {
				return nil, errors.Trace(err)
			}
			sentMsgs = append(sentMsgs, msgs...)
			if task.Accept != nil {
				task.Accept()
			}
			continue
		}

		// Check if accepting one more task exceeds maxTaskConcurrency.
		if r.runningTasks.Len() == r.maxTaskConcurrency {
			log.Debug("schedulerv3: too many running task",
				zap.String("namespace", r.changefeedID.Namespace),
				zap.String("changefeed", r.changefeedID.ID))
			// Does not use break, in case there is burst balance task
			// in the remaining tasks.
			continue
		}

		var span tablepb.Span
		if task.AddTable != nil {
			span = task.AddTable.Span
		} else if task.RemoveTable != nil {
			span = task.RemoveTable.Span
		} else if task.MoveTable != nil {
			span = task.MoveTable.Span
		}

		// Skip task if the table is already running a task,
		// or the table has removed.
		if _, ok := r.runningTasks.Get(span); ok {
			log.Info("schedulerv3: ignore task, already exists",
				zap.String("namespace", r.changefeedID.Namespace),
				zap.String("changefeed", r.changefeedID.ID),
				zap.Any("task", task))
			continue
		}
		if _, ok := r.spans.Get(span); !ok && task.AddTable == nil {
			log.Info("schedulerv3: ignore task, table not found",
				zap.String("namespace", r.changefeedID.Namespace),
				zap.String("changefeed", r.changefeedID.ID),
				zap.Any("task", task))
			continue
		}

		var msgs []*schedulepb.Message
		var err error
		if task.AddTable != nil {
			msgs, err = r.handleAddTableTask(task.AddTable)
		} else if task.RemoveTable != nil {
			msgs, err = r.handleRemoveTableTask(task.RemoveTable)
		} else if task.MoveTable != nil {
			msgs, err = r.handleMoveTableTask(task.MoveTable)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		r.runningTasks.ReplaceOrInsert(span, task)
		if task.Accept != nil {
			task.Accept()
		}
	}
	return sentMsgs, nil
}

func (r *Manager) handleAddTableTask(
	task *AddTable,
) ([]*schedulepb.Message, error) {
	r.acceptAddTableTask++
	var err error
	table, ok := r.spans.Get(task.Span)
	if !ok {
		table, err = NewReplicationSet(task.Span, task.CheckpointTs, nil, r.changefeedID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		r.spans.ReplaceOrInsert(task.Span, table)
	}
	return table.handleAddTable(task.CaptureID)
}

func (r *Manager) handleRemoveTableTask(
	task *RemoveTable,
) ([]*schedulepb.Message, error) {
	r.acceptRemoveTableTask++
	table, _ := r.spans.Get(task.Span)
	if table.hasRemoved() {
		log.Info("schedulerv3: table has removed",
			zap.String("namespace", r.changefeedID.Namespace),
			zap.String("changefeed", r.changefeedID.ID),
			zap.Int64("tableID", task.Span.TableID))
		r.spans.Delete(task.Span)
		return nil, nil
	}
	return table.handleRemoveTable()
}

func (r *Manager) handleMoveTableTask(
	task *MoveTable,
) ([]*schedulepb.Message, error) {
	r.acceptMoveTableTask++
	table, _ := r.spans.Get(task.Span)
	return table.handleMoveTable(task.DestCapture)
}

func (r *Manager) handleBurstBalanceTasks(
	task *BurstBalance,
) ([]*schedulepb.Message, error) {
	r.acceptBurstBalanceTask++
	perCapture := make(map[model.CaptureID]int)
	for _, task := range task.AddTables {
		perCapture[task.CaptureID]++
	}
	for _, task := range task.RemoveTables {
		perCapture[task.CaptureID]++
	}
	fields := make([]zap.Field, 0)
	for captureID, count := range perCapture {
		fields = append(fields, zap.Int(captureID, count))
	}
	fields = append(fields, zap.Int("addTable", len(task.AddTables)))
	fields = append(fields, zap.Int("removeTable", len(task.RemoveTables)))
	fields = append(fields, zap.Int("moveTable", len(task.MoveTables)))
	fields = append(fields, zap.String("namespace", r.changefeedID.Namespace))
	fields = append(fields, zap.String("changefeed", r.changefeedID.ID))
	log.Info("schedulerv3: handle burst balance task", fields...)

	sentMsgs := make([]*schedulepb.Message, 0, len(task.AddTables))
	for i := range task.AddTables {
		addTable := task.AddTables[i]
		if _, ok := r.runningTasks.Get(addTable.Span); ok {
			// Skip add table if the table is already running a task.
			continue
		}
		msgs, err := r.handleAddTableTask(&addTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		// Just for place holding.
		r.runningTasks.ReplaceOrInsert(addTable.Span, &ScheduleTask{})
	}
	for i := range task.RemoveTables {
		removeTable := task.RemoveTables[i]
		if _, ok := r.runningTasks.Get(removeTable.Span); ok {
			// Skip add table if the table is already running a task.
			continue
		}
		msgs, err := r.handleRemoveTableTask(&removeTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		// Just for place holding.
		r.runningTasks.ReplaceOrInsert(removeTable.Span, &ScheduleTask{})
	}
	for i := range task.MoveTables {
		moveTable := task.MoveTables[i]
		if _, ok := r.runningTasks.Get(moveTable.Span); ok {
			// Skip add table if the table is already running a task.
			continue
		}
		msgs, err := r.handleMoveTableTask(&moveTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		// Just for place holding.
		r.runningTasks.ReplaceOrInsert(moveTable.Span, &ScheduleTask{})
	}
	return sentMsgs, nil
}

// ReplicationSets return all tracking replication set
// Caller must not modify the returned map.
func (r *Manager) ReplicationSets() *spanz.BtreeMap[*ReplicationSet] {
	return r.spans
}

// RunningTasks return running tasks.
// Caller must not modify the returned map.
func (r *Manager) RunningTasks() *spanz.BtreeMap[*ScheduleTask] {
	return r.runningTasks
}

// AdvanceCheckpoint tries to advance checkpoint and returns current checkpoint.
func (r *Manager) AdvanceCheckpoint(
	currentTables *TableRanges,
	currentPDTime time.Time,
	barrier *schedulepb.BarrierWithMinTs,
	redoMetaManager redo.MetaManager,
) (watermark schedulepb.Watermark) {
	var redoFlushedResolvedTs model.Ts
	limitBarrierWithRedo := func(watermark *schedulepb.Watermark) {
		flushedMeta := redoMetaManager.GetFlushedMeta()
		redoFlushedResolvedTs = flushedMeta.ResolvedTs
		log.Debug("owner gets flushed redo meta",
			zap.String("namespace", r.changefeedID.Namespace),
			zap.String("changefeed", r.changefeedID.ID),
			zap.Uint64("flushedCheckpointTs", flushedMeta.CheckpointTs),
			zap.Uint64("flushedResolvedTs", flushedMeta.ResolvedTs))
		if flushedMeta.ResolvedTs < watermark.ResolvedTs {
			watermark.ResolvedTs = flushedMeta.ResolvedTs
		}

		if watermark.CheckpointTs > watermark.ResolvedTs {
			watermark.CheckpointTs = watermark.ResolvedTs
		}

		if barrier.GlobalBarrierTs > watermark.ResolvedTs {
			barrier.GlobalBarrierTs = watermark.ResolvedTs
		}
	}
	defer func() {
		if redoFlushedResolvedTs != 0 && barrier.GlobalBarrierTs > redoFlushedResolvedTs {
			log.Panic("barrierTs should never greater than redo flushed",
				zap.String("namespace", r.changefeedID.Namespace),
				zap.String("changefeed", r.changefeedID.ID),
				zap.Uint64("barrierTs", barrier.GlobalBarrierTs),
				zap.Uint64("redoFlushedResolvedTs", redoFlushedResolvedTs))
		}
	}()

	r.slowestPuller = tablepb.Span{}
	r.slowestSink = tablepb.Span{}

	watermark = schedulepb.Watermark{
		CheckpointTs:     math.MaxUint64,
		ResolvedTs:       math.MaxUint64,
		LastSyncedTs:     0,
		PullerResolvedTs: math.MaxUint64,
	}

	cannotProceed := false
	currentTables.Iter(func(tableID model.TableID, tableStart, tableEnd tablepb.Span) bool {
		tableSpanFound, tableHasHole := false, false
		tableSpanStartFound, tableSpanEndFound := false, false
		lastSpan := tablepb.Span{}
		r.spans.AscendRange(tableStart, tableEnd,
			func(span tablepb.Span, table *ReplicationSet) bool {
				if lastSpan.TableID != 0 && !bytes.Equal(lastSpan.EndKey, span.StartKey) {
					log.Warn("schedulerv3: span hole detected, skip advance checkpoint",
						zap.String("namespace", r.changefeedID.Namespace),
						zap.String("changefeed", r.changefeedID.ID),
						zap.String("lastSpan", lastSpan.String()),
						zap.String("span", span.String()))
					tableHasHole = true
					return false
				}
				lastSpan = span
				tableSpanFound = true
				if bytes.Equal(span.StartKey, tableStart.StartKey) {
					tableSpanStartFound = true
				}
				if bytes.Equal(span.EndKey, tableEnd.StartKey) {
					tableSpanEndFound = true
				}

				// Find the minimum checkpoint ts and resolved ts.
				if watermark.CheckpointTs > table.Checkpoint.CheckpointTs {
					watermark.CheckpointTs = table.Checkpoint.CheckpointTs
					r.slowestSink = span
				}
				if watermark.ResolvedTs > table.Checkpoint.ResolvedTs {
					watermark.ResolvedTs = table.Checkpoint.ResolvedTs
				}

				// Find the max lastSyncedTs of all tables.
				if watermark.LastSyncedTs < table.Checkpoint.LastSyncedTs {
					watermark.LastSyncedTs = table.Checkpoint.LastSyncedTs
				}
				// Find the minimum puller resolved ts.
				if pullerCkpt, ok := table.Stats.StageCheckpoints["puller-egress"]; ok {
					if watermark.PullerResolvedTs > pullerCkpt.ResolvedTs {
						watermark.PullerResolvedTs = pullerCkpt.ResolvedTs
						r.slowestPuller = span
					}
				}

				return true
			})
		if !tableSpanFound || !tableSpanStartFound || !tableSpanEndFound || tableHasHole {
			// Can not advance checkpoint there is a span missing.
			now := time.Now()
			if r.lastMissTableID != 0 && r.lastMissTableID == tableID &&
				now.Sub(r.lastLogMissTime) > logMissingTableInterval {
				log.Warn("schedulerv3: cannot advance checkpoint since missing span",
					zap.String("namespace", r.changefeedID.Namespace),
					zap.String("changefeed", r.changefeedID.ID),
					zap.Bool("tableSpanFound", tableSpanFound),
					zap.Bool("tableSpanStartFound", tableSpanStartFound),
					zap.Bool("tableSpanEndFound", tableSpanEndFound),
					zap.Bool("tableHasHole", tableHasHole),
					zap.Int64("tableID", tableID))
				r.lastLogMissTime = now
			}
			r.lastMissTableID = tableID
			cannotProceed = true
			return false
		}
		r.lastMissTableID = 0
		return true
	})
	if cannotProceed {
		if redoMetaManager.Enabled() {
			// If redo is enabled, GlobalBarrierTs should be limited by redo flushed meta.
			watermark.ResolvedTs = barrier.RedoBarrierTs
			watermark.LastSyncedTs = checkpointCannotProceed
			watermark.PullerResolvedTs = checkpointCannotProceed
			limitBarrierWithRedo(&watermark)
		}
		return schedulepb.Watermark{
			CheckpointTs:     checkpointCannotProceed,
			ResolvedTs:       checkpointCannotProceed,
			LastSyncedTs:     checkpointCannotProceed,
			PullerResolvedTs: checkpointCannotProceed,
		}
	}

	// If currentTables is empty, we should advance newResolvedTs to global barrier ts and
	// advance newCheckpointTs to min table barrier ts.
	if watermark.ResolvedTs == math.MaxUint64 || watermark.CheckpointTs == math.MaxUint64 {
		if watermark.CheckpointTs != watermark.ResolvedTs || currentTables.Len() != 0 {
			log.Panic("schedulerv3: newCheckpointTs and newResolvedTs should be both maxUint64 "+
				"if currentTables is empty",
				zap.Uint64("newCheckpointTs", watermark.CheckpointTs),
				zap.Uint64("newResolvedTs", watermark.ResolvedTs),
				zap.Any("currentTables", currentTables))
		}
		watermark.ResolvedTs = barrier.GlobalBarrierTs
		watermark.CheckpointTs = barrier.MinTableBarrierTs
	}

	if watermark.CheckpointTs > barrier.MinTableBarrierTs {
		watermark.CheckpointTs = barrier.MinTableBarrierTs
		// TODO: add panic after we fix the bug that newCheckpointTs > minTableBarrierTs.
		// log.Panic("schedulerv3: newCheckpointTs should not be larger than minTableBarrierTs",
		// 	zap.Uint64("newCheckpointTs", watermark.CheckpointTs),
		// 	zap.Uint64("newResolvedTs", watermark.ResolvedTs),
		// 	zap.Any("currentTables", currentTables.currentTables),
		// 	zap.Any("barrier", barrier.Barrier),
		// 	zap.Any("minTableBarrierTs", barrier.MinTableBarrierTs))
	}

	// If changefeed's checkpoint lag is larger than 30s,
	// log the 4 slowlest table infos every minute, which can
	// help us find the problematic tables.
	checkpointLag := currentPDTime.Sub(oracle.GetTimeFromTS(watermark.CheckpointTs))
	if checkpointLag > logSlowTablesLagThreshold &&
		time.Since(r.lastLogSlowTablesTime) > logSlowTablesInterval {
		r.logSlowTableInfo(currentPDTime)
		r.lastLogSlowTablesTime = time.Now()
	}

	if redoMetaManager.Enabled() {
		if watermark.ResolvedTs > barrier.RedoBarrierTs {
			watermark.ResolvedTs = barrier.RedoBarrierTs
		}
		redoMetaManager.UpdateMeta(watermark.CheckpointTs, watermark.ResolvedTs)
		log.Debug("owner updates redo meta",
			zap.String("namespace", r.changefeedID.Namespace),
			zap.String("changefeed", r.changefeedID.ID),
			zap.Uint64("newCheckpointTs", watermark.CheckpointTs),
			zap.Uint64("newResolvedTs", watermark.ResolvedTs))
		limitBarrierWithRedo(&watermark)
	}

	return watermark
}

func (r *Manager) logSlowTableInfo(currentPDTime time.Time) {
	// find the slow tables
	r.spans.Ascend(func(span tablepb.Span, table *ReplicationSet) bool {
		lag := currentPDTime.Sub(oracle.GetTimeFromTS(table.Checkpoint.CheckpointTs))
		if lag > logSlowTablesLagThreshold {
			heap.Push(&r.slowTableHeap, table)
			if r.slowTableHeap.Len() > defaultSlowTableHeapSize {
				heap.Pop(&r.slowTableHeap)
			}
		}
		return true
	})

	num := r.slowTableHeap.Len()
	for i := 0; i < num; i++ {
		table := heap.Pop(&r.slowTableHeap).(*ReplicationSet)
		log.Info("schedulerv3: slow table",
			zap.String("namespace", r.changefeedID.Namespace),
			zap.String("changefeed", r.changefeedID.ID),
			zap.Int64("tableID", table.Span.TableID),
			zap.String("tableStatus", table.State.String()),
			zap.Uint64("checkpointTs", table.Checkpoint.CheckpointTs),
			zap.Uint64("resolvedTs", table.Checkpoint.ResolvedTs),
			zap.Duration("checkpointLag", currentPDTime.
				Sub(oracle.GetTimeFromTS(table.Checkpoint.CheckpointTs))))
	}
}

// CollectMetrics collects metrics.
func (r *Manager) CollectMetrics() {
	cf := r.changefeedID
	tableGauge.
		WithLabelValues(cf.Namespace, cf.ID).Set(float64(r.spans.Len()))
	if table, ok := r.spans.Get(r.slowestSink); ok {
		slowestTableIDGauge.
			WithLabelValues(cf.Namespace, cf.ID).Set(float64(r.slowestSink.TableID))
		slowestTableStateGauge.
			WithLabelValues(cf.Namespace, cf.ID).Set(float64(table.State))
		phyCkpTs := oracle.ExtractPhysical(table.Checkpoint.CheckpointTs)
		slowestTableCheckpointTsGauge.
			WithLabelValues(cf.Namespace, cf.ID).Set(float64(phyCkpTs))
		phyRTs := oracle.ExtractPhysical(table.Checkpoint.ResolvedTs)
		slowestTableResolvedTsGauge.
			WithLabelValues(cf.Namespace, cf.ID).Set(float64(phyRTs))

		// Slow table latency metrics.
		phyCurrentTs := oracle.ExtractPhysical(table.Stats.CurrentTs)
		for stage, checkpoint := range table.Stats.StageCheckpoints {
			// Checkpoint ts
			phyCkpTs := oracle.ExtractPhysical(checkpoint.CheckpointTs)
			slowestTableStageCheckpointTsGaugeVec.
				WithLabelValues(cf.Namespace, cf.ID, stage).Set(float64(phyCkpTs))
			checkpointLag := float64(phyCurrentTs-phyCkpTs) / 1e3
			slowestTableStageCheckpointTsLagGaugeVec.
				WithLabelValues(cf.Namespace, cf.ID, stage).Set(checkpointLag)
			slowestTableStageCheckpointTsLagHistogramVec.
				WithLabelValues(cf.Namespace, cf.ID, stage).Observe(checkpointLag)
			// Resolved ts
			phyRTs := oracle.ExtractPhysical(checkpoint.ResolvedTs)
			slowestTableStageResolvedTsGaugeVec.
				WithLabelValues(cf.Namespace, cf.ID, stage).Set(float64(phyRTs))
			resolvedTsLag := float64(phyCurrentTs-phyRTs) / 1e3
			slowestTableStageResolvedTsLagGaugeVec.
				WithLabelValues(cf.Namespace, cf.ID, stage).Set(resolvedTsLag)
			slowestTableStageResolvedTsLagHistogramVec.
				WithLabelValues(cf.Namespace, cf.ID, stage).Observe(resolvedTsLag)
		}
		// Barrier ts
		stage := "barrier"
		phyBTs := oracle.ExtractPhysical(table.Stats.BarrierTs)
		slowestTableStageResolvedTsGaugeVec.
			WithLabelValues(cf.Namespace, cf.ID, stage).Set(float64(phyBTs))
		barrierTsLag := float64(phyCurrentTs-phyBTs) / 1e3
		slowestTableStageResolvedTsLagGaugeVec.
			WithLabelValues(cf.Namespace, cf.ID, stage).Set(barrierTsLag)
		slowestTableStageResolvedTsLagHistogramVec.
			WithLabelValues(cf.Namespace, cf.ID, stage).Observe(barrierTsLag)
		// Region count
		slowestTableRegionGaugeVec.
			WithLabelValues(cf.Namespace, cf.ID).Set(float64(table.Stats.RegionCount))
	}
	metricAcceptScheduleTask := acceptScheduleTaskCounter.MustCurryWith(map[string]string{
		"namespace": cf.Namespace, "changefeed": cf.ID,
	})
	metricAcceptScheduleTask.WithLabelValues("addTable").Add(float64(r.acceptAddTableTask))
	r.acceptAddTableTask = 0
	metricAcceptScheduleTask.WithLabelValues("removeTable").Add(float64(r.acceptRemoveTableTask))
	r.acceptRemoveTableTask = 0
	metricAcceptScheduleTask.WithLabelValues("moveTable").Add(float64(r.acceptMoveTableTask))
	r.acceptMoveTableTask = 0
	metricAcceptScheduleTask.WithLabelValues("burstBalance").Add(float64(r.acceptBurstBalanceTask))
	r.acceptBurstBalanceTask = 0
	runningScheduleTaskGauge.
		WithLabelValues(cf.Namespace, cf.ID).Set(float64(r.runningTasks.Len()))
	var stateCounters [6]int
	r.spans.Ascend(func(span tablepb.Span, table *ReplicationSet) bool {
		switch table.State {
		case ReplicationSetStateUnknown:
			stateCounters[ReplicationSetStateUnknown]++
		case ReplicationSetStateAbsent:
			stateCounters[ReplicationSetStateAbsent]++
		case ReplicationSetStatePrepare:
			stateCounters[ReplicationSetStatePrepare]++
		case ReplicationSetStateCommit:
			stateCounters[ReplicationSetStateCommit]++
		case ReplicationSetStateReplicating:
			stateCounters[ReplicationSetStateReplicating]++
		case ReplicationSetStateRemoving:
			stateCounters[ReplicationSetStateRemoving]++
		}
		return true
	})
	for s, counter := range stateCounters {
		tableStateGauge.
			WithLabelValues(cf.Namespace, cf.ID, ReplicationSetState(s).String()).
			Set(float64(counter))
	}

	if table, ok := r.spans.Get(r.slowestSink); ok {
		if pullerCkpt, ok := table.Stats.StageCheckpoints["puller-egress"]; ok {
			phyCkptTs := oracle.ExtractPhysical(pullerCkpt.ResolvedTs)
			slowestTablePullerResolvedTs.WithLabelValues(cf.Namespace, cf.ID).Set(float64(phyCkptTs))

			phyCurrentTs := oracle.ExtractPhysical(table.Stats.CurrentTs)
			lag := float64(phyCurrentTs-phyCkptTs) / 1e3
			slowestTablePullerResolvedTsLag.WithLabelValues(cf.Namespace, cf.ID).Set(lag)
		}
	}
}

// CleanMetrics cleans metrics.
func (r *Manager) CleanMetrics() {
	cf := r.changefeedID
	tableGauge.DeleteLabelValues(cf.Namespace, cf.ID)
	slowestTableIDGauge.DeleteLabelValues(cf.Namespace, cf.ID)
	slowestTableStateGauge.DeleteLabelValues(cf.Namespace, cf.ID)
	slowestTableCheckpointTsGauge.DeleteLabelValues(cf.Namespace, cf.ID)
	slowestTableResolvedTsGauge.DeleteLabelValues(cf.Namespace, cf.ID)
	runningScheduleTaskGauge.DeleteLabelValues(cf.Namespace, cf.ID)
	metricAcceptScheduleTask := acceptScheduleTaskCounter.MustCurryWith(map[string]string{
		"namespace": cf.Namespace, "changefeed": cf.ID,
	})
	metricAcceptScheduleTask.DeleteLabelValues("addTable")
	metricAcceptScheduleTask.DeleteLabelValues("removeTable")
	metricAcceptScheduleTask.DeleteLabelValues("moveTable")
	metricAcceptScheduleTask.DeleteLabelValues("burstBalance")
	var stateCounters [6]int
	for s := range stateCounters {
		tableStateGauge.
			DeleteLabelValues(cf.Namespace, cf.ID, ReplicationSetState(s).String())
	}
	slowestTableStageCheckpointTsGaugeVec.Reset()
	slowestTableStageResolvedTsGaugeVec.Reset()
	slowestTableStageCheckpointTsLagGaugeVec.Reset()
	slowestTableStageResolvedTsLagGaugeVec.Reset()
	slowestTableStageCheckpointTsLagHistogramVec.Reset()
	slowestTableStageResolvedTsLagHistogramVec.Reset()
	slowestTableRegionGaugeVec.Reset()
}

// SetReplicationSetForTests is only used in tests.
func (r *Manager) SetReplicationSetForTests(rs *ReplicationSet) {
	r.spans.ReplaceOrInsert(rs.Span, rs)
}

// GetReplicationSetForTests is only used in tests.
func (r *Manager) GetReplicationSetForTests() *spanz.BtreeMap[*ReplicationSet] {
	return r.spans
}

// TableRanges wraps current tables and their ranges.
type TableRanges struct {
	currentTables []model.TableID
	cache         struct {
		tableRange map[model.TableID]struct{ start, end tablepb.Span }
		lastGC     time.Time
	}
}

// UpdateTables current tables.
func (t *TableRanges) UpdateTables(currentTables []model.TableID) {
	if time.Since(t.cache.lastGC) > 10*time.Minute {
		t.cache.tableRange = make(map[model.TableID]struct {
			start tablepb.Span
			end   tablepb.Span
		})
		t.cache.lastGC = time.Now()
	}
	t.currentTables = currentTables
}

// Len returns the length of current tables.
func (t *TableRanges) Len() int {
	return len(t.currentTables)
}

// Iter iterate current tables.
func (t *TableRanges) Iter(fn func(tableID model.TableID, tableStart, tableEnd tablepb.Span) bool) {
	for _, tableID := range t.currentTables {
		tableRange, ok := t.cache.tableRange[tableID]
		if !ok {
			tableRange.start, tableRange.end = spanz.TableIDToComparableRange(tableID)
			t.cache.tableRange[tableID] = tableRange
		}
		if !fn(tableID, tableRange.start, tableRange.end) {
			break
		}
	}
}
