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
	TableID     model.TableID
	DestCapture model.CaptureID
}

func (t MoveTable) String() string {
	return fmt.Sprintf("MoveTable, tableID: %d, dest: %s",
		t.TableID, t.DestCapture)
}

// AddTable is a schedule task for adding a table.
type AddTable struct {
	TableID      model.TableID
	CaptureID    model.CaptureID
	CheckpointTs model.Ts
}

func (t AddTable) String() string {
	return fmt.Sprintf("AddTable, tableID: %d, capture: %s, checkpointTs: %d",
		t.TableID, t.CaptureID, t.CheckpointTs)
}

// RemoveTable is a schedule task for removing a table.
type RemoveTable struct {
	TableID   model.TableID
	CaptureID model.CaptureID
}

func (t RemoveTable) String() string {
	return fmt.Sprintf("RemoveTable, tableID: %d, capture: %s",
		t.TableID, t.CaptureID)
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
	tables map[model.TableID]*ReplicationSet

	runningTasks       map[model.TableID]*ScheduleTask
	maxTaskConcurrency int

	changefeedID           model.ChangeFeedID
	slowestPuller          model.TableID
	slowestSink            model.TableID
	slowTableHeap          SetHeap
	acceptAddTableTask     int
	acceptRemoveTableTask  int
	acceptMoveTableTask    int
	acceptBurstBalanceTask int

	lastLogSlowTablesTime time.Time
	lastLogMissTime       time.Time
}

// NewReplicationManager returns a new replication manager.
func NewReplicationManager(
	maxTaskConcurrency int, changefeedID model.ChangeFeedID,
) *Manager {
	slowTableHeap := make(SetHeap, 0, defaultSlowTableHeapSize)
	heap.Init(&slowTableHeap)

	return &Manager{
		tables:                make(map[int64]*ReplicationSet),
		runningTasks:          make(map[int64]*ScheduleTask),
		maxTaskConcurrency:    maxTaskConcurrency,
		changefeedID:          changefeedID,
		slowTableHeap:         slowTableHeap,
		lastLogSlowTablesTime: time.Now(),
	}
}

// HandleCaptureChanges handles capture changes.
func (r *Manager) HandleCaptureChanges(
	init map[model.CaptureID][]tablepb.TableStatus,
	removed map[model.CaptureID][]tablepb.TableStatus,
	checkpointTs model.Ts,
) ([]*schedulepb.Message, error) {
	if init != nil {
		if len(r.tables) != 0 {
			log.Panic("schedulerv3: init again",
				zap.String("namespace", r.changefeedID.Namespace),
				zap.String("changefeed", r.changefeedID.ID),
				zap.Any("init", init), zap.Any("tables", r.tables))
		}
		tableStatus := map[model.TableID]map[model.CaptureID]*tablepb.TableStatus{}
		for captureID, tables := range init {
			for i := range tables {
				table := tables[i]
				if _, ok := tableStatus[table.TableID]; !ok {
					tableStatus[table.TableID] = map[model.CaptureID]*tablepb.TableStatus{}
				}
				tableStatus[table.TableID][captureID] = &table
			}
		}
		for tableID, status := range tableStatus {
			table, err := NewReplicationSet(
				tableID, checkpointTs, status, r.changefeedID)
			if err != nil {
				return nil, errors.Trace(err)
			}
			r.tables[tableID] = table
		}
	}
	sentMsgs := make([]*schedulepb.Message, 0)
	if removed != nil {
		for _, table := range r.tables {
			for captureID := range removed {
				msgs, affected, err := table.handleCaptureShutdown(captureID)
				if err != nil {
					return nil, errors.Trace(err)
				}
				sentMsgs = append(sentMsgs, msgs...)
				if affected {
					// Cleanup its running task.
					delete(r.runningTasks, table.TableID)
				}
			}
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
		table, ok := r.tables[status.TableID]
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
				zap.Int64("tableID", status.TableID))
			delete(r.tables, status.TableID)
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

	table, ok := r.tables[status.TableID]
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
			zap.Int64("tableID", status.TableID))
		delete(r.tables, status.TableID)
	}
	return msgs, nil
}

// HandleTasks handles schedule tasks.
func (r *Manager) HandleTasks(
	tasks []*ScheduleTask,
) ([]*schedulepb.Message, error) {
	// Check if a running task is finished.
	for tableID := range r.runningTasks {
		if table, ok := r.tables[tableID]; ok {
			// If table is back to Replicating or Removed,
			// the running task is finished.
			if table.State == ReplicationSetStateReplicating || table.hasRemoved() {
				delete(r.runningTasks, tableID)
			}
		} else {
			// No table found, remove the task
			delete(r.runningTasks, tableID)
		}
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
		if len(r.runningTasks) == r.maxTaskConcurrency {
			log.Debug("schedulerv3: too many running task",
				zap.String("namespace", r.changefeedID.Namespace),
				zap.String("changefeed", r.changefeedID.ID))
			// Does not use break, in case there is burst balance task
			// in the remaining tasks.
			continue
		}

		var tableID model.TableID
		if task.AddTable != nil {
			tableID = task.AddTable.TableID
		} else if task.RemoveTable != nil {
			tableID = task.RemoveTable.TableID
		} else if task.MoveTable != nil {
			tableID = task.MoveTable.TableID
		}

		// Skip task if the table is already running a task,
		// or the table has removed.
		if _, ok := r.runningTasks[tableID]; ok {
			log.Info("schedulerv3: ignore task, already exists",
				zap.String("namespace", r.changefeedID.Namespace),
				zap.String("changefeed", r.changefeedID.ID),
				zap.Any("task", task))
			continue
		}
		if _, ok := r.tables[tableID]; !ok && task.AddTable == nil {
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
		r.runningTasks[tableID] = task
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
	table := r.tables[task.TableID]
	if table == nil {
		table, err = NewReplicationSet(
			task.TableID, task.CheckpointTs, nil, r.changefeedID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		r.tables[task.TableID] = table
	}
	return table.handleAddTable(task.CaptureID)
}

func (r *Manager) handleRemoveTableTask(
	task *RemoveTable,
) ([]*schedulepb.Message, error) {
	r.acceptRemoveTableTask++
	table := r.tables[task.TableID]
	if table.hasRemoved() {
		log.Info("schedulerv3: table has removed",
			zap.String("namespace", r.changefeedID.Namespace),
			zap.String("changefeed", r.changefeedID.ID),
			zap.Int64("tableID", task.TableID))
		delete(r.tables, task.TableID)
		return nil, nil
	}
	return table.handleRemoveTable()
}

func (r *Manager) handleMoveTableTask(
	task *MoveTable,
) ([]*schedulepb.Message, error) {
	r.acceptMoveTableTask++
	table := r.tables[task.TableID]
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
		if r.runningTasks[addTable.TableID] != nil {
			// Skip add table if the table is already running a task.
			continue
		}
		msgs, err := r.handleAddTableTask(&addTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		// Just for place holding.
		r.runningTasks[addTable.TableID] = &ScheduleTask{}
	}
	for i := range task.RemoveTables {
		removeTable := task.RemoveTables[i]
		if r.runningTasks[removeTable.TableID] != nil {
			// Skip add table if the table is already running a task.
			continue
		}
		msgs, err := r.handleRemoveTableTask(&removeTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		// Just for place holding.
		r.runningTasks[removeTable.TableID] = &ScheduleTask{}
	}
	for i := range task.MoveTables {
		moveTable := task.MoveTables[i]
		if r.runningTasks[moveTable.TableID] != nil {
			// Skip add table if the table is already running a task.
			continue
		}
		msgs, err := r.handleMoveTableTask(&moveTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		// Just for place holding.
		r.runningTasks[moveTable.TableID] = &ScheduleTask{}
	}
	return sentMsgs, nil
}

// ReplicationSets return all tracking replication set
// Caller must not modify the returned map.
func (r *Manager) ReplicationSets() map[model.TableID]*ReplicationSet {
	return r.tables
}

// RunningTasks return running tasks.
// Caller must not modify the returned map.
func (r *Manager) RunningTasks() map[model.TableID]*ScheduleTask {
	return r.runningTasks
}

// AdvanceCheckpoint tries to advance checkpoint and returns current checkpoint.
func (r *Manager) AdvanceCheckpoint(
	currentTables []model.TableID,
	currentTime time.Time,
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

	r.slowestPuller = model.TableID(0)
	r.slowestSink = model.TableID(0)
	resolvedTsOfSlowestSink := model.Ts(math.MaxUint64)

	watermark = schedulepb.Watermark{
		CheckpointTs:     math.MaxUint64,
		ResolvedTs:       math.MaxUint64,
		LastSyncedTs:     0,
		PullerResolvedTs: math.MaxUint64,
	}
	for _, tableID := range currentTables {
		table, ok := r.tables[tableID]
		if !ok {
			// Can not advance checkpoint there is a span missing.
			now := time.Now()
			if now.Sub(r.lastLogMissTime) > logMissingTableInterval {
				// Can not advance checkpoint there is a table missing.
				log.Warn("schedulerv3: cannot advance checkpoint since missing table",
					zap.String("namespace", r.changefeedID.Namespace),
					zap.String("changefeed", r.changefeedID.ID),
					zap.Int64("tableID", tableID))
				r.lastLogMissTime = now
			}
			if redoMetaManager.Enabled() {
				// If redo is enabled, GlobalBarrierTs should be limited by redo flushed meta.
				watermark.ResolvedTs = barrier.RedoBarrierTs
				limitBarrierWithRedo(&watermark)
			}
			return schedulepb.Watermark{
				CheckpointTs:     checkpointCannotProceed,
				ResolvedTs:       checkpointCannotProceed,
				LastSyncedTs:     checkpointCannotProceed,
				PullerResolvedTs: checkpointCannotProceed,
			}
		}
		// Find the minimum checkpoint ts and resolved ts.
		if watermark.CheckpointTs > table.Checkpoint.CheckpointTs ||
			(watermark.CheckpointTs == table.Checkpoint.CheckpointTs &&
				resolvedTsOfSlowestSink > table.Checkpoint.ResolvedTs) {
			watermark.CheckpointTs = table.Checkpoint.CheckpointTs
			r.slowestSink = tableID
			resolvedTsOfSlowestSink = table.Checkpoint.ResolvedTs
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
				r.slowestPuller = tableID
			}
		}
	}

	// If currentTables is empty, we should advance newResolvedTs to global barrier ts and
	// advance newCheckpointTs to min table barrier ts.
	if watermark.ResolvedTs == math.MaxUint64 || watermark.CheckpointTs == math.MaxUint64 {
		if watermark.CheckpointTs != watermark.ResolvedTs || len(currentTables) != 0 {
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
	checkpointLag := currentTime.Sub(oracle.GetTimeFromTS(watermark.CheckpointTs))
	if checkpointLag > logSlowTablesLagThreshold &&
		time.Since(r.lastLogSlowTablesTime) > logSlowTablesInterval {
		r.logSlowTableInfo(currentTables, currentTime)
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

func (r *Manager) logSlowTableInfo(currentTables []model.TableID, currentTime time.Time) {
	// find the slow tables
	for _, tableID := range currentTables {
		table, ok := r.tables[tableID]
		if !ok {
			continue
		}
		lag := currentTime.Sub(oracle.GetTimeFromTS(table.Checkpoint.CheckpointTs))
		if lag < logSlowTablesLagThreshold {
			continue
		}
		heap.Push(&r.slowTableHeap, table)
		if r.slowTableHeap.Len() > defaultSlowTableHeapSize {
			heap.Pop(&r.slowTableHeap)
		}
	}

	num := r.slowTableHeap.Len()
	for i := 0; i < num; i++ {
		table := heap.Pop(&r.slowTableHeap).(*ReplicationSet)
		log.Info("schedulerv3: slow table",
			zap.String("namespace", r.changefeedID.Namespace),
			zap.String("changefeed", r.changefeedID.ID),
			zap.Int64("tableID", table.TableID),
			zap.String("tableStatus", table.Stats.String()),
			zap.Uint64("checkpointTs", table.Checkpoint.CheckpointTs),
			zap.Uint64("resolvedTs", table.Checkpoint.ResolvedTs),
			zap.Duration("checkpointLag", currentTime.
				Sub(oracle.GetTimeFromTS(table.Checkpoint.CheckpointTs))))
	}
}

// CollectMetrics collects metrics.
func (r *Manager) CollectMetrics() {
	cf := r.changefeedID
	tableGauge.
		WithLabelValues(cf.Namespace, cf.ID).Set(float64(len(r.tables)))
	if table, ok := r.tables[r.slowestSink]; ok {
		slowestTableIDGauge.
			WithLabelValues(cf.Namespace, cf.ID).Set(float64(r.slowestSink))
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
		WithLabelValues(cf.Namespace, cf.ID).Set(float64(len(r.runningTasks)))
	var stateCounters [6]int
	for _, table := range r.tables {
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
	}
	for s, counter := range stateCounters {
		tableStateGauge.
			WithLabelValues(cf.Namespace, cf.ID, ReplicationSetState(s).String()).
			Set(float64(counter))
	}

	if table, ok := r.tables[r.slowestPuller]; ok {
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
	for _, table := range r.tables {
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
	}
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
	r.tables[rs.TableID] = rs
}

// GetReplicationSetForTests is only used in tests.
func (r *Manager) GetReplicationSetForTests() map[model.TableID]*ReplicationSet {
	return r.tables
}
