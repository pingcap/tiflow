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
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"go.uber.org/zap"
)

type callback func()

// burstBalance for changefeed set up or unplanned TiCDC node failure.
// TiCDC needs to balance interrupted tables as soon as possible.
type burstBalance struct {
	// Add tables to captures
	AddTables, RemoveTables map[model.TableID]model.CaptureID
	CheckpointTs            model.Ts
}

type moveTable struct {
	TableID     model.TableID
	DestCapture model.CaptureID
}

type addTable struct {
	TableID      model.TableID
	CaptureID    model.CaptureID
	CheckpointTs model.Ts
}

type removeTable struct {
	TableID   model.TableID
	CaptureID model.CaptureID
}

type scheduleTask struct {
	moveTable    *moveTable
	addTable     *addTable
	removeTable  *removeTable
	burstBalance *burstBalance

	accept callback
}

type replicationManager struct {
	tables map[model.TableID]*ReplicationSet

	runningTasks       map[model.TableID]*scheduleTask
	maxTaskConcurrency int
}

func newReplicationManager(maxTaskConcurrency int) *replicationManager {
	return &replicationManager{
		tables:             make(map[int64]*ReplicationSet),
		runningTasks:       make(map[int64]*scheduleTask),
		maxTaskConcurrency: maxTaskConcurrency,
	}
}

func (r *replicationManager) HandleCaptureChanges(
	changes *captureChanges, checkpointTs model.Ts,
) ([]*schedulepb.Message, error) {
	if changes.Init != nil {
		if len(r.tables) != 0 {
			log.Panic("tpscheduler: init again",
				zap.Any("init", changes.Init), zap.Any("tables", r.tables))
		}
		tableStatus := map[model.TableID]map[model.CaptureID]*schedulepb.TableStatus{}
		for captureID, tables := range changes.Init {
			for i := range tables {
				table := tables[i]
				if _, ok := tableStatus[table.TableID]; !ok {
					tableStatus[table.TableID] = map[string]*schedulepb.TableStatus{}
				}
				tableStatus[table.TableID][captureID] = &table
			}
		}
		for tableID, status := range tableStatus {
			table, err := newReplicationSet(tableID, checkpointTs, status)
			if err != nil {
				return nil, errors.Trace(err)
			}
			r.tables[tableID] = table
		}
	}
	sentMsgs := make([]*schedulepb.Message, 0)
	if changes.Removed != nil {
		for _, table := range r.tables {
			for captureID := range changes.Removed {
				msgs, err := table.handleCaptureShutdown(captureID)
				if err != nil {
					return nil, errors.Trace(err)
				}
				sentMsgs = append(sentMsgs, msgs...)
			}
		}
	}
	return sentMsgs, nil
}

func (r *replicationManager) HandleMessage(
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
			log.Warn("tpscheduler: ignore message",
				zap.Stringer("type", msg.MsgType), zap.Any("message", msg))
		}
	}
	return sentMsgs, nil
}

func (r *replicationManager) handleMessageHeartbeatResponse(
	from model.CaptureID, msg *schedulepb.HeartbeatResponse,
) ([]*schedulepb.Message, error) {
	sentMsgs := make([]*schedulepb.Message, 0)
	for _, status := range msg.Tables {
		table, ok := r.tables[status.TableID]
		if !ok {
			log.Info("tpscheduler: ignore table status no table found",
				zap.Any("message", status))
			continue
		}
		msgs, err := table.handleTableStatus(from, &status)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if table.hasRemoved() {
			log.Info("tpscheduler: table has removed", zap.Int64("tableID", status.TableID))
			delete(r.tables, status.TableID)
		}
		sentMsgs = append(sentMsgs, msgs...)
	}
	return sentMsgs, nil
}

func (r *replicationManager) handleMessageDispatchTableResponse(
	from model.CaptureID, msg *schedulepb.DispatchTableResponse,
) ([]*schedulepb.Message, error) {
	var status *schedulepb.TableStatus
	switch resp := msg.Response.(type) {
	case *schedulepb.DispatchTableResponse_AddTable:
		status = resp.AddTable.Status
	case *schedulepb.DispatchTableResponse_RemoveTable:
		status = resp.RemoveTable.Status
	default:
		log.Warn("tpscheduler: ignore unknown dispatch table response",
			zap.Any("message", msg))
		return nil, nil
	}

	table, ok := r.tables[status.TableID]
	if !ok {
		log.Info("tpscheduler: ignore table status no table found",
			zap.Any("message", status))
		return nil, nil
	}
	msgs, err := table.handleTableStatus(from, status)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if table.hasRemoved() {
		log.Info("tpscheduler: table has removed", zap.Int64("tableID", status.TableID))
		delete(r.tables, status.TableID)
	}
	return msgs, nil
}

func (r *replicationManager) HandleTasks(
	tasks []*scheduleTask,
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
		if task.burstBalance != nil {
			msgs, err := r.handleBurstBalanceTasks(task.burstBalance)
			if err != nil {
				return nil, errors.Trace(err)
			}
			sentMsgs = append(sentMsgs, msgs...)
			if task.accept != nil {
				task.accept()
			}
			continue
		}

		// Check if accepting one more task exceeds maxTaskConcurrency.
		if len(r.runningTasks) == r.maxTaskConcurrency {
			log.Debug("tpscheduler: too many running task")
			// Does not use break, in case there is burst balance task
			// in the remaining tasks.
			continue
		}

		var tableID model.TableID
		if task.addTable != nil {
			tableID = task.addTable.TableID
		} else if task.removeTable != nil {
			tableID = task.removeTable.TableID
		} else if task.moveTable != nil {
			tableID = task.moveTable.TableID
		}

		// Skip task if the table is already running a task,
		// or the table has removed.
		if _, ok := r.runningTasks[tableID]; ok {
			log.Info("tpscheduler: ignore task, already exists",
				zap.Any("task", task))
			continue
		}
		if _, ok := r.tables[tableID]; !ok && task.addTable == nil {
			log.Info("tpscheduler: ignore task, table not found",
				zap.Any("task", task))
			continue
		}

		var msgs []*schedulepb.Message
		var err error
		if task.addTable != nil {
			msgs, err = r.handleAddTableTask(task.addTable)
		} else if task.removeTable != nil {
			msgs, err = r.handleRemoveTableTask(task.removeTable)
		} else if task.moveTable != nil {
			msgs, err = r.handleMoveTableTask(task.moveTable)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		r.runningTasks[tableID] = task
		if task.accept != nil {
			task.accept()
		}
	}
	return sentMsgs, nil
}

func (r *replicationManager) handleAddTableTask(
	task *addTable,
) ([]*schedulepb.Message, error) {
	var err error
	table := r.tables[task.TableID]
	if table == nil {
		table, err = newReplicationSet(task.TableID, task.CheckpointTs, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		r.tables[task.TableID] = table
	}
	return table.handleAddTable(task.CaptureID)
}

func (r *replicationManager) handleRemoveTableTask(
	task *removeTable,
) ([]*schedulepb.Message, error) {
	table := r.tables[task.TableID]
	if table.hasRemoved() {
		log.Info("tpscheduler: table has removed", zap.Int64("tableID", task.TableID))
		delete(r.tables, task.TableID)
		return nil, nil
	}
	return table.handleRemoveTable()
}

func (r *replicationManager) handleMoveTableTask(
	task *moveTable,
) ([]*schedulepb.Message, error) {
	table := r.tables[task.TableID]
	return table.handleMoveTable(task.DestCapture)
}

func (r *replicationManager) handleBurstBalanceTasks(
	task *burstBalance,
) ([]*schedulepb.Message, error) {
	perCapture := make(map[model.CaptureID]int)
	for _, captureID := range task.AddTables {
		perCapture[captureID]++
	}
	for _, captureID := range task.RemoveTables {
		perCapture[captureID]++
	}
	fields := make([]zap.Field, 0, len(perCapture))
	for captureID, count := range perCapture {
		fields = append(fields, zap.Int(captureID, count))
	}
	fields = append(fields, zap.Int("total", len(task.AddTables)+len(task.RemoveTables)))
	log.Info("tpscheduler: handle burst balance task", fields...)

	checkpointTs := task.CheckpointTs
	sentMsgs := make([]*schedulepb.Message, 0, len(task.AddTables))
	for tableID, captureID := range task.AddTables {
		if r.runningTasks[tableID] != nil {
			// Skip add table if the table is already running a task.
			continue
		}
		msgs, err := r.handleAddTableTask(&addTable{
			TableID: tableID, CaptureID: captureID, CheckpointTs: checkpointTs,
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		// Just for place holding.
		r.runningTasks[tableID] = &scheduleTask{}
	}
	for tableID, captureID := range task.RemoveTables {
		if r.runningTasks[tableID] != nil {
			// Skip add table if the table is already running a task.
			continue
		}
		msgs, err := r.handleRemoveTableTask(&removeTable{
			TableID: tableID, CaptureID: captureID,
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		// Just for place holding.
		r.runningTasks[tableID] = &scheduleTask{}
	}
	return sentMsgs, nil
}

// ReplicationSets return all tracking replication set
// Caller must not modify the return replication sets.
func (r *replicationManager) ReplicationSets() map[model.TableID]*ReplicationSet {
	return r.tables
}

func (r *replicationManager) AdvanceCheckpoint(
	currentTables []model.TableID,
) (newCheckpointTs, newResolvedTs model.Ts) {
	newCheckpointTs, newResolvedTs = math.MaxUint64, math.MaxUint64
	checkpointChanged := false
	for _, tableID := range currentTables {
		table, ok := r.tables[tableID]
		if !ok {
			// Can not advance checkpoint there is a table missing.
			return checkpointCannotProceed, checkpointCannotProceed
		}
		// Find the minimum checkpoint ts and resolved ts.
		if newCheckpointTs > table.Checkpoint.CheckpointTs {
			newCheckpointTs = table.Checkpoint.CheckpointTs
			checkpointChanged = true
		}
		if newResolvedTs > table.Checkpoint.ResolvedTs {
			newResolvedTs = table.Checkpoint.ResolvedTs
			checkpointChanged = true
		}
	}
	if !checkpointChanged {
		return checkpointCannotProceed, checkpointCannotProceed
	}
	return newCheckpointTs, newResolvedTs
}
