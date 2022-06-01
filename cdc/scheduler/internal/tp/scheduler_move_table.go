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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

var _ scheduler = &moveTableScheduler{}

type moveTableScheduler struct {
	tasks map[model.TableID]*scheduleTask
}

func newMoveTableScheduler() *moveTableScheduler {
	return &moveTableScheduler{
		tasks: make(map[model.TableID]*scheduleTask),
	}
}

func (m *moveTableScheduler) Name() string {
	return string(schedulerTypeMoveTable)
}

func (m *moveTableScheduler) addTask(tableID model.TableID, target model.CaptureID) bool {
	// previous triggered task not finished yet, decline the new manual move table request.
	if _, ok := m.tasks[tableID]; ok {
		return false
	}
	m.tasks[tableID] = &scheduleTask{
		moveTable: &moveTable{
			TableID:     tableID,
			DestCapture: target,
		},
	}
	return true
}

func (m *moveTableScheduler) Schedule(
	checkpointTs model.Ts,
	currentTables []model.TableID,
	captures map[model.CaptureID]*CaptureStatus,
	replications map[model.TableID]*ReplicationSet,
) []*scheduleTask {
	allTables := make(map[model.TableID]struct{})
	for _, tableID := range currentTables {
		allTables[tableID] = struct{}{}
	}

	for tableID, task := range m.tasks {
		// table may not in the all current tables if it was removed after manual move table triggered.
		if _, ok := allTables[tableID]; !ok {
			log.Warn("tpscheduler: move table ignored, since the table cannot found",
				zap.Int64("tableID", tableID),
				zap.String("captureID", task.moveTable.DestCapture))
			delete(m.tasks, tableID)
			continue
		}
		// the target capture may not in `initialize` state after manual move table triggered.
		captureStatus, ok := captures[task.moveTable.DestCapture]
		if !ok {
			log.Warn("tpscheduler: move table ignored, since the target capture cannot found",
				zap.Int64("tableID", tableID),
				zap.String("captureID", task.moveTable.DestCapture))
			delete(m.tasks, tableID)
			continue
		}
		if captureStatus.State != CaptureStateInitialized {
			log.Warn("tpscheduler: move table ignored, "+
				"since the target capture is not initialized",
				zap.Int64("tableID", tableID),
				zap.String("captureID", task.moveTable.DestCapture),
				zap.Any("captureState", captureStatus.State))
			delete(m.tasks, tableID)
			continue
		}
		rep, ok := replications[tableID]
		if !ok {
			log.Warn("tpscheduler: move table ignored, "+
				"since the table cannot found in the replication set",
				zap.Int64("tableID", tableID),
				zap.String("captureID", task.moveTable.DestCapture))
			delete(m.tasks, tableID)
			continue
		}
		// only move replicating table.
		if rep.State != ReplicationSetStateReplicating {
			log.Info("tpscheduler: move table ignored, since the table is not replicating now",
				zap.Int64("tableID", tableID),
				zap.String("captureID", task.moveTable.DestCapture),
				zap.Any("replicationState", rep.State))
			delete(m.tasks, tableID)
		}
	}

	tasks := make([]*scheduleTask, 0)
	for _, task := range m.tasks {
		tasks = append(tasks, task)
	}
	m.tasks = make(map[model.TableID]*scheduleTask)

	return tasks
}
