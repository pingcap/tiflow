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

package v3

import (
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

var _ scheduler = &moveTableScheduler{}

type moveTableScheduler struct {
	mu    sync.Mutex
	tasks map[model.TableID]*scheduleTask
}

func newMoveTableScheduler() *moveTableScheduler {
	return &moveTableScheduler{
		tasks: make(map[model.TableID]*scheduleTask),
	}
}

func (m *moveTableScheduler) Name() string {
	return "move-table-scheduler"
}

func (m *moveTableScheduler) addTask(tableID model.TableID, target model.CaptureID) bool {
	// previous triggered task not accepted yet, decline the new manual move table request.
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tasks[tableID]; ok {
		return false
	}
	m.tasks[tableID] = &scheduleTask{
		moveTable: &moveTable{
			TableID:     tableID,
			DestCapture: target,
		},
		accept: func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			delete(m.tasks, tableID)
		},
	}
	return true
}

func (m *moveTableScheduler) Schedule(
	_ model.Ts,
	currentTables []model.TableID,
	captures map[model.CaptureID]*CaptureStatus,
	replications map[model.TableID]*ReplicationSet,
) []*scheduleTask {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]*scheduleTask, 0)

	if len(m.tasks) == 0 {
		return result
	}

	if len(captures) == 0 {
		return result
	}

	allTables := newTableSet()
	for _, tableID := range currentTables {
		allTables.add(tableID)
	}

	for tableID, task := range m.tasks {
		// table may not in the all current tables
		// if it was removed after manual move table triggered.
		if !allTables.contain(tableID) {
			log.Warn("schedulerv3: move table ignored, since the table cannot found",
				zap.Int64("tableID", tableID),
				zap.String("captureID", task.moveTable.DestCapture))
			delete(m.tasks, tableID)
			continue
		}

		// the target capture may offline after manual move table triggered.
		status, ok := captures[task.moveTable.DestCapture]
		if !ok {
			log.Info("schedulerv3: move table ignored, since the target capture cannot found",
				zap.Int64("tableID", tableID),
				zap.String("captureID", task.moveTable.DestCapture))
			delete(m.tasks, tableID)
			continue
		}
		if status.State != CaptureStateInitialized {
			log.Warn("schedulerv3: move table ignored,"+
				"since the target capture is not initialized",
				zap.Int64("tableID", tableID),
				zap.String("captureID", task.moveTable.DestCapture),
				zap.Any("state", status.State))
			delete(m.tasks, tableID)
			continue
		}

		rep, ok := replications[tableID]
		if !ok {
			log.Warn("schedulerv3: move table ignored, "+
				"since the table cannot found in the replication set",
				zap.Int64("tableID", tableID),
				zap.String("captureID", task.moveTable.DestCapture))
			delete(m.tasks, tableID)
			continue
		}
		// only move replicating table.
		if rep.State != ReplicationSetStateReplicating {
			log.Info("schedulerv3: move table ignored, since the table is not replicating now",
				zap.Int64("tableID", tableID),
				zap.String("captureID", task.moveTable.DestCapture),
				zap.Any("replicationState", rep.State))
			delete(m.tasks, tableID)
		}
	}

	for _, task := range m.tasks {
		result = append(result, task)
	}

	return result
}
