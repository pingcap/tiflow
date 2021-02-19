// Copyright 2021 PingCAP, Inc.
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
	"math"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

type scheduler interface {
	GetTasks() (map[model.TableID]*tableTask, bool)
	PutTasks(tables map[model.TableID]*tableTask)
}

type schedulerImpl struct {
	ownerState *ownerReactorState
	cfID       model.ChangeFeedID
}

func newScheduler(ownerState *ownerReactorState, cfID model.ChangeFeedID) *schedulerImpl {
	return &schedulerImpl{
		ownerState: ownerState,
		cfID:       cfID,
	}
}

func (s *schedulerImpl) GetTasks() (map[model.TableID]*tableTask, bool) {
	if s.ownerState.IsInitialized() {
		return nil, false
	}

	tableToCaptureMap := s.ownerState.GetTableToCaptureMap(s.cfID)
	positions := s.ownerState.TaskPositions[s.cfID]

	ret := make(map[model.TableID]*tableTask)
	for tableID, captureID := range tableToCaptureMap {
		position, ok := positions[captureID]
		if !ok {
			log.Warn("Position not found", zap.String("captureID", captureID))
			return nil, false
		}

		ret[tableID] = &tableTask{
			TableID:      tableID,
			CheckpointTs: position.CheckPointTs,
			ResolvedTs:   0,
		}
	}

	return ret, true
}

func (s *schedulerImpl) PutTasks(tables map[model.TableID]*tableTask) {
	// We do NOT want to touch these tables because they are being deleted.
	// We wait for the deletion(s) to finish before redispatching.
	pendingList := s.cleanUpOperations()
	pendingSet := make(map[model.TableID]struct{})
	for _, tableID := range pendingList {
		pendingSet[tableID] = struct{}{}
	}

	tableToCaptureMap := s.ownerState.GetTableToCaptureMap(s.cfID)

	// handle adding table
	for tableID, task := range tables {
		if _, ok := pendingSet[tableID]; ok {
			// Table has a pending deletion. Skip.
			continue
		}

		if _, ok := tableToCaptureMap[tableID]; !ok {
			// Table is not assigned to a capture.
			target := s.getMinWorkloadCapture()
			if target == "" {
				log.Warn("no capture is active")
				break
			}

			replicaInfo := model.TableReplicaInfo{
				StartTs:     task.CheckpointTs,
				MarkTableID: 0,  // TODO support cyclic replication
			}

			log.Info("Dispatching table",
				zap.Int64("table-id", tableID),
				zap.String("target-capture", target),
				zap.String("changefeed-id", s.cfID))

			s.ownerState.DispatchTable(s.cfID, target, tableID, replicaInfo)
		}
	}

	// handle deleting table
	for tableID, captureID := range tableToCaptureMap {
		if _, ok := pendingSet[tableID]; ok {
			// Table has a pending deletion. Skip.
			continue
		}

		if _, ok := tables[tableID]; !ok {
			// Table should be deleted from the capture
			log.Info("Stopping table",
				zap.Int64("table-id", tableID),
				zap.String("capture", captureID),
				zap.String("changefeed-id", s.cfID))

			s.ownerState.StartDeletingTable(s.cfID, captureID, tableID)
		}
	}
}

// cleanUpOperations returns tablesIDs of tables that are NOT suitable for immediate redispatching.
func (s *schedulerImpl) cleanUpOperations() []model.TableID {
	var pendingList []model.TableID

	for captureID, taskStatus := range s.ownerState.TaskStatuses[s.cfID] {
		for tableID, operation := range taskStatus.Operation {
			if operation.Status == model.OperFinished {
				s.ownerState.CleanOperation(s.cfID, captureID, tableID)
				if operation.Delete {
					s.ownerState.RemoveTable(s.cfID, captureID, tableID)
				}
			} else {
				if operation.Delete {
					pendingList = append(pendingList, tableID)
				}
			}
		}
	}

	return pendingList
}

func (s *schedulerImpl) getMinWorkloadCapture() model.CaptureID {
	workloads := make(map[model.CaptureID]int)

	for captureID := range s.ownerState.Captures {
		workloads[captureID] = 0
	}

	for _, captureStatuses := range s.ownerState.TaskStatuses {
		for captureID, task := range captureStatuses {
			workloads[captureID] += len(task.Tables)
		}
	}

	minCapture := ""
	minWorkLoad := math.MaxInt32
	for captureID, workload := range workloads {
		if workload < minWorkLoad {
			minCapture = captureID
			minWorkLoad = workload
		}
	}

	return minCapture
}
