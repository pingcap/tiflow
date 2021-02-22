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
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

type scheduler interface {
	GetTasks() (map[model.TableID]*tableTask, bool)
	PutTasks(tables map[model.TableID]*tableTask)
	SetAffinity(tableID model.TableID, captureID model.CaptureID, ttl int)
	IsReady() bool
}

type schedulerImpl struct {
	ownerState *ownerReactorState
	cfID       model.ChangeFeedID

	mu         sync.Mutex
	affinities map[model.TableID]*affinity

	needRebalance bool
}

type affinity struct {
	targetCapture model.CaptureID
	deadline      time.Time
}

func newScheduler(ownerState *ownerReactorState, cfID model.ChangeFeedID) *schedulerImpl {
	ret := &schedulerImpl{
		ownerState: ownerState,
		cfID:       cfID,
	}

	ownerState.SetNewCaptureHandler(func(captureID model.CaptureID) {
		ret.onNewCapture(captureID)
	})

	return ret
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
			ResolvedTs:   position.ResolvedTs,
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

			if affCapture := s.lookUpAffinity(tableID); affCapture != "" {
				log.Info("Dispatching table using affinity",
					zap.String("cfID", s.cfID),
					zap.String("tableID", string(tableID)),
					zap.String("target-capture", affCapture))
				target = affCapture
			}

			replicaInfo := model.TableReplicaInfo{
				StartTs:     task.CheckpointTs + 1,
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

	if s.needRebalance {
		s.needRebalance = false
		s.triggerRebalance()
	}
}

// cleanUpOperations returns tablesIDs of tables that are NOT suitable for immediate redispatching.
func (s *schedulerImpl) cleanUpOperations() []model.TableID {
	var pendingList []model.TableID

	for captureID, taskStatus := range s.ownerState.TaskStatuses[s.cfID] {
		for tableID, operation := range taskStatus.Operation {
			if operation.Status == model.OperFinished {
				s.ownerState.CleanOperation(s.cfID, captureID, tableID)
			} else {
				if operation.Delete {
					pendingList = append(pendingList, tableID)
				}
			}
		}
	}

	return pendingList
}

func (s *schedulerImpl) IsReady() bool {
	return !s.cleanUpStaleCaptureStatus()
}

func (s *schedulerImpl) SetAffinity(tableID model.TableID, captureID model.CaptureID, ttl int) {
	log.Info("Setting table affinity",
		zap.String("cfID", s.cfID),
		zap.String("captureID", captureID),
		zap.Int("tableID", int(tableID)),
		zap.Int("ttl", ttl))

	s.mu.Lock()
	defer s.mu.Unlock()

	s.affinities[tableID] = &affinity{
		targetCapture: captureID,
		deadline:      time.Now().Add(time.Duration(ttl) * time.Second),
	}
}

func (s *schedulerImpl) onNewCapture(_ model.CaptureID) {
	s.needRebalance = true
}

func (s *schedulerImpl) lookUpAffinity(tableID model.TableID) model.CaptureID {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanUpAffinities()

	af, ok := s.affinities[tableID]
	if !ok {
		return ""
	}

	if !s.ownerState.CaptureExists(af.targetCapture) {
		delete(s.affinities, tableID)
		return ""
	}

	return af.targetCapture
}

// cleanUpAffinities must be called with mu locked.
func (s *schedulerImpl) cleanUpAffinities() {
	for tableID, af := range s.affinities {
		if af.deadline.Before(time.Now()) {
			delete(s.affinities, tableID)
		}
	}
}

func (s *schedulerImpl) triggerRebalance() {
	tableToCaptureMap := s.ownerState.GetTableToCaptureMap(s.cfID)
	totalTableNum := len(tableToCaptureMap)
	captureNum := len(s.ownerState.Captures)

	upperLimitPerCapture := int(math.Ceil(float64(totalTableNum) / float64(captureNum)))

	log.Info("Start rebalancing",
		zap.String("cfID", s.cfID),
		zap.Int("table-num", totalTableNum),
		zap.Int("capture-num", captureNum),
		zap.Int("target-limit", upperLimitPerCapture))

	for captureID := range s.ownerState.Captures {
		if !s.ownerState.CaptureExists(captureID) {
			log.Debug("triggerRebalance: capture not found", zap.String("captureID", captureID))
			continue
		}

		captureTables := s.ownerState.GetCaptureTables(s.cfID, captureID)

		// Use rand.Perm as a randomization source for choosing victims uniformly.
		randPerm := rand.Perm(len(captureTables))

		for i := 0; i < len(captureTables) - upperLimitPerCapture; i++ {
			victimIdx := randPerm[i]
			victimTableID := captureTables[victimIdx]

			log.Info("triggerRebalance: Stopping table",
				zap.Int64("table-id", victimTableID),
				zap.String("capture", captureID),
				zap.String("changefeed-id", s.cfID))

			s.ownerState.StartDeletingTable(s.cfID, captureID, victimTableID)
		}
	}
}

func (s *schedulerImpl) getMinWorkloadCapture() model.CaptureID {
	workloads := make(map[model.CaptureID]int)

	for captureID := range s.ownerState.Captures {
		workloads[captureID] = 0
	}

	for _, captureStatuses := range s.ownerState.TaskStatuses {
		for captureID, task := range captureStatuses {
			if _, ok := workloads[captureID]; ok {
				workloads[captureID] += len(task.Tables)
			}
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

func (s *schedulerImpl) cleanUpStaleCaptureStatus() bool {
	if _, ok := s.ownerState.TaskStatuses[s.cfID]; !ok {
		return false
	}

	hasPending := false
	for captureID := range s.ownerState.TaskStatuses[s.cfID] {
		if !s.ownerState.CaptureExists(captureID) {
			log.Info("cleaning up stale capture",
				zap.String("cfID", s.cfID),
				zap.String("captureID", captureID))
			s.ownerState.CleanUpTaskStatus(s.cfID, captureID)
			s.ownerState.CleanUpTaskPosition(s.cfID, captureID)
			hasPending = true
		}
	}

	return hasPending
}
