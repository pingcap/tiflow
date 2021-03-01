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

package owner

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// scheduler is designed to abstract away the complexities associated with the Etcd data model.
type scheduler interface {
	// PutTasks is used to pass ALL the tables that need replicating to the scheduler.
	// It should be safe to be called repeatedly with the same argument, as long as the argument correctly
	// represents the current tables.
	// USE ONLY after IsReady() returns true.
	PutTasks(tables map[model.TableID]*tableTask)
	// SetAffinity sets a table's affinity to a capture.
	// Affinities record a binding (often temporary) of tables to captures, and it is a useful mechanism to
	// implement manual table migration.
	SetAffinity(tableID model.TableID, captureID model.CaptureID, ttl int)
	// IsReady returns true if and only if the scheduler is ready to perform operations.
	// IsReady always returns the same value within a EtcdWorker tick.
	IsReady() bool
}

type schedulerImpl struct {
	ownerState *ownerReactorState
	cfID       model.ChangeFeedID

	// the affinities mutex, guarding against concurrent access from the HTTP handle
	mu         sync.Mutex
	affinities map[model.TableID]*affinity

	// this flag is set by a capture-added event in the ownerState
	needRebalance         bool
	captureWorkloadDeltas map[model.CaptureID]int
}

// affinity is used to record a table-capture affinity setting.
type affinity struct {
	targetCapture model.CaptureID
	deadline      time.Time
}

func newScheduler(ownerState *ownerReactorState, cfID model.ChangeFeedID) *schedulerImpl {
	ret := &schedulerImpl{
		ownerState: ownerState,
		cfID:       cfID,
		affinities: make(map[model.TableID]*affinity),
	}

	ownerState.SetNewCaptureHandler(func(captureID model.CaptureID) {
		ret.onNewCapture(captureID)
	})

	return ret
}

func (s *schedulerImpl) PutTasks(tables map[model.TableID]*tableTask) {
	s.captureWorkloadDeltas = make(map[model.CaptureID]int)

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
					zap.Int("tableID", int(tableID)),
					zap.String("target-capture", affCapture))
				target = affCapture
			}

			replicaInfo := model.TableReplicaInfo{
				StartTs:     task.CheckpointTs + 1,
				MarkTableID: 0, // TODO support cyclic replication
			}

			log.Info("Dispatching table",
				zap.Int64("table-id", tableID),
				zap.String("target-capture", target),
				zap.String("changefeed-id", s.cfID))

			s.ownerState.DispatchTable(s.cfID, target, tableID, replicaInfo)
			s.captureWorkloadDeltas[target]++
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
			s.captureWorkloadDeltas[captureID]--
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
				// Only those tables that are being deleted are added to the pendingList,
				// because while it is unsafe to try to dispatch a table in the process of deleting,
				// it is safe to try to delete a table immediately after it being dispatched.
				// In summary, being run by two capture is dangerous, but not being run at all is safe.
				if operation.Delete {
					pendingList = append(pendingList, tableID)
				}
			}
		}
	}

	return pendingList
}

// IsReady returns whether the scheduler is ready to process new requests.
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
		captureTables := s.ownerState.GetCaptureTables(s.cfID, captureID)

		// Use rand.Perm as a randomization source for choosing victims uniformly.
		randPerm := rand.Perm(len(captureTables))

		for i := 0; i < len(captureTables)-upperLimitPerCapture; i++ {
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
		workloads[captureID] = s.captureWorkloadDeltas[captureID]
	}

	for _, captureStatuses := range s.ownerState.TaskStatuses {
		for captureID, taskStatus := range captureStatuses {
			if _, ok := workloads[captureID]; ok {
				workloads[captureID] += len(taskStatus.Tables)
			}
		}
	}

	minCapture := ""
	minWorkLoad := math.MaxInt32
	for captureID, workload := range workloads {
		if workload < 0 {
			// TODO investigate and remove this log
			log.Debug("negative workload, bug?",
				zap.Reflect("workloads", workloads),
				zap.Reflect("deltas", s.captureWorkloadDeltas))
			workload = 0
		}

		if workload < minWorkLoad {
			minCapture = captureID
			minWorkLoad = workload
		}
	}

	return minCapture
}

// cleanUpStaleCaptureStatus cleans up TaskStatus and TaskPosition for captures that have just gone offline.
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
