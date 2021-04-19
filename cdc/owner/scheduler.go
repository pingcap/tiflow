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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

type schedulerJobType string

const (
	schedulerJobTypeAddTable    schedulerJobType = "ADD"
	schedulerJobTypeRemoveTable schedulerJobType = "REMOVE"
)

type schedulerJob struct {
	Tp      schedulerJobType
	TableID model.TableID
	// if the operation is a delete operation, boundaryTs is checkpoint ts
	// if the operation is a add operation, boundaryTs is start ts
	BoundaryTs    uint64
	TargetCapture model.CaptureID
}

type schedulerImpl struct {
	state  *model.ChangefeedReactorState
	schema schema4Owner

	moveTableTarget       map[model.TableID]model.CaptureID
	needRebalanceNextTick bool
}

func newScheduler(state *model.ChangefeedReactorState, schema schema4Owner) *schedulerImpl {
	return &schedulerImpl{
		state:           state,
		schema:          schema,
		moveTableTarget: make(map[model.TableID]model.CaptureID),
	}
}

// Tick is a main process of scheduler, it calls some internal handle function
// and returns a bool represents Is it possible that there are tables that do not exist in taskStatus
// if some table are not exist in taskStatus(in taskStatus.Tables nor in taskStatus.Operation),
// we should not push up resolvedTs
func (s *schedulerImpl) Tick() (consistent bool) {
	s.cleanUpOperations()
	pendingJob := s.syncTablesWithSchemaManager()
	s.handleJobs(pendingJob)
	s.rebalance()
	// Tables state in the schema manager and tables state in etcd can be inconsistent only if some pending jobs is not distributed.
	return len(pendingJob) == 0
}

func (s *schedulerImpl) MoveTable(tableID model.TableID, target model.CaptureID) {
	table2CaptureIndex := s.table2CaptureIndex()
	captureID, exist := table2CaptureIndex[tableID]
	if !exist {
		return
	}
	s.moveTableTarget[tableID] = target
	s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, error) {
		if status == nil {
			// the capture may be down, just skip remove this table
			return status, nil
		}
		if status.Operation != nil && status.Operation[tableID] != nil {
			// skip remove this table to avoid the remove operation created by rebalance function to influence the operation created by other function
			return status, nil
		}
		status.RemoveTable(tableID, s.state.Status.ResolvedTs, false)
		return status, nil
	})
}

func (s *schedulerImpl) Rebalance() {
	s.needRebalanceNextTick = true
}

func (s *schedulerImpl) table2CaptureIndex() map[model.TableID]model.CaptureID {
	table2CaptureIndex := make(map[model.TableID]model.CaptureID)
	for captureID, taskStatus := range s.state.TaskStatuses {
		for tableID := range taskStatus.Tables {
			table2CaptureIndex[tableID] = captureID
		}
		for tableID := range taskStatus.Operation {
			table2CaptureIndex[tableID] = captureID
		}
	}
	return table2CaptureIndex
}

func (s *schedulerImpl) getMinWorkloadCapture(pendingJob []*schedulerJob) model.CaptureID {
	workloads := make(map[model.CaptureID]uint64)

	for captureID, taskWorkload := range s.state.Workloads {
		for _, workload := range taskWorkload {
			workloads[captureID] += workload.Workload
		}
	}

	for _, pendingJob := range pendingJob {
		switch pendingJob.Tp {
		case schedulerJobTypeAddTable:
			workloads[pendingJob.TargetCapture] += 1
		case schedulerJobTypeRemoveTable:
			workloads[pendingJob.TargetCapture] -= 1
		default:
			log.Panic("Unreachable, please report a bug", zap.Any("job", pendingJob))
		}
	}

	minCapture := ""
	minWorkLoad := uint64(math.MaxUint64)
	for captureID, workload := range workloads {
		if workload < minWorkLoad {
			minCapture = captureID
			minWorkLoad = workload
		}
	}

	if minCapture == "" {
		log.Panic("Unreachable, no capture is found")
	}

	return minCapture
}

func (s *schedulerImpl) syncTablesWithSchemaManager() []*schedulerJob {
	var pendingJob []*schedulerJob
	allTableShouldBeListened := s.schema.AllPhysicalTables()
	allTableListeningNow := s.table2CaptureIndex()
	globalCheckpointTs := s.state.Status.CheckpointTs
	for _, tableID := range allTableShouldBeListened {
		if _, exist := allTableListeningNow[tableID]; exist {
			delete(allTableListeningNow, tableID)
			continue
		}
		target, exist := s.moveTableTarget[tableID]
		if exist {
			delete(s.moveTableTarget, tableID)
		} else {
			target = s.getMinWorkloadCapture(pendingJob)
		}
		// table which should be listened but not, add adding-table job to pending job list
		pendingJob = append(pendingJob, &schedulerJob{
			Tp:            schedulerJobTypeAddTable,
			TableID:       tableID,
			BoundaryTs:    globalCheckpointTs + 1,
			TargetCapture: target,
		})
	}
	// The remaining tables is the tables which should be not listened
	tableShouldNotListened := allTableListeningNow
	for tableID, captureID := range tableShouldNotListened {
		opts := s.state.TaskStatuses[captureID].Operation
		if opts != nil || opts[tableID] != nil || opts[tableID].Delete {
			// the table is removing, skip
			continue
		}
		pendingJob = append(pendingJob, &schedulerJob{
			Tp:            schedulerJobTypeRemoveTable,
			TableID:       tableID,
			BoundaryTs:    globalCheckpointTs,
			TargetCapture: captureID,
		})
	}
	return pendingJob
}

func (s *schedulerImpl) handleJobs(jobs []*schedulerJob) {
	for _, job := range jobs {
		s.state.PatchTaskStatus(job.TargetCapture, func(status *model.TaskStatus) (*model.TaskStatus, error) {
			switch job.Tp {
			case schedulerJobTypeAddTable:
				if status == nil {
					// if task status is not found, we can just skip to set adding-table operation, this table will be added in next tick
					log.Warn("task status of the capture is not found, may be the capture is already down. specify a new capture and redo the job", zap.Any("job", job))
					return status, nil
				}
				status.AddTable(job.TableID, &model.TableReplicaInfo{
					StartTs:     job.BoundaryTs,
					MarkTableID: 0, // TODO support mark table
				}, job.BoundaryTs)
			case schedulerJobTypeRemoveTable:
				if status == nil {
					log.Warn("task status of the capture is not found, may be the capture is already down. specify a new capture and redo the job", zap.Any("job", job))
					return status, nil
				}
				status.RemoveTable(job.TableID, job.BoundaryTs, false)
			default:
				log.Panic("Unreachable, please report a bug", zap.Any("job", job))
			}
			return status, nil
		})
	}
}

// cleanUpOperations clean up the finished operations.
func (s *schedulerImpl) cleanUpOperations() {
	for captureID, taskStatus := range s.state.TaskStatuses {
		if len(taskStatus.Operation) == 0 {
			continue
		}
		s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, error) {
			for tableID, operation := range status.Operation {
				if operation.Status == model.OperFinished {
					delete(status.Operation, tableID)
				}
			}
			return status, nil
		})
	}
}

func (s *schedulerImpl) rebalance() {
	needRebanlance := false
	totalTableNum := 0
	for _, taskStatus := range s.state.TaskStatuses {
		totalTableNum += len(taskStatus.Tables)
		if len(taskStatus.Tables) == 0 {
			needRebanlance = true
		}
	}
	if !needRebanlance && !s.needRebalanceNextTick {
		return
	}
	s.needRebalanceNextTick = false

	captureNum := len(s.state.TaskStatuses)
	upperLimitPerCapture := int(math.Ceil(float64(totalTableNum) / float64(captureNum)))

	log.Info("Start rebalancing",
		zap.String("changefeed", s.state.ID),
		zap.Int("table-num", totalTableNum),
		zap.Int("capture-num", captureNum),
		zap.Int("target-limit", upperLimitPerCapture))

	for captureID, taskStatus := range s.state.TaskStatuses {
		tableNum2Remove := len(taskStatus.Tables) - upperLimitPerCapture
		if tableNum2Remove <= 0 {
			continue
		}
		for tableID := range taskStatus.Tables {
			if tableNum2Remove <= 0 {
				break
			}
			s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, error) {
				if status == nil {
					// the capture may be down, just skip remove this table
					return status, nil
				}
				if status.Operation != nil && status.Operation[tableID] != nil {
					// skip remove this table to avoid the remove operation created by rebalance function to influence the operation created by other function
					return status, nil
				}
				status.RemoveTable(tableID, s.state.Status.ResolvedTs, false)
				return status, nil
			})
			log.Info("Rebalance: Remove table",
				zap.Int64("table-id", tableID),
				zap.String("capture", captureID),
				zap.String("changefeed-id", s.state.ID))
			tableNum2Remove--
		}
	}
}
