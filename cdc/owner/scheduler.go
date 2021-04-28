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

type moveTableJob struct {
	tableID model.TableID
	target  model.CaptureID
}

type scheduler struct {
	state *model.ChangefeedReactorState

	// we need a ttl map here
	moveTableTarget          map[model.TableID]model.CaptureID
	boundaryTsOfRemovedTable map[model.TableID]model.Ts
	moveTableJobQueue        []*moveTableJob
	needRebalanceNextTick    bool
}

func newScheduler() *scheduler {
	return &scheduler{
		moveTableTarget:          make(map[model.TableID]model.CaptureID),
		boundaryTsOfRemovedTable: make(map[model.TableID]model.Ts),
	}
}

// Tick is a main process of scheduler, it calls some internal handle function
// and returns a bool represents Is it possible that there are tables that do not exist in taskStatus
// if some table are not exist in taskStatus(in taskStatus.Tables nor in taskStatus.Operation),
// we should not push up resolvedTs
func (s *scheduler) Tick(state *model.ChangefeedReactorState, allTableShouldBeListened []model.TableID) (allTableInListened bool){
	s.state = state
	s.cleanUpOperations()
	pendingJob := s.syncTablesWithSchemaManager(allTableShouldBeListened)
	s.handleJobs(pendingJob)
	s.rebalance()
	s.handleMoveTableJob()
	return len(pendingJob)== 0
}

func (s *scheduler) MoveTable(tableID model.TableID, target model.CaptureID) {
	s.moveTableJobQueue = append(s.moveTableJobQueue, &moveTableJob{
		tableID: tableID,
		target:  target,
	})
}

func (s *scheduler) handleMoveTableJob() {
	if len(s.moveTableJobQueue) == 0 {
		return
	}
	table2CaptureIndex := s.table2CaptureIndex()
	for _, job := range s.moveTableJobQueue {
		source, exist := table2CaptureIndex[job.tableID]
		if !exist {
			return
		}
		s.moveTableTarget[job.tableID] = job.target
		s.state.PatchTaskStatus(source, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			if status == nil {
				// the capture may be down, just skip remove this table
				return status, false, nil
			}
			if status.Operation != nil && status.Operation[job.tableID] != nil {
				// skip remove this table to avoid the remove operation created by rebalance function to influence the operation created by other function
				return status, false, nil
			}
			status.RemoveTable(job.tableID, s.state.Status.ResolvedTs, false)
			return status, true, nil
		})
	}
	s.moveTableJobQueue = nil
}

func (s *scheduler) Rebalance() {
	s.needRebalanceNextTick = true
}

func (s *scheduler) table2CaptureIndex() map[model.TableID]model.CaptureID {
	table2CaptureIndex := make(map[model.TableID]model.CaptureID)
	for captureID, taskStatus := range s.state.TaskStatuses {
		for tableID := range taskStatus.Tables {
			if preCaptureID, exist := table2CaptureIndex[tableID]; exist && preCaptureID != captureID {
				log.Panic("TODO") // TODO
			}
			table2CaptureIndex[tableID] = captureID
		}
		for tableID := range taskStatus.Operation {
			if preCaptureID, exist := table2CaptureIndex[tableID]; exist && preCaptureID != captureID {
				log.Panic("TODO")
			}
			table2CaptureIndex[tableID] = captureID
		}
	}
	return table2CaptureIndex
}

func (s *scheduler) getMinWorkloadCapture(pendingJob []*schedulerJob) model.CaptureID {
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

func (s *scheduler) syncTablesWithSchemaManager(allTableShouldBeListened []model.TableID) []*schedulerJob {
	var pendingJob []*schedulerJob
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
		boundaryTs := globalCheckpointTs + 1
		if boundaryTsOfRemoved, exist := s.boundaryTsOfRemovedTable[tableID]; exist && boundaryTs < boundaryTsOfRemoved {
			boundaryTs = boundaryTsOfRemoved
		}
		// TODO gc about boundaryTsOfRemovedTable
		pendingJob = append(pendingJob, &schedulerJob{
			Tp:            schedulerJobTypeAddTable,
			TableID:       tableID,
			BoundaryTs:    boundaryTs,
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

func (s *scheduler) handleJobs(jobs []*schedulerJob) {
	for _, job := range jobs {
		s.state.PatchTaskStatus(job.TargetCapture, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			switch job.Tp {
			case schedulerJobTypeAddTable:
				if status == nil {
					// if task status is not found, we can just skip to set adding-table operation, this table will be added in next tick
					log.Warn("task status of the capture is not found, may be the capture is already down. specify a new capture and redo the job", zap.Any("job", job))
					return status, false, nil
				}
				status.AddTable(job.TableID, &model.TableReplicaInfo{
					StartTs:     job.BoundaryTs,
					MarkTableID: 0, // TODO support mark table
				}, job.BoundaryTs)
			case schedulerJobTypeRemoveTable:
				if status == nil {
					log.Warn("task status of the capture is not found, may be the capture is already down. specify a new capture and redo the job", zap.Any("job", job))
					return status, false, nil
				}
				status.RemoveTable(job.TableID, job.BoundaryTs, false)
			default:
				log.Panic("Unreachable, please report a bug", zap.Any("job", job))
			}
			return status, true, nil
		})
	}
}

// cleanUpOperations clean up the finished operations.
func (s *scheduler) cleanUpOperations() {
	for captureID := range s.state.TaskStatuses {
		s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			changed := false
			for tableID, operation := range status.Operation {
				if operation.Status == model.OperFinished {
					if operation.Delete {
						s.boundaryTsOfRemovedTable[tableID] = operation.BoundaryTs
					}
					delete(status.Operation, tableID)
					changed = true
				}
			}
			return status, changed, nil
		})
	}
}

func (s *scheduler) rebalance() {
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
			s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
				if status == nil {
					// the capture may be down, just skip remove this table
					return status, false, nil
				}
				if status.Operation != nil && status.Operation[tableID] != nil {
					// skip remove this table to avoid the remove operation created by rebalance function to influence the operation created by other function
					return status, false, nil
				}
				status.RemoveTable(tableID, s.state.Status.ResolvedTs, false)
				return status, true, nil
			})
			log.Info("Rebalance: Move table",
				zap.Int64("table-id", tableID),
				zap.String("capture", captureID),
				zap.String("changefeed-id", s.state.ID))
			tableNum2Remove--
		}
	}
}
