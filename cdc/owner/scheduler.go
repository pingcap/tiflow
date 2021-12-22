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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
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
	state         *model.ChangefeedReactorState
	currentTables []model.TableID
	captures      map[model.CaptureID]*model.CaptureInfo

	moveTableTargets      map[model.TableID]model.CaptureID
	moveTableJobQueue     []*moveTableJob
	needRebalanceNextTick bool
	lastTickCaptureCount  int
}

func newScheduler() *scheduler {
	return &scheduler{
		moveTableTargets: make(map[model.TableID]model.CaptureID),
	}
}

// Tick is the main function of scheduler. It dispatches tables to captures and handles move-table and rebalance events.
// Tick returns a bool representing whether the changefeed's state can be updated in this tick.
// The state can be updated only if all the tables which should be listened to have been dispatched to captures and no operations have been sent to captures in this tick.
func (s *scheduler) Tick(state *model.ChangefeedReactorState, currentTables []model.TableID, captures map[model.CaptureID]*model.CaptureInfo) (shouldUpdateState bool, err error) {
	s.state = state
	s.currentTables = currentTables
	s.captures = captures

	s.cleanUpFinishedOperations()
	pendingJob, err := s.syncTablesWithCurrentTables()
	if err != nil {
		return false, errors.Trace(err)
	}
	s.dispatchToTargetCaptures(pendingJob)
	if len(pendingJob) != 0 {
		log.Debug("scheduler:generated pending job to be executed", zap.Any("pendingJob", pendingJob))
	}
	s.handleJobs(pendingJob)

	// only if the pending job list is empty and no table is being rebalanced or moved,
	// can the global resolved ts and checkpoint ts be updated
	shouldUpdateState = len(pendingJob) == 0
	shouldUpdateState = s.rebalance() && shouldUpdateState
	shouldUpdateStateInMoveTable, err := s.handleMoveTableJob()
	if err != nil {
		return false, errors.Trace(err)
	}
	shouldUpdateState = shouldUpdateStateInMoveTable && shouldUpdateState
	s.lastTickCaptureCount = len(captures)
	return shouldUpdateState, nil
}

func (s *scheduler) MoveTable(tableID model.TableID, target model.CaptureID) {
	s.moveTableJobQueue = append(s.moveTableJobQueue, &moveTableJob{
		tableID: tableID,
		target:  target,
	})
}

// handleMoveTableJob handles the move table job add be MoveTable function
func (s *scheduler) handleMoveTableJob() (shouldUpdateState bool, err error) {
	shouldUpdateState = true
	if len(s.moveTableJobQueue) == 0 {
		return
	}
	table2CaptureIndex, err := s.table2CaptureIndex()
	if err != nil {
		return false, errors.Trace(err)
	}
	for _, job := range s.moveTableJobQueue {
		source, exist := table2CaptureIndex[job.tableID]
		if !exist {
			return
		}
		s.moveTableTargets[job.tableID] = job.target
		job := job
		shouldUpdateState = false
		// for all move table job, here just remove the table from the source capture.
		// and the table removed by this function will be added to target capture by syncTablesWithCurrentTables in the next tick.
		s.state.PatchTaskStatus(source, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			if status == nil {
				// the capture may be down, just skip remove this table
				return status, false, nil
			}
			if status.Operation != nil && status.Operation[job.tableID] != nil {
				// skip removing this table to avoid the remove operation created by the rebalance function interfering with the operation created by another function
				return status, false, nil
			}
			status.RemoveTable(job.tableID, s.state.Status.CheckpointTs, false)
			return status, true, nil
		})
	}
	s.moveTableJobQueue = nil
	return
}

func (s *scheduler) Rebalance() {
	s.needRebalanceNextTick = true
}

func (s *scheduler) table2CaptureIndex() (map[model.TableID]model.CaptureID, error) {
	table2CaptureIndex := make(map[model.TableID]model.CaptureID)
	for captureID, taskStatus := range s.state.TaskStatuses {
		for tableID := range taskStatus.Tables {
			if preCaptureID, exist := table2CaptureIndex[tableID]; exist && preCaptureID != captureID {
				return nil, cerror.ErrTableListenReplicated.GenWithStackByArgs(tableID, preCaptureID, captureID)
			}
			table2CaptureIndex[tableID] = captureID
		}
		for tableID := range taskStatus.Operation {
			if preCaptureID, exist := table2CaptureIndex[tableID]; exist && preCaptureID != captureID {
				return nil, cerror.ErrTableListenReplicated.GenWithStackByArgs(tableID, preCaptureID, captureID)
			}
			table2CaptureIndex[tableID] = captureID
		}
	}
	return table2CaptureIndex, nil
}

// dispatchToTargetCaptures sets the the TargetCapture of scheduler jobs
// If the TargetCapture of a job is not set, it chooses a capture with the minimum workload and sets the TargetCapture to the capture.
func (s *scheduler) dispatchToTargetCaptures(pendingJobs []*schedulerJob) {
	workloads := make(map[model.CaptureID]uint64)

	for captureID := range s.captures {
		workloads[captureID] = 0
		taskWorkload := s.state.Workloads[captureID]
		if taskWorkload == nil {
			continue
		}
		for _, workload := range taskWorkload {
			workloads[captureID] += workload.Workload
		}
	}

	for _, pendingJob := range pendingJobs {
		if pendingJob.TargetCapture == "" {
			target, exist := s.moveTableTargets[pendingJob.TableID]
			if !exist {
				continue
			}
			pendingJob.TargetCapture = target
			delete(s.moveTableTargets, pendingJob.TableID)
			continue
		}
		switch pendingJob.Tp {
		case schedulerJobTypeAddTable:
			workloads[pendingJob.TargetCapture] += 1
		case schedulerJobTypeRemoveTable:
			workloads[pendingJob.TargetCapture] -= 1
		default:
			log.Panic("Unreachable, please report a bug",
				zap.String("changefeed", s.state.ID), zap.Any("job", pendingJob))
		}
	}

	getMinWorkloadCapture := func() model.CaptureID {
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

	for _, pendingJob := range pendingJobs {
		if pendingJob.TargetCapture != "" {
			continue
		}
		minCapture := getMinWorkloadCapture()
		pendingJob.TargetCapture = minCapture
		workloads[minCapture] += 1
	}
}

// syncTablesWithCurrentTables iterates all current tables to check whether it should be listened or not.
// this function will return schedulerJob to make sure all tables will be listened.
func (s *scheduler) syncTablesWithCurrentTables() ([]*schedulerJob, error) {
	var pendingJob []*schedulerJob
	allTableListeningNow, err := s.table2CaptureIndex()
	if err != nil {
		return nil, errors.Trace(err)
	}
	globalCheckpointTs := s.state.Status.CheckpointTs
	for _, tableID := range s.currentTables {
		if _, exist := allTableListeningNow[tableID]; exist {
			delete(allTableListeningNow, tableID)
			continue
		}
		// For each table which should be listened but is not, add an adding-table job to the pending job list
		pendingJob = append(pendingJob, &schedulerJob{
			Tp:         schedulerJobTypeAddTable,
			TableID:    tableID,
			BoundaryTs: globalCheckpointTs,
		})
	}
	// The remaining tables are the tables which should be not listened
	tablesThatShouldNotBeListened := allTableListeningNow
	for tableID, captureID := range tablesThatShouldNotBeListened {
		opts := s.state.TaskStatuses[captureID].Operation
		if opts != nil && opts[tableID] != nil && opts[tableID].Delete {
			// the table is being removed, skip
			continue
		}
		pendingJob = append(pendingJob, &schedulerJob{
			Tp:            schedulerJobTypeRemoveTable,
			TableID:       tableID,
			BoundaryTs:    globalCheckpointTs,
			TargetCapture: captureID,
		})
	}
	return pendingJob, nil
}

func (s *scheduler) handleJobs(jobs []*schedulerJob) {
	for _, job := range jobs {
		job := job
		s.state.PatchTaskStatus(job.TargetCapture, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			switch job.Tp {
			case schedulerJobTypeAddTable:
				if status == nil {
					// if task status is not found, we can just skip adding the adding-table operation, since this table will be added in the next tick
					log.Warn("task status of the capture is not found, may be the capture is already down. specify a new capture and redo the job", zap.Any("job", job))
					return status, false, nil
				}
				status.AddTable(job.TableID, &model.TableReplicaInfo{
					StartTs:     job.BoundaryTs,
					MarkTableID: 0, // mark table ID will be set in processors
				}, job.BoundaryTs)
			case schedulerJobTypeRemoveTable:
				failpoint.Inject("OwnerRemoveTableError", func() {
					// just skip removing this table
					failpoint.Return(status, false, nil)
				})
				if status == nil {
					log.Warn("Task status of the capture is not found. Maybe the capture is already down. Specify a new capture and redo the job", zap.Any("job", job))
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

// cleanUpFinishedOperations clean up the finished operations.
func (s *scheduler) cleanUpFinishedOperations() {
	for captureID := range s.state.TaskStatuses {
		s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			if status == nil {
				log.Warn("task status of the capture is not found, may be the key in etcd was deleted", zap.String("captureID", captureID), zap.String("changeFeedID", s.state.ID))
				return status, false, nil
			}

			changed := false
			for tableID, operation := range status.Operation {
				if operation.Status == model.OperFinished {
					delete(status.Operation, tableID)
					changed = true
				}
			}
			return status, changed, nil
		})
	}
}

func (s *scheduler) rebalance() (shouldUpdateState bool) {
	if !s.shouldRebalance() {
		// if no table is rebalanced, we can update the resolved ts and checkpoint ts
		return true
	}
	// we only support rebalance by table number for now
	return s.rebalanceByTableNum()
}

func (s *scheduler) shouldRebalance() bool {
	if s.needRebalanceNextTick {
		s.needRebalanceNextTick = false
		return true
	}
	if s.lastTickCaptureCount != len(s.captures) {
		// a new capture online and no table distributed to the capture
		// or some captures offline
		return true
	}
	// TODO periodic trigger rebalance
	return false
}

// rebalanceByTableNum removes tables from captures replicating an above-average number of tables.
// the removed table will be dispatched again by syncTablesWithCurrentTables function
func (s *scheduler) rebalanceByTableNum() (shouldUpdateState bool) {
	totalTableNum := len(s.currentTables)
	captureNum := len(s.captures)
	upperLimitPerCapture := int(math.Ceil(float64(totalTableNum) / float64(captureNum)))
	shouldUpdateState = true

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

		// here we pick `tableNum2Remove` tables to delete,
		// and then the removed tables will be dispatched by `syncTablesWithCurrentTables` function in the next tick
		for tableID := range taskStatus.Tables {
			tableID := tableID
			if tableNum2Remove <= 0 {
				break
			}
			shouldUpdateState = false
			s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
				if status == nil {
					// the capture may be down, just skip remove this table
					return status, false, nil
				}
				if status.Operation != nil && status.Operation[tableID] != nil {
					// skip remove this table to avoid the remove operation created by rebalance function to influence the operation created by other function
					return status, false, nil
				}
				status.RemoveTable(tableID, s.state.Status.CheckpointTs, false)
				log.Info("Rebalance: Move table",
					zap.Int64("table-id", tableID),
					zap.String("capture", captureID),
					zap.String("changefeed-id", s.state.ID))
				return status, true, nil
			})
			tableNum2Remove--
		}
	}
	return
}
