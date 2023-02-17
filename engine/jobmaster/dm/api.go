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

package dm

import (
	"context"
	"fmt"
	"sync"
	"time"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/pkg/errors"
)

// TaskStatus represents status of a task
type TaskStatus struct {
	ExpectedStage  metadata.TaskStage         `json:"expected_stage"`
	WorkerID       frameModel.WorkerID        `json:"worker_id"`
	ConfigOutdated bool                       `json:"config_outdated"`
	Status         *dmpkg.QueryStatusResponse `json:"status"`
	Duration       time.Duration              `json:"duration"`
}

// JobStatus represents status of a job
type JobStatus struct {
	JobID frameModel.MasterID `json:"job_id"`
	// taskID -> Status
	TaskStatus map[string]TaskStatus `json:"task_status"`
	// FinishedUnitStatus records the finished unit status of a task. This field
	// is not atomic with TaskStatus (current status).
	FinishedUnitStatus map[string][]*metadata.FinishedTaskStatus `json:"finished_unit_status,omitempty"`
}

// ShardTable represents create table statements of a source table.
type ShardTable struct {
	Current string
	Next    string
}

// DDLLock represents ddl lock of a target table.
type DDLLock struct {
	// source table -> [current table, pending table(conflict table)]
	ShardTables map[metadata.SourceTable]ShardTable
}

// ShowDDLLocksResponse represents response of show ddl locks.
type ShowDDLLocksResponse struct {
	Locks map[metadata.TargetTable]DDLLock
}

// QueryJobStatus is the api of query job status.
func (jm *JobMaster) QueryJobStatus(ctx context.Context, tasks []string) (*JobStatus, error) {
	jobState, err := jm.metadata.JobStore().Get(ctx)
	if err != nil {
		return nil, err
	}
	job := jobState.(*metadata.Job)

	if len(tasks) == 0 {
		for task := range job.Tasks {
			tasks = append(tasks, task)
		}
	}

	var expectedCfgModRevision uint64
	for _, task := range job.Tasks {
		expectedCfgModRevision = task.Cfg.ModRevision
		break
	}

	var (
		workerStatusMap = jm.workerManager.WorkerStatus()
		wg              sync.WaitGroup
		mu              sync.Mutex
		jobStatus       = &JobStatus{
			JobID:      jm.ID(),
			TaskStatus: make(map[string]TaskStatus),
		}
		unitState      *metadata.UnitState
		existUnitState bool
	)

	// need to get unit state here, so we calculate duration
	state, err := jm.metadata.UnitStateStore().Get(ctx)
	if err != nil && errors.Cause(err) != metadata.ErrStateNotFound {
		return nil, err
	}
	unitState, existUnitState = state.(*metadata.UnitState)
	for _, task := range tasks {
		taskID := task
		wg.Add(1)
		go func() {
			defer wg.Done()

			var (
				queryStatusResp *dmpkg.QueryStatusResponse
				workerID        string
				cfgModRevision  uint64
				expectedStage   metadata.TaskStage
				createdTime     time.Time
				duration        time.Duration
			)

			// task not exist
			if t, ok := job.Tasks[taskID]; !ok {
				queryStatusResp = &dmpkg.QueryStatusResponse{ErrorMsg: fmt.Sprintf("task %s for job not found", taskID)}
			} else {
				expectedStage = t.Stage
				workerStatus, ok := workerStatusMap[taskID]
				if !ok {
					// worker unscheduled
					queryStatusResp = &dmpkg.QueryStatusResponse{ErrorMsg: fmt.Sprintf("worker for task %s not found", taskID)}
				} else if workerStatus.Stage != runtime.WorkerFinished {
					workerID = workerStatus.ID
					cfgModRevision = workerStatus.CfgModRevision
					queryStatusResp = jm.QueryStatus(ctx, taskID)
				}
			}

			if existUnitState {
				if status, ok := unitState.CurrentUnitStatus[taskID]; ok {
					createdTime = status.CreatedTime
				}
			}
			if !createdTime.IsZero() {
				duration = time.Since(createdTime)
			}

			mu.Lock()
			jobStatus.TaskStatus[taskID] = TaskStatus{
				ExpectedStage:  expectedStage,
				WorkerID:       workerID,
				Status:         queryStatusResp,
				ConfigOutdated: cfgModRevision != expectedCfgModRevision,
				Duration:       duration,
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	// should be done after we get current task-status, since some unit status might be missing if
	// current unit finish between we get finished unit status and get current task-status
	state, err = jm.metadata.UnitStateStore().Get(ctx)
	if err != nil && errors.Cause(err) != metadata.ErrStateNotFound {
		return nil, err
	}
	unitState, existUnitState = state.(*metadata.UnitState)
	if existUnitState {
		jobStatus.FinishedUnitStatus = unitState.FinishedUnitStatus
	}

	return jobStatus, nil
}

// QueryStatus query status for a task
func (jm *JobMaster) QueryStatus(ctx context.Context, taskID string) *dmpkg.QueryStatusResponse {
	req := &dmpkg.QueryStatusRequest{
		Task: taskID,
	}
	resp, err := jm.messageAgent.SendRequest(ctx, taskID, dmpkg.QueryStatus, req)
	if err != nil {
		return &dmpkg.QueryStatusResponse{ErrorMsg: err.Error()}
	}
	return resp.(*dmpkg.QueryStatusResponse)
}

// operateTask operate task.
func (jm *JobMaster) operateTask(ctx context.Context, op dmpkg.OperateType, cfg *config.JobCfg, tasks []string) error {
	switch op {
	case dmpkg.Resume, dmpkg.Pause, dmpkg.Update:
		return jm.taskManager.OperateTask(ctx, op, cfg, tasks)
	default:
		return errors.Errorf("unsupported op type %d for operate task", op)
	}
}

// GetJobCfg gets job config.
func (jm *JobMaster) GetJobCfg(ctx context.Context) (*config.JobCfg, error) {
	return jm.metadata.JobStore().GetJobCfg(ctx)
}

// UpdateJobCfg updates job config.
func (jm *JobMaster) UpdateJobCfg(ctx context.Context, cfg *config.JobCfg) error {
	if err := jm.preCheck(ctx, cfg); err != nil {
		return err
	}
	if err := jm.operateTask(ctx, dmpkg.Update, cfg, nil); err != nil {
		return err
	}
	// we don't know whether we can remove the old checkpoint, so we just create new checkpoint when update.
	if err := jm.checkpointAgent.Create(ctx, cfg); err != nil {
		return err
	}

	jm.workerManager.SetNextCheckTime(time.Now())
	return nil
}

// Binlog implements the api of binlog request.
func (jm *JobMaster) Binlog(ctx context.Context, req *dmpkg.BinlogRequest) (*dmpkg.BinlogResponse, error) {
	if len(req.Sources) == 0 {
		state, err := jm.metadata.JobStore().Get(ctx)
		if err != nil {
			return nil, err
		}
		job := state.(*metadata.Job)
		for task := range job.Tasks {
			req.Sources = append(req.Sources, task)
		}
	}

	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		binlogResp = &dmpkg.BinlogResponse{
			Results: make(map[string]*dmpkg.CommonTaskResponse, len(req.Sources)),
		}
	)
	for _, task := range req.Sources {
		taskID := task
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := &dmpkg.BinlogTaskRequest{
				Op:        req.Op,
				BinlogPos: req.BinlogPos,
				Sqls:      req.Sqls,
			}
			resp := jm.BinlogTask(ctx, taskID, req)
			mu.Lock()
			binlogResp.Results[taskID] = resp
			mu.Unlock()
		}()
	}
	wg.Wait()
	return binlogResp, nil
}

// BinlogTask implements the api of binlog task request.
func (jm *JobMaster) BinlogTask(ctx context.Context, taskID string, req *dmpkg.BinlogTaskRequest) *dmpkg.CommonTaskResponse {
	// TODO: we may check the workerType via TaskManager/WorkerManger to reduce request connection.
	resp, err := jm.messageAgent.SendRequest(ctx, taskID, dmpkg.BinlogTask, req)
	if err != nil {
		return &dmpkg.CommonTaskResponse{ErrorMsg: err.Error()}
	}
	return resp.(*dmpkg.CommonTaskResponse)
}

// BinlogSchema implements the api of binlog schema request.
func (jm *JobMaster) BinlogSchema(ctx context.Context, req *dmpkg.BinlogSchemaRequest) *dmpkg.BinlogSchemaResponse {
	if len(req.Sources) == 0 {
		return &dmpkg.BinlogSchemaResponse{ErrorMsg: "must specify at least one source"}
	}

	var (
		mu                   sync.Mutex
		wg                   sync.WaitGroup
		binlogSchemaResponse = &dmpkg.BinlogSchemaResponse{
			Results: make(map[string]*dmpkg.CommonTaskResponse, len(req.Sources)),
		}
	)
	for _, task := range req.Sources {
		taskID := task
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := &dmpkg.BinlogSchemaTaskRequest{
				Op:         req.Op,
				Source:     taskID,
				Database:   req.Database,
				Table:      req.Table,
				Schema:     req.Schema,
				Flush:      req.Flush,
				Sync:       req.Sync,
				FromSource: req.FromSource,
				FromTarget: req.FromTarget,
			}
			resp := jm.BinlogSchemaTask(ctx, taskID, req)
			mu.Lock()
			binlogSchemaResponse.Results[taskID] = resp
			mu.Unlock()
		}()
	}
	wg.Wait()
	return binlogSchemaResponse
}

// BinlogSchemaTask implements the api of binlog schema task request.
func (jm *JobMaster) BinlogSchemaTask(ctx context.Context, taskID string, req *dmpkg.BinlogSchemaTaskRequest) *dmpkg.CommonTaskResponse {
	// TODO: we may check the workerType via TaskManager/WorkerManger to reduce request connection.
	resp, err := jm.messageAgent.SendRequest(ctx, taskID, dmpkg.BinlogSchemaTask, req)
	if err != nil {
		return &dmpkg.CommonTaskResponse{ErrorMsg: err.Error()}
	}
	return resp.(*dmpkg.CommonTaskResponse)
}

// ShowDDLLocks implements the api of show ddl locks request.
func (jm *JobMaster) ShowDDLLocks(ctx context.Context) ShowDDLLocksResponse {
	return jm.ddlCoordinator.ShowDDLLocks(ctx)
}
