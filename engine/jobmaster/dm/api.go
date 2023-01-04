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

	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	dmproto "github.com/pingcap/tiflow/engine/pkg/dm/proto"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// TaskStatus represents status of a task
type TaskStatus struct {
	ExpectedStage  metadata.TaskStage           `json:"expected_stage"`
	WorkerID       frameModel.WorkerID          `json:"worker_id"`
	ConfigOutdated bool                         `json:"config_outdated"`
	Status         *dmproto.QueryStatusResponse `json:"status"`
	Duration       time.Duration                `json:"duration"`
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
				queryStatusResp *dmproto.QueryStatusResponse
				workerID        string
				cfgModRevision  uint64
				expectedStage   metadata.TaskStage
				createdTime     time.Time
				duration        time.Duration
			)

			// task not exist
			if t, ok := job.Tasks[taskID]; !ok {
				queryStatusResp = &dmproto.QueryStatusResponse{ErrorMsg: fmt.Sprintf("task %s for job not found", taskID)}
			} else {
				expectedStage = t.Stage
				workerStatus, ok := workerStatusMap[taskID]
				if !ok {
					// worker unscheduled
					queryStatusResp = &dmproto.QueryStatusResponse{ErrorMsg: fmt.Sprintf("worker for task %s not found", taskID)}
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
func (jm *JobMaster) QueryStatus(ctx context.Context, taskID string) *dmproto.QueryStatusResponse {
	req := &dmproto.QueryStatusRequest{
		Task: taskID,
	}
	resp, err := jm.messageAgent.SendRequest(ctx, taskID, dmproto.QueryStatus, req)
	if err != nil {
		return &dmproto.QueryStatusResponse{ErrorMsg: err.Error()}
	}
	return resp.(*dmproto.QueryStatusResponse)
}

// operateTask operate task.
func (jm *JobMaster) operateTask(ctx context.Context, op dmproto.OperateType, cfg *config.JobCfg, tasks []string) error {
	switch op {
	case dmproto.Resume, dmproto.Pause, dmproto.Update:
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
	if err := jm.operateTask(ctx, dmproto.Update, cfg, nil); err != nil {
		return err
	}
	// we don't know whether we can remove the old checkpoint, so we just create new checkpoint when update.
	if err := jm.checkpointAgent.Create(ctx, cfg); err != nil {
		return err
	}
	if err := jm.ddlCoordinator.Reset(ctx); err != nil {
		return err
	}

	jm.workerManager.SetNextCheckTime(time.Now())
	return nil
}

// Binlog implements the api of binlog request.
func (jm *JobMaster) Binlog(ctx context.Context, req *dmproto.BinlogRequest) (*dmproto.BinlogResponse, error) {
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
		binlogResp = &dmproto.BinlogResponse{
			Results: make(map[string]*dmproto.CommonTaskResponse, len(req.Sources)),
		}
	)
	for _, task := range req.Sources {
		taskID := task
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := &dmproto.BinlogTaskRequest{
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
func (jm *JobMaster) BinlogTask(ctx context.Context, taskID string, req *dmproto.BinlogTaskRequest) *dmproto.CommonTaskResponse {
	// TODO: we may check the workerType via TaskManager/WorkerManger to reduce request connection.
	resp, err := jm.messageAgent.SendRequest(ctx, taskID, dmproto.BinlogTask, req)
	if err != nil {
		return &dmproto.CommonTaskResponse{ErrorMsg: err.Error()}
	}
	return resp.(*dmproto.CommonTaskResponse)
}

// BinlogSchema implements the api of binlog schema request.
func (jm *JobMaster) BinlogSchema(ctx context.Context, req *dmproto.BinlogSchemaRequest) *dmproto.BinlogSchemaResponse {
	if len(req.Sources) == 0 {
		return &dmproto.BinlogSchemaResponse{ErrorMsg: "must specify at least one source"}
	}

	var (
		mu                   sync.Mutex
		wg                   sync.WaitGroup
		binlogSchemaResponse = &dmproto.BinlogSchemaResponse{
			Results: make(map[string]*dmproto.CommonTaskResponse, len(req.Sources)),
		}
	)
	for _, task := range req.Sources {
		taskID := task
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := &dmproto.BinlogSchemaTaskRequest{
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
func (jm *JobMaster) BinlogSchemaTask(ctx context.Context, taskID string, req *dmproto.BinlogSchemaTaskRequest) *dmproto.CommonTaskResponse {
	// TODO: we may check the workerType via TaskManager/WorkerManger to reduce request connection.
	resp, err := jm.messageAgent.SendRequest(ctx, taskID, dmproto.BinlogSchemaTask, req)
	if err != nil {
		return &dmproto.CommonTaskResponse{ErrorMsg: err.Error()}
	}
	return resp.(*dmproto.CommonTaskResponse)
}

// ShowDDLLocks implements the api of show ddl locks request.
func (jm *JobMaster) ShowDDLLocks(ctx context.Context) ShowDDLLocksResponse {
	return jm.ddlCoordinator.ShowDDLLocks(ctx)
}

// DeleteDDLLocks implements the api of delete ddl locks request.
func (jm *JobMaster) DeleteDDLLocks(ctx context.Context) error {
	if err := jm.ddlCoordinator.Reset(ctx); err != nil {
		return err
	}
	if needed, err := jm.needRestartAllWorkers(ctx); err != nil {
		return errors.Trace(err)
	} else if needed {
		if err := jm.restartAllWorkers(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// CoordinateDDL implements the api of coordinate ddl request.
func (jm *JobMaster) CoordinateDDL(ctx context.Context, req *dmproto.CoordinateDDLRequest) *dmproto.CoordinateDDLResponse {
	ddls, conflictStage, err := jm.ddlCoordinator.Coordinate(ctx, (*metadata.DDLItem)(req))
	if conflictStage == optimism.ConflictNeedRestart {
		if err := jm.restartAllWorkers(ctx); err != nil {
			jm.Logger().Error("fail to restart all workers", zap.Error(err))
		} else if err := jm.ddlCoordinator.Reset(ctx); err != nil {
			jm.Logger().Error("fail to restart coordinate ddl", zap.Error(err))
		}
	}
	resp := &dmproto.CoordinateDDLResponse{
		DDLs:          ddls,
		ConflictStage: conflictStage,
	}
	if err != nil {
		resp.ErrorMsg = err.Error()
	}
	return resp
}

// Redirect is the function declaration for message agent to receive redirect ddl response.
func (jm *JobMaster) RedirectDDL(ctx context.Context) *dmproto.CommonTaskResponse {
	return nil
}
