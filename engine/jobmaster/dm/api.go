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
	"encoding/json"
	"fmt"
	"sync"

	"github.com/pingcap/errors"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
)

// TaskStatus represents status of a task
type TaskStatus struct {
	ExpectedStage metadata.TaskStage
	WorkerID      frameModel.WorkerID
	Status        *dmpkg.QueryStatusResponse
}

// JobStatus represents status of a job
type JobStatus struct {
	JobMasterID frameModel.MasterID
	WorkerID    frameModel.WorkerID
	// taskID -> Status
	TaskStatus map[string]TaskStatus
}

// QueryJobStatus is the api of query job status.
func (jm *JobMaster) QueryJobStatus(ctx context.Context, tasks []string) (*JobStatus, error) {
	state, err := jm.metadata.JobStore().Get(ctx)
	if err != nil {
		return nil, err
	}
	job := state.(*metadata.Job)

	if len(tasks) == 0 {
		for task := range job.Tasks {
			tasks = append(tasks, task)
		}
	}

	var (
		workerStatusMap = jm.workerManager.WorkerStatus()
		wg              sync.WaitGroup
		mu              sync.Mutex
		jobStatus       = &JobStatus{
			JobMasterID: jm.JobMasterID(),
			WorkerID:    jm.ID(),
			TaskStatus:  make(map[string]TaskStatus),
		}
	)

	for _, task := range tasks {
		taskID := task
		wg.Add(1)
		go func() {
			defer wg.Done()

			var (
				queryStatusResp *dmpkg.QueryStatusResponse
				workerID        string
				expectedStage   metadata.TaskStage
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
				} else if workerStatus.Stage == runtime.WorkerFinished {
					// task finished
					workerID = workerStatus.ID
					queryStatusResp = &dmpkg.QueryStatusResponse{Unit: workerStatus.Unit, Stage: metadata.StageFinished}
				} else {
					workerID = workerStatus.ID
					queryStatusResp = jm.QueryStatus(ctx, taskID)
				}
			}

			mu.Lock()
			jobStatus.TaskStatus[taskID] = TaskStatus{
				ExpectedStage: expectedStage,
				WorkerID:      workerID,
				Status:        queryStatusResp,
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
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

// OperateTask operate task.
func (jm *JobMaster) OperateTask(ctx context.Context, op dmpkg.OperateType, cfg *config.JobCfg, tasks []string) error {
	switch op {
	case dmpkg.Resume, dmpkg.Pause:
		return jm.taskManager.OperateTask(ctx, op, cfg, tasks)
	default:
		return errors.Errorf("unsupport op type %d for operate task", op)
	}
}

// GetJobCfg gets job config.
func (jm *JobMaster) GetJobCfg(ctx context.Context) (*config.JobCfg, error) {
	state, err := jm.metadata.JobStore().Get(ctx)
	if err != nil {
		return nil, err
	}
	job := state.(*metadata.Job)

	taskCfgs := make([]*config.TaskCfg, 0, len(job.Tasks))
	for _, task := range job.Tasks {
		taskCfgs = append(taskCfgs, task.Cfg)
	}
	return config.FromTaskCfgs(taskCfgs), nil
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

// DebugJob debugs job.
func (jm *JobMaster) DebugJob(ctx context.Context, req *pb.DebugJobRequest) *pb.DebugJobResponse {
	var (
		resp interface{}
		err  error
	)
	switch req.Command {
	case dmpkg.QueryStatus:
		var jsonArg struct {
			Tasks []string
		}
		if err := json.Unmarshal([]byte(req.JsonArg), &jsonArg); err != nil {
			return &pb.DebugJobResponse{Err: &pb.Error{
				Code:    pb.ErrorCode_UnknownError,
				Message: err.Error(),
			}}
		}
		resp, err = jm.QueryJobStatus(ctx, jsonArg.Tasks)
	case dmpkg.OperateTask:
		var jsonArg struct {
			Tasks []string
			Op    dmpkg.OperateType
		}
		if err := json.Unmarshal([]byte(req.JsonArg), &jsonArg); err != nil {
			return &pb.DebugJobResponse{Err: &pb.Error{
				Code:    pb.ErrorCode_UnknownError,
				Message: err.Error(),
			}}
		}
		err = jm.OperateTask(ctx, jsonArg.Op, nil, jsonArg.Tasks)
	case dmpkg.GetJobCfg:
		resp, err = jm.GetJobCfg(ctx)
	case dmpkg.Binlog:
		var binlogReq dmpkg.BinlogRequest
		if err := json.Unmarshal([]byte(req.JsonArg), &binlogReq); err != nil {
			return &pb.DebugJobResponse{Err: &pb.Error{
				Code:    pb.ErrorCode_UnknownError,
				Message: err.Error(),
			}}
		}
		resp, err = jm.Binlog(ctx, &binlogReq)
	case dmpkg.BinlogSchema:
		var binlogSchemaReq dmpkg.BinlogSchemaRequest
		if err := json.Unmarshal([]byte(req.JsonArg), &binlogSchemaReq); err != nil {
			return &pb.DebugJobResponse{Err: &pb.Error{
				Code:    pb.ErrorCode_UnknownError,
				Message: err.Error(),
			}}
		}
		resp = jm.BinlogSchema(ctx, &binlogSchemaReq)
	default:
	}

	if err != nil {
		return &pb.DebugJobResponse{Err: &pb.Error{
			Code:    pb.ErrorCode_UnknownError,
			Message: err.Error(),
		}}
	}
	jsonRet, err := json.Marshal(resp)
	if err != nil {
		return &pb.DebugJobResponse{Err: &pb.Error{
			Code:    pb.ErrorCode_UnknownError,
			Message: err.Error(),
		}}
	}
	return &pb.DebugJobResponse{JsonRet: string(jsonRet)}
}
