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
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
)

// TaskStatus represents status of a task
type TaskStatus struct {
	ExpectedStage metadata.TaskStage
	WorkerID      libModel.WorkerID
	Status        *dmpkg.QueryStatusResponse
}

// JobStatus represents status of a job
type JobStatus struct {
	JobMasterID libModel.MasterID
	WorkerID    libModel.WorkerID
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
