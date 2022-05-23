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

	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
)

// TaskStatus represents status of a task
type TaskStatus struct {
	ExpectedStage metadata.TaskStage
	WorkerID      libModel.WorkerID
	Status        dmpkg.QueryStatusResponse
}

// JobStatus represents status of a job
type JobStatus struct {
	JobMasterID libModel.MasterID
	WorkerID    libModel.WorkerID
	// taskID -> Status
	TaskStatus map[string]TaskStatus
}

// QueryStatus is the api of query status request
func (jm *JobMaster) QueryStatus(ctx context.Context, tasks []string) (*JobStatus, error) {
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
				queryStatusResp dmpkg.QueryStatusResponse
				workerID        string
			)

			workerStatus, ok := workerStatusMap[taskID]
			if !ok {
				// worker unscheduled
				queryStatusResp = dmpkg.QueryStatusResponse{ErrorMsg: fmt.Sprintf("worker for task %s not found", taskID)}
			} else {
				workerID = workerStatus.ID
				queryStatusResp = jm.QueryTaskStatus(ctx, taskID, workerStatus)
			}

			mu.Lock()
			jobStatus.TaskStatus[taskID] = TaskStatus{
				ExpectedStage: job.Tasks[taskID].Stage,
				WorkerID:      workerID,
				Status:        queryStatusResp,
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return jobStatus, nil
}

// QueryTaskStatus query status for a task
func (jm *JobMaster) QueryTaskStatus(ctx context.Context, taskID string, workerStatus runtime.WorkerStatus) dmpkg.QueryStatusResponse {
	if workerStatus.Stage == runtime.WorkerFinished {
		status, ok := jm.taskManager.GetTaskStatus(taskID)
		if !ok || status.GetStage() != metadata.StageFinished {
			return dmpkg.QueryStatusResponse{ErrorMsg: fmt.Sprintf("finished task status for task %s not found", taskID)}
		}
		return dmpkg.QueryStatusResponse{TaskStatus: status}
	}

	req := dmpkg.QueryStatusRequest{
		Task: taskID,
	}
	resp, err := jm.messageAgent.SendRequest(ctx, taskID, dmpkg.QueryStatus, req)
	if err != nil {
		return dmpkg.QueryStatusResponse{ErrorMsg: err.Error()}
	}
	return resp.(dmpkg.QueryStatusResponse)
}
