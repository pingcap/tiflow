package dm

import (
	"context"
	"fmt"
	"sync"

	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	dmpkg "github.com/hanfei1991/microcosm/pkg/dm"
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
				queryStatusResp *dmpkg.QueryStatusResponse
				workerID        string
			)

			workerStatus, ok := workerStatusMap[taskID]
			if !ok {
				// worker unscheduled
				queryStatusResp = &dmpkg.QueryStatusResponse{ErrorMsg: fmt.Sprintf("worker for task %s not found", taskID)}
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
func (jm *JobMaster) QueryTaskStatus(ctx context.Context, taskID string, workerStatus runtime.WorkerStatus) *dmpkg.QueryStatusResponse {
	if workerStatus.Stage == runtime.WorkerFinished {
		status, ok := jm.taskManager.GetTaskStatus(taskID)
		if !ok || status.GetStage() != metadata.StageFinished {
			return &dmpkg.QueryStatusResponse{ErrorMsg: fmt.Sprintf("finished task status for task %s not found", taskID)}
		}
		return &dmpkg.QueryStatusResponse{TaskStatus: status}
	}

	taskStatus, err := jm.messageAgent.QueryStatus(ctx, taskID)
	if err != nil {
		return &dmpkg.QueryStatusResponse{ErrorMsg: err.Error()}
	}
	return taskStatus
}
