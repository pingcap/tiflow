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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/ticker"
)

var (
	taskNormalInterval = time.Second * 30
	taskErrorInterval  = time.Second * 10
)

// TaskManager checks and operates task.
type TaskManager struct {
	*ticker.DefaultTicker

	jobStore     *metadata.JobStore
	messageAgent dmpkg.MessageAgent
	// tasks record the runtime task status
	// taskID -> TaskStatus
	tasks sync.Map
}

// NewTaskManager creates a new TaskManager instance
func NewTaskManager(initTaskStatus []runtime.TaskStatus, jobStore *metadata.JobStore, messageAgent dmpkg.MessageAgent) *TaskManager {
	taskManager := &TaskManager{
		DefaultTicker: ticker.NewDefaultTicker(taskNormalInterval, taskErrorInterval),
		jobStore:      jobStore,
		messageAgent:  messageAgent,
	}
	taskManager.DefaultTicker.Ticker = taskManager

	for _, taskStatus := range initTaskStatus {
		taskManager.UpdateTaskStatus(taskStatus)
	}
	return taskManager
}

// OperateTask updates the task status in metadata and triggers the task manager to check and operate task.
// called by user request.
func (tm *TaskManager) OperateTask(ctx context.Context, op dmpkg.OperateType, jobCfg *config.JobCfg, tasks []string) (err error) {
	log.L().Info("operate task", zap.Int("op", int(op)), zap.Strings("tasks", tasks))
	defer func() {
		if err == nil {
			tm.SetNextCheckTime(time.Now())
		}
	}()

	var stage metadata.TaskStage
	switch op {
	case dmpkg.Create, dmpkg.Update:
		return tm.jobStore.Put(ctx, metadata.NewJob(jobCfg))
	case dmpkg.Delete:
		return tm.jobStore.Delete(ctx)
	case dmpkg.Resume:
		stage = metadata.StageRunning
	case dmpkg.Pause:
		stage = metadata.StagePaused
	default:
		return errors.New("unknown operate type")
	}

	return tm.jobStore.UpdateStages(ctx, tasks, stage)
}

// UpdateTaskStatus is called when receive task status from worker.
func (tm *TaskManager) UpdateTaskStatus(taskStatus runtime.TaskStatus) {
	log.L().Debug("update task status", zap.String("task_id", taskStatus.Task), zap.Int("stage", int(taskStatus.Stage)), zap.Int("unit", int(taskStatus.Unit)))
	tm.tasks.Store(taskStatus.Task, taskStatus)
}

// TaskStatus return the task status.
func (tm *TaskManager) TaskStatus() map[string]runtime.TaskStatus {
	result := make(map[string]runtime.TaskStatus)
	tm.tasks.Range(func(key, value interface{}) bool {
		result[key.(string)] = value.(runtime.TaskStatus)
		return true
	})
	return result
}

// TickImpl removes tasks that are not in the job config.
// TickImpl checks and operates task if needed.
func (tm *TaskManager) TickImpl(ctx context.Context) error {
	log.L().Info("start to check and operate tasks")
	state, err := tm.jobStore.Get(ctx)
	if err != nil {
		log.L().Error("get job state failed", zap.Error(err))
		tm.onJobNotExist(ctx)
		return err
	}
	job := state.(*metadata.Job)

	tm.removeTaskStatus(job)
	return tm.checkAndOperateTasks(ctx, job)
}

func (tm *TaskManager) checkAndOperateTasks(ctx context.Context, job *metadata.Job) error {
	var (
		runningTask runtime.TaskStatus
		recordError error
	)

	// check and operate task
	for taskID, persistentTask := range job.Tasks {
		task, ok := tm.tasks.Load(taskID)
		if ok {
			runningTask = task.(runtime.TaskStatus)
		}

		// task unbounded or worker offline
		if !ok || runningTask.Stage == metadata.StageUnscheduled {
			recordError = errors.New("get task running status failed")
			log.L().Error("failed to schedule task", zap.String("task_id", taskID), zap.Error(recordError))
			continue
		}

		op := genOp(runningTask.Stage, persistentTask.Stage)
		if op == dmpkg.None {
			log.L().Debug("task status will not be changed", zap.String("task_id", taskID), zap.Int("stage", int(runningTask.Stage)))
			continue
		}

		log.L().Info("unexpected task status", zap.String("task_id", taskID), zap.Int("op", int(op)),
			zap.Int("expected_stage", int(persistentTask.Stage)), zap.Int("stage", int(runningTask.Stage)))
		// operateTaskMessage should be a asynchronous request
		if err := tm.operateTaskMessage(ctx, taskID, op); err != nil {
			recordError = err
			log.L().Error("operate task failed", zap.Error(recordError))
			continue
		}
	}
	return recordError
}

// remove all tasks, usually happened when delete jobs.
func (tm *TaskManager) onJobNotExist(ctx context.Context) {
	log.L().Info("clear all task status")
	tm.tasks.Range(func(key, value interface{}) bool {
		tm.tasks.Delete(key)
		return true
	})
}

// remove deleted task status, usually happened when update-job delete some tasks.
func (tm *TaskManager) removeTaskStatus(job *metadata.Job) {
	tm.tasks.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		if _, ok := job.Tasks[taskID]; !ok {
			log.L().Info("remove task status", zap.String("task_id", taskID))
			tm.tasks.Delete(taskID)
		}
		return true
	})
}

// GetTaskStatus gets task status by taskID
func (tm *TaskManager) GetTaskStatus(taskID string) (runtime.TaskStatus, bool) {
	value, ok := tm.tasks.Load(taskID)
	if !ok {
		return runtime.NewOfflineStatus(taskID), false
	}
	return value.(runtime.TaskStatus), true
}

func genOp(runtimeStage, expectedStage metadata.TaskStage) dmpkg.OperateType {
	switch {
	case runtimeStage == metadata.StageRunning && expectedStage == metadata.StagePaused:
		return dmpkg.Pause
	case runtimeStage == metadata.StagePaused && expectedStage == metadata.StageRunning:
		return dmpkg.Resume
	// TODO: support update
	default:
		return dmpkg.None
	}
}

func (tm *TaskManager) operateTaskMessage(ctx context.Context, taskID string, op dmpkg.OperateType) error {
	msg := &dmpkg.OperateTaskMessage{
		Task: taskID,
		Op:   op,
	}
	return tm.messageAgent.SendMessage(ctx, taskID, dmpkg.OperateTask, msg)
}
