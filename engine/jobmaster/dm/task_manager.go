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

	dmconfig "github.com/pingcap/tiflow/dm/config"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/engine/pkg/dm/ticker"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	taskNormalInterval = time.Second * 30
	taskErrorInterval  = time.Second * 10
)

// TaskManager checks and operates task.
type TaskManager struct {
	*ticker.DefaultTicker

	jobID        string
	jobStore     *metadata.JobStore
	messageAgent dmpkg.MessageAgent
	logger       *zap.Logger
	// tasks record the runtime task status
	// taskID -> TaskStatus
	tasks sync.Map

	gaugeVec *prometheus.GaugeVec
}

// NewTaskManager creates a new TaskManager instance
func NewTaskManager(
	jobID string,
	initTaskStatus []runtime.TaskStatus,
	jobStore *metadata.JobStore,
	messageAgent dmpkg.MessageAgent,
	pLogger *zap.Logger,
	metricFactory promutil.Factory,
) *TaskManager {
	taskManager := &TaskManager{
		jobID:         jobID,
		DefaultTicker: ticker.NewDefaultTicker(taskNormalInterval, taskErrorInterval),
		jobStore:      jobStore,
		logger:        pLogger.With(zap.String("component", "task_manager")),
		messageAgent:  messageAgent,
		gaugeVec: metricFactory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "dm",
				Subsystem: "worker",
				Name:      "task_state",
				Help:      "task state of dm worker in this job",
			}, []string{"task", "source_id"}),
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
	tm.logger.Info("operate task", zap.Stringer("op", op), zap.Strings("tasks", tasks))
	defer func() {
		if err == nil {
			tm.SetNextCheckTime(time.Now())
		}
	}()

	var stage metadata.TaskStage
	switch op {
	case dmpkg.Create:
		return tm.jobStore.Put(ctx, metadata.NewJob(jobCfg))
	case dmpkg.Update:
		return tm.jobStore.UpdateConfig(ctx, jobCfg)
	// Deleting marks the job as deleting.
	case dmpkg.Deleting:
		return tm.jobStore.MarkDeleting(ctx)
	// Delete deletes the job in metadata.
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
	tm.logger.Debug(
		"update task status",
		zap.String("task_id", taskStatus.Task),
		zap.Stringer("stage", taskStatus.Stage),
		zap.Stringer("unit", taskStatus.Unit),
		zap.Uint64("config_modify_revison", taskStatus.CfgModRevision),
	)
	tm.tasks.Store(taskStatus.Task, taskStatus)
	tm.gaugeVec.WithLabelValues(tm.jobID, taskStatus.Task).Set(float64(taskStatus.Stage))
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
	tm.logger.Info("start to check and operate tasks")
	state, err := tm.jobStore.Get(ctx)
	if err != nil || state.(*metadata.Job).Deleting {
		tm.logger.Info("on job deleting", zap.Error(err))
		tm.onJobDel()
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
			tm.logger.Error("failed to schedule task", zap.String("task_id", taskID), zap.Error(recordError))
			continue
		}

		op := genOp(runningTask.Stage, runningTask.StageUpdatedTime, persistentTask.Stage, persistentTask.StageUpdatedTime)
		if op == dmpkg.None {
			tm.logger.Debug(
				"task status will not be changed",
				zap.String("task_id", taskID),
				zap.Stringer("stage", runningTask.Stage),
			)
			continue
		}

		tm.logger.Info(
			"unexpected task status",
			zap.String("task_id", taskID),
			zap.Stringer("op", op),
			zap.Stringer("expected_stage", persistentTask.Stage),
			zap.Stringer("stage", runningTask.Stage),
		)
		// operateTaskMessage should be a asynchronous request
		if err := tm.operateTaskMessage(ctx, taskID, op); err != nil {
			recordError = err
			tm.logger.Error("operate task failed", zap.Error(recordError))
			continue
		}
	}
	return recordError
}

// remove all tasks, usually happened when delete jobs.
func (tm *TaskManager) onJobDel() {
	tm.logger.Info("clear all task status")
	tm.tasks.Range(func(key, value interface{}) bool {
		tm.tasks.Delete(key)
		tm.gaugeVec.DeleteLabelValues(tm.jobID, key.(string))
		return true
	})
}

// remove deleted task status, usually happened when update-job delete some tasks.
func (tm *TaskManager) removeTaskStatus(job *metadata.Job) {
	tm.tasks.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		if _, ok := job.Tasks[taskID]; !ok {
			tm.logger.Info("remove task status", zap.String("task_id", taskID))
			tm.tasks.Delete(taskID)
			tm.gaugeVec.DeleteLabelValues(tm.jobID, taskID)
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

func genOp(
	runningStage metadata.TaskStage,
	runningStageUpdatedTime time.Time,
	expectedStage metadata.TaskStage,
	expectedStageUpdatedTime time.Time,
) dmpkg.OperateType {
	switch {
	case expectedStage == metadata.StagePaused && (runningStage == metadata.StageRunning || runningStage == metadata.StageError):
		return dmpkg.Pause
	case expectedStage == metadata.StageRunning:
		if runningStage == metadata.StagePaused {
			return dmpkg.Resume
		}
		// only resume a error task for a manual Resume action by checking expectedStageUpdatedTime
		if runningStage == metadata.StageError && expectedStageUpdatedTime.After(runningStageUpdatedTime) {
			return dmpkg.Resume
		}
		return dmpkg.None
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

func (tm *TaskManager) allFinished(ctx context.Context) bool {
	state, err := tm.jobStore.Get(ctx)
	if err != nil {
		return false
	}
	job := state.(*metadata.Job)

	for taskID, task := range job.Tasks {
		t, ok := tm.tasks.Load(taskID)
		if !ok {
			return false
		}
		runningTask := t.(runtime.TaskStatus)
		if runningTask.Stage != metadata.StageFinished {
			return false
		}
		// update if we add new task mode
		if runningTask.Unit != frameModel.WorkerDMLoad || task.Cfg.TaskMode != dmconfig.ModeFull {
			return false
		}
	}
	return true
}
