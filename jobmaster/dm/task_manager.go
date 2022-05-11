package dm

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	"github.com/hanfei1991/microcosm/jobmaster/dm/ticker"
)

// OperateType represents internal operate type in DM
// TODO: use OperateType in lib or move OperateType to lib.
type OperateType int

// These op may updated in later pr.
// NOTICE: consider to only use Update cmd to add/remove task.
// e.g. start-task/stop-task -s source in origin DM will be replaced by update-job now.
const (
	Create OperateType = iota
	Pause
	Resume
	Update
	Delete
)

var (
	taskNormalInterval = time.Second * 30
	taskErrorInterval  = time.Second * 10
)

// TaskAgent defines an interface to operate task
type TaskAgent interface {
	OperateTask(ctx context.Context, taskID string, stage metadata.TaskStage) error
}

// TaskManager checks and operates task.
type TaskManager struct {
	*ticker.DefaultTicker

	jobStore  *metadata.JobStore
	taskAgent TaskAgent
	// tasks record the runtime task status
	// taskID -> TaskStatus
	tasks sync.Map
}

func NewTaskManager(initTaskStatus []runtime.TaskStatus, jobStore *metadata.JobStore, agent TaskAgent) *TaskManager {
	taskManager := &TaskManager{
		DefaultTicker: ticker.NewDefaultTicker(taskNormalInterval, taskErrorInterval),
		jobStore:      jobStore,
		taskAgent:     agent,
	}
	taskManager.DefaultTicker.Ticker = taskManager

	for _, taskStatus := range initTaskStatus {
		taskManager.UpdateTaskStatus(taskStatus)
	}
	return taskManager
}

// OperateTask updates the task status in metadata and triggers the task manager to check and operate task.
// called by user request.
func (tm *TaskManager) OperateTask(ctx context.Context, op OperateType, jobCfg *config.JobCfg, tasks []string) (err error) {
	log.L().Info("operate task", zap.Int("op", int(op)), zap.Strings("tasks", tasks))
	defer func() {
		if err == nil {
			tm.SetNextCheckTime(time.Now())
		}
	}()

	var stage metadata.TaskStage
	switch op {
	case Create, Update:
		return tm.jobStore.Put(ctx, metadata.NewJob(jobCfg))
	case Delete:
		return tm.jobStore.Delete(ctx)
	case Resume:
		stage = metadata.StageRunning
	case Pause:
		stage = metadata.StagePaused
	default:
		return errors.New("unknown operate type")
	}

	return tm.jobStore.UpdateStages(ctx, tasks, stage)
}

// UpdateTaskStatus is called when receive task status from worker.
func (tm *TaskManager) UpdateTaskStatus(taskStatus runtime.TaskStatus) {
	log.L().Debug("update task status", zap.String("task_id", taskStatus.GetTask()), zap.Int("stage", int(taskStatus.GetStage())), zap.Int("unit", int(taskStatus.GetUnit())))
	tm.tasks.Store(taskStatus.GetTask(), taskStatus)
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
		if !ok || runningTask.GetStage() == metadata.StageUnscheduled {
			recordError = errors.New("get task running status failed")
			log.L().Error("failed to schedule task", zap.String("task_id", taskID), zap.Error(recordError))
			continue
		}

		if taskAsExpected(persistentTask, runningTask) {
			log.L().Debug("task status as expected", zap.String("task_id", taskID), zap.Int("stage", int(runningTask.GetStage())))
			continue
		}

		log.L().Info("unexpected task status", zap.String("task_id", taskID), zap.Int("expected_stage", int(persistentTask.Stage)), zap.Int("stage", int(runningTask.GetStage())))
		// OperateTask should be a asynchronous request
		if err := tm.taskAgent.OperateTask(ctx, taskID, persistentTask.Stage); err != nil {
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

// check a task runs as expected.
func taskAsExpected(persistentTask *metadata.Task, taskStatus runtime.TaskStatus) bool {
	// TODO: when running is expected but task is paused, we may still need return true,
	// because worker will resume it automatically.
	return persistentTask.Stage == taskStatus.GetStage()
}
