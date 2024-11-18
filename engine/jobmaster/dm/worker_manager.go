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
	"github.com/pingcap/tiflow/engine/framework"
	"github.com/pingcap/tiflow/engine/framework/logutil"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/engine/pkg/dm/ticker"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"go.uber.org/zap"
)

var (
	// WorkerNormalInterval is check interval when no error returns in tick
	WorkerNormalInterval = time.Second * 30
	// WorkerErrorInterval is check interval when any error returns in tick
	WorkerErrorInterval = time.Second * 10
)

// WorkerAgent defines an interface for create worker.
type WorkerAgent interface {
	// for create worker
	CreateWorker(
		workerType framework.WorkerType,
		config framework.WorkerConfig,
		opts ...framework.CreateWorkerOpt,
	) (frameModel.WorkerID, error)
}

// CheckpointAgent defines an interface for checkpoint.
type CheckpointAgent interface {
	IsFresh(ctx context.Context, workerType frameModel.WorkerType, taskCfg *metadata.Task) (bool, error)
}

// WorkerManager checks and schedules workers.
type WorkerManager struct {
	*ticker.DefaultTicker

	jobID           string
	jobStore        *metadata.JobStore
	unitStore       *metadata.UnitStateStore
	workerAgent     WorkerAgent
	messageAgent    dmpkg.MessageAgent
	checkpointAgent CheckpointAgent
	logger          *zap.Logger

	storageType resModel.ResourceType

	// workerStatusMap record the runtime worker status
	// taskID -> WorkerStatus
	workerStatusMap sync.Map
}

// NewWorkerManager creates a new WorkerManager instance
func NewWorkerManager(
	jobID string,
	initWorkerStatus []runtime.WorkerStatus,
	jobStore *metadata.JobStore,
	unitStore *metadata.UnitStateStore,
	workerAgent WorkerAgent,
	messageAgent dmpkg.MessageAgent,
	checkpointAgent CheckpointAgent,
	pLogger *zap.Logger,
	storageType resModel.ResourceType,
) *WorkerManager {
	workerManager := &WorkerManager{
		DefaultTicker:   ticker.NewDefaultTicker(WorkerNormalInterval, WorkerErrorInterval),
		jobID:           jobID,
		jobStore:        jobStore,
		unitStore:       unitStore,
		workerAgent:     workerAgent,
		messageAgent:    messageAgent,
		checkpointAgent: checkpointAgent,
		logger:          pLogger.With(zap.String("component", "worker_manager")),
		storageType:     storageType,
	}

	workerManager.DefaultTicker.Ticker = workerManager

	for _, workerStatus := range initWorkerStatus {
		workerManager.UpdateWorkerStatus(workerStatus)
	}
	return workerManager
}

// UpdateWorkerStatus is called when receive worker status.
func (wm *WorkerManager) UpdateWorkerStatus(workerStatus runtime.WorkerStatus) {
	wm.logger.Debug("update worker status", zap.String("task_id", workerStatus.TaskID), zap.String(logutil.ConstFieldWorkerKey, workerStatus.ID))
	wm.workerStatusMap.Store(workerStatus.TaskID, workerStatus)
}

// WorkerStatus return the worker status.
func (wm *WorkerManager) WorkerStatus() map[string]runtime.WorkerStatus {
	result := make(map[string]runtime.WorkerStatus)
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		result[key.(string)] = value.(runtime.WorkerStatus)
		return true
	})
	return result
}

// TickImpl remove offline workers.
// TickImpl stop unneeded workers.
// TickImpl create new workers if needed.
func (wm *WorkerManager) TickImpl(ctx context.Context) error {
	wm.logger.Info("start to schedule workers")
	wm.removeOfflineWorkers()

	state, err := wm.jobStore.Get(ctx)
	if err != nil || state.(*metadata.Job).Deleting {
		wm.logger.Info("on job deleting", zap.Error(err))
		if err2 := wm.onJobDel(ctx); err2 != nil {
			return err2
		}
		return err
	}
	job := state.(*metadata.Job)

	var recordError error
	if err := wm.stopUnneededWorkers(ctx, job); err != nil {
		recordError = err
	}
	if err := wm.stopOutdatedWorkers(ctx, job); err != nil {
		recordError = err
	}
	if err := wm.checkAndScheduleWorkers(ctx, job); err != nil {
		recordError = err
	}
	return recordError
}

// remove offline worker status, usually happened when worker is offline.
func (wm *WorkerManager) removeOfflineWorkers() {
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		worker := value.(runtime.WorkerStatus)
		if worker.IsOffline() {
			wm.logger.Info("remove offline worker status", zap.String("task_id", worker.TaskID))
			wm.workerStatusMap.Delete(key)
		} else if worker.CreateFailed() {
			wm.logger.Info("remove failed worker status when creating", zap.String("task_id", worker.TaskID))
			wm.workerStatusMap.Delete(key)
		}
		return true
	})
}

// stop all workers, usually happened when delete jobs.
func (wm *WorkerManager) onJobDel(ctx context.Context) error {
	var recordError error
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		workerStatus := value.(runtime.WorkerStatus)
		if workerStatus.IsTombStone() {
			return true
		}
		wm.logger.Info("stop worker", zap.String("task_id", key.(string)), zap.String(logutil.ConstFieldWorkerKey, value.(runtime.WorkerStatus).ID))
		if err := wm.stopWorker(ctx, key.(string), workerStatus.ID); err != nil {
			recordError = err
		}
		return true
	})
	return recordError
}

// stop unneeded workers, usually happened when update-job delete some tasks.
func (wm *WorkerManager) stopUnneededWorkers(ctx context.Context, job *metadata.Job) error {
	var recordError error
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		if _, ok := job.Tasks[taskID]; !ok {
			workerStatus := value.(runtime.WorkerStatus)
			if workerStatus.IsTombStone() {
				return true
			}
			wm.logger.Info("stop unneeded worker", zap.String("task_id", taskID), zap.String(logutil.ConstFieldWorkerKey, value.(runtime.WorkerStatus).ID))
			if err := wm.stopWorker(ctx, taskID, value.(runtime.WorkerStatus).ID); err != nil {
				recordError = err
			}
		}
		return true
	})
	return recordError
}

// stop outdated workers, usually happened when update job cfgs.
func (wm *WorkerManager) stopOutdatedWorkers(ctx context.Context, job *metadata.Job) error {
	var recordError error
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		workerStatus := value.(runtime.WorkerStatus)
		task, ok := job.Tasks[taskID]
		if !ok || task.Cfg.ModRevision == workerStatus.CfgModRevision {
			return true
		}
		if workerStatus.IsTombStone() {
			return true
		}
		wm.logger.Info("stop outdated worker", zap.String("task_id", taskID), zap.String(logutil.ConstFieldWorkerKey, value.(runtime.WorkerStatus).ID),
			zap.Uint64("config_modify_revision", workerStatus.CfgModRevision), zap.Uint64("expected_config_modify_revision", task.Cfg.ModRevision))
		if err := wm.stopWorker(ctx, taskID, value.(runtime.WorkerStatus).ID); err != nil {
			recordError = err
		}
		return true
	})
	return recordError
}

// checkAndScheduleWorkers check whether a task need a new worker.
// If there is no related worker, create a new worker.
// If task is finished, check whether need a new worker.
// TODO: support incremental -> all mode switch.
func (wm *WorkerManager) checkAndScheduleWorkers(ctx context.Context, job *metadata.Job) error {
	var (
		runningWorker runtime.WorkerStatus
		nextUnit      frameModel.WorkerType
		isFresh       bool
		err           error
		recordError   error
	)

	// check and schedule workers
	for taskID, persistentTask := range job.Tasks {
		worker, ok := wm.workerStatusMap.Load(taskID)
		if ok {
			runningWorker = worker.(runtime.WorkerStatus)
			nextUnit = getNextUnit(persistentTask, runningWorker)
			isFresh = nextUnit != runningWorker.Unit
		} else if nextUnit, isFresh, err = wm.getCurrentUnit(ctx, persistentTask); err != nil {
			wm.logger.Error("get current unit failed", zap.String("task", taskID), zap.Error(err))
			recordError = err
			continue
		}

		if ok && runningWorker.RunAsExpected() && nextUnit == runningWorker.Unit {
			wm.logger.Debug("worker status as expected", zap.String("task_id", taskID), zap.Stringer("worker_stage", runningWorker.Stage), zap.Stringer("unit", runningWorker.Unit))
			continue
		} else if !ok {
			wm.logger.Info("task has no worker", zap.String("task_id", taskID), zap.Stringer("unit", nextUnit))
		} else if !runningWorker.RunAsExpected() {
			wm.logger.Info("unexpected worker status", zap.String("task_id", taskID), zap.Stringer("worker_stage", runningWorker.Stage), zap.Stringer("unit", runningWorker.Unit), zap.Stringer("next_unit", nextUnit))
		} else {
			wm.logger.Info("switch to next unit", zap.String("task_id", taskID), zap.Stringer("next_unit", nextUnit))
		}

		var resources []resModel.ResourceID
		taskCfg := persistentTask.Cfg
		// first worker don't need local resource.
		// unfresh sync unit don't need local resource.(if we need to save table checkpoint for loadTableStructureFromDump in future, we can save it before saving global checkpoint.)
		// TODO: storage should be created/discarded in jobmaster instead of worker.
		if workerIdxInSeq(persistentTask.Cfg.TaskMode, nextUnit) != 0 && !(nextUnit == frameModel.WorkerDMSync && !isFresh) {
			resID := NewDMResourceID(wm.jobID, persistentTask.Cfg.Upstreams[0].SourceID, wm.storageType)
			resources = append(resources, resID)
		}

		// FIXME: remove this after fix https://github.com/pingcap/tiflow/issues/7304
		if nextUnit != frameModel.WorkerDMSync || isFresh {
			taskCfg.NeedExtStorage = true
		}

		// createWorker should be an asynchronous operation
		if err := wm.createWorker(ctx, taskID, nextUnit, taskCfg, resources...); err != nil {
			recordError = err
			continue
		}
	}
	return recordError
}

var workerSeqMap = map[string][]frameModel.WorkerType{
	dmconfig.ModeAll: {
		frameModel.WorkerDMDump,
		frameModel.WorkerDMLoad,
		frameModel.WorkerDMSync,
	},
	dmconfig.ModeFull: {
		frameModel.WorkerDMDump,
		frameModel.WorkerDMLoad,
	},
	dmconfig.ModeIncrement: {
		frameModel.WorkerDMSync,
	},
	dmconfig.ModeDump: {
		frameModel.WorkerDMDump,
	},
	dmconfig.ModeLoadSync: {
		frameModel.WorkerDMLoad,
		frameModel.WorkerDMSync,
	},
}

func (wm *WorkerManager) getCurrentUnit(ctx context.Context, task *metadata.Task) (frameModel.WorkerType, bool, error) {
	workerSeq, ok := workerSeqMap[task.Cfg.TaskMode]
	if !ok {
		wm.logger.Panic("Unexpected TaskMode", zap.String("TaskMode", task.Cfg.TaskMode))
	}

	for i := len(workerSeq) - 1; i >= 0; i-- {
		isFresh, err := wm.checkpointAgent.IsFresh(ctx, workerSeq[i], task)
		if err != nil {
			return 0, false, err
		}
		if !isFresh {
			return workerSeq[i], false, nil
		}
	}

	return workerSeq[0], true, nil
}

func workerIdxInSeq(taskMode string, worker frameModel.WorkerType) int {
	for i, w := range workerSeqMap[taskMode] {
		if w == worker {
			return i
		}
	}
	return -1
}

func nextWorkerIdxAndType(taskMode string, currWorker frameModel.WorkerType) (int, frameModel.WorkerType) {
	workerSeq := workerSeqMap[taskMode]
	idx := workerIdxInSeq(taskMode, currWorker)
	if idx == len(workerSeq)-1 {
		return idx, workerSeq[idx]
	}
	return idx + 1, workerSeq[idx+1]
}

func getNextUnit(task *metadata.Task, worker runtime.WorkerStatus) frameModel.WorkerType {
	if worker.Stage != runtime.WorkerFinished {
		return worker.Unit
	}

	_, workerType := nextWorkerIdxAndType(task.Cfg.TaskMode, worker.Unit)
	return workerType
}

func (wm *WorkerManager) createWorker(
	ctx context.Context,
	taskID string,
	unit frameModel.WorkerType,
	taskCfg *config.TaskCfg,
	resources ...resModel.ResourceID,
) error {
	wm.logger.Info("start to create worker", zap.String("task_id", taskID), zap.Stringer("unit", unit))
	workerID, err := wm.workerAgent.CreateWorker(unit, taskCfg,
		framework.CreateWorkerWithResourceRequirements(resources...))
	if err != nil {
		wm.logger.Error("failed to create workers", zap.String("task_id", taskID), zap.Stringer("unit", unit), zap.Error(err))
	}
	if len(workerID) != 0 {
		//	There are two mechanisms for create workers status.
		//	1.	create worker status when no error.
		//		It is possible that the worker will be created twice, so the create needs to be idempotent.
		//	2.	create worker status even if there is error.
		//		When create fails, we create it again until the next time we receive WorkerDispatchFailed/WorkerOffline event, so the create interval will be longer.
		//	We need to handle the intermediate state.
		//	We choose the second mechanism now.
		//	If a worker is created but never receives a dispatch/online/offline event(2 ticker?), we should remove it.
		wm.UpdateWorkerStatus(runtime.InitWorkerStatus(taskID, unit, workerID))

		// create success, record unit state
		if err := wm.unitStore.ReadModifyWrite(ctx, func(state *metadata.UnitState) error {
			wm.logger.Debug("start to update current unit state", zap.String("task", taskID), zap.Stringer("unit", unit))
			status, ok := state.CurrentUnitStatus[taskID]
			if !ok {
				state.CurrentUnitStatus[taskID] = &metadata.UnitStatus{
					Unit:        unit,
					Task:        taskID,
					CreatedTime: time.Now(),
				}
			} else {
				if status.Unit != unit {
					status.CreatedTime = time.Now()
					status.Unit = unit
				}
			}
			return nil
		}); err != nil {
			wm.logger.Error("update current unit state failed", zap.String("task", taskID), zap.Stringer("unit", unit), zap.Error(err))
			return err
		}
	}
	return err
}

func (wm *WorkerManager) stopWorker(ctx context.Context, taskID string, workerID frameModel.WorkerID) error {
	wm.logger.Info("start to stop worker", zap.String("task_id", taskID), zap.String("worker_id", workerID))

	msg := &dmpkg.StopWorkerMessage{
		Task: taskID,
	}
	if err := wm.messageAgent.SendMessage(ctx, taskID, dmpkg.StopWorker, msg); err != nil {
		wm.logger.Error("failed to stop worker", zap.String("task_id", taskID), zap.String("worker_id", workerID), zap.Error(err))
		return err
	}
	// workerStatus will be removed when the worker is offline.
	return nil
}

func (wm *WorkerManager) removeWorkerStatusByWorkerID(workerID frameModel.WorkerID) {
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		if value.(runtime.WorkerStatus).ID == workerID {
			wm.workerStatusMap.Delete(key)
			return false
		}
		return true
	})
}

func (wm *WorkerManager) allTombStone() bool {
	result := true
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		workerStatus := value.(runtime.WorkerStatus)
		if !workerStatus.IsTombStone() {
			result = false
			return false
		}
		return true
	})
	return result
}
