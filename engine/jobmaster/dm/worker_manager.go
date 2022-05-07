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

	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/ticker"
	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
)

var (
	WorkerNormalInterval = time.Second * 30
	WorkerErrorInterval  = time.Second * 10
)

type WorkerAgent interface {
	CreateWorker(ctx context.Context, taskID string, workerType libModel.WorkerType, taskCfg *config.TaskCfg, resources ...resourcemeta.ResourceID) (libModel.WorkerID, error)
	StopWorker(ctx context.Context, taskID string, workerID libModel.WorkerID) error
}

type CheckpointAgent interface {
	IsFresh(ctx context.Context, workerType libModel.WorkerType, taskCfg *metadata.Task) (bool, error)
}

type WorkerManager struct {
	*ticker.DefaultTicker

	jobStore        *metadata.JobStore
	workerAgent     WorkerAgent
	checkpointAgent CheckpointAgent

	// workerStatusMap record the runtime worker status
	// taskID -> WorkerStatus
	workerStatusMap sync.Map
}

// WorkerManager checks and schedules workers.
func NewWorkerManager(initWorkerStatus []runtime.WorkerStatus, jobStore *metadata.JobStore, workerAgent WorkerAgent, checkpointAgent CheckpointAgent) *WorkerManager {
	workerManager := &WorkerManager{
		DefaultTicker:   ticker.NewDefaultTicker(WorkerNormalInterval, WorkerErrorInterval),
		jobStore:        jobStore,
		workerAgent:     workerAgent,
		checkpointAgent: checkpointAgent,
	}
	workerManager.DefaultTicker.Ticker = workerManager

	for _, workerStatus := range initWorkerStatus {
		workerManager.UpdateWorkerStatus(workerStatus)
	}
	return workerManager
}

// UpdateWorkerStatus is called when receive worker status.
func (wm *WorkerManager) UpdateWorkerStatus(workerStatus runtime.WorkerStatus) {
	log.L().Debug("update worker status", zap.String("task_id", workerStatus.TaskID), zap.String("worker_id", workerStatus.ID))
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
	log.L().Info("start to schedule workers")
	wm.removeOfflineWorkers()

	state, err := wm.jobStore.Get(ctx)
	if err != nil {
		log.L().Error("get job state failed", zap.Error(err))
		if err2 := wm.onJobNotExist(ctx); err2 != nil {
			return err2
		}
		return err
	}
	job := state.(*metadata.Job)

	var recordError error
	if err := wm.stopUnneededWorkers(ctx, job); err != nil {
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
			log.L().Info("remove offline worker status", zap.String("task_id", worker.TaskID))
			wm.workerStatusMap.Delete(key)
		} else if worker.CreateFailed() {
			log.L().Info("remove failed worker status when creating", zap.String("task_id", worker.TaskID))
			wm.workerStatusMap.Delete(key)
		}
		return true
	})
}

// stop all workers, usually happened when delete jobs.
func (wm *WorkerManager) onJobNotExist(ctx context.Context) error {
	var recordError error
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		log.L().Info("stop worker", zap.String("task_id", key.(string)), zap.String("worker_id", value.(runtime.WorkerStatus).ID))
		if err := wm.stopWorker(ctx, key.(string), value.(runtime.WorkerStatus).ID); err != nil {
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
			log.L().Info("stop unneeded worker", zap.String("task_id", taskID), zap.String("worker_id", value.(runtime.WorkerStatus).ID))
			if err := wm.stopWorker(ctx, taskID, value.(runtime.WorkerStatus).ID); err != nil {
				recordError = err
			}
		}
		return true
	})
	return recordError
}

// checkAndScheduleWorkers check whether a task need a new worker.
// If there is no related worker, create a new worker.
// If task is finished, check whether need a new worker.
// This function does not handle taskCfg updated(update-job).
// TODO: support update taskCfg, or we may need to send update request manually.
func (wm *WorkerManager) checkAndScheduleWorkers(ctx context.Context, job *metadata.Job) error {
	var (
		runningWorker runtime.WorkerStatus
		nextUnit      libModel.WorkerType
		err           error
		recordError   error
	)

	// check and schedule workers
	for taskID, persistentTask := range job.Tasks {
		worker, ok := wm.workerStatusMap.Load(taskID)
		if ok {
			runningWorker = worker.(runtime.WorkerStatus)
			nextUnit = getNextUnit(persistentTask, runningWorker)
		} else if nextUnit, err = wm.getCurrentUnit(ctx, persistentTask); err != nil {
			log.L().Error("get current unit failed", zap.String("task", taskID), zap.Error(err))
			recordError = err
			continue
		}

		if ok && runningWorker.RunAsExpected() && nextUnit == runningWorker.Unit {
			log.L().Debug("worker status as expected", zap.String("task_id", taskID), zap.Int("worker_stage", int(runningWorker.Stage)), zap.Int64("unit", int64(runningWorker.Unit)))
			continue
		} else if !ok {
			log.L().Info("task has no worker", zap.String("task_id", taskID), zap.Int64("unit", int64(nextUnit)))
		} else if !runningWorker.RunAsExpected() {
			log.L().Info("unexpected worker status", zap.String("task_id", taskID), zap.Int("worker_stage", int(runningWorker.Stage)), zap.Int64("unit", int64(runningWorker.Unit)), zap.Int64("next_unit", int64(nextUnit)))
		} else {
			log.L().Info("switch to next unit", zap.String("task_id", taskID), zap.Int64("next_unit", int64(runningWorker.Unit)))
		}

		var resources []resourcemeta.ResourceID
		// we can assure only first worker don't need local resource.
		if workerIdxInSeq(persistentTask.Cfg.TaskMode, nextUnit) != 0 {
			resources = append(resources, NewDMResourceID(persistentTask.Cfg.Name, persistentTask.Cfg.Upstreams[0].SourceID))
		}

		// createWorker should be an asynchronous operation
		if err := wm.createWorker(ctx, taskID, nextUnit, persistentTask.Cfg, resources...); err != nil {
			recordError = err
			continue
		}
	}
	return recordError
}

var workerSeqMap = map[string][]libModel.WorkerType{
	dmconfig.ModeAll: {
		lib.WorkerDMDump,
		lib.WorkerDMLoad,
		lib.WorkerDMSync,
	},
	dmconfig.ModeFull: {
		lib.WorkerDMDump,
		lib.WorkerDMLoad,
	},
	dmconfig.ModeIncrement: {
		lib.WorkerDMSync,
	},
}

func (wm *WorkerManager) getCurrentUnit(ctx context.Context, task *metadata.Task) (libModel.WorkerType, error) {
	workerSeq, ok := workerSeqMap[task.Cfg.TaskMode]
	if !ok {
		log.L().Panic("Unexpected TaskMode", zap.String("TaskMode", task.Cfg.TaskMode))
	}

	for i := len(workerSeq) - 1; i >= 0; i-- {
		isFresh, err := wm.checkpointAgent.IsFresh(ctx, workerSeq[i], task)
		if err != nil {
			return 0, err
		}
		if !isFresh {
			return workerSeq[i], nil
		}
	}

	return workerSeq[0], nil
}

func workerIdxInSeq(taskMode string, worker libModel.WorkerType) int {
	workerSeq, ok := workerSeqMap[taskMode]
	if !ok {
		log.L().Panic("Unexpected TaskMode", zap.String("TaskMode", taskMode))
	}
	for i, w := range workerSeq {
		if w == worker {
			return i
		}
	}
	log.L().Panic("worker not found",
		zap.String("taskMode", taskMode),
		zap.Any("currWorker", worker))
	return -1
}

func nextWorkerIdxAndType(taskMode string, currWorker libModel.WorkerType) (int, libModel.WorkerType) {
	workerSeq, ok := workerSeqMap[taskMode]
	if !ok {
		log.L().Panic("Unexpected TaskMode", zap.String("TaskMode", taskMode))
	}

	idx := workerIdxInSeq(taskMode, currWorker)
	if idx == len(workerSeq)-1 {
		log.L().Error("workerSeq overflow",
			zap.String("taskMode", taskMode),
			zap.Any("currWorker", currWorker))
		return idx, workerSeq[idx]
	}
	return idx + 1, workerSeq[idx+1]
}

func getNextUnit(task *metadata.Task, worker runtime.WorkerStatus) libModel.WorkerType {
	if worker.Stage != runtime.WorkerFinished {
		return worker.Unit
	}

	_, workerType := nextWorkerIdxAndType(task.Cfg.TaskMode, worker.Unit)
	return workerType
}

func (wm *WorkerManager) createWorker(
	ctx context.Context,
	taskID string,
	unit libModel.WorkerType,
	taskCfg *config.TaskCfg,
	resources ...resourcemeta.ResourceID,
) error {
	log.L().Info("start to create worker", zap.String("task_id", taskID), zap.Int64("unit", int64(unit)))
	workerID, err := wm.workerAgent.CreateWorker(ctx, taskID, unit, taskCfg, resources...)
	if err != nil {
		log.L().Error("failed to create workers", zap.String("task_id", taskID), zap.Int64("unit", int64(unit)), zap.Error(err))
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
	}
	return err
}

func (wm *WorkerManager) stopWorker(ctx context.Context, taskID string, workerID libModel.WorkerID) error {
	log.L().Info("start to stop worker", zap.String("task_id", taskID), zap.String("worker_id", workerID))
	if err := wm.workerAgent.StopWorker(ctx, taskID, workerID); err != nil {
		log.L().Error("failed to stop worker", zap.String("task_id", taskID), zap.String("worker_id", workerID), zap.Error(err))
		return err
	}
	//	There are two mechanisms for removing worker status.
	//	1.	remove worker status when no error.
	//		It is possible that the worker will be stopped twice, so the stop needs to be idempotent.
	//	2.	remove worker status even if there is error.
	//		When stop fails, we stop it again until the next time we receive worker online status, so the stop interval will be longer.
	//	We choose the first mechanism now.
	wm.workerStatusMap.Delete(taskID)
	return nil
}

func (wm *WorkerManager) removeWorkerStatusByWorkerID(workerID libModel.WorkerID) {
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		if value.(runtime.WorkerStatus).ID == workerID {
			wm.workerStatusMap.Delete(key)
			return false
		}
		return true
	})
}
