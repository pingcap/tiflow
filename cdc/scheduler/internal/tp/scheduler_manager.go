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

package tp

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

type schedulerManager struct {
	changefeedID model.ChangeFeedID

	schedulers   map[schedulerType]scheduler
	tasksCounter map[struct{ scheduler, task string }]int
}

func newSchedulerManager(changefeedID model.ChangeFeedID,
	balanceInterval time.Duration,
) *schedulerManager {
	sm := &schedulerManager{
		changefeedID: changefeedID,
		schedulers:   make(map[schedulerType]scheduler),
		tasksCounter: make(map[struct {
			scheduler string
			task      string
		}]int),
	}

	sm.schedulers[schedulerTypeBasic] = newBasicScheduler()
	sm.schedulers[schedulerTypeBalance] = newBalanceScheduler(balanceInterval)
	sm.schedulers[schedulerTypeMoveTable] = newMoveTableScheduler()
	sm.schedulers[schedulerTypeRebalance] = newRebalanceScheduler()
	sm.schedulers[schedulerTypeDrainCapture] = newDrainCaptureScheduler()

	return sm
}

func (sm *schedulerManager) Schedule(
	checkpointTs model.Ts,
	currentTables []model.TableID,
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
	replications map[model.TableID]*ReplicationSet) []*scheduleTask {

	for _, scheduler := range sm.schedulers {
		tasks := scheduler.Schedule(checkpointTs, currentTables, aliveCaptures, replications)
		for _, t := range tasks {
			name := struct {
				scheduler, task string
			}{scheduler: scheduler.Name(), task: t.Name()}
			sm.tasksCounter[name]++
		}
		if len(tasks) != 0 {
			log.Info("tpscheduler: new schedule task",
				zap.Int("task", len(tasks)),
				zap.String("scheduler", scheduler.Name()))
			return tasks
		}
	}

	return nil
}

func (sm *schedulerManager) MoveTable(tableID model.TableID, target model.CaptureID) {
	scheduler, ok := sm.schedulers[schedulerTypeMoveTable]
	if !ok {
		log.Panic("tpscheduler: move table scheduler not found")
	}
	moveTableScheduler, ok := scheduler.(*moveTableScheduler)
	if !ok {
		log.Panic("tpscheduler: invalid move table scheduler found")
	}
	if !moveTableScheduler.addTask(tableID, target) {
		log.Info("tpscheduler: manual move Table task ignored, "+
			"since the last triggered task not finished",
			zap.Int64("tableID", tableID),
			zap.String("targetCapture", target))
	}
}

func (sm *schedulerManager) Rebalance() {
	scheduler, ok := sm.schedulers[schedulerTypeRebalance]
	if !ok {
		log.Panic("tpscheduler: rebalance scheduler not found")
	}
	rebalanceScheduler, ok := scheduler.(*rebalanceScheduler)
	if !ok {
		log.Panic("tpscheduler: invalid rebalance scheduler found")
	}

	atomic.StoreInt32(&rebalanceScheduler.rebalance, 1)
}

func (sm *schedulerManager) DrainCapture(target model.CaptureID) bool {
	scheduler, ok := sm.schedulers[schedulerTypeDrainCapture]
	if !ok {
		log.Panic("tpscheduler: drain capture scheduler not found")
	}
	drainCaptureScheduler, ok := scheduler.(*drainCaptureScheduler)
	if !ok {
		log.Panic("tpscheduler: invalid drain capture scheduler found")
	}

	return drainCaptureScheduler.setTarget(target)
}

func (sm *schedulerManager) DrainingTarget() model.CaptureID {
	return sm.schedulers[schedulerTypeDrainCapture].(*drainCaptureScheduler).getTarget()
}

func (sm *schedulerManager) CollectMetrics() {
	cf := sm.changefeedID
	for name, counter := range sm.tasksCounter {
		scheduleTaskCounter.
			WithLabelValues(cf.Namespace, cf.ID, name.scheduler, name.task).
			Add(float64(counter))
		sm.tasksCounter[name] = 0
	}
}

func (sm *schedulerManager) CleanMetrics() {
	cf := sm.changefeedID
	for name := range sm.tasksCounter {
		scheduleTaskCounter.DeleteLabelValues(cf.Namespace, cf.ID, name.scheduler, name.task)
	}
}
