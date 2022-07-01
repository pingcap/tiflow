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
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

type schedulerManager struct {
	changefeedID model.ChangeFeedID

	schedulers         []scheduler
	tasksCounter       map[struct{ scheduler, task string }]int
	maxTaskConcurrency int
}

func newSchedulerManager(
	changefeedID model.ChangeFeedID, cfg *config.SchedulerConfig,
) *schedulerManager {
	sm := &schedulerManager{
		maxTaskConcurrency: cfg.MaxTaskConcurrency,
		changefeedID:       changefeedID,
		schedulers:         make([]scheduler, schedulerPriorityMax),
		tasksCounter: make(map[struct {
			scheduler string
			task      string
		}]int),
	}

	sm.schedulers[schedulerPriorityBasic] = newBasicScheduler()
	sm.schedulers[schedulerPriorityDrainCapture] = newDrainCaptureScheduler(cfg.MaxTaskConcurrency)
	sm.schedulers[schedulerPriorityBalance] = newBalanceScheduler(
		time.Duration(cfg.CheckBalanceInterval), cfg.MaxTaskConcurrency)
	sm.schedulers[schedulerPriorityMoveTable] = newMoveTableScheduler()
	sm.schedulers[schedulerPriorityRebalance] = newRebalanceScheduler()

	return sm
}

func (sm *schedulerManager) Schedule(
	checkpointTs model.Ts,
	currentTables []model.TableID,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	replications map[model.TableID]*ReplicationSet,
	runTasking map[model.TableID]*scheduleTask,
) []*scheduleTask {
	for sid, scheduler := range sm.schedulers {
		// Basic scheduler bypasses max task check, because it handles the most
		// critical scheduling, eg. add table via CREATE TABLE DDL.
		if sid != int(schedulerPriorityBasic) {
			if len(runTasking) >= sm.maxTaskConcurrency {
				// Do not generate more scheduling tasks if there are too many
				// running tasks.
				return nil
			}
		}
		tasks := scheduler.Schedule(checkpointTs, currentTables, aliveCaptures, replications)
		for _, t := range tasks {
			name := struct {
				scheduler, task string
			}{scheduler: scheduler.Name(), task: t.Name()}
			sm.tasksCounter[name]++
		}
		if len(tasks) != 0 {
			log.Info("tpscheduler: new schedule task",
				zap.String("namespace", sm.changefeedID.Namespace),
				zap.String("changefeed", sm.changefeedID.ID),
				zap.Int("task", len(tasks)),
				zap.String("scheduler", scheduler.Name()))
			return tasks
		}
	}

	return nil
}

func (sm *schedulerManager) MoveTable(tableID model.TableID, target model.CaptureID) {
	scheduler := sm.schedulers[schedulerPriorityMoveTable]
	moveTableScheduler, ok := scheduler.(*moveTableScheduler)
	if !ok {
		log.Panic("tpscheduler: invalid move table scheduler found")
	}
	if !moveTableScheduler.addTask(tableID, target) {
		log.Info("tpscheduler: manual move Table task ignored, "+
			"since the last triggered task not finished",
			zap.String("namespace", sm.changefeedID.Namespace),
			zap.String("changefeed", sm.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.String("targetCapture", target))
	}
}

func (sm *schedulerManager) Rebalance() {
	scheduler := sm.schedulers[schedulerPriorityRebalance]
	rebalanceScheduler, ok := scheduler.(*rebalanceScheduler)
	if !ok {
		log.Panic("tpscheduler: invalid rebalance scheduler found")
	}

	atomic.StoreInt32(&rebalanceScheduler.rebalance, 1)
}

func (sm *schedulerManager) DrainCapture(target model.CaptureID) bool {
	scheduler := sm.schedulers[schedulerPriorityDrainCapture]
	drainCaptureScheduler, ok := scheduler.(*drainCaptureScheduler)
	if !ok {
		log.Panic("tpscheduler: invalid drain capture scheduler found")
	}

	return drainCaptureScheduler.setTarget(target)
}

func (sm *schedulerManager) DrainingTarget() model.CaptureID {
	return sm.schedulers[schedulerPriorityDrainCapture].(*drainCaptureScheduler).getTarget()
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
