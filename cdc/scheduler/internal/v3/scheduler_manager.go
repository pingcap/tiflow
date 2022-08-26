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

package v3

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

<<<<<<< HEAD:cdc/scheduler/internal/v3/scheduler_manager.go
type schedulerManager struct {
=======
// Manager manages schedulers and generates schedule tasks.
type Manager struct { //nolint:revive
>>>>>>> ce736c3f5 (schedulerV3(ticdc): burst add table in a batch way to delay resource allocation (#6836)):cdc/scheduler/internal/v3/scheduler/scheduler_manager.go
	changefeedID model.ChangeFeedID

	schedulers         []scheduler
	tasksCounter       map[struct{ scheduler, task string }]int
	maxTaskConcurrency int
}

func newSchedulerManager(
	changefeedID model.ChangeFeedID, cfg *config.SchedulerConfig,
<<<<<<< HEAD:cdc/scheduler/internal/v3/scheduler_manager.go
) *schedulerManager {
	sm := &schedulerManager{
=======
) *Manager {
	sm := &Manager{
>>>>>>> ce736c3f5 (schedulerV3(ticdc): burst add table in a batch way to delay resource allocation (#6836)):cdc/scheduler/internal/v3/scheduler/scheduler_manager.go
		maxTaskConcurrency: cfg.MaxTaskConcurrency,
		changefeedID:       changefeedID,
		schedulers:         make([]scheduler, schedulerPriorityMax),
		tasksCounter: make(map[struct {
			scheduler string
			task      string
		}]int),
	}

	sm.schedulers[schedulerPriorityBasic] = newBasicScheduler(
		cfg.AddTableBatchSize, changefeedID)
	sm.schedulers[schedulerPriorityDrainCapture] = newDrainCaptureScheduler(
		cfg.MaxTaskConcurrency, changefeedID)
	sm.schedulers[schedulerPriorityBalance] = newBalanceScheduler(
		time.Duration(cfg.CheckBalanceInterval), cfg.MaxTaskConcurrency)
	sm.schedulers[schedulerPriorityMoveTable] = newMoveTableScheduler(changefeedID)
	sm.schedulers[schedulerPriorityRebalance] = newRebalanceScheduler(changefeedID)

	return sm
}

<<<<<<< HEAD:cdc/scheduler/internal/v3/scheduler_manager.go
func (sm *schedulerManager) Schedule(
=======
// Schedule generates schedule tasks based on the inputs.
func (sm *Manager) Schedule(
>>>>>>> ce736c3f5 (schedulerV3(ticdc): burst add table in a batch way to delay resource allocation (#6836)):cdc/scheduler/internal/v3/scheduler/scheduler_manager.go
	checkpointTs model.Ts,
	currentTables []model.TableID,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	replications map[model.TableID]*ReplicationSet,
	runTasking map[model.TableID]*scheduleTask,
) []*scheduleTask {
	for sid, scheduler := range sm.schedulers {
		// Basic scheduler bypasses max task check, because it handles the most
		// critical scheduling, e.g. add table via CREATE TABLE DDL.
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
			log.Info("schedulerv3: new schedule task",
				zap.String("namespace", sm.changefeedID.Namespace),
				zap.String("changefeed", sm.changefeedID.ID),
				zap.Int("task", len(tasks)),
				zap.String("scheduler", scheduler.Name()))
			return tasks
		}
	}

	return nil
}

<<<<<<< HEAD:cdc/scheduler/internal/v3/scheduler_manager.go
func (sm *schedulerManager) MoveTable(tableID model.TableID, target model.CaptureID) {
=======
// MoveTable moves a table to the target capture.
func (sm *Manager) MoveTable(tableID model.TableID, target model.CaptureID) {
>>>>>>> ce736c3f5 (schedulerV3(ticdc): burst add table in a batch way to delay resource allocation (#6836)):cdc/scheduler/internal/v3/scheduler/scheduler_manager.go
	scheduler := sm.schedulers[schedulerPriorityMoveTable]
	moveTableScheduler, ok := scheduler.(*moveTableScheduler)
	if !ok {
		log.Panic("schedulerv3: invalid move table scheduler found",
			zap.String("namespace", sm.changefeedID.Namespace),
			zap.String("changefeed", sm.changefeedID.ID))
	}
	if !moveTableScheduler.addTask(tableID, target) {
		log.Info("schedulerv3: manual move Table task ignored, "+
			"since the last triggered task not finished",
			zap.String("namespace", sm.changefeedID.Namespace),
			zap.String("changefeed", sm.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.String("targetCapture", target))
	}
}

<<<<<<< HEAD:cdc/scheduler/internal/v3/scheduler_manager.go
func (sm *schedulerManager) Rebalance() {
=======
// Rebalance rebalance tables.
func (sm *Manager) Rebalance() {
>>>>>>> ce736c3f5 (schedulerV3(ticdc): burst add table in a batch way to delay resource allocation (#6836)):cdc/scheduler/internal/v3/scheduler/scheduler_manager.go
	scheduler := sm.schedulers[schedulerPriorityRebalance]
	rebalanceScheduler, ok := scheduler.(*rebalanceScheduler)
	if !ok {
		log.Panic("schedulerv3: invalid rebalance scheduler found",
			zap.String("namespace", sm.changefeedID.Namespace),
			zap.String("changefeed", sm.changefeedID.ID))
	}

	atomic.StoreInt32(&rebalanceScheduler.rebalance, 1)
}

<<<<<<< HEAD:cdc/scheduler/internal/v3/scheduler_manager.go
func (sm *schedulerManager) DrainCapture(target model.CaptureID) bool {
=======
// DrainCapture drains all tables in the target capture.
func (sm *Manager) DrainCapture(target model.CaptureID) bool {
>>>>>>> ce736c3f5 (schedulerV3(ticdc): burst add table in a batch way to delay resource allocation (#6836)):cdc/scheduler/internal/v3/scheduler/scheduler_manager.go
	scheduler := sm.schedulers[schedulerPriorityDrainCapture]
	drainCaptureScheduler, ok := scheduler.(*drainCaptureScheduler)
	if !ok {
		log.Panic("schedulerv3: invalid drain capture scheduler found",
			zap.String("namespace", sm.changefeedID.Namespace),
			zap.String("changefeed", sm.changefeedID.ID))
	}

	return drainCaptureScheduler.setTarget(target)
}

<<<<<<< HEAD:cdc/scheduler/internal/v3/scheduler_manager.go
func (sm *schedulerManager) DrainingTarget() model.CaptureID {
	return sm.schedulers[schedulerPriorityDrainCapture].(*drainCaptureScheduler).getTarget()
}

func (sm *schedulerManager) CollectMetrics() {
=======
// DrainingTarget returns a capture id that is currently been draining.
func (sm *Manager) DrainingTarget() model.CaptureID {
	return sm.schedulers[schedulerPriorityDrainCapture].(*drainCaptureScheduler).getTarget()
}

// CollectMetrics collects metrics.
func (sm *Manager) CollectMetrics() {
>>>>>>> ce736c3f5 (schedulerV3(ticdc): burst add table in a batch way to delay resource allocation (#6836)):cdc/scheduler/internal/v3/scheduler/scheduler_manager.go
	cf := sm.changefeedID
	for name, counter := range sm.tasksCounter {
		scheduleTaskCounter.
			WithLabelValues(cf.Namespace, cf.ID, name.scheduler, name.task).
			Add(float64(counter))
		sm.tasksCounter[name] = 0
	}
}

<<<<<<< HEAD:cdc/scheduler/internal/v3/scheduler_manager.go
func (sm *schedulerManager) CleanMetrics() {
=======
// CleanMetrics cleans metrics.
func (sm *Manager) CleanMetrics() {
>>>>>>> ce736c3f5 (schedulerV3(ticdc): burst add table in a batch way to delay resource allocation (#6836)):cdc/scheduler/internal/v3/scheduler/scheduler_manager.go
	cf := sm.changefeedID
	for name := range sm.tasksCounter {
		scheduleTaskCounter.DeleteLabelValues(cf.Namespace, cf.ID, name.scheduler, name.task)
	}
}
