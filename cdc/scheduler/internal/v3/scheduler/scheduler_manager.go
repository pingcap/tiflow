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

package scheduler

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

// Manager manages schedulers and generates schedule tasks.
type Manager struct { //nolint:revive
	changefeedID model.ChangeFeedID

	schedulers         []scheduler
	tasksCounter       map[struct{ scheduler, task string }]int
	maxTaskConcurrency int
}

// NewSchedulerManager returns a new scheduler manager.
func NewSchedulerManager(
	changefeedID model.ChangeFeedID, cfg *config.SchedulerConfig,
) *Manager {
	sm := &Manager{
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
		time.Duration(cfg.CheckBalanceInterval), cfg.MaxTaskConcurrency, sm.changefeedID)
	sm.schedulers[schedulerPriorityMoveTable] = newMoveTableScheduler(changefeedID)
	sm.schedulers[schedulerPriorityRebalance] = newRebalanceScheduler(changefeedID)

	return sm
}

// Schedule generates schedule tasks based on the inputs.
func (sm *Manager) Schedule(
	checkpointTs model.Ts,
	currentSpans []tablepb.Span,
	aliveCaptures map[model.CaptureID]*member.CaptureStatus,
	replications *spanz.BtreeMap[*replication.ReplicationSet],
	runTasking *spanz.BtreeMap[*replication.ScheduleTask],
) []*replication.ScheduleTask {
	for sid, scheduler := range sm.schedulers {
		// Basic scheduler bypasses max task check, because it handles the most
		// critical scheduling, e.g. add table via CREATE TABLE DDL.
		if sid != int(schedulerPriorityBasic) {
			if runTasking.Len() >= sm.maxTaskConcurrency {
				// Do not generate more scheduling tasks if there are too many
				// running tasks.
				return nil
			}
		}
		tasks := scheduler.Schedule(checkpointTs, currentSpans, aliveCaptures, replications)
		for _, t := range tasks {
			name := struct {
				scheduler, task string
			}{scheduler: scheduler.Name(), task: t.Name()}
			sm.tasksCounter[name]++
		}
		if len(tasks) != 0 {
			return tasks
		}
	}
	return nil
}

// MoveTable moves a table to the target capture.
func (sm *Manager) MoveTable(span tablepb.Span, target model.CaptureID) {
	scheduler := sm.schedulers[schedulerPriorityMoveTable]
	moveTableScheduler, ok := scheduler.(*moveTableScheduler)
	if !ok {
		log.Panic("schedulerv3: invalid move table scheduler found",
			zap.String("namespace", sm.changefeedID.Namespace),
			zap.String("changefeed", sm.changefeedID.ID))
	}
	if !moveTableScheduler.addTask(span, target) {
		log.Info("schedulerv3: manual move Table task ignored, "+
			"since the last triggered task not finished",
			zap.String("namespace", sm.changefeedID.Namespace),
			zap.String("changefeed", sm.changefeedID.ID),
			zap.String("span", span.String()),
			zap.String("targetCapture", target))
	}
}

// Rebalance rebalance tables.
func (sm *Manager) Rebalance() {
	scheduler := sm.schedulers[schedulerPriorityRebalance]
	rebalanceScheduler, ok := scheduler.(*rebalanceScheduler)
	if !ok {
		log.Panic("schedulerv3: invalid rebalance scheduler found",
			zap.String("namespace", sm.changefeedID.Namespace),
			zap.String("changefeed", sm.changefeedID.ID))
	}

	atomic.StoreInt32(&rebalanceScheduler.rebalance, 1)
}

// DrainCapture drains all tables in the target capture.
func (sm *Manager) DrainCapture(target model.CaptureID) bool {
	scheduler := sm.schedulers[schedulerPriorityDrainCapture]
	drainCaptureScheduler, ok := scheduler.(*drainCaptureScheduler)
	if !ok {
		log.Panic("schedulerv3: invalid drain capture scheduler found",
			zap.String("namespace", sm.changefeedID.Namespace),
			zap.String("changefeed", sm.changefeedID.ID))
	}

	return drainCaptureScheduler.setTarget(target)
}

// DrainingTarget returns a capture id that is currently been draining.
func (sm *Manager) DrainingTarget() model.CaptureID {
	return sm.schedulers[schedulerPriorityDrainCapture].(*drainCaptureScheduler).getTarget()
}

// CollectMetrics collects metrics.
func (sm *Manager) CollectMetrics() {
	cf := sm.changefeedID
	for name, counter := range sm.tasksCounter {
		scheduleTaskCounter.
			WithLabelValues(cf.Namespace, cf.ID, name.scheduler, name.task).
			Add(float64(counter))
		sm.tasksCounter[name] = 0
	}
}

// CleanMetrics cleans metrics.
func (sm *Manager) CleanMetrics() {
	cf := sm.changefeedID
	for name := range sm.tasksCounter {
		scheduleTaskCounter.DeleteLabelValues(cf.Namespace, cf.ID, name.scheduler, name.task)
	}
}
