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
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
)

const (
	checkpointCannotProceed = internal.CheckpointCannotProceed
	metricsInterval         = 10 * time.Second
)

var _ internal.Scheduler = (*coordinator)(nil)

type coordinator struct {
	// A mutex for concurrent access of coordinator in
	// internal.Scheduler and internal.InfoProvider API.
	mu sync.Mutex

	version      string
	revision     schedulepb.OwnerRevision
	captureID    model.CaptureID
	trans        transport
	schedulers   map[schedulerType]scheduler
	replicationM *replicationManager
	captureM     *captureManager

	lastCollectTime time.Time
	changefeedID    model.ChangeFeedID
	tasksCounter    map[struct{ scheduler, task string }]int
}

// NewCoordinator returns a two phase scheduler.
func NewCoordinator(
	ctx context.Context,
	captureID model.CaptureID,
	changefeedID model.ChangeFeedID,
	checkpointTs model.Ts,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	ownerRevision int64,
	cfg *config.SchedulerConfig,
) (internal.Scheduler, error) {
	trans, err := newTransport(ctx, changefeedID, schedulerRole, messageServer, messageRouter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	coord := newCoordinator(captureID, changefeedID, ownerRevision, cfg)
	coord.trans = trans
	return coord, nil
}

func newCoordinator(
	captureID model.CaptureID,
	changefeedID model.ChangeFeedID,
	ownerRevision int64,
	cfg *config.SchedulerConfig,
) *coordinator {
	revision := schedulepb.OwnerRevision{Revision: ownerRevision}
	schedulers := make(map[schedulerType]scheduler)
	schedulers[schedulerTypeBasic] = newBasicScheduler()
	schedulers[schedulerTypeBalance] = newBalanceScheduler(cfg)
	schedulers[schedulerTypeMoveTable] = newMoveTableScheduler()
	schedulers[schedulerTypeRebalance] = newRebalanceScheduler()

	return &coordinator{
		version:      version.ReleaseSemver(),
		revision:     revision,
		captureID:    captureID,
		schedulers:   schedulers,
		replicationM: newReplicationManager(cfg.MaxTaskConcurrency, changefeedID),
		captureM:     newCaptureManager(changefeedID, revision, cfg.HeartbeatTick),
		tasksCounter: make(map[struct {
			scheduler string
			task      string
		}]int),
	}
}

func (c *coordinator) Tick(
	ctx context.Context,
	// Latest global checkpoint of the changefeed
	checkpointTs model.Ts,
	// All tables that SHOULD be replicated (or started) at the current checkpoint.
	currentTables []model.TableID,
	// All captures that are alive according to the latest Etcd states.
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) (newCheckpointTs, newResolvedTs model.Ts, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.poll(ctx, checkpointTs, currentTables, aliveCaptures)
}

func (c *coordinator) MoveTable(tableID model.TableID, target model.CaptureID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.captureM.CheckAllCaptureInitialized() {
		log.Info("tpscheduler: manual move table task ignored, "+
			"since not all captures initialized",
			zap.Int64("tableID", tableID),
			zap.String("targetCapture", target))
		return
	}
	scheduler, ok := c.schedulers[schedulerTypeMoveTable]
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

func (c *coordinator) Rebalance() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.captureM.CheckAllCaptureInitialized() {
		log.Info("tpscheduler: manual rebalance task ignored, " +
			"since not all captures initialized")
		return
	}
	scheduler, ok := c.schedulers[schedulerTypeRebalance]
	if !ok {
		log.Panic("tpscheduler: rebalance scheduler not found")
	}
	rebalanceScheduler, ok := scheduler.(*rebalanceScheduler)
	if !ok {
		log.Panic("tpscheduler: invalid rebalance scheduler found")
	}
	atomic.StoreInt32(&rebalanceScheduler.rebalance, 1)
}

func (c *coordinator) Close(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	_ = c.trans.Close()
}

// ===========

func (c *coordinator) poll(
	ctx context.Context, checkpointTs model.Ts, currentTables []model.TableID,
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) (newCheckpointTs, newResolvedTs model.Ts, err error) {
	recvMsgs, err := c.recvMsgs(ctx)
	if err != nil {
		return checkpointCannotProceed, checkpointCannotProceed, errors.Trace(err)
	}

	var msgBuf []*schedulepb.Message
	c.captureM.HandleMessage(recvMsgs)
	msgs := c.captureM.Tick(c.replicationM.ReplicationSets())
	msgBuf = append(msgBuf, msgs...)
	msgs = c.captureM.HandleAliveCaptureUpdate(aliveCaptures)
	msgBuf = append(msgBuf, msgs...)
	if !c.captureM.CheckAllCaptureInitialized() {
		// Skip handling messages and tasks for replication manager,
		// as not all capture are initialized.
		return checkpointCannotProceed, checkpointCannotProceed, c.sendMsgs(ctx, msgBuf)
	}

	// Handle capture membership changes.
	if changes := c.captureM.TakeChanges(); changes != nil {
		msgs, err = c.replicationM.HandleCaptureChanges(changes, checkpointTs)
		if err != nil {
			return checkpointCannotProceed, checkpointCannotProceed, errors.Trace(err)
		}
		msgBuf = append(msgBuf, msgs...)
	}

	// Handle received messages to advance replication set.
	msgs, err = c.replicationM.HandleMessage(recvMsgs)
	if err != nil {
		return checkpointCannotProceed, checkpointCannotProceed, errors.Trace(err)
	}
	msgBuf = append(msgBuf, msgs...)

	// Generate schedule tasks based on the current status.
	replications := c.replicationM.ReplicationSets()
	allTasks := make([]*scheduleTask, 0)
	for _, scheduler := range c.schedulers {
		tasks := scheduler.Schedule(checkpointTs, currentTables, aliveCaptures, replications)
		if len(tasks) != 0 {
			log.Info("tpscheduler: new schedule task",
				zap.Int("task", len(tasks)),
				zap.String("scheduler", scheduler.Name()))
		}
		allTasks = append(allTasks, tasks...)
		for _, t := range tasks {
			name := struct {
				scheduler, task string
			}{scheduler: scheduler.Name(), task: t.Name()}
			c.tasksCounter[name]++
		}
	}

	// Handle generated schedule tasks.
	msgs, err = c.replicationM.HandleTasks(allTasks)
	if err != nil {
		return checkpointCannotProceed, checkpointCannotProceed, errors.Trace(err)
	}
	msgBuf = append(msgBuf, msgs...)

	// Send new messages.
	err = c.sendMsgs(ctx, msgBuf)
	if err != nil {
		return checkpointCannotProceed, checkpointCannotProceed, errors.Trace(err)
	}

	c.maybeCollectMetrics()

	// Checkpoint calculation
	newCheckpointTs, newResolvedTs = c.replicationM.AdvanceCheckpoint(currentTables)
	return newCheckpointTs, newResolvedTs, nil
}

func (c *coordinator) recvMsgs(ctx context.Context) ([]*schedulepb.Message, error) {
	recvMsgs, err := c.trans.Recv(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	n := 0
	for _, val := range recvMsgs {
		// Filter stale messages and lost messages.
		if val.Header.OwnerRevision == c.revision && val.To == c.captureID {
			recvMsgs[n] = val
			n++
		}
	}
	return recvMsgs[:n], nil
}

func (c *coordinator) sendMsgs(ctx context.Context, msgs []*schedulepb.Message) error {
	for i := range msgs {
		m := msgs[i]
		// Correctness check.
		if len(m.To) == 0 || m.MsgType == schedulepb.MsgUnknown {
			log.Panic("invalid message no destination or unknown message type",
				zap.Any("message", m))
		}

		epoch := schedulepb.ProcessorEpoch{}
		if capture := c.captureM.Captures[m.To]; capture != nil {
			epoch = capture.Epoch
		}
		m.Header = &schedulepb.Message_Header{
			Version:        c.version,
			OwnerRevision:  c.revision,
			ProcessorEpoch: epoch,
		}
		m.From = c.captureID

	}
	return c.trans.Send(ctx, msgs)
}

func (c *coordinator) maybeCollectMetrics() {
	now := time.Now()
	if now.Sub(c.lastCollectTime) < metricsInterval {
		return
	}
	c.lastCollectTime = now

	cf := c.replicationM.changefeedID
	for name, counter := range c.tasksCounter {
		scheduleTaskCounter.
			WithLabelValues(cf.Namespace, cf.ID, name.scheduler, name.task).
			Add(float64(counter))
		c.tasksCounter[name] = 0
	}
	c.replicationM.CollectMetrics()
	c.captureM.CollectMetrics()
}
