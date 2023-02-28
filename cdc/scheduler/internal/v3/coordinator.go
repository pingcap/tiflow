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
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/scheduler"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/transport"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/pdutil"
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
	trans        transport.Transport
	replicationM *replication.Manager
	captureM     *member.CaptureManager
	schedulerM   *scheduler.Manager
	pdClock      pdutil.Clock

	lastCollectTime time.Time
	changefeedID    model.ChangeFeedID
}

// NewCoordinator returns a two phase scheduler.
func NewCoordinator(
	ctx context.Context,
	captureID model.CaptureID,
	changefeedID model.ChangeFeedID,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	ownerRevision int64,
	cfg *config.SchedulerConfig,
	pdClock pdutil.Clock,
) (internal.Scheduler, error) {
	trans, err := transport.NewTransport(
		ctx, changefeedID, transport.SchedulerRole, messageServer, messageRouter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	coord := newCoordinator(captureID, changefeedID, ownerRevision, cfg)
	coord.trans = trans
	coord.pdClock = pdClock
	return coord, nil
}

func newCoordinator(
	captureID model.CaptureID,
	changefeedID model.ChangeFeedID,
	ownerRevision int64,
	cfg *config.SchedulerConfig,
) *coordinator {
	revision := schedulepb.OwnerRevision{Revision: ownerRevision}

	return &coordinator{
		version:   version.ReleaseSemver(),
		revision:  revision,
		captureID: captureID,
		replicationM: replication.NewReplicationManager(
			cfg.MaxTaskConcurrency, changefeedID),
		captureM:     member.NewCaptureManager(captureID, changefeedID, revision, cfg),
		schedulerM:   scheduler.NewSchedulerManager(changefeedID, cfg),
		changefeedID: changefeedID,
	}
}

// Tick implement the scheduler interface
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

// MoveTable implement the scheduler interface
func (c *coordinator) MoveTable(tableID model.TableID, target model.CaptureID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.captureM.CheckAllCaptureInitialized() {
		log.Info("schedulerv3: manual move table task ignored, "+
			"since not all captures initialized",
			zap.String("namespace", c.changefeedID.Namespace),
			zap.String("changefeed", c.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.String("targetCapture", target))
		return
	}

	c.schedulerM.MoveTable(tableID, target)
}

// Rebalance implement the scheduler interface
func (c *coordinator) Rebalance() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.captureM.CheckAllCaptureInitialized() {
		log.Info("schedulerv3: manual rebalance task ignored, "+
			"since not all captures initialized",
			zap.String("namespace", c.changefeedID.Namespace),
			zap.String("changefeed", c.changefeedID.ID))
		return
	}

	c.schedulerM.Rebalance()
}

// DrainCapture implement the scheduler interface
// return the count of table replicating on the target capture, and true if the request processed.
func (c *coordinator) DrainCapture(target model.CaptureID) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.captureM.CheckAllCaptureInitialized() {
		log.Info("schedulerv3: drain capture request ignored, "+
			"since not all captures initialized",
			zap.String("target", target),
			zap.String("namespace", c.changefeedID.Namespace),
			zap.String("changefeed", c.changefeedID.ID))
		// return false to let client retry.
		return 0, cerror.ErrSchedulerRequestFailed.
			GenWithStack("not all captures initialized")
	}

	var count int
	for _, rep := range c.replicationM.ReplicationSets() {
		if rep.Primary == target {
			count++
		}
	}

	if count == 0 {
		log.Info("schedulerv3: drain capture request ignored, "+
			"the target capture has no replicating table",
			zap.String("namespace", c.changefeedID.Namespace),
			zap.String("changefeed", c.changefeedID.ID),
			zap.String("target", target))
		return count, nil
	}

	// when draining the capture, tables need to be dispatched to other
	// capture except the draining one, so at least should have 2 captures alive.
	if len(c.captureM.Captures) <= 1 {
		log.Warn("schedulerv3: drain capture request ignored, "+
			"only one captures alive",
			zap.String("namespace", c.changefeedID.Namespace),
			zap.String("changefeed", c.changefeedID.ID),
			zap.String("target", target),
			zap.Int("tableCount", count))
		return count, nil
	}

	// the owner is the drain target. In the rolling upgrade scenario, owner should be drained
	// at the last, this should be guaranteed by the caller, since it knows the draining order.
	if target == c.captureID {
		log.Warn("schedulerv3: drain capture request ignored, "+
			"the target is the owner",
			zap.String("namespace", c.changefeedID.Namespace),
			zap.String("changefeed", c.changefeedID.ID),
			zap.String("target", target), zap.Int("tableCount", count))
		return count, nil
	}

	if !c.schedulerM.DrainCapture(target) {
		log.Info("schedulerv3: drain capture request ignored, "+
			"since there is capture draining",
			zap.String("namespace", c.changefeedID.Namespace),
			zap.String("changefeed", c.changefeedID.ID),
			zap.String("target", target),
			zap.Int("tableCount", count))
	}

	return count, nil
}

func (c *coordinator) Close(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	_ = c.trans.Close()
	c.captureM.CleanMetrics()
	c.replicationM.CleanMetrics()
	c.schedulerM.CleanMetrics()

	log.Info("schedulerv3: coordinator closed",
		zap.Any("ownerRev", c.captureM.OwnerRev),
		zap.String("namespace", c.changefeedID.Namespace),
		zap.String("changefeed", c.changefeedID.ID))
}

// ===========

func (c *coordinator) poll(
	ctx context.Context,
	checkpointTs model.Ts,
	currentTables []model.TableID,
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) (newCheckpointTs, newResolvedTs model.Ts, err error) {
	c.maybeCollectMetrics()

	recvMsgs, err := c.recvMsgs(ctx)
	if err != nil {
		return checkpointCannotProceed, checkpointCannotProceed, errors.Trace(err)
	}

	var msgBuf []*schedulepb.Message
	c.captureM.HandleMessage(recvMsgs)
	msgs := c.captureM.Tick(c.replicationM.ReplicationSets(), c.schedulerM.DrainingTarget())
	msgBuf = append(msgBuf, msgs...)
	msgs = c.captureM.HandleAliveCaptureUpdate(aliveCaptures)
	msgBuf = append(msgBuf, msgs...)

	// Handle received messages to advance replication set.
	msgs, err = c.replicationM.HandleMessage(recvMsgs)
	if err != nil {
		return checkpointCannotProceed, checkpointCannotProceed, errors.Trace(err)
	}
	msgBuf = append(msgBuf, msgs...)

	pdTime := time.Now()
	// only nil in unit test
	if c.pdClock != nil {
		pdTime, err = c.pdClock.CurrentTime()
		if err != nil {
			log.Warn("schedulerv3: failed to get pd time", zap.Error(err))
		}
	}

	if !c.captureM.CheckAllCaptureInitialized() {
		// Skip generating schedule tasks for replication manager,
		// as not all capture are initialized.
		newCheckpointTs, newResolvedTs = c.replicationM.AdvanceCheckpoint(currentTables, pdTime)
		return newCheckpointTs, newResolvedTs, c.sendMsgs(ctx, msgBuf)
	}

	// Handle capture membership changes.
	if changes := c.captureM.TakeChanges(); changes != nil {
		msgs, err = c.replicationM.HandleCaptureChanges(
			changes.Init, changes.Removed, checkpointTs)
		if err != nil {
			return checkpointCannotProceed, checkpointCannotProceed, errors.Trace(err)
		}
		msgBuf = append(msgBuf, msgs...)
	}

	// Generate schedule tasks based on the current status.
	replications := c.replicationM.ReplicationSets()
	runningTasks := c.replicationM.RunningTasks()
	allTasks := c.schedulerM.Schedule(
		checkpointTs, currentTables, c.captureM.Captures, replications, runningTasks)

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

	// Checkpoint calculation
	newCheckpointTs, newResolvedTs = c.replicationM.AdvanceCheckpoint(currentTables, pdTime)
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
				zap.String("namespace", c.changefeedID.Namespace),
				zap.String("changefeed", c.changefeedID.ID),
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

	c.schedulerM.CollectMetrics()
	c.replicationM.CollectMetrics()
	c.captureM.CollectMetrics()
}
