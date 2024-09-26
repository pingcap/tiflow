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
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/compat"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/keyspan"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/scheduler"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/transport"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
)

const (
	// When heavy operations (such as network IO and serialization) take too much time, the program
	// should print a warning log, and if necessary, the timeout should be exposed externally through
	// monitor.
	tickLogsWarnDuration    = 1 * time.Second
	checkpointCannotProceed = internal.CheckpointCannotProceed
	metricsInterval         = 10 * time.Second
)

var _ internal.Scheduler = (*coordinator)(nil)

type coordinator struct {
	// A mutex for concurrent access of coordinator in
	// internal.Scheduler and internal.InfoProvider API.
	mu sync.Mutex

	version         string
	revision        schedulepb.OwnerRevision
	changefeedEpoch uint64
	captureID       model.CaptureID
	trans           transport.Transport
	replicationM    *replication.Manager
	captureM        *member.CaptureManager
	schedulerM      *scheduler.Manager
	reconciler      *keyspan.Reconciler
	compat          *compat.Compat
	pdClock         pdutil.Clock
	tableRanges     replication.TableRanges
	redoMetaManager redo.MetaManager

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
	changefeedEpoch uint64,
	up *upstream.Upstream,
	cfg *config.SchedulerConfig,
	redoMetaManager redo.MetaManager,
) (internal.Scheduler, error) {
	trans, err := transport.NewTransport(
		ctx, changefeedID, transport.SchedulerRole, messageServer, messageRouter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	reconciler, err := keyspan.NewReconciler(changefeedID, up, cfg.ChangefeedSettings)
	if err != nil {
		return nil, errors.Trace(err)
	}
	revision := schedulepb.OwnerRevision{Revision: ownerRevision}
	return &coordinator{
		version:         version.ReleaseSemver(),
		revision:        revision,
		changefeedEpoch: changefeedEpoch,
		captureID:       captureID,
		trans:           trans,
		replicationM: replication.NewReplicationManager(
			cfg.MaxTaskConcurrency, changefeedID),
		captureM:        member.NewCaptureManager(captureID, changefeedID, revision, cfg),
		schedulerM:      scheduler.NewSchedulerManager(changefeedID, cfg),
		reconciler:      reconciler,
		changefeedID:    changefeedID,
		compat:          compat.New(cfg, map[model.CaptureID]*model.CaptureInfo{}),
		pdClock:         up.PDClock,
		redoMetaManager: redoMetaManager,
	}, nil
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
	barrier *schedulepb.BarrierWithMinTs,
) (watermark schedulepb.Watermark, err error) {
	startTime := time.Now()
	defer func() {
		costTime := time.Since(startTime)
		if costTime > tickLogsWarnDuration {
			log.Warn("scheduler tick took too long",
				zap.String("namespace", c.changefeedID.Namespace),
				zap.String("changefeed", c.changefeedID.ID),
				zap.Duration("duration", costTime))
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.poll(ctx, checkpointTs, currentTables, aliveCaptures, barrier)
}

// MoveTable implement the scheduler interface
// FIXME: tableID should be Span.
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

	span := spanz.TableIDToComparableSpan(tableID)
	c.schedulerM.MoveTable(span, target)
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
			zap.String("namespace", c.changefeedID.Namespace),
			zap.String("changefeed", c.changefeedID.ID),
			zap.String("target", target))
		// return count 1 to let client retry.
		return 1, nil
	}

	var count int
	c.replicationM.ReplicationSets().Ascend(
		func(_ tablepb.Span, rep *replication.ReplicationSet) bool {
			if rep.Primary == target {
				count++
			}
			return true
		})

	if count == 0 {
		log.Info("schedulerv3: drain capture request ignored, "+
			"the target capture has no replicating table",
			zap.String("namespace", c.changefeedID.Namespace),
			zap.String("changefeed", c.changefeedID.ID),
			zap.String("target", target))
		return count, nil
	}

	// when draining the capture, tables need to be dispatched to other capture
	// except the draining one, so there should be at least two live captures.
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
		zap.String("namespace", c.changefeedID.Namespace),
		zap.String("changefeed", c.changefeedID.ID),
		zap.Any("ownerRev", c.captureM.OwnerRev))
}

// ===========

func (c *coordinator) poll(
	ctx context.Context,
	checkpointTs model.Ts,
	currentTables []model.TableID,
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
	barrier *schedulepb.BarrierWithMinTs,
) (watermark schedulepb.Watermark, err error) {
	c.maybeCollectMetrics()
	if c.compat.UpdateCaptureInfo(aliveCaptures) {
		spanReplicationEnabled := c.compat.CheckSpanReplicationEnabled()
		log.Info("schedulerv3: compat update capture info",
			zap.String("namespace", c.changefeedID.Namespace),
			zap.String("changefeed", c.changefeedID.ID),
			zap.Any("captures", aliveCaptures),
			zap.Bool("spanReplicationEnabled", spanReplicationEnabled))
	}

	recvMsgs, err := c.recvMsgs(ctx)
	if err != nil {
		return schedulepb.Watermark{
			CheckpointTs:     checkpointCannotProceed,
			ResolvedTs:       checkpointCannotProceed,
			LastSyncedTs:     checkpointCannotProceed,
			PullerResolvedTs: checkpointCannotProceed,
		}, errors.Trace(err)
	}

	var msgBuf []*schedulepb.Message
	c.captureM.HandleMessage(recvMsgs)

	msgs := c.captureM.HandleAliveCaptureUpdate(aliveCaptures)
	msgBuf = append(msgBuf, msgs...)

	// Handle received messages to advance replication set.
	msgs, err = c.replicationM.HandleMessage(recvMsgs)
	if err != nil {
		return schedulepb.Watermark{
			CheckpointTs:     checkpointCannotProceed,
			ResolvedTs:       checkpointCannotProceed,
			LastSyncedTs:     checkpointCannotProceed,
			PullerResolvedTs: checkpointCannotProceed,
		}, errors.Trace(err)
	}
	msgBuf = append(msgBuf, msgs...)

	pdTime := time.Now()
	// only nil in unit test
	if c.pdClock != nil {
		pdTime = c.pdClock.CurrentTime()
	}

	c.tableRanges.UpdateTables(currentTables)
	if !c.captureM.CheckAllCaptureInitialized() {
		// Skip generating schedule tasks for replication manager,
		// as not all capture are initialized.
		watermark = c.replicationM.AdvanceCheckpoint(&c.tableRanges, pdTime, barrier, c.redoMetaManager)
		// tick capture manager after checkpoint calculation to take account resolvedTs in barrier
		// when redo is enabled
		msgs = c.captureM.Tick(c.replicationM.ReplicationSets(),
			c.schedulerM.DrainingTarget(), barrier.Barrier)
		msgBuf = append(msgBuf, msgs...)
		return watermark, c.sendMsgs(ctx, msgBuf)
	}

	// Handle capture membership changes.
	if changes := c.captureM.TakeChanges(); changes != nil {
		msgs, err = c.replicationM.HandleCaptureChanges(
			changes.Init, changes.Removed, checkpointTs)
		if err != nil {
			return schedulepb.Watermark{
				CheckpointTs:     checkpointCannotProceed,
				ResolvedTs:       checkpointCannotProceed,
				LastSyncedTs:     checkpointCannotProceed,
				PullerResolvedTs: checkpointCannotProceed,
			}, errors.Trace(err)
		}
		msgBuf = append(msgBuf, msgs...)
	}

	// Generate schedule tasks based on the current status.
	replications := c.replicationM.ReplicationSets()
	runningTasks := c.replicationM.RunningTasks()
	currentSpans := c.reconciler.Reconcile(
		ctx, &c.tableRanges, replications, c.captureM.Captures, c.compat)
	allTasks := c.schedulerM.Schedule(
		checkpointTs, currentSpans, c.captureM.Captures, replications, runningTasks)

	// Handle generated schedule tasks.
	msgs, err = c.replicationM.HandleTasks(allTasks)
	if err != nil {
		return schedulepb.Watermark{
			CheckpointTs:     checkpointCannotProceed,
			ResolvedTs:       checkpointCannotProceed,
			LastSyncedTs:     checkpointCannotProceed,
			PullerResolvedTs: checkpointCannotProceed,
		}, errors.Trace(err)
	}
	msgBuf = append(msgBuf, msgs...)

	// Checkpoint calculation
	watermark = c.replicationM.AdvanceCheckpoint(&c.tableRanges, pdTime, barrier, c.redoMetaManager)

	// tick capture manager after checkpoint calculation to take account resolvedTs in barrier
	// when redo is enabled
	msgs = c.captureM.Tick(c.replicationM.ReplicationSets(),
		c.schedulerM.DrainingTarget(), barrier.Barrier)
	msgBuf = append(msgBuf, msgs...)

	// Send new messages.
	err = c.sendMsgs(ctx, msgBuf)
	if err != nil {
		return schedulepb.Watermark{
			CheckpointTs:     checkpointCannotProceed,
			ResolvedTs:       checkpointCannotProceed,
			LastSyncedTs:     checkpointCannotProceed,
			PullerResolvedTs: checkpointCannotProceed,
		}, errors.Trace(err)
	}

	return watermark, nil
}

func (c *coordinator) recvMsgs(ctx context.Context) ([]*schedulepb.Message, error) {
	recvMsgs, err := c.trans.Recv(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	n := 0
	for _, msg := range recvMsgs {
		// Filter stale messages and lost messages.
		if msg.Header.OwnerRevision != c.revision || msg.To != c.captureID {
			// Owner revision must match and capture ID must match.
			continue
		}
		if c.compat.CheckChangefeedEpochEnabled(msg.From) {
			if msg.Header.ChangefeedEpoch.Epoch != c.changefeedEpoch {
				// Changefeed epoch must match.
				continue
			}
		}
		recvMsgs[n] = msg
		n++
	}
	c.compat.AfterTransportReceive(recvMsgs[:n])
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
			ChangefeedEpoch: schedulepb.ChangefeedEpoch{
				Epoch: c.changefeedEpoch,
			},
		}
		m.From = c.captureID
	}
	c.compat.BeforeTransportSend(msgs)
	return c.trans.Send(ctx, msgs)
}

func (c *coordinator) maybeCollectMetrics() {
	now := time.Now()
	if now.Sub(c.lastCollectTime) < metricsInterval {
		return
	}
	c.lastCollectTime = now

	pdTime := now
	// only nil in unit test
	if c.pdClock != nil {
		pdTime = c.pdClock.CurrentTime()
	}

	c.schedulerM.CollectMetrics()
	c.replicationM.CollectMetrics(pdTime)
	c.captureM.CollectMetrics()
}
