// Copyright 2021 PingCAP, Inc.
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

package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	sinkv1 "github.com/pingcap/tiflow/cdc/sink"
	"github.com/pingcap/tiflow/cdc/sink/flowcontrol"
	sinkv2 "github.com/pingcap/tiflow/cdc/sinkv2/tablesink"
	"github.com/pingcap/tiflow/pkg/actor"
	"github.com/pingcap/tiflow/pkg/actor/message"
	serverConfig "github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/tikv/client-go/v2/oracle"
	uberatomic "go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

var (
	_       tablepb.TablePipeline         = (*tableActor)(nil)
	_       actor.Actor[pmessage.Message] = (*tableActor)(nil)
	stopped                               = uint32(1)
)

// Assume 1KB per row in upstream TiDB, it takes about 250 MB (1024*4*64) for
// replicating 1024 tables in the worst case.
const defaultOutputChannelSize = 64

const sinkFlushInterval = 500 * time.Millisecond

type tableActor struct {
	actorID actor.ID
	mb      actor.Mailbox[pmessage.Message]
	router  *actor.Router[pmessage.Message]

	upstream *upstream.Upstream

	// all goroutines in tableActor should be spawned from this wg
	wg *errgroup.Group
	// backend mounter
	mg entry.MounterGroup
	// backend tableSink
	tableSinkV1 sinkv1.Sink
	tableSinkV2 sinkv2.TableSink
	redoDMLMgr  redo.DMLManager

	state tablepb.TableState

	pullerNode *pullerNode
	sinkNode   *sinkNode
	// contains all nodes except pullerNode
	nodes []*ActorNode

	sortNode *sorterNode

	// states of table actor
	started     bool
	stopped     uint32
	stopLock    sync.Mutex
	sinkStopped uberatomic.Bool

	// TODO: try to reduce these config fields below in the future
	tableID        int64
	targetTs       model.Ts
	memoryQuota    uint64
	replicaInfo    *model.TableReplicaInfo
	replicaConfig  *serverConfig.ReplicaConfig
	changefeedVars *cdcContext.ChangefeedVars
	globalVars     *cdcContext.GlobalVars
	// these fields below are used in logs and metrics only
	changefeedID model.ChangeFeedID
	tableName    string

	// use to report error to processor
	reportErr func(error)
	// tablePipelineCtx use to drive the nodes in table pipeline.
	tablePipelineCtx context.Context
	// cancel use to cancel all goroutines spawned from table actor
	cancel context.CancelFunc

	lastFlushSinkTime time.Time
}

// NewTableActor creates a table actor and starts it.
func NewTableActor(
	cdcCtx cdcContext.Context,
	up *upstream.Upstream,
	mg entry.MounterGroup,
	tableID model.TableID,
	tableName string,
	replicaInfo *model.TableReplicaInfo,
	sinkV1 sinkv1.Sink,
	sinkV2 sinkv2.TableSink,
	redoDMLMgr redo.DMLManager,
	targetTs model.Ts,
) (tablepb.TablePipeline, error) {
	config := cdcCtx.ChangefeedVars().Info.Config
	changefeedVars := cdcCtx.ChangefeedVars()
	globalVars := cdcCtx.GlobalVars()

	actorID := globalVars.TableActorSystem.ActorID()
	mb := actor.NewMailbox[pmessage.Message](actorID, defaultOutputChannelSize)
	// Cancel should be able to release all sub-goroutines in this actor.
	ctx, cancel := context.WithCancel(cdcCtx)
	// All sub-goroutines should be spawn in this wait group.
	wg, cctx := errgroup.WithContext(ctx)

	table := &tableActor{
		// all errors in table actor will be reported to processor
		reportErr: cdcCtx.Throw,
		mb:        mb,
		wg:        wg,
		cancel:    cancel,

		state:         tablepb.TableStatePreparing,
		tableID:       tableID,
		tableName:     tableName,
		memoryQuota:   serverConfig.GetGlobalServerConfig().PerTableMemoryQuota,
		upstream:      up,
		mg:            mg,
		replicaInfo:   replicaInfo,
		replicaConfig: config,
		tableSinkV1:   sinkV1,
		tableSinkV2:   sinkV2,
		redoDMLMgr:    redoDMLMgr,
		targetTs:      targetTs,
		started:       false,

		sortNode: nil,

		changefeedID:   changefeedVars.ID,
		changefeedVars: changefeedVars,
		globalVars:     globalVars,
		router:         globalVars.TableActorSystem.Router(),
		actorID:        actorID,

		tablePipelineCtx: cctx,
	}

	startTime := time.Now()
	if err := table.start(cctx); err != nil {
		table.stop()
		return nil, errors.Trace(err)
	}
	err := globalVars.TableActorSystem.System().Spawn(mb, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("table actor started",
		zap.String("namespace", table.changefeedID.Namespace),
		zap.String("changefeed", table.changefeedID.ID),
		zap.Int64("tableID", tableID),
		zap.String("tableName", tableName),
		zap.Uint64("checkpointTs", replicaInfo.StartTs),
		zap.Uint64("quota", table.memoryQuota),
		zap.Bool("redoLogEnabled", table.redoDMLMgr.Enabled()),
		zap.Bool("splitTxn", table.replicaConfig.Sink.TxnAtomicity.ShouldSplitTxn()),
		zap.Duration("duration", time.Since(startTime)))
	return table, nil
}

// OnClose implements Actor interface.
// TODO: implements table actor stop here.
func (t *tableActor) OnClose() {
}

func (t *tableActor) Poll(ctx context.Context, msgs []message.Message[pmessage.Message]) bool {
	for i := range msgs {
		if atomic.LoadUint32(&t.stopped) == stopped {
			// No need to handle remaining messages.
			return false
		}

		var err error
		switch msgs[i].Tp {
		case message.TypeValue:
			switch msgs[i].Value.Tp {
			case pmessage.MessageTypeBarrier:
				err = t.handleBarrierMsg(t.tablePipelineCtx, msgs[i].Value.BarrierTs)
			case pmessage.MessageTypeTick:
				err = t.handleTickMsg(t.tablePipelineCtx)
			}
		case message.TypeStop:
			t.handleStopMsg(t.tablePipelineCtx)
		}
		if err != nil {
			log.Error("failed to process message, stop table actor ",
				zap.String("tableName", t.tableName),
				zap.Int64("tableID", t.tableID),
				zap.Any("message", msgs[i]),
				zap.Error(err))
			t.handleError(err)
			break
		}

		// process message for each node, pull message from parent node and then send it to next node
		if !t.sinkStopped.Load() {
			if err := t.handleDataMsg(ctx); err != nil {
				log.Error("failed to process message, stop table actor ",
					zap.String("tableName", t.tableName),
					zap.Int64("tableID", t.tableID), zap.Error(err))
				t.handleError(err)
				break
			}
		}
	}
	if atomic.LoadUint32(&t.stopped) == stopped {
		log.Error("table actor removed",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.String("tableName", t.tableName),
			zap.Int64("tableID", t.tableID))
		return false
	}
	return true
}

func (t *tableActor) handleDataMsg(ctx context.Context) error {
	for _, n := range t.nodes {
		if err := n.TryRun(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (t *tableActor) handleBarrierMsg(ctx context.Context, barrierTs model.Ts) error {
	t.sortNode.updateBarrierTs(barrierTs)
	return t.sinkNode.updateBarrierTs(ctx, barrierTs)
}

func (t *tableActor) handleTickMsg(ctx context.Context) error {
	// tick message flush the raw event to sink, follow the old pipeline implementation, batch flush the events  every 500ms
	if time.Since(t.lastFlushSinkTime) > sinkFlushInterval {
		_, err := t.sinkNode.HandleMessage(ctx, pmessage.TickMessage())
		if err != nil {
			return err
		}
		t.lastFlushSinkTime = time.Now()
	}
	return nil
}

func (t *tableActor) handleStopMsg(ctx context.Context) {
	// Already stopped, no need to handle stop message again.
	if t.sinkStopped.Load() {
		return
	}
	t.sinkStopped.Store(true)
	// async stops sinkNode and tableSink
	go func() {
		_, err := t.sinkNode.HandleMessage(ctx,
			pmessage.CommandMessage(&pmessage.Command{Tp: pmessage.CommandTypeStop}),
		)
		if err != nil {
			t.handleError(err)
		}
	}()
}

func (t *tableActor) start(sdtTableContext context.Context) error {
	if t.started {
		log.Panic("start an already started table",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.Int64("tableID", t.tableID),
			zap.String("tableName", t.tableName))
	}

	splitTxn := t.replicaConfig.Sink.TxnAtomicity.ShouldSplitTxn()

	flowController := flowcontrol.NewTableFlowController(t.memoryQuota,
		t.redoDMLMgr.Enabled(), splitTxn)
	sorterNode := newSorterNode(t.tableName, t.tableID,
		t.replicaInfo.StartTs, flowController,
		t.mg, &t.state, t.changefeedID, t.redoDMLMgr.Enabled(),
		t.upstream.PDClient,
	)
	t.sortNode = sorterNode

	sortActorNodeContext := newContext(sdtTableContext, t.tableName,
		t.globalVars.TableActorSystem.Router(),
		t.actorID, t.changefeedVars, t.globalVars, t.reportErr)
	if err := startSorter(t, sortActorNodeContext); err != nil {
		log.Error("sorter fails to start",
			zap.String("tableName", t.tableName),
			zap.Int64("tableID", t.tableID),
			zap.Error(err))
		return err
	}

	pullerNode := newPullerNode(t.tableID, t.replicaInfo.StartTs, t.tableName, t.changefeedVars.ID)
	pullerActorNodeContext := newContext(sdtTableContext,
		t.tableName,
		t.globalVars.TableActorSystem.Router(),
		t.actorID, t.changefeedVars, t.globalVars, t.reportErr)
	t.pullerNode = pullerNode
	if err := startPuller(t, pullerActorNodeContext); err != nil {
		log.Error("puller fails to start",
			zap.String("tableName", t.tableName),
			zap.Int64("tableID", t.tableID),
			zap.Error(err))
		return err
	}
	var messageFetchFunc asyncMessageHolderFunc = func() *pmessage.Message {
		return sortActorNodeContext.tryGetProcessedMessage()
	}

	actorSinkNode := newSinkNode(
		t.tableID,
		t.tableSinkV1,
		t.tableSinkV2,
		t.replicaInfo.StartTs, t.targetTs, flowController, t.redoDMLMgr,
		&t.state, t.changefeedID, t.replicaConfig.EnableOldValue, splitTxn,
	)
	t.sinkNode = actorSinkNode

	// construct sink actor node, it gets message from sortNode
	var messageProcessFunc asyncMessageProcessorFunc = func(
		ctx context.Context, msg pmessage.Message,
	) (bool, error) {
		return actorSinkNode.HandleMessage(sdtTableContext, msg)
	}
	t.nodes = append(t.nodes, NewActorNode(messageFetchFunc, messageProcessFunc))

	t.started = true
	return nil
}

// stop will set this table actor state to stopped and releases all goroutines spawned
// from this table actor
func (t *tableActor) stop() {
	log.Debug("table actor begin to stop....",
		zap.String("namespace", t.changefeedID.Namespace),
		zap.String("changefeed", t.changefeedID.ID),
		zap.String("tableName", t.tableName))
	t.stopLock.Lock()
	defer t.stopLock.Unlock()
	if atomic.LoadUint32(&t.stopped) == stopped {
		log.Warn("table actor is already stopped",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.String("tableName", t.tableName))
		return
	}
	atomic.StoreUint32(&t.stopped, stopped)

	if t.sortNode != nil {
		// releaseResource will send a message to sorter router
		t.sortNode.releaseResource()
	}

	t.cancel()
	if t.sinkNode != nil && !t.sinkStopped.Load() {
		if err := t.sinkNode.stop(t.tablePipelineCtx); err != nil {
			switch errors.Cause(err) {
			case context.Canceled, cerror.ErrTableProcessorStoppedSafely:
			default:
				log.Warn("close sink failed",
					zap.String("namespace", t.changefeedID.Namespace),
					zap.String("changefeed", t.changefeedID.ID),
					zap.String("tableName", t.tableName),
					zap.Error(err))
			}
		}
	}
	log.Info("table actor stopped",
		zap.String("namespace", t.changefeedID.Namespace),
		zap.String("changefeed", t.changefeedID.ID),
		zap.String("tableName", t.tableName),
		zap.Int64("tableID", t.tableID))
}

// handleError stops the table actor at first and then reports the error to processor
func (t *tableActor) handleError(err error) {
	t.stop()
	if !cerror.ErrTableProcessorStoppedSafely.Equal(err) {
		t.reportErr(err)
	}
}

// ============ Implement TablePipeline, must be thread-safe ============

// ResolvedTs returns the resolved ts in this table pipeline
func (t *tableActor) ResolvedTs() model.Ts {
	// TODO: after TiCDC introduces p2p based resolved ts mechanism, TiCDC nodes
	// will be able to cooperate replication state directly. Then we will add
	// another replication barrier for consistent replication instead of reusing
	// the global resolved-ts.
	if t.redoDMLMgr.Enabled() {
		return t.redoDMLMgr.GetResolvedTs(t.tableID)
	}
	return t.sortNode.ResolvedTs()
}

// CheckpointTs returns the checkpoint ts in this table pipeline
func (t *tableActor) CheckpointTs() model.Ts {
	return t.sinkNode.CheckpointTs()
}

// if the actor system is slow, too many warn log will be printed, so we need to add rate limit
var updateBarrierTsLogRateLimiter = rate.NewLimiter(rate.Every(time.Millisecond*500), 1)

// UpdateBarrierTs updates the barrier ts in this table pipeline
func (t *tableActor) UpdateBarrierTs(ts model.Ts) {
	msg := pmessage.BarrierMessage(ts)
	err := t.router.Send(t.actorID, message.ValueMessage(msg))
	if err != nil {
		if updateBarrierTsLogRateLimiter.Allow() {
			log.Warn("send fails",
				zap.Any("msg", msg),
				zap.String("tableName", t.tableName),
				zap.Int64("tableID", t.tableID),
				zap.Error(err))
		}
	}
}

// AsyncStop tells the pipeline to stop, and returns true if the pipeline is already stopped.
func (t *tableActor) AsyncStop() bool {
	// TypeStop stop the sinkNode only ,the processor stop the sink to release some resource
	// and then stop the whole table pipeline by call Cancel
	msg := message.StopMessage[pmessage.Message]()
	err := t.router.Send(t.actorID, msg)
	if err != nil {
		if cerror.ErrMailboxFull.Equal(err) {
			return false
		}
		if cerror.ErrActorNotFound.Equal(err) || cerror.ErrActorStopped.Equal(err) {
			return true
		}
		log.Panic("send fails", zap.Any("msg", msg), zap.Error(err))
	}
	return true
}

// Stats returns the statistics of this table pipeline
func (t *tableActor) Stats() tablepb.Stats {
	pullerStats := t.pullerNode.plr.Stats()
	sinkStats := t.sinkNode.Stats()
	now := t.upstream.PDClock.CurrentTime()

	stats := tablepb.Stats{
		RegionCount: pullerStats.RegionCount,
		CurrentTs:   oracle.ComposeTS(oracle.GetPhysical(now), 0),
		BarrierTs:   sinkStats.BarrierTs,
		StageCheckpoints: map[string]tablepb.Checkpoint{
			"puller-ingress": {
				CheckpointTs: pullerStats.CheckpointTsIngress,
				ResolvedTs:   pullerStats.ResolvedTsIngress,
			},
			"puller-egress": {
				CheckpointTs: pullerStats.CheckpointTsEgress,
				ResolvedTs:   pullerStats.ResolvedTsEgress,
			},
			"sink": {
				CheckpointTs: sinkStats.CheckpointTs,
				ResolvedTs:   sinkStats.ResolvedTs,
			},
		},
	}

	sorterStats := t.sortNode.sorter.Stats()
	stats.StageCheckpoints["sorter-ingress"] = tablepb.Checkpoint{
		CheckpointTs: sorterStats.CheckpointTsIngress,
		ResolvedTs:   sorterStats.ResolvedTsIngress,
	}
	stats.StageCheckpoints["sorter-egress"] = tablepb.Checkpoint{
		CheckpointTs: sorterStats.CheckpointTsEgress,
		ResolvedTs:   sorterStats.ResolvedTsEgress,
	}

	return stats
}

// State returns the state of this table pipeline
func (t *tableActor) State() tablepb.TableState {
	return t.state.Load()
}

// ID returns the ID of source table and mark table
func (t *tableActor) ID() int64 {
	return t.tableID
}

// Name returns the quoted schema and table name
func (t *tableActor) Name() string {
	return t.tableName
}

// Cancel stops this table pipeline immediately and destroy all resources
// created by this table pipeline
func (t *tableActor) Cancel() {
	// cancel wait group, release resource and mark the state as stopped
	t.stop()
	// actor is closed, tick actor to remove this actor router
	msg := pmessage.TickMessage()
	if err := t.router.Send(t.mb.ID(), message.ValueMessage(msg)); err != nil {
		log.Warn("fails to send Stop message",
			zap.String("tableName", t.tableName),
			zap.Int64("tableID", t.tableID),
			zap.Error(err))
	}
}

// Wait waits for table pipeline destroyed
func (t *tableActor) Wait() {
	_ = t.wg.Wait()
}

// MemoryConsumption return the memory consumption in bytes
func (t *tableActor) MemoryConsumption() uint64 {
	return t.sortNode.flowController.GetConsumption()
}

func (t *tableActor) Start(ts model.Ts) {
	if atomic.CompareAndSwapInt32(&t.sortNode.started, 0, 1) {
		t.sortNode.startTsCh <- ts
		close(t.sortNode.startTsCh)
	}
}

func (t *tableActor) RemainEvents() int64 {
	return t.sortNode.remainEvent()
}

// for ut
var startPuller = func(t *tableActor, ctx *actorNodeContext) error {
	return t.pullerNode.startWithSorterNode(ctx, t.upstream, t.wg, t.sortNode, t.replicaConfig.BDRMode)
}

var startSorter = func(t *tableActor, ctx *actorNodeContext) error {
	eventSorter, err := createSorter(ctx, t.tableName, t.tableID)
	if err != nil {
		return errors.Trace(err)
	}
	return t.sortNode.start(ctx, t.wg, t.actorID, t.router, eventSorter)
}
