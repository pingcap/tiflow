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
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/sink"
	"github.com/pingcap/tiflow/cdc/sink/flowcontrol"
	"github.com/pingcap/tiflow/pkg/actor"
	"github.com/pingcap/tiflow/pkg/actor/message"
	serverConfig "github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	_       TablePipeline                 = (*tableActor)(nil)
	_       actor.Actor[pmessage.Message] = (*tableActor)(nil)
	stopped                               = uint32(1)
)

const sinkFlushInterval = 500 * time.Millisecond

type tableActor struct {
	actorID actor.ID
	mb      actor.Mailbox[pmessage.Message]
	router  *actor.Router[pmessage.Message]

	upStream *upstream.Upstream

	// all goroutines in tableActor should be spawned from this wg
	wg *errgroup.Group
	// backend mounter
	mg entry.MounterGroup
	// backend tableSink
	tableSink   sink.Sink
	redoManager redo.LogManager

	pullerNode *pullerNode
	sortNode   *sorterNode
	sinkNode   *sinkNode
	// contains all nodes except pullerNode
	nodes []*ActorNode

	// states of table actor
	started     bool
	stopped     uint32
	stopLock    sync.Mutex
	sinkStopped bool

	// TODO: try to reduce these config fields below in the future
	tableID        int64
	markTableID    int64
	cyclicEnabled  bool
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
	// stopCtx use to stop table actor
	stopCtx context.Context
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
	sink sink.Sink,
	redoManager redo.LogManager,
	targetTs model.Ts,
) (TablePipeline, error) {
	config := cdcCtx.ChangefeedVars().Info.Config
	cyclicEnabled := config.Cyclic != nil && config.Cyclic.IsEnabled()
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

		tableID:       tableID,
		markTableID:   replicaInfo.MarkTableID,
		tableName:     tableName,
		cyclicEnabled: cyclicEnabled,
		memoryQuota:   serverConfig.GetGlobalServerConfig().PerTableMemoryQuota,
		upStream:      up,
		mg:            mg,
		replicaInfo:   replicaInfo,
		replicaConfig: config,
		tableSink:     sink,
		redoManager:   redoManager,
		targetTs:      targetTs,
		started:       false,

		changefeedID:   changefeedVars.ID,
		changefeedVars: changefeedVars,
		globalVars:     globalVars,
		router:         globalVars.TableActorSystem.Router(),
		actorID:        actorID,

		stopCtx: cctx,
	}

	startTime := time.Now()
	log.Info("table actor starting",
		zap.String("namespace", table.changefeedID.Namespace),
		zap.String("changefeed", table.changefeedID.ID),
		zap.String("tableName", tableName),
		zap.Int64("tableID", tableID))
	if err := table.start(cctx); err != nil {
		table.stop(err)
		return nil, errors.Trace(err)
	}
	err := globalVars.TableActorSystem.System().Spawn(mb, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("table actor started",
		zap.String("namespace", table.changefeedID.Namespace),
		zap.String("changefeed", table.changefeedID.ID),
		zap.String("tableName", tableName),
		zap.Int64("tableID", tableID),
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
				err = t.handleBarrierMsg(ctx, msgs[i].Value.BarrierTs)
			case pmessage.MessageTypeTick:
				err = t.handleTickMsg(ctx)
			}
		case message.TypeStop:
			t.handleStopMsg(ctx)
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
		if !t.sinkStopped {
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
	t.sinkStopped = true
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
	log.Debug("creating table flow controller",
		zap.String("namespace", t.changefeedID.Namespace),
		zap.String("changefeed", t.changefeedID.ID),
		zap.Int64("tableID", t.tableID),
		zap.String("tableName", t.tableName),
		zap.Uint64("quota", t.memoryQuota))

	splitTxn := t.replicaConfig.Sink.TxnAtomicity.ShouldSplitTxn()

	flowController := flowcontrol.NewTableFlowController(t.memoryQuota,
		t.redoManager.Enabled(), splitTxn)
	sorterNode := newSorterNode(t.tableName, t.tableID,
		t.replicaInfo.StartTs, flowController,
		t.mg, t.replicaConfig, t.changefeedID, t.upStream.PDClient,
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

	pullerNode := newPullerNode(t.tableID, t.replicaInfo, t.tableName,
		t.changefeedVars.ID, t.upStream)
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

	messageFetchFunc, err := t.getSinkAsyncMessageHolder(sdtTableContext, sortActorNodeContext)
	if err != nil {
		return errors.Trace(err)
	}

	actorSinkNode := newSinkNode(t.tableID, t.tableSink,
		t.replicaInfo.StartTs,
		t.targetTs, flowController, splitTxn, t.redoManager)
	actorSinkNode.initWithReplicaConfig(true, t.replicaConfig)
	t.sinkNode = actorSinkNode

	// construct sink actor node, it gets message from sortNode or cyclicNode
	var messageProcessFunc asyncMessageProcessorFunc = func(
		ctx context.Context, msg pmessage.Message,
	) (bool, error) {
		return actorSinkNode.HandleMessage(sdtTableContext, msg)
	}
	t.nodes = append(t.nodes, NewActorNode(messageFetchFunc, messageProcessFunc))

	t.started = true
	log.Info("table actor is started",
		zap.String("tableName", t.tableName),
		zap.Int64("tableID", t.tableID))
	return nil
}

func (t *tableActor) getSinkAsyncMessageHolder(
	sdtTableContext context.Context,
	sortActorNodeContext *actorNodeContext) (AsyncMessageHolder, error,
) {
	var messageFetchFunc asyncMessageHolderFunc = func() *pmessage.Message {
		return sortActorNodeContext.tryGetProcessedMessage()
	}
	// check if cyclic feature is enabled
	if t.cyclicEnabled {
		cyclicNode := newCyclicMarkNode(t.markTableID)
		cyclicActorNodeContext := newCyclicNodeContext(
			newContext(sdtTableContext, t.tableName,
				t.globalVars.TableActorSystem.Router(),
				t.actorID, t.changefeedVars,
				t.globalVars, t.reportErr))
		if err := cyclicNode.InitTableActor(t.changefeedVars.Info.Config.Cyclic.ReplicaID,
			t.changefeedVars.Info.Config.Cyclic.FilterReplicaID, true); err != nil {
			log.Error("failed to start cyclic node",
				zap.String("tableName", t.tableName),
				zap.Int64("tableID", t.tableID),
				zap.Error(err))
			return nil, err
		}

		// construct cyclic actor node if it's enabled, it gets message from sortNode
		var messageProcessFunc asyncMessageProcessorFunc = func(
			ctx context.Context, msg pmessage.Message,
		) (bool, error) {
			return cyclicNode.TryHandleDataMessage(cyclicActorNodeContext, msg)
		}
		t.nodes = append(t.nodes, NewActorNode(messageFetchFunc, messageProcessFunc))
		messageFetchFunc = func() *pmessage.Message {
			return cyclicActorNodeContext.tryGetProcessedMessage()
		}
	}
	return messageFetchFunc, nil
}

// stop will set this table actor state to stopped and releases all goroutines spawned
// from this table actor
func (t *tableActor) stop(err error) {
	log.Info("table actor begin to stop....",
		zap.String("namespace", t.changefeedID.Namespace),
		zap.String("changefeed", t.changefeedID.ID),
		zap.String("tableName", t.tableName))
	t.stopLock.Lock()
	defer t.stopLock.Unlock()
	if atomic.LoadUint32(&t.stopped) == stopped {
		log.Info("table actor is already stopped",
			zap.String("namespace", t.changefeedID.Namespace),
			zap.String("changefeed", t.changefeedID.ID),
			zap.String("tableName", t.tableName))
		return
	}
	atomic.StoreUint32(&t.stopped, stopped)
	if t.sortNode != nil {
		// releaseResource will send a message to sorter router
		t.sortNode.releaseResource(t.changefeedID)
	}
	t.cancel()
	if t.sinkNode != nil {
		if err := t.sinkNode.releaseResource(t.stopCtx); err != nil {
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
		zap.Int64("tableID", t.tableID),
		zap.Error(err))
}

// handleError stops the table actor at first and then reports the error to processor
func (t *tableActor) handleError(err error) {
	t.stop(err)
	if !cerror.ErrTableProcessorStoppedSafely.Equal(err) {
		t.reportErr(err)
	}
}

// ============ Implement TablePipline, must be threadsafe ============

// ResolvedTs returns the resolved ts in this table pipeline
func (t *tableActor) ResolvedTs() model.Ts {
	// TODO: after TiCDC introduces p2p based resolved ts mechanism, TiCDC nodes
	// will be able to cooperate replication status directly. Then we will add
	// another replication barrier for consistent replication instead of reusing
	// the global resolved-ts.
	if t.redoManager.Enabled() {
		return t.redoManager.GetResolvedTs(t.tableID)
	}
	return t.sortNode.ResolvedTs()
}

// CheckpointTs returns the checkpoint ts in this table pipeline
func (t *tableActor) CheckpointTs() model.Ts {
	return t.sinkNode.CheckpointTs()
}

// UpdateBarrierTs updates the barrier ts in this table pipeline
func (t *tableActor) UpdateBarrierTs(ts model.Ts) {
	msg := pmessage.BarrierMessage(ts)
	err := t.router.Send(t.actorID, message.ValueMessage(msg))
	if err != nil {
		log.Warn("send fails",
			zap.Reflect("msg", msg),
			zap.String("tableName", t.tableName),
			zap.Int64("tableID", t.tableID),
			zap.Error(err))
	}
}

// AsyncStop tells the pipeline to stop, and returns true if the pipeline is already stopped.
func (t *tableActor) AsyncStop(targetTs model.Ts) bool {
	// TypeStop stop the sinkNode only ,the processor stop the sink to release some resource
	// and then stop the whole table pipeline by call Cancel
	msg := message.StopMessage[pmessage.Message]()
	err := t.router.Send(t.actorID, msg)
	log.Info("send async stop signal to table",
		zap.String("tableName", t.tableName),
		zap.Int64("tableID", t.tableID),
		zap.Uint64("targetTs", targetTs))
	if err != nil {
		if cerror.ErrMailboxFull.Equal(err) {
			return false
		}
		if cerror.ErrActorNotFound.Equal(err) || cerror.ErrActorStopped.Equal(err) {
			return true
		}
		log.Panic("send fails", zap.Reflect("msg", msg), zap.Error(err))
	}
	return true
}

// Workload returns the workload of this table pipeline
func (t *tableActor) Workload() model.WorkloadInfo {
	// We temporarily set the value to constant 1
	return workload
}

// Status returns the status of this table pipeline
func (t *tableActor) Status() TableStatus {
	return t.sinkNode.Status()
}

// ID returns the ID of source table and mark table
func (t *tableActor) ID() (tableID, markTableID int64) {
	return t.tableID, t.markTableID
}

// Name returns the quoted schema and table name
func (t *tableActor) Name() string {
	return t.tableName
}

// Cancel stops this table pipeline immediately and destroy all resources
// created by this table pipeline
func (t *tableActor) Cancel() {
	// cancel wait group, release resource and mark the state as stopped
	t.stop(nil)
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

// for ut
var startPuller = func(t *tableActor, ctx *actorNodeContext) error {
	return t.pullerNode.start(ctx, t.wg, true, t.sortNode)
}

var startSorter = func(t *tableActor, ctx *actorNodeContext) error {
	eventSorter, err := createSorter(ctx, t.tableName, t.tableID)
	if err != nil {
		return errors.Trace(err)
	}
	return t.sortNode.start(ctx, true, t.wg, t.actorID, t.router, eventSorter)
}
