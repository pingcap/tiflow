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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/cdc/sink/common"
	"github.com/pingcap/ticdc/pkg/actor"
	"github.com/pingcap/ticdc/pkg/actor/message"
	serverConfig "github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	_ TablePipeline = (*tableActor)(nil)
	_ actor.Actor   = (*tableActor)(nil)
)

type tableActor struct {
	cancel    context.CancelFunc
	wg        *errgroup.Group
	reportErr func(error)

	mb actor.Mailbox

	changefeedID string
	// quoted schema and table, used in metrics only
	tableName     string
	tableID       int64
	markTableID   int64
	cyclicEnabled bool
	memoryQuota   uint64
	mounter       entry.Mounter
	replicaInfo   *model.TableReplicaInfo
	sink          sink.Sink
	targetTs      model.Ts

	started bool
	stopped bool
	err     error

	info   *cdcContext.ChangefeedVars
	vars   *cdcContext.GlobalVars
	router *actor.Router

	actorMessageHandler ActorMessageHandler
	sinkNode            TableActorSinkNode

	nodes []*Node
}

// NewTableActor creates a table actor.
func NewTableActor(cdcCtx cdcContext.Context,
	mounter entry.Mounter,
	tableID model.TableID,
	tableName string,
	replicaInfo *model.TableReplicaInfo,
	sink sink.Sink,
	targetTs model.Ts,
	nodeCreator TablePipelineNodeCreator,
) (TablePipeline, error) {
	config := cdcCtx.ChangefeedVars().Info.Config
	cyclicEnabled := config.Cyclic != nil && config.Cyclic.IsEnabled()
	info := cdcCtx.ChangefeedVars()
	vars := cdcCtx.GlobalVars()

	mb := actor.NewMailbox(actor.ID(tableID), defaultOutputChannelSize)
	// Cancel should be able to release all sub-goroutines in the actor.
	ctx, cancel := context.WithCancel(cdcCtx)
	// All sub-goroutines should be spawn in the wait group.
	wg, cctx := errgroup.WithContext(ctx)
	table := &tableActor{
		reportErr: cdcCtx.Throw,
		mb:        mb,
		wg:        wg,
		cancel:    cancel,

		tableID:       tableID,
		markTableID:   replicaInfo.MarkTableID,
		tableName:     tableName,
		cyclicEnabled: cyclicEnabled,
		memoryQuota:   serverConfig.GetGlobalServerConfig().PerTableMemoryQuota,
		mounter:       mounter,
		replicaInfo:   replicaInfo,
		sink:          sink,
		targetTs:      targetTs,
		started:       false,

		info:   info,
		vars:   vars,
		router: vars.TableActorRouter,
	}

	log.Info("spawn and start table actor", zap.Int64("tableID", tableID))
	if err := table.start(cctx, nodeCreator); err != nil {
		table.stop(err)
		return nil, errors.Trace(err)
	}
	err := vars.TableActorSystem.Spawn(mb, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("spawn and start table actor done", zap.Int64("tableID", tableID))
	return table, nil
}

func (t *tableActor) Poll(ctx context.Context, msgs []message.Message) bool {
	for i := range msgs {
		if t.stopped {
			// No need to handle remaining messages.
			break
		}

		switch msgs[i].Tp {
		case message.TypeTick, message.TypeBarrier:
			err := t.actorMessageHandler.HandleActorMessage(ctx, msgs[i])
			if err != nil {
				t.stop(err)
			}
		case message.TypeStop:
			t.stop(nil)
			return false
		}
		// process message for each node
		for _, n := range t.nodes {
			if err := n.TryRun(ctx); err != nil {
				log.Error("failed to process message, stop table actor ", zap.Int64("tableID", t.tableID), zap.Error(err))
				t.stop(err)
				break
			}
		}
	}
	// Report error to processor if there is any.
	t.checkError()
	return !t.stopped
}

func (t *tableActor) start(ctx context.Context, tablePipelineNodeCreator TablePipelineNodeCreator) error {
	if t.started {
		log.Panic("start an already started table",
			zap.String("changefeedID", t.changefeedID),
			zap.Int64("tableID", t.tableID),
			zap.String("tableName", t.tableName))
	}
	log.Debug("creating table flow controller",
		zap.String("changefeedID", t.changefeedID),
		zap.Int64("tableID", t.tableID),
		zap.String("tableName", t.tableName),
		zap.Uint64("quota", t.memoryQuota))

	actorPullerNode := tablePipelineNodeCreator.NewPullerNode(t.tableID, t.tableName, t.replicaInfo)
	if err := actorPullerNode.StartActorNode(ctx, t.router, t.wg, t.info, t.vars); err != nil {
		log.Error("puller fails to start", zap.Error(err))
		return err
	}

	flowController := common.NewTableFlowController(t.memoryQuota)
	actorSorterNode := tablePipelineNodeCreator.NewSorterNode(t.tableID, t.tableName, t.replicaInfo.StartTs, flowController, t.mounter)
	if err := actorSorterNode.StartActorNode(ctx, t.router, t.wg, t.info, t.vars); err != nil {
		log.Error("sorter fails to start", zap.Error(err))
		return err
	}
	t.nodes = append(t.nodes, &Node{
		parentNode:    actorPullerNode,
		dataProcessor: actorSorterNode,
	})

	var cyclicNode TableActorDataNode
	if t.cyclicEnabled {
		cyclicNode = tablePipelineNodeCreator.NewCyclicNode(t.replicaInfo.MarkTableID)
		if err := cyclicNode.StartActorNode(ctx, t.router, t.wg, t.info, t.vars); err != nil {
			log.Error("sink fails to start", zap.Error(err))
			return err
		}
		t.nodes = append(t.nodes, &Node{
			parentNode:    actorSorterNode,
			dataProcessor: cyclicNode,
		})
	}

	actorSinkNode := tablePipelineNodeCreator.NewSinkNode(t.sink, t.replicaInfo.StartTs, t.targetTs, flowController)
	if err := actorSinkNode.StartActorNode(ctx, t.router, t.wg, t.info, t.vars); err != nil {
		log.Error("sink fails to start", zap.Error(err))
		return err
	}
	t.actorMessageHandler = actorSinkNode
	t.sinkNode = actorSinkNode
	node := &Node{dataProcessor: actorSinkNode}
	if t.cyclicEnabled {
		node.parentNode = cyclicNode
	} else {
		node.parentNode = actorSorterNode
	}
	t.nodes = append(t.nodes, node)
	t.started = true
	log.Info("table actor is started", zap.Int64("tableID", t.tableID))
	return nil
}

func (t *tableActor) stop(err error) {
	if t.stopped {
		return
	}
	t.stopped = true
	t.err = err
	t.cancel()
	log.Info("table actor will be stopped",
		zap.Int64("tableID", t.tableID), zap.Error(err))
}

func (t *tableActor) checkError() {
	if t.err != nil {
		t.reportErr(t.err)
		t.err = nil
	}
}

// ============ Implement TablePipline, must be threadsafe ============

// ResolvedTs returns the resolved ts in this table pipeline
func (t *tableActor) ResolvedTs() model.Ts {
	return t.sinkNode.ResolvedTs()
}

// CheckpointTs returns the checkpoint ts in this table pipeline
func (t *tableActor) CheckpointTs() model.Ts {
	return t.sinkNode.CheckpointTs()
}

// UpdateBarrierTs updates the barrier ts in this table pipeline
func (t *tableActor) UpdateBarrierTs(ts model.Ts) {
	if t.sinkNode.BarrierTs() != ts {
		msg := message.BarrierMessage(ts)
		err := t.router.Send(actor.ID(t.tableID), msg)
		if err != nil {
			log.Warn("send fails", zap.Reflect("msg", msg), zap.Error(err))
		}
	}
}

// AsyncStop tells the pipeline to stop, and returns true is the pipeline is already stopped.
func (t *tableActor) AsyncStop(targetTs model.Ts) bool {
	msg := message.StopMessage()
	err := t.router.Send(actor.ID(t.tableID), msg)
	log.Info("send async stop signal to table", zap.Int64("tableID", t.tableID), zap.Uint64("targetTs", targetTs))
	if err != nil {
		if cerror.ErrMailboxFull.Equal(err) {
			return false
		}
		if cerror.ErrSendToClosedPipeline.Equal(err) {
			return true
		}
		log.Panic("send fails", zap.Reflect("msg", msg), zap.Error(err))
	}
	return true
}

// Workload returns the workload of this table
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

// Cancel stops this table actor immediately and destroy all resources
// created by this table pipeline
func (t *tableActor) Cancel() {
	// TODO(neil): pass context.
	if err := t.router.SendB(context.TODO(), t.mb.ID(), message.StopMessage()); err != nil {
		log.Warn("fails to send Stop message",
			zap.Uint64("tableID", uint64(t.tableID)))
	}
}

// Wait waits for table pipeline destroyed
func (t *tableActor) Wait() {
	fmt.Println("wait started")
	_ = t.wg.Wait()
	fmt.Println("wait done")
}

type Node struct {
	eventStash    *pipeline.Message
	parentNode    AsyncDataHolder
	dataProcessor AsyncDataProcessor
}

func (n *Node) TryRun(ctx context.Context) error {
	for {
		// batch?
		if n.eventStash == nil {
			n.eventStash = n.parentNode.TryGetProcessedMessage()
		}
		if n.eventStash == nil {
			return nil
		}
		ok, err := n.dataProcessor.TryHandleDataMessage(ctx, *n.eventStash)
		// process message failed, stop table actor
		if err != nil {
			return errors.Trace(err)
		}

		if ok {
			n.eventStash = nil
		} else {
			return nil
		}
	}
}

type AsyncDataProcessor interface {
	TryHandleDataMessage(ctx context.Context, msg pipeline.Message) (bool, error)
}

type AsyncDataProcessorFunc func(ctx context.Context, msg pipeline.Message) (bool, error)

func (fn AsyncDataProcessorFunc) TryHandleDataMessage(ctx context.Context, msg pipeline.Message) (bool, error) {
	return fn(ctx, msg)
}

type NodeStarter interface {
	StartActorNode(ctx context.Context, router *actor.Router, wg *errgroup.Group, info *cdcContext.ChangefeedVars, vars *cdcContext.GlobalVars) error
}

type AsyncDataHolder interface {
	TryGetProcessedMessage() *pipeline.Message
}

type AsyncDataHolderFunc func() *pipeline.Message

func (fn AsyncDataHolderFunc) TryGetProcessedMessage() *pipeline.Message {
	return fn()
}

type TableActorDataNode interface {
	AsyncDataProcessor
	NodeStarter
	AsyncDataHolder
}

type TableActorSinkNode interface {
	TableActorDataNode
	Status() TableStatus
	ResolvedTs() model.Ts
	CheckpointTs() model.Ts
	BarrierTs() model.Ts
	HandleActorMessage(ctx context.Context, msg message.Message) error
}

type ActorMessageHandler interface {
	HandleActorMessage(ctx context.Context, msg message.Message) error
}

type ActorMessageHandlerFunc func(ctx context.Context, msg message.Message) error

func (fn ActorMessageHandlerFunc) HandleActorMessage(ctx context.Context, msg message.Message) error {
	return fn(ctx, msg)
}

type TablePipelineNodeCreator interface {
	NewPullerNode(tableID model.TableID, tableName string, replicaInfo *model.TableReplicaInfo) TableActorDataNode
	NewSorterNode(tableID model.TableID, tableName string, startTs model.Ts, flowController tableFlowController, mounter entry.Mounter) TableActorDataNode
	NewCyclicNode(markTableID model.TableID) TableActorDataNode
	NewSinkNode(sink sink.Sink, startTs model.Ts, targetTs model.Ts, flowController tableFlowController) TableActorSinkNode
}

type nodeCreatorImpl struct{}

func (n *nodeCreatorImpl) NewPullerNode(tableID model.TableID, tableName string, replicaInfo *model.TableReplicaInfo) TableActorDataNode {
	return newPullerNode(tableID, replicaInfo, tableName).(*pullerNode)
}

func (n *nodeCreatorImpl) NewSorterNode(tableID model.TableID, tableName string, startTs model.Ts, flowController tableFlowController, mounter entry.Mounter) TableActorDataNode {
	return newSorterNode(tableName, tableID, startTs, flowController, mounter)
}

func (n *nodeCreatorImpl) NewCyclicNode(markTableID model.TableID) TableActorDataNode {
	return newCyclicMarkNode(markTableID).(*cyclicMarkNode)
}

func (n *nodeCreatorImpl) NewSinkNode(sink sink.Sink, startTs model.Ts, targetTs model.Ts, flowController tableFlowController) TableActorSinkNode {
	return newSinkNode(sink, startTs, targetTs, flowController)
}

func NewTablePipelineNodeCreator() TablePipelineNodeCreator {
	return &nodeCreatorImpl{}
}

func tryGetProcessedMessageFromChan(outputCh chan pipeline.Message) *pipeline.Message {
	select {
	case msg, ok := <-outputCh:
		if !ok {
			return nil
		}
		return &msg
	default:
		return nil
	}
}

func trySendMessageToNextNode(ctx context.Context, isTableActorMode bool, outputChan chan pipeline.Message, msg pipeline.Message) bool {
	if isTableActorMode {
		select {
		case outputChan <- msg:
			return true
		default:
			return false
		}
	} else {
		ctx.(pipeline.NodeContext).SendToNextNode(msg)
		return true
	}
}
