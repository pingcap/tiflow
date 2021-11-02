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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/actor"
	"github.com/pingcap/ticdc/pkg/actor/message"
	serverConfig "github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

var (
	defaultSystem *actor.System
	defaultRouter *actor.Router
)

func init() {
	defaultSystem, defaultRouter = actor.NewSystemBuilder("table").Build()
	defaultSystem.Start(context.Background())
}

type tableActor struct {
	cancel    context.CancelFunc
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

	info *cdcContext.ChangefeedVars
	vars *cdcContext.GlobalVars

	pullerNode *pullerNode
	sorterNode *sorterNode
	cyclicNode *cyclicMarkNode
	sinkNode   *sinkNode
}

var _ TablePipeline = (*tableActor)(nil)
var _ actor.Actor = (*tableActor)(nil)

func (t *tableActor) Poll(ctx context.Context, msgs []message.Message) bool {
	return !t.stopped
}

func (t *tableActor) start() error {
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

	log.Info("table actor is started", zap.Int64("tableID", t.tableID))
	return nil
}

func (t *tableActor) stop(err error) {
	if t.stopped {
		// It's stopped already.
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
	if t.sinkNode.barrierTs != ts {
		msg := message.BarrierMessage(ts)
		err := defaultRouter.Send(actor.ID(t.tableID), msg)
		if err != nil {
			log.Warn("send fails", zap.Reflect("msg", msg), zap.Error(err))
		}
	}
}

// AsyncStop tells the pipeline to stop, and returns true is the pipeline is already stopped.
func (t *tableActor) AsyncStop(targetTs model.Ts) bool {
	msg := message.StopMessage()
	err := defaultRouter.Send(actor.ID(t.tableID), msg)
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
	if err := t.mb.SendB(context.TODO(), message.StopMessage()); err != nil {
		log.Warn("fails to send Stop message",
			zap.Uint64("tableID", uint64(t.tableID)))
	}
}

// Wait waits for table pipeline destroyed
func (t *tableActor) Wait() {
	//todo: wait
}

// NewTableActor creates a table actor.
func NewTableActor(cdcCtx cdcContext.Context,
	mounter entry.Mounter,
	tableID model.TableID,
	tableName string,
	replicaInfo *model.TableReplicaInfo,
	sink sink.Sink,
	targetTs model.Ts,
) (TablePipeline, error) {
	config := cdcCtx.ChangefeedVars().Info.Config
	cyclicEnabled := config.Cyclic != nil && config.Cyclic.IsEnabled()
	info := cdcCtx.ChangefeedVars()
	vars := cdcCtx.GlobalVars()

	mb := actor.NewMailbox(actor.ID(tableID), defaultOutputChannelSize)
	table := &tableActor{
		reportErr: cdcCtx.Throw,
		mb:        mb,

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

		info: info,
		vars: vars,
	}

	log.Info("spawn and start table actor", zap.Int64("tableID", tableID))
	if err := table.start(); err != nil {
		return nil, err
	}
	err := defaultSystem.Spawn(mb, table)
	if err != nil {
		return nil, err
	}
	log.Info("spawn and start table actor done", zap.Int64("tableID", tableID))
	return table, nil
}
