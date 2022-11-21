// Copyright 2020 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/sink"
	"github.com/pingcap/tiflow/cdc/sink/flowcontrol"
	serverConfig "github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pipeline"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
)

const (
	// TODO determine a reasonable default value
	// This is part of sink performance optimization
	resolvedTsInterpolateInterval = 200 * time.Millisecond
)

// TablePipeline is a pipeline which capture the change log from tikv in a table
type TablePipeline interface {
	// ID returns the ID of source table and mark table
	ID() (tableID, markTableID int64)
	// Name returns the quoted schema and table name
	Name() string
	// ResolvedTs returns the resolved ts in this table pipeline
	ResolvedTs() model.Ts
	// CheckpointTs returns the checkpoint ts in this table pipeline
	CheckpointTs() model.Ts
	// UpdateBarrierTs updates the barrier ts in this table pipeline
	UpdateBarrierTs(ts model.Ts)
	// AsyncStop tells the pipeline to stop, and returns true is the pipeline is already stopped.
	AsyncStop(targetTs model.Ts) bool
	// Workload returns the workload of this table
	Workload() model.WorkloadInfo
	// Status returns the status of this table pipeline
	Status() TableStatus
	// Cancel stops this table pipeline immediately and destroy all resources created by this table pipeline
	Cancel()
	// Wait waits for table pipeline destroyed
	Wait()
}

type tablePipelineImpl struct {
	p *pipeline.Pipeline

	tableID     int64
	markTableID int64
	tableName   string // quoted schema and table, used in metrics only

	sorterNode  *sorterNode
	sinkNode    *sinkNode
	redoManager redo.LogManager
	cancel      context.CancelFunc

	replConfig *serverConfig.ReplicaConfig
}

// TODO find a better name or avoid using an interface
// We use an interface here for ease in unit testing.
type tableFlowController interface {
	Consume(
		msg *model.PolymorphicEvent,
		size uint64,
		blockCallBack func(batchID uint64) error,
	) error
	Release(resolved model.ResolvedTs)
	Abort()
	GetConsumption() uint64
}

// ResolvedTs returns the resolved ts in this table pipeline
func (t *tablePipelineImpl) ResolvedTs() model.Ts {
	// TODO: after TiCDC introduces p2p based resolved ts mechanism, TiCDC nodes
	// will be able to cooperate replication status directly. Then we will add
	// another replication barrier for consistent replication instead of reusing
	// the global resolved-ts.
	if t.redoManager.Enabled() {
		return t.redoManager.GetResolvedTs(t.tableID)
	}
	return t.sorterNode.ResolvedTs()
}

// CheckpointTs returns the checkpoint ts in this table pipeline
func (t *tablePipelineImpl) CheckpointTs() model.Ts {
	return t.sinkNode.CheckpointTs()
}

// UpdateBarrierTs updates the barrier ts in this table pipeline
func (t *tablePipelineImpl) UpdateBarrierTs(ts model.Ts) {
	err := t.p.SendToFirstNode(pmessage.BarrierMessage(ts))
	if err != nil && !cerror.ErrSendToClosedPipeline.Equal(err) && !cerror.ErrPipelineTryAgain.Equal(err) {
		log.Panic("unexpect error from send to first node", zap.Error(err))
	}
}

// AsyncStop tells the pipeline to stop, and returns true if the pipeline is already stopped.
func (t *tablePipelineImpl) AsyncStop(targetTs model.Ts) bool {
	err := t.p.SendToFirstNode(pmessage.CommandMessage(&pmessage.Command{
		Tp: pmessage.CommandTypeStop,
	}))
	log.Info("send async stop signal to table", zap.Int64("tableID", t.tableID), zap.Uint64("targetTs", targetTs))
	if err != nil {
		if cerror.ErrPipelineTryAgain.Equal(err) {
			return false
		}
		if cerror.ErrSendToClosedPipeline.Equal(err) {
			return true
		}
		log.Panic("unexpect error from send to first node", zap.Error(err))
	}
	return true
}

var workload = model.WorkloadInfo{Workload: 1}

// Workload returns the workload of this table
func (t *tablePipelineImpl) Workload() model.WorkloadInfo {
	// TODO(leoppro) calculate the workload of this table
	// We temporarily set the value to constant 1
	return workload
}

// Status returns the status of this table pipeline, sinkNode maintains the table status
func (t *tablePipelineImpl) Status() TableStatus {
	return t.sinkNode.Status()
}

// ID returns the ID of source table and mark table
func (t *tablePipelineImpl) ID() (tableID, markTableID int64) {
	return t.tableID, t.markTableID
}

// Name returns the quoted schema and table name
func (t *tablePipelineImpl) Name() string {
	return t.tableName
}

// Cancel stops this table pipeline immediately and destroy all resources created by this table pipeline
func (t *tablePipelineImpl) Cancel() {
	t.cancel()
}

// Wait waits for table pipeline destroyed
func (t *tablePipelineImpl) Wait() {
	t.p.Wait()
}

// Assume 1KB per row in upstream TiDB, it takes about 250 MB (1024*4*64) for
// replicating 1024 tables in the worst case.
const defaultOutputChannelSize = 64

// There are 4 or 5 runners in table pipeline: header, puller, sorter,
// sink, cyclic if cyclic replication is enabled
const defaultRunnersSize = 4

// NewTablePipeline creates a table pipeline
// TODO(leoppro): implement a mock kvclient to test the table pipeline
func NewTablePipeline(ctx cdcContext.Context,
	mounter entry.MounterGroup,
	tableID model.TableID,
	tableName string,
	replicaInfo *model.TableReplicaInfo,
	sink sink.Sink,
	targetTs model.Ts,
	upstream *upstream.Upstream,
	redoManager redo.LogManager,
) TablePipeline {
	ctx, cancel := cdcContext.WithCancel(ctx)
	changefeed := ctx.ChangefeedVars().ID
	replConfig := ctx.ChangefeedVars().Info.Config
	tablePipeline := &tablePipelineImpl{
		tableID:     tableID,
		markTableID: replicaInfo.MarkTableID,
		tableName:   tableName,
		cancel:      cancel,
		replConfig:  replConfig,
		redoManager: redoManager,
	}

	perTableMemoryQuota := serverConfig.GetGlobalServerConfig().PerTableMemoryQuota
	log.Debug("creating table flow controller",
		zap.String("namesapce", ctx.ChangefeedVars().ID.Namespace),
		zap.String("changefeed", ctx.ChangefeedVars().ID.ID),
		zap.String("tableName", tableName),
		zap.Int64("tableID", tableID),
		zap.Uint64("quota", perTableMemoryQuota))
	splitTxn := replConfig.Sink.TxnAtomicity.ShouldSplitTxn()

	flowController := flowcontrol.NewTableFlowController(perTableMemoryQuota,
		redoManager.Enabled(), splitTxn)
	config := ctx.ChangefeedVars().Info.Config
	cyclicEnabled := config.Cyclic != nil && config.Cyclic.IsEnabled()
	runnerSize := defaultRunnersSize
	if cyclicEnabled {
		runnerSize++
	}

	p := pipeline.NewPipeline(ctx, 500*time.Millisecond, runnerSize, defaultOutputChannelSize)
	sorterNode := newSorterNode(tableName, tableID, replicaInfo.StartTs,
		flowController, mounter, replConfig, changefeed, upstream.PDClient)
	sinkNode := newSinkNode(tableID, sink, replicaInfo.StartTs,
		targetTs, flowController, splitTxn, redoManager)

	p.AppendNode(ctx, "puller", newPullerNode(tableID, replicaInfo, tableName,
		changefeed, upstream))
	p.AppendNode(ctx, "sorter", sorterNode)
	if cyclicEnabled {
		p.AppendNode(ctx, "cyclic", newCyclicMarkNode(replicaInfo.MarkTableID))
	}
	p.AppendNode(ctx, "sink", sinkNode)

	tablePipeline.p = p
	tablePipeline.sorterNode = sorterNode
	tablePipeline.sinkNode = sinkNode
	return tablePipeline
}
