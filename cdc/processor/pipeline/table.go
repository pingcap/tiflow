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
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/cdc/sink/common"
	serverConfig "github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
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
	tableName   string // quoted schema and table, used in metircs only

	sinkNode *sinkNode
	cancel   context.CancelFunc
}

// TODO find a better name or avoid using an interface
// We use an interface here for ease in unit testing.
type tableFlowController interface {
	Consume(commitTs uint64, size uint64, blockCallBack func() error) error
	Release(resolvedTs uint64)
	Abort()
	GetConsumption() uint64
}

// ResolvedTs returns the resolved ts in this table pipeline
func (t *tablePipelineImpl) ResolvedTs() model.Ts {
	return t.sinkNode.ResolvedTs()
}

// CheckpointTs returns the checkpoint ts in this table pipeline
func (t *tablePipelineImpl) CheckpointTs() model.Ts {
	return t.sinkNode.CheckpointTs()
}

// UpdateBarrierTs updates the barrier ts in this table pipeline
func (t *tablePipelineImpl) UpdateBarrierTs(ts model.Ts) {
	err := t.p.SendToFirstNode(pipeline.BarrierMessage(ts))
	if err != nil && !cerror.ErrSendToClosedPipeline.Equal(err) && !cerror.ErrPipelineTryAgain.Equal(err) {
		log.Panic("unexpect error from send to first node", zap.Error(err))
	}
}

// AsyncStop tells the pipeline to stop, and returns true is the pipeline is already stopped.
func (t *tablePipelineImpl) AsyncStop(targetTs model.Ts) bool {
	err := t.p.SendToFirstNode(pipeline.CommandMessage(&pipeline.Command{
		Tp:        pipeline.CommandTypeStopAtTs,
		StoppedTs: targetTs,
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

// Status returns the status of this table pipeline
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

// NewTablePipeline creates a table pipeline
// TODO(leoppro): implement a mock kvclient to test the table pipeline
func NewTablePipeline(ctx cdcContext.Context,
	limitter *puller.BlurResourceLimitter,
	mounter entry.Mounter,
	tableID model.TableID,
	tableName string,
	replicaInfo *model.TableReplicaInfo,
	sink sink.Sink,
	targetTs model.Ts) TablePipeline {
	ctx, cancel := cdcContext.WithCancel(ctx)
	tablePipeline := &tablePipelineImpl{
		tableID:     tableID,
		markTableID: replicaInfo.MarkTableID,
		tableName:   tableName,
		cancel:      cancel,
	}

	perTableMemoryQuota := serverConfig.GetGlobalServerConfig().PerTableMemoryQuota
	log.Debug("creating table flow controller",
		zap.String("changefeed-id", ctx.ChangefeedVars().ID),
		zap.String("table-name", tableName),
		zap.Int64("table-id", tableID),
		zap.Uint64("quota", perTableMemoryQuota))
	flowController := common.NewTableFlowController(perTableMemoryQuota)

	p := pipeline.NewPipeline(ctx, 2*time.Second)
	p.AppendNode(ctx, "puller", newPullerNode(limitter, tableID, replicaInfo, tableName))
	p.AppendNode(ctx, "sorter", newSorterNode(tableName, tableID, flowController, mounter))
	p.AppendNode(ctx, "mounter", newMounterNode())
	config := ctx.ChangefeedVars().Info.Config
	if config.Cyclic != nil && config.Cyclic.IsEnabled() {
		p.AppendNode(ctx, "cyclic", newCyclicMarkNode(replicaInfo.MarkTableID))
	}
	tablePipeline.sinkNode = newSinkNode(sink, replicaInfo.StartTs, targetTs, flowController)
	p.AppendNode(ctx, "sink", tablePipeline.sinkNode)
	tablePipeline.p = p
	return tablePipeline
}
