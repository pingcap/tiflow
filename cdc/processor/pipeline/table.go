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
	stdContext "context"
	"time"

	"github.com/pingcap/ticdc/cdc/sink"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/pingcap/ticdc/pkg/security"
	tidbkv "github.com/pingcap/tidb/kv"
	"go.uber.org/zap"
)

// TablePipeline is a pipeline which capture the change log from tikv in a table
type TablePipeline struct {
	p *pipeline.Pipeline

	tableID     int64
	markTableID int64
	tableName   string // quoted schema and table, used in metircs only

	sinkNode *sinkNode
	cancel   stdContext.CancelFunc
}

// ResolvedTs returns the resolved ts in this table pipeline
func (t *TablePipeline) ResolvedTs() model.Ts {
	return t.sinkNode.ResolvedTs()
}

// CheckpointTs returns the checkpoint ts in this table pipeline
func (t *TablePipeline) CheckpointTs() model.Ts {
	return t.sinkNode.CheckpointTs()
}

// UpdateBarrierTs updates the barrier ts in this table pipeline
func (t *TablePipeline) UpdateBarrierTs(ts model.Ts) {
	err := t.p.SendToFirstNode(pipeline.BarrierMessage(ts))
	if err != nil && !cerror.ErrSendToClosedPipeline.Equal(err) {
		log.Panic("unexpect error from send to first node", zap.Error(err))
	}
}

// AsyncStop tells the pipeline to stop, and returns true is the pipeline is already stopped.
func (t *TablePipeline) AsyncStop(targetTs model.Ts) {
	err := t.p.SendToFirstNode(pipeline.CommandMessage(&pipeline.Command{
		Tp:        pipeline.CommandTypeStopAtTs,
		StoppedTs: targetTs,
	}))
	if err != nil && !cerror.ErrSendToClosedPipeline.Equal(err) {
		log.Panic("unexpect error from send to first node", zap.Error(err))
	}
}

var workload = model.WorkloadInfo{Workload: 1}

// Workload returns the workload of this table
func (t *TablePipeline) Workload() model.WorkloadInfo {
	// TODO(leoppro) calculate the workload of this table
	// We temporarily set the value to constant 1
	return workload
}

// Status returns the status of this table pipeline
func (t *TablePipeline) Status() TableStatus {
	return t.sinkNode.Status()
}

// ID returns the ID of source table and mark table
func (t *TablePipeline) ID() (tableID, markTableID int64) {
	return t.tableID, t.markTableID
}

// Name returns the quoted schema and table name
func (t *TablePipeline) Name() string {
	return t.tableName
}

// Cancel stops this table pipeline immediately and destroy all resources created by this table pipeline
func (t *TablePipeline) Cancel() {
	t.cancel()
}

// Wait waits for all node destroyed and returns errors
func (t *TablePipeline) Wait() []error {
	return t.p.Wait()
}

// NewTablePipeline creates a table pipeline
// TODO(leoppro): the parameters in this function are too much, try to move some parameters into ctx.Vars().
// TODO(leoppro): implement a mock kvclient to test the table pipeline
func NewTablePipeline(ctx context.Context,
	credential *security.Credential,
	kvStorage tidbkv.Storage,
	limitter *puller.BlurResourceLimitter,
	mounter entry.Mounter,
	sortEngine model.SortEngine,
	sortDir string,
	tableID model.TableID,
	tableName string,
	replicaInfo *model.TableReplicaInfo,
	sink sink.Sink,
	targetTs model.Ts) (context.Context, *TablePipeline) {
	ctx, cancel := context.WithCancel(ctx)
	tablePipeline := &TablePipeline{
		tableID:     tableID,
		markTableID: replicaInfo.MarkTableID,
		tableName:   tableName,
		cancel:      cancel,
	}

	ctx, p := pipeline.NewPipeline(ctx, 500*time.Millisecond)
	p.AppendNode(ctx, "puller", newPullerNode(credential, kvStorage, limitter, tableID, replicaInfo, tableName))
	p.AppendNode(ctx, "sorter", newSorterNode(sortEngine, sortDir, tableName))
	p.AppendNode(ctx, "mounter", newMounterNode(mounter))
	tablePipeline.sinkNode = newSinkNode(sink, replicaInfo.StartTs, targetTs)
	p.AppendNode(ctx, "sink", tablePipeline.sinkNode)
	tablePipeline.p = p
	return ctx, tablePipeline
}
