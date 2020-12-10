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

package processor

import (
	stdContext "context"
	"sync/atomic"

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
	p           *pipeline.Pipeline
	resolvedTs  uint64
	status      TableStatus
	tableID     int64
	markTableID int64
	tableName   string // quoted schema and table, used in metircs only
	cancel      stdContext.CancelFunc
}

// ResolvedTs returns the resolved ts in this table pipeline
func (t *TablePipeline) ResolvedTs() model.Ts {
	return atomic.LoadUint64(&t.resolvedTs)
}

// AsyncStop tells the pipeline to stop, and returns true is the pipeline is already stopped.
func (t *TablePipeline) AsyncStop() {
	if atomic.CompareAndSwapInt32(&t.status, TableStatusRunning, TableStatusStopping) ||
		atomic.CompareAndSwapInt32(&t.status, TableStatusInitializing, TableStatusStopping) {
		err := t.p.SendToFirstNode(pipeline.CommandMessage(&pipeline.Command{
			Tp: pipeline.CommandTypeShouldStop,
		}))
		if err != nil && !cerror.ErrSendToClosedPipeline.Equal(err) {
			log.Panic("unexpect error from send to first node", zap.Error(err))
		}
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
	return atomic.LoadInt32(&t.status)
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
// TODO: the parameters in this function are too much, try to move some parameters into ctx.Vars().
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
	targetTs model.Ts,
	outputCh chan *model.PolymorphicEvent,
	resolvedTsListener func(*TablePipeline, model.Ts)) (context.Context, *TablePipeline) {
	ctx, cancel := context.WithCancel(ctx)
	tablePipeline := &TablePipeline{
		resolvedTs:  replicaInfo.StartTs,
		tableID:     tableID,
		markTableID: replicaInfo.MarkTableID,
		tableName:   tableName,
		cancel:      cancel,
	}
	listener := func(resolvedTs model.Ts) {
		atomic.StoreUint64(&tablePipeline.resolvedTs, resolvedTs)
		resolvedTsListener(tablePipeline, resolvedTs)
	}

	ctx, p := pipeline.NewPipeline(ctx)
	p.AppendNode(ctx, "puller", newPullerNode(credential, kvStorage, limitter, tableID, replicaInfo, tableName))
	p.AppendNode(ctx, "mounter", newMounterNode(mounter))
	p.AppendNode(ctx, "sorter", newSorterNode(sortEngine, sortDir, tableName))
	p.AppendNode(ctx, "safe_stopper", newSafeStopperNode(targetTs))
	p.AppendNode(ctx, "output", newOutputNode(outputCh, &tablePipeline.status, listener))
	tablePipeline.p = p
	return ctx, tablePipeline
}
