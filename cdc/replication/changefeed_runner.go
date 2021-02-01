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

package replication

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"go.uber.org/zap"
)

type changeFeedRunner interface {
	Tick(ctx context.Context) error
}

type changeFeedRunnerImpl struct {
	cfID           model.ChangeFeedID

	sink           sink.Sink
	sinkErrCh      <-chan error
	ddlHandler     ddlHandler
	schemaSnapshot *entry.SingleSchemaSnapshot
	filter         *filter.Filter

	ownerState      *ownerReactorState
	changeFeedState *changeFeedState

	ddlJobQueue     []*timodel.Job
	schemas         map[model.SchemaID][]model.TableID
}

func (c *changeFeedRunnerImpl) Tick(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-c.sinkErrCh:
		return errors.Trace(err)
	default:
	}

	// Update per-table status
	for _, tableID := range c.ownerState.GetChangeFeedActiveTables(c.cfID) {
		progress := c.ownerState.GetTableProgress(c.cfID, tableID)
		c.changeFeedState.SetTableResolvedTs(tableID, progress.resolvedTs)
		c.changeFeedState.SetTableCheckpointTs(tableID, progress.checkpointTs)
	}

	// Receive DDL and update DDL intake status
	ddlResolvedTs, newDDLJobs, err := c.ddlHandler.PullDDL()
	if err != nil {
		return errors.Trace(err)
	}

	c.ddlJobQueue = append(c.ddlJobQueue, newDDLJobs...)

	for _, job := range newDDLJobs {
		c.changeFeedState.AddDDLBarrier(job.BinlogInfo.FinishedTS)
	}
	c.changeFeedState.SetDDLResolvedTs(ddlResolvedTs)

	// Run DDL
	if barrier := c.changeFeedState.ShouldRunDDL(); barrier != nil {

	}

}

func (c *changeFeedRunnerImpl) sendDDLToSink(ctx context.Context, todoDDLJob *timodel.Job) error {
	ddlEvent := new(model.DDLEvent)
	preTableInfo, err := c.schemaSnapshot.PreTableInfo(todoDDLJob)
	if err != nil {
		return errors.Trace(err)
	}

	err = c.schemaSnapshot.HandleDDL(todoDDLJob)
	if err != nil {
		return errors.Trace(err)
	}
	err = c.schemaSnapshot.FillSchemaName(todoDDLJob)
	if err != nil {
		return errors.Trace(err)
	}

	ddlEvent.FromJob(todoDDLJob, preTableInfo)

	skip, err := c.applyJob(ctx, todoDDLJob)
	if err != nil {
		return errors.Trace(err)
	}
	if skip {
		log.Info("ddl job ignored", zap.String("changefeed", c.id), zap.Reflect("job", todoDDLJob))
		c.ddlJobHistory = c.ddlJobHistory[1:]
		c.ddlExecutedTs = todoDDLJob.BinlogInfo.FinishedTS
		c.ddlState = model.ChangeFeedSyncDML
		return nil
	}

	executed := false
	if !c.cyclicEnabled || c.info.Config.Cyclic.SyncDDL {
		failpoint.Inject("InjectChangefeedDDLError", func() {
			failpoint.Return(cerror.ErrExecDDLFailed.GenWithStackByArgs())
		})

		ddlEvent.Query = binloginfo.AddSpecialComment(ddlEvent.Query)
		log.Debug("DDL processed to make special features mysql-compatible", zap.String("query", ddlEvent.Query))
		err = c.sink.EmitDDLEvent(ctx, ddlEvent)
		// If DDL executing failed, pause the changefeed and print log, rather
		// than return an error and break the running of this owner.
		if err != nil {
			if cerror.ErrDDLEventIgnored.NotEqual(err) {
				c.ddlState = model.ChangeFeedDDLExecuteFailed
				log.Error("Execute DDL failed",
					zap.String("ChangeFeedID", c.id),
					zap.Error(err),
					zap.Reflect("ddlJob", todoDDLJob))
				return cerror.ErrExecDDLFailed.GenWithStackByArgs()
			}
		} else {
			executed = true
		}
	}
	if executed {
		log.Info("Execute DDL succeeded", zap.String("changefeed", c.id), zap.Reflect("ddlJob", todoDDLJob))
	} else {
		log.Info("Execute DDL ignored", zap.String("changefeed", c.id), zap.Reflect("ddlJob", todoDDLJob))
	}
}

func (c *changeFeedRunnerImpl) applyDDL(job *timodel.Job) (skip bool, err error) {
	schemaID := job.SchemaID
	if job.BinlogInfo != nil && job.BinlogInfo.TableInfo != nil && c.schemaSnapshot.IsIneligibleTableID(job.BinlogInfo.TableInfo.ID) {
		// TODO is this really necessary?
		tableID := job.BinlogInfo.TableInfo.ID
		c.changeFeedState.MarkDDLDone(ddlResult{
			FinishTs: job.BinlogInfo.FinishedTS,
			Action:   DropTableAction,
			tableID:  tableID,
		})
		return true, nil
	}

	err = func() error {
		// case table id set may change
		switch job.Type {
		case timodel.ActionCreateSchema:
			c.addSchema(schemaID)
		case timodel.ActionDropSchema:
			c.dropSchema(schemaID, job.BinlogInfo.FinishedTS)
		case timodel.ActionCreateTable, timodel.ActionRecoverTable:
			addID := job.BinlogInfo.TableInfo.ID
			table, exist := c.schema.TableByID(addID)
			if !exist {
				return cerror.ErrSnapshotTableNotFound.GenWithStackByArgs(addID)
			}
			c.addTable(table, job.BinlogInfo.FinishedTS)
		case timodel.ActionDropTable:
			dropID := job.TableID
			c.removeTable(schemaID, dropID, job.BinlogInfo.FinishedTS)
		case timodel.ActionRenameTable:
			tableName, exist := c.schema.GetTableNameByID(job.TableID)
			if !exist {
				return cerror.ErrSnapshotTableNotFound.GenWithStackByArgs(job.TableID)
			}
			// no id change just update name
			c.tables[job.TableID] = tableName
		case timodel.ActionTruncateTable:
			dropID := job.TableID
			c.removeTable(schemaID, dropID, job.BinlogInfo.FinishedTS)

			addID := job.BinlogInfo.TableInfo.ID
			table, exist := c.schema.TableByID(addID)
			if !exist {
				return cerror.ErrSnapshotTableNotFound.GenWithStackByArgs(addID)
			}
			c.addTable(table, job.BinlogInfo.FinishedTS)
		case timodel.ActionTruncateTablePartition, timodel.ActionAddTablePartition, timodel.ActionDropTablePartition:
			c.updatePartition(job.BinlogInfo.TableInfo, job.BinlogInfo.FinishedTS)
		}
		return nil
	}()
	if err != nil {
		log.Error("failed to applyJob, start to print debug info", zap.Error(err))
		c.schema.PrintStatus(log.Error)
	}
	return false, err
}
