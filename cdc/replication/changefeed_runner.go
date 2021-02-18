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
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	"github.com/pingcap/ticdc/pkg/util"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"go.uber.org/zap"
)

type changeFeedRunner interface {
	Tick(ctx context.Context) error
	InitTables(ctx context.Context, checkpointTs uint64) error
	SetOwnerState(state *ownerReactorState)
}

type changeFeedRunnerImpl struct {
	cfID   model.ChangeFeedID
	config *config.ReplicaConfig

	sink       sink.Sink
	sinkErrCh  <-chan error
	ddlHandler ddlHandler

	schemaManager *schemaManager
	filter        *filter.Filter

	ownerState      *ownerReactorState
	changeFeedState *changeFeedState

	ddlJobQueue []*ddlJobWithPreTableInfo
}

func (c *changeFeedRunnerImpl) InitTables(ctx context.Context, checkpointTs uint64) error {
	if c.changeFeedState != nil {
		log.Panic("InitTables: unexpected state", zap.String("cfID", c.cfID))
	}

	if c.ownerState == nil {
		log.Panic("InitTables: ownerState not set")
	}

	kvStore, err := util.KVStorageFromCtx(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	meta, err := kv.GetSnapshotMeta(kvStore, checkpointTs)
	if err != nil {
		return errors.Trace(err)
	}
	schemaSnap, err := entry.NewSingleSchemaSnapshotFromMeta(meta, checkpointTs, c.config.ForceReplicate)
	if err != nil {
		return errors.Trace(err)
	}

	filter, err := filter.NewFilter(c.config)
	if err != nil {
		return errors.Trace(err)
	}

	c.schemaManager = newSchemaManager(schemaSnap, filter, c.config.Cyclic)

	sinkTableInfos := make([]*model.SimpleTableInfo, 0, len(schemaSnap.CloneTables()))
	for tid, table := range schemaSnap.CloneTables() {
		if filter.ShouldIgnoreTable(table.Schema, table.Table) {
			continue
		}
		if c.config.Cyclic.IsEnabled() && mark.IsMarkTable(table.Schema, table.Table) {
			// skip the mark table if cyclic is enabled
			continue
		}

		tblInfo, ok := schemaSnap.TableByID(tid)
		if !ok {
			log.Warn("table not found for table ID", zap.Int64("tid", tid))
			continue
		}

		// TODO separate function for initializing SimpleTableInfo
		sinkTableInfo := new(model.SimpleTableInfo)
		sinkTableInfo.TableID = tid
		sinkTableInfo.ColumnInfo = make([]*model.ColumnInfo, len(tblInfo.Cols()))

		for i, colInfo := range tblInfo.Cols() {
			sinkTableInfo.ColumnInfo[i] = new(model.ColumnInfo)
			sinkTableInfo.ColumnInfo[i].FromTiColumnInfo(colInfo)
		}

		sinkTableInfos = append(sinkTableInfos, sinkTableInfo)
	}

	// We call an unpartitioned table a partition too for simplicity
	existingPartitions := c.ownerState.GetTableToCaptureMap(c.cfID)
	allPartitions := c.schemaManager.AllPartitions()

	initTableTasks := make(map[model.TableID]*tableTask)

	for _, tableID := range allPartitions {
		tableTask := &tableTask{
			TableID:      tableID,
			CheckpointTs: checkpointTs,
			ResolvedTs:   checkpointTs,
		}

		if _, ok := existingPartitions[tableID]; ok {
			// The table is currently being replicated by a processor
			progress := c.ownerState.GetTableProgress(c.cfID, tableID)
			if progress != nil {
				tableTask.CheckpointTs = progress.checkpointTs
				tableTask.ResolvedTs = progress.resolvedTs
			}
		}

		initTableTasks[tableID] = tableTask
	}

	c.changeFeedState = newChangeFeedState(initTableTasks, checkpointTs, newScheduler(c.ownerState, c.cfID))

	return nil
}

func (c *changeFeedRunnerImpl) Tick(ctx context.Context) error {
	if c.ownerState == nil {
		log.Panic("InitTables: ownerState not set")
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-c.sinkErrCh:
		return errors.Trace(err)
	default:
	}

	log.Debug("runner tick", zap.String("cfID", c.cfID))
	defer log.Debug("runner tick end", zap.String("cfID", c.cfID))

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

	for _, job := range newDDLJobs {
		log.Debug("new DDL job received", zap.Reflect("job", job))
		c.ddlJobQueue = append(c.ddlJobQueue, &ddlJobWithPreTableInfo{job, nil})
		c.changeFeedState.AddDDLBarrier(job.BinlogInfo.FinishedTS)
	}
	if len(newDDLJobs) > 0 {
		c.preFilterDDL()
	}
	c.changeFeedState.SetDDLResolvedTs(ddlResolvedTs)

	// Run DDL
	if barrier := c.changeFeedState.ShouldRunDDL(); barrier != nil {
		tableActions, err := c.handleDDL(ctx, c.ddlJobQueue[0])
		if err != nil {
			return errors.Trace(err)
		}

		c.changeFeedState.MarkDDLDone(ddlResult{
			FinishTs: c.ddlJobQueue[0].BinlogInfo.FinishedTS,
			Actions:  tableActions,
		})

		c.preFilterDDL()
	}

	resolvedTs := c.changeFeedState.ResolvedTs()
	checkpointTs := c.changeFeedState.CheckpointTs()
	c.ownerState.UpdateChangeFeedStatus(c.cfID, resolvedTs, checkpointTs)

	c.changeFeedState.SyncTasks()
	return nil
}

func (c *changeFeedRunnerImpl) handleDDL(ctx context.Context, job *ddlJobWithPreTableInfo) ([]tableAction, error) {
	ddlEvent := new(model.DDLEvent)

	actions, err := c.schemaManager.ApplyDDL(job.Job)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ddlEvent.FromJob(job.Job, job.preTableInfo)

	executed := false
	if !c.config.Cyclic.IsEnabled() || c.config.Cyclic.SyncDDL {
		failpoint.Inject("InjectChangefeedDDLError", func() {
			failpoint.Return(cerror.ErrExecDDLFailed.GenWithStackByArgs())
		})

		ddlEvent.Query = binloginfo.AddSpecialComment(ddlEvent.Query)
		err = c.sink.EmitDDLEvent(ctx, ddlEvent)
		// If DDL executing failed, pause the changefeed and print log, rather
		// than return an error and break the running of this owner.
		if err != nil {
			if cerror.ErrDDLEventIgnored.NotEqual(err) {
				log.Error("Execute DDL failed",
					zap.String("ChangeFeedID", c.cfID),
					zap.Error(err),
					zap.Reflect("ddlJob", job))
				return nil, cerror.ErrExecDDLFailed.GenWithStackByArgs()
			}
		} else {
			executed = true
		}
	}

	if executed {
		log.Info("Execute DDL succeeded", zap.String("changefeed", c.cfID), zap.Reflect("ddlJob", job))
	} else {
		log.Info("Execute DDL ignored", zap.String("changefeed", c.cfID), zap.Reflect("ddlJob", job))
	}

	return actions, nil
}

func (c *changeFeedRunnerImpl) preFilterDDL() {
	shouldIgnoreTable := func(schemaName string, tableName string) bool {
		if c.filter.ShouldIgnoreTable(schemaName, tableName) {
			return true
		}
		if c.config.Cyclic.IsEnabled() && mark.IsMarkTable(schemaName, tableName) {
			return true
		}
		return false
	}

	for len(c.ddlJobQueue) > 0 {
		nextJobCandidate := c.ddlJobQueue[0]
		tableInfo, ok := c.schemaManager.schemaSnapshot.TableByID(nextJobCandidate.TableID)
		if !ok {
			log.Panic("preFilterDDL: tableID not found", zap.Stringer("job", nextJobCandidate.Job))
		}

		preSchemaName := tableInfo.TableName.Schema
		preTableName := tableInfo.TableName.Table

		if nextJobCandidate.Type == timodel.ActionRenameTable {
			postSchemaName := nextJobCandidate.BinlogInfo.DBInfo.Name.String()
			postTableName := nextJobCandidate.BinlogInfo.TableInfo.Name.String()

			if shouldIgnoreTable(preSchemaName, preTableName) && shouldIgnoreTable(postSchemaName, postTableName) {
				goto ignoreDDL
			}
		} else {
			if shouldIgnoreTable(preSchemaName, preTableName) {
				goto ignoreDDL
			}
		}
		// DDL is not ignored
		break
	ignoreDDL:
		c.ddlJobQueue = c.ddlJobQueue[1:]
		c.changeFeedState.PopDDLBarrier()
		continue
	}
}

func (c *changeFeedRunnerImpl) SetOwnerState(state *ownerReactorState) {
	c.ownerState = state
}
