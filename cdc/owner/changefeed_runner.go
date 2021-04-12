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

package owner

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"go.uber.org/zap"
)

type changeFeedRunner interface {
	Tick(ctx context.Context) error
	InitTables(ctx context.Context, checkpointTs uint64) error
	SetOwnerState(state *ownerReactorState)
	Close()
}

type changeFeedRunnerImpl struct {
	cfID   model.ChangeFeedID
	config *config.ReplicaConfig

	sink      sink.Sink
	sinkErrCh <-chan error

	ddlHandler ddlHandler
	ddlCancel  func()
	ddlErrCh   <-chan error

	schemaManager *schemaManager
	filter        *filter.Filter

	ownerState      *ownerReactorState
	changeFeedState *changeFeedState
}

func (c *changeFeedRunnerImpl) InitTables(ctx context.Context, startTs uint64) error {
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
	meta, err := kv.GetSnapshotMeta(kvStore, startTs)
	if err != nil {
		return errors.Trace(err)
	}
	schemaSnap, err := entry.NewSingleSchemaSnapshotFromMeta(meta, startTs, c.config.ForceReplicate)
	if err != nil {
		return errors.Trace(err)
	}

	filter, err := filter.NewFilter(c.config)
	if err != nil {
		return errors.Trace(err)
	}

	c.schemaManager = newSchemaManager(schemaSnap, filter, c.config)

	c.sink.Initialize(ctx, c.schemaManager.SinkTableInfos())

	// We call an unpartitioned table a partition too for simplicity
	existingPhysicalTables := c.ownerState.GetTableToCaptureMap(c.cfID)
	allPhysicalTables := c.schemaManager.AllPhysicalTables()

	initTableTasks := make(map[model.TableID]*tableTask)

	for _, tableID := range allPhysicalTables {
		tableTask := &tableTask{
			TableID:      tableID,
			CheckpointTs: startTs - 1,
			ResolvedTs:   startTs - 1,
		}

		if _, ok := existingPhysicalTables[tableID]; ok {
			// The table is currently being replicated by a processor
			progress := c.ownerState.GetTableProgress(c.cfID, tableID)
			if progress != nil {
				tableTask.CheckpointTs = progress.checkpointTs
				tableTask.ResolvedTs = progress.resolvedTs
			}
		}

		initTableTasks[tableID] = tableTask
	}

	c.changeFeedState = newChangeFeedState(initTableTasks, startTs, newScheduler(c.ownerState, c.cfID))

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
	case err := <-c.ddlErrCh:
		return errors.Trace(err)
	default:
	}

	// Update per-table status
	for _, tableID := range c.ownerState.GetChangeFeedActiveTables(c.cfID) {
		progress := c.ownerState.GetTableProgress(c.cfID, tableID)
		if progress == nil {
			// Possibly the capture that is supposed to run the table is not running yet.
			continue
		}
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
	}
	c.schemaManager.AppendDDLJob(newDDLJobs...)
	c.changeFeedState.SetDDLResolvedTs(ddlResolvedTs)
	jobToExec, err := c.schemaManager.TopDDLJobToExec()
	if err != nil {
		return errors.Trace(err)
	}
	if jobToExec != nil {
		log.Info("LEOPPRO add barrier", zap.Reflect("job", jobToExec))
		c.changeFeedState.AddDDLBarrier(jobToExec)
	}

	c.changeFeedState.CalcResolvedTsAndCheckpointTs()

	log.Info("LEOPPRO show barrier", zap.Any("barriers", c.changeFeedState.barriers.inner))
	blocked, barrierType, barrierIndex, barrierTs := c.changeFeedState.BlockingByBarrier()
	if blocked {
		switch barrierType {
		case DDLJobBarrier:
			if barrierIndex != uint64(jobToExec.ID) {
				log.Panic("LEOPPRO", zap.Any("barrierType", barrierType), zap.Any("barrierIndex", barrierIndex), zap.Any("barrierTs", barrierTs), zap.Any("job", jobToExec))
			}
			err := c.execDDL(ctx, jobToExec)
			if err != nil {
				return errors.Trace(err)
			}
			tableActions, err := c.schemaManager.MarkDDLExecuted()
			if err != nil {
				return errors.Trace(err)
			}
			c.changeFeedState.ApplyTableActions(tableActions)
			c.changeFeedState.ClearBarrier(barrierType, barrierIndex)
		case SyncPointBarrier:
			// todo
			panic("not implemented")
		case DDLResolvedTs:
			// just go to next tick
		}
	}

	resolvedTs := c.changeFeedState.ResolvedTs
	checkpointTs := c.changeFeedState.CheckpointTs
	log.Debug("Updating changeFeed", zap.Uint64("resolvedTs", resolvedTs), zap.Uint64("checkpointTs", checkpointTs))
	c.ownerState.UpdateChangeFeedStatus(c.cfID, resolvedTs, checkpointTs)

	c.changeFeedState.SyncTasks()
	return nil
}

func (c *changeFeedRunnerImpl) execDDL(ctx context.Context, job *timodel.Job) error {
	ddlEvent, err := c.schemaManager.BuildDDLEvent(job)
	if err != nil {
		return errors.Trace(err)
	}

	executed := false
	if !c.config.Cyclic.IsEnabled() || c.config.Cyclic.SyncDDL {
		failpoint.Inject("InjectChangefeedDDLError", func() {
			failpoint.Return(cerror.ErrExecDDLFailed.GenWithStackByArgs())
		})

		ddlEvent.Query = binloginfo.AddSpecialComment(ddlEvent.Query)
		err := c.sink.EmitDDLEvent(ctx, ddlEvent)
		// If DDL executing failed, pause the changefeed and print log, rather
		// than return an error and break the running of this owner.
		if err != nil {
			if cerror.ErrDDLEventIgnored.NotEqual(err) {
				log.Error("Execute DDL failed",
					zap.String("ChangeFeedID", c.cfID),
					zap.Error(err),
					zap.Reflect("ddlJob", job))
				return cerror.ErrExecDDLFailed.GenWithStackByArgs()
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
	return nil
}

func (c *changeFeedRunnerImpl) SetOwnerState(state *ownerReactorState) {
	c.ownerState = state
}

func (c *changeFeedRunnerImpl) Close() {
	c.ddlCancel()
	err := c.sink.Close()
	if err != nil {
		log.Warn("Sink closed with error", zap.Error(err))
	}
}
