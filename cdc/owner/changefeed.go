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

	"github.com/pingcap/ticdc/pkg/util"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"go.uber.org/zap"
)

type changefeed struct {
	state *model.ChangefeedReactorState

	scheduler        *scheduler
	barriers         *barriers
	feedStateManager *feedStateManager

	schema    schema4Owner
	sink      sink.Sink
	ddlPuller ddlPuller

	sinkErrCh <-chan error
	ddlErrCh  <-chan error
}

/*
func (c *changeFeed) InitTables(ctx context.Context, startTs uint64) error {
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
*/

func (c *changefeed) Tick(ctx context.Context, state *model.ChangefeedReactorState, captures map[model.CaptureID]*model.CaptureInfo) {
	if err := c.tick(ctx, state, captures); err != nil {
		var code string
		if terror, ok := err.(*errors.Error); ok {
			code = string(terror.RFCCode())
		} else {
			code = string(cerror.ErrOwnerUnknown.RFCCode())
		}
		c.feedStateManager.AppendError2Changefeed(&model.RunningError{
			Addr:    util.CaptureAddrFromCtx(ctx),
			Code:    code,
			Message: err.Error(),
		})
	}
}

func (c *changefeed) tick(ctx context.Context, state *model.ChangefeedReactorState, captures map[model.CaptureID]*model.CaptureInfo) error {
	c.state = state
	c.feedStateManager.Tick(state)
	if !c.feedStateManager.ShouldRunning() {
		// TODO release resource
		return nil
	}
	if !c.preCheck(captures) {
		return nil
	}

	// TODO init

	// TODO check error

	barrierTs, err := c.handleBarrier(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	allTableShouldBeListened := c.schema.AllPhysicalTables()
	allListeningTablesNum := len(allTableShouldBeListened)
	c.scheduler.Tick(c.state, allTableShouldBeListened)
	c.state.PatchStatusByTaskStatusAndPosition(func(status *model.ChangeFeedStatus,
		taskPositions map[model.CaptureID]*model.TaskPosition,
		taskStatuses map[model.CaptureID]*model.TaskStatus) (*model.ChangeFeedStatus, bool, error) {
		// check if the table count in etcd is equal to allListeningTablesNum
		// if not equals, skip to update resolvedTs and checkpointTs
		tableCountInEtcd := 0
		for _, taskStatus := range taskStatuses {
			tableCountInEtcd += len(taskStatus.Tables)
			for _, opt := range taskStatus.Operation {
				if opt.Delete {
					tableCountInEtcd++
				}
			}
		}
		if tableCountInEtcd != allListeningTablesNum {
			return status, false, nil
		}

		resolvedTs := barrierTs
		for _, position := range taskPositions {
			if resolvedTs > position.ResolvedTs {
				resolvedTs = position.ResolvedTs
			}
		}
		for _, taskStatus := range taskStatuses {
			for _, opt := range taskStatus.Operation {
				if resolvedTs > opt.BoundaryTs {
					resolvedTs = opt.BoundaryTs
				}
			}
		}
		checkpointTs := resolvedTs
		for _, position := range taskPositions {
			if checkpointTs > position.CheckPointTs {
				checkpointTs = position.CheckPointTs
			}
		}
		changed := false
		if status.ResolvedTs != resolvedTs {
			status.ResolvedTs = resolvedTs
			changed = true
		}
		if status.CheckpointTs != checkpointTs {
			status.CheckpointTs = checkpointTs
			changed = true
		}
		return status, changed, nil
	})
	return nil
}

func (c *changefeed) initialize() {
}

func (c *changefeed) preCheck(captures map[model.CaptureID]*model.CaptureInfo) (passCheck bool) {
	passCheck = true
	if c.state.Status == nil {
		c.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			if status == nil {
				status = &model.ChangeFeedStatus{
					ResolvedTs:   c.state.Info.StartTs - 1,
					CheckpointTs: c.state.Info.StartTs - 1,
					AdminJobType: model.AdminNone,
				}
				return status, true, nil
			}
			return status, false, nil
		})
		passCheck = false
	}
	for captureID := range captures {
		if _, exist := c.state.TaskStatuses[captureID]; !exist {
			c.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
				if status == nil {
					status = new(model.TaskStatus)
					return status, true, nil
				}
				return status, false, nil
			})
			passCheck = false
		}
	}
	return
}

//func (c *changefeed) cleanupStatus(captures map[model.CaptureID]*model.CaptureInfo) (cleared bool) {
//	for captureID := range c.state.TaskStatuses {
//		if _, exist := captures[captureID]; !exist {
//			c.cleanAllStatusByCapture(captureID)
//			cleared = false
//		}
//	}
//	for captureID := range c.state.TaskPositions {
//		if _, exist := captures[captureID]; !exist {
//			c.cleanAllStatusByCapture(captureID)
//			cleared = false
//		}
//	}
//	for captureID := range c.state.Workloads {
//		if _, exist := captures[captureID]; !exist {
//			c.cleanAllStatusByCapture(captureID)
//			cleared = false
//		}
//	}
//	return
//}
//
//func (c *changefeed) cleanAllStatusByCapture(captureID model.CaptureID) {
//	c.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
//		return nil, status != nil, nil
//	})
//	c.state.PatchTaskPosition(captureID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
//		return nil, position != nil, nil
//	})
//	c.state.PatchTaskWorkload(captureID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
//		return nil, workload != nil, nil
//	})
//}

func (c *changefeed) handleBarrier(ctx context.Context) (uint64, error) {
	barrierTp, barrierTs := c.barriers.Min()
	blocked := barrierTs == c.state.Status.CheckpointTs
	switch barrierTp {
	case DDLJobBarrier:
		ddlResolvedTs, ddlJob := c.ddlPuller.FrontDDL()
		if ddlJob == nil {
			c.barriers.Update(DDLJobBarrier, ddlResolvedTs)
			return barrierTs, nil
		}
		if !blocked {
			return barrierTs, nil
		}
		err := c.schema.HandleDDL(ddlJob)
		if err != nil {
			return 0, errors.Trace(err)
		}

		err = c.execDDL(ctx, ddlJob)
		if err != nil {
			return 0, errors.Trace(err)
		}
		c.ddlPuller.PopFrontDDL()
		newDDLResolvedTs, _ := c.ddlPuller.FrontDDL()
		c.barriers.Update(DDLJobBarrier, newDDLResolvedTs)
	case SyncPointBarrier:
		// todo
		panic("not implemented")
	case FinishBarrier:
		c.feedStateManager.MarkFinished()
	default:
		log.Panic("Unknown barrier type", zap.Int("barrier type", barrierTp))
	}
	return barrierTs, nil
}

func (c *changefeed) execDDL(ctx context.Context, job *timodel.Job) error {
	if job.BinlogInfo != nil && job.BinlogInfo.TableInfo != nil && c.schema.IsIneligibleTableID(job.BinlogInfo.TableInfo.ID) {
		return nil
	}
	cyclicConfig := c.state.Info.Config.Cyclic
	if cyclicConfig.IsEnabled() && !cyclicConfig.SyncDDL {
		return nil
	}
	failpoint.Inject("InjectChangefeedDDLError", func() {
		failpoint.Return(cerror.ErrExecDDLFailed.GenWithStackByArgs())
	})
	ddlEvent, err := c.schema.BuildDDLEvent(job)
	if err != nil {
		return errors.Trace(err)
	}
	ddlEvent.Query = binloginfo.AddSpecialComment(ddlEvent.Query)
	err = c.sink.EmitDDLEvent(ctx, ddlEvent)
	// If DDL executing failed, pause the changefeed and print log, rather
	// than return an error and break the running of this owner.
	if err != nil {
		if cerror.ErrDDLEventIgnored.NotEqual(err) {
			log.Error("Execute DDL failed",
				zap.String("ChangeFeedID", c.state.ID),
				zap.Error(err),
				zap.Reflect("ddlJob", job))
			return cerror.ErrExecDDLFailed.GenWithStackByArgs()
		}
		log.Info("Execute DDL ignored", zap.String("changefeed", c.state.ID), zap.Reflect("ddlJob", job))
		return nil
	}
	log.Info("Execute DDL succeeded", zap.String("changefeed", c.state.ID), zap.Reflect("ddlJob", job))
	return nil
}

func (c *changefeed) Close() {
	c.ddlPuller.Close()
	err := c.sink.Close()
	if err != nil {
		log.Warn("Sink closed with error", zap.Error(err))
	}
}
