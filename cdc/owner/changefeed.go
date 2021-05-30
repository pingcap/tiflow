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
	"reflect"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
)

type changefeed struct {
	state *model.ChangefeedReactorState

	scheduler        *scheduler
	barriers         *barriers
	feedStateManager *feedStateManager
	gcManager        *gcManager

	schema        *schemaWrap4Owner
	sink          AsyncSink
	ddlPuller     DDLPuller
	initialized   bool
	ddlEventCache *model.DDLEvent

	errCh  chan error
	cancel context.CancelFunc
	wg     sync.WaitGroup

	newDDLPuller func(ctx cdcContext.Context, startTs uint64) (DDLPuller, error)
	newSink      func(ctx cdcContext.Context) (AsyncSink, error)
}

func newChangefeed(gcManager *gcManager) *changefeed {
	c := &changefeed{
		scheduler:        newScheduler(),
		barriers:         newBarriers(),
		feedStateManager: new(feedStateManager),
		gcManager:        gcManager,

		errCh:  make(chan error, defaultErrChSize),
		cancel: func() {},

		newDDLPuller: newDDLPuller,
	}
	c.newSink = newAsyncSink
	return c
}

func newChangefeed4Test(
	gcManager *gcManager,
	newDDLPuller func(ctx cdcContext.Context, startTs uint64) (DDLPuller, error),
	newSink func(ctx cdcContext.Context) (AsyncSink, error),
) *changefeed {
	c := newChangefeed(gcManager)
	c.newDDLPuller = newDDLPuller
	c.newSink = newSink
	return c
}

func (c *changefeed) Tick(ctx cdcContext.Context, state *model.ChangefeedReactorState, captures map[model.CaptureID]*model.CaptureInfo) {
	ctx = cdcContext.WithErrorHandler(ctx, func(err error) error {
		c.errCh <- errors.Trace(err)
		return nil
	})
	if err := c.tick(ctx, state, captures); err != nil {
		log.Error("an error occurred in Owner", zap.String("changefeedID", c.state.ID), zap.Error(err), zap.Stringer("tp", reflect.TypeOf(err)))
		var code string
		if rfcCode, ok := cerror.RFCCode(err); ok {
			code = string(rfcCode)
		} else {
			code = string(cerror.ErrOwnerUnknown.RFCCode())
		}
		c.feedStateManager.HandleError(&model.RunningError{
			Addr:    util.CaptureAddrFromCtx(ctx),
			Code:    code,
			Message: err.Error(),
		})
		c.releaseResources()
	}
}

func (c *changefeed) tick(ctx cdcContext.Context, state *model.ChangefeedReactorState, captures map[model.CaptureID]*model.CaptureInfo) error {
	c.state = state
	c.feedStateManager.Tick(state)
	if !c.feedStateManager.ShouldRunning() {
		c.releaseResources()
		return nil
	}

	checkpointTs := c.state.Info.GetCheckpointTs(c.state.Status)
	if err := c.gcManager.CheckStaleCheckpointTs(ctx, checkpointTs); err != nil {
		return errors.Trace(err)
	}
	if !c.preCheck(captures) {
		return nil
	}
	if err := c.initialize(ctx); err != nil {
		return errors.Trace(err)
	}

	select {
	case err := <-c.errCh:
		return errors.Trace(err)
	default:
	}

	c.sink.EmitCheckpointTs(ctx, checkpointTs)
	barrierTs, err := c.handleBarrier(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	shouldUpdateState := c.scheduler.Tick(c.state, c.schema.AllPhysicalTables(), captures)
	if shouldUpdateState {
		c.updateStatus(barrierTs)
	}
	return nil
}

func (c *changefeed) initialize(ctx cdcContext.Context) error {
	if c.initialized {
		return nil
	}
	// empty the errCh
LOOP:
	for {
		select {
		case <-c.errCh:
		default:
			break LOOP
		}
	}
	startTs := c.state.Info.GetCheckpointTs(c.state.Status)
	log.Info("initialize changefeed", zap.String("changefeed", c.state.ID),
		zap.Stringer("info", c.state.Info),
		zap.Uint64("checkpoint ts", startTs))
	failpoint.Inject("NewChangefeedNoRetryError", func() {
		failpoint.Return(cerror.ErrStartTsBeforeGC.GenWithStackByArgs(startTs-300, startTs))
	})

	failpoint.Inject("NewChangefeedRetryError", func() {
		failpoint.Return(errors.New("failpoint injected retriable error"))
	})

	if c.state.Info.Config.CheckGCSafePoint {
		err := util.CheckSafetyOfStartTs(ctx, ctx.GlobalVars().PDClient, c.state.ID, startTs)
		if err != nil {
			return errors.Trace(err)
		}
	}
	c.barriers.Update(ddlJobBarrier, startTs)
	c.barriers.Update(syncPointBarrier, startTs)
	c.barriers.Update(finishBarrier, c.state.Info.GetTargetTs())
	var err error
	c.schema, err = newSchemaWrap4Owner(ctx.GlobalVars().KVStorage, startTs, c.state.Info.Config)
	if err != nil {
		return errors.Trace(err)
	}
	cancelCtx, cancel := cdcContext.WithCancel(ctx)
	c.cancel = cancel
	c.sink, err = c.newSink(cancelCtx)
	err = c.sink.Initialize(cancelCtx, c.schema.SinkTableInfos())
	if err != nil {
		return errors.Trace(err)
	}
	c.ddlPuller, err = c.newDDLPuller(cancelCtx, startTs)
	if err != nil {
		return errors.Trace(err)
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ctx.Throw(c.ddlPuller.Run(cancelCtx))
	}()
	c.initialized = true
	return nil
}

func (c *changefeed) releaseResources() {
	if !c.initialized {
		return
	}
	log.Info("close changefeed", zap.String("changefeed", c.state.ID),
		zap.Stringer("info", c.state.Info))
	c.cancel()
	c.cancel = func() {}
	c.ddlPuller.Close()
	c.schema = nil
	c.initialized = false
	if err := c.sink.Close(); err != nil {
		log.Warn("release the owner resources failed", zap.String("changefeedID", c.state.ID), zap.Error(err))
	}
	c.wg.Wait()
}

func (c *changefeed) preCheck(captures map[model.CaptureID]*model.CaptureInfo) (passCheck bool) {
	passCheck = true
	if c.state.Status == nil {
		c.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			if status == nil {
				status = &model.ChangeFeedStatus{
					ResolvedTs:   c.state.Info.StartTs,
					CheckpointTs: c.state.Info.StartTs,
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
	for captureID := range c.state.TaskStatuses {
		if _, exist := captures[captureID]; !exist {
			c.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
				return nil, status != nil, nil
			})
			passCheck = false
		}
	}

	for captureID := range c.state.TaskPositions {
		if _, exist := captures[captureID]; !exist {
			c.state.PatchTaskPosition(captureID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
				return nil, position != nil, nil
			})
			passCheck = false
		}
	}
	for captureID := range c.state.Workloads {
		if _, exist := captures[captureID]; !exist {
			c.state.PatchTaskWorkload(captureID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
				return nil, workload != nil, nil
			})
			passCheck = false
		}
	}
	return
}

func (c *changefeed) handleBarrier(ctx cdcContext.Context) (uint64, error) {
	barrierTp, barrierTs := c.barriers.Min()
	blocked := (barrierTs == c.state.Status.CheckpointTs) && (barrierTs == c.state.Status.ResolvedTs)
	if blocked && c.state.Info.SyncPointEnabled {
		if err := c.sink.SinkSyncpoint(ctx, barrierTs); err != nil {
			return 0, errors.Trace(err)
		}
	}
	switch barrierTp {
	case ddlJobBarrier:
		ddlResolvedTs, ddlJob := c.ddlPuller.FrontDDL()
		if ddlJob == nil || ddlResolvedTs != barrierTs {
			c.barriers.Update(ddlJobBarrier, ddlResolvedTs)
			return barrierTs, nil
		}
		if !blocked {
			return barrierTs, nil
		}
		done, err := c.asyncExecDDL(ctx, ddlJob)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if !done {
			return barrierTs, nil
		}
		c.ddlPuller.PopFrontDDL()
		newDDLResolvedTs, _ := c.ddlPuller.FrontDDL()
		c.barriers.Update(ddlJobBarrier, newDDLResolvedTs)

	case syncPointBarrier:
		if !c.state.Info.SyncPointEnabled {
			c.barriers.Remove(syncPointBarrier)
			return barrierTs, nil
		}
		if !blocked {
			return barrierTs, nil
		}
		nextSyncPointTs := oracle.GoTimeToTS(oracle.GetTimeFromTS(barrierTs).Add(c.state.Info.SyncPointInterval))
		c.barriers.Update(syncPointBarrier, nextSyncPointTs)

	case finishBarrier:
		if !blocked {
			return barrierTs, nil
		}
		c.feedStateManager.MarkFinished()
	default:
		log.Panic("Unknown barrier type", zap.Int("barrier type", int(barrierTp)))
	}
	return barrierTs, nil
}

func (c *changefeed) asyncExecDDL(ctx cdcContext.Context, job *timodel.Job) (done bool, err error) {
	if job.BinlogInfo == nil {
		log.Warn("ignore the invalid DDL job", zap.Reflect("job", job))
		return true, nil
	}
	cyclicConfig := c.state.Info.Config.Cyclic
	if cyclicConfig.IsEnabled() && !cyclicConfig.SyncDDL {
		return true, nil
	}
	if c.ddlEventCache == nil || c.ddlEventCache.CommitTs != job.BinlogInfo.FinishedTS {
		ddlEvent, err := c.schema.BuildDDLEvent(job)
		if err != nil {
			return false, errors.Trace(err)
		}
		err = c.schema.HandleDDL(job)
		if err != nil {
			return false, errors.Trace(err)
		}
		ddlEvent.Query = binloginfo.AddSpecialComment(ddlEvent.Query)
		c.ddlEventCache = ddlEvent
	}
	if job.BinlogInfo.TableInfo != nil && c.schema.IsIneligibleTableID(job.BinlogInfo.TableInfo.ID) {
		log.Warn("ignore the DDL job of ineligible table", zap.Reflect("job", job))
		return true, nil
	}
	done, err = c.sink.EmitDDLEvent(ctx, c.ddlEventCache)
	if err != nil {
		return false, err
	}
	if done {
		c.ddlEventCache = nil
	}
	return done, nil
}

func (c *changefeed) updateStatus(barrierTs model.Ts) {
	resolvedTs := barrierTs
	for _, position := range c.state.TaskPositions {
		if resolvedTs > position.ResolvedTs {
			resolvedTs = position.ResolvedTs
		}
	}
	for _, taskStatus := range c.state.TaskStatuses {
		for _, opt := range taskStatus.Operation {
			if resolvedTs > opt.BoundaryTs {
				resolvedTs = opt.BoundaryTs
			}
		}
	}
	checkpointTs := resolvedTs
	for _, position := range c.state.TaskPositions {
		if checkpointTs > position.CheckPointTs {
			checkpointTs = position.CheckPointTs
		}
	}
	c.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
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
}

func (c *changefeed) Close() {
	c.releaseResources()
}
