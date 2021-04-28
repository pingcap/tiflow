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
	"time"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/store/tikv/oracle"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	defaultErrChSize = 1024
)

type changefeed struct {
	state *model.ChangefeedReactorState

	scheduler        *scheduler
	barriers         *barriers
	feedStateManager *feedStateManager
	gcManager        *gcManager

	schema      schema4Owner
	sink        sink.Sink
	ddlPuller   ddlPuller
	initialized bool

	pdClient   pd.Client
	credential *security.Credential

	gcTTL int64

	errCh  chan error
	cancel context.CancelFunc
}

func newChangefeed(pdClient pd.Client, credential *security.Credential, gcManager *gcManager) *changefeed {
	serverConfig := config.GetGlobalServerConfig()
	gcTTL := serverConfig.GcTTL
	return &changefeed{
		scheduler:        newScheduler(),
		barriers:         newBarriers(),
		feedStateManager: new(feedStateManager),
		gcTTL:            gcTTL,
		gcManager:        gcManager,

		pdClient:   pdClient,
		credential: credential,
		errCh:      make(chan error, defaultErrChSize),
		cancel:     func() {},
	}
}

func (c *changefeed) Tick(ctx context.Context, state *model.ChangefeedReactorState, captures map[model.CaptureID]*model.CaptureInfo) {
	if err := c.tick(ctx, state, captures); err != nil {
		log.Error("an error occurred in Owner", zap.String("changefeedID", c.state.ID), zap.Error(err))
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
		if err := c.releaseResources(); err != nil {
			log.Error("release the owner resources failed", zap.String("changefeedID", c.state.ID), zap.Error(err))
		}
	}
}

func (c *changefeed) tick(ctx context.Context, state *model.ChangefeedReactorState, captures map[model.CaptureID]*model.CaptureInfo) error {
	c.state = state
	c.feedStateManager.Tick(state)
	if !c.feedStateManager.ShouldRunning() {
		return c.releaseResources()
	}

	checkpointTs := c.state.Info.GetCheckpointTs(c.state.Status)
	gcSafePointTs := c.gcManager.GcSafePointTs()
	if checkpointTs < gcSafePointTs || time.Since(oracle.GetTimeFromTS(checkpointTs)) > time.Duration(c.gcTTL)*time.Second {
		return cerror.ErrStartTsBeforeGC.GenWithStackByArgs(checkpointTs, gcSafePointTs)
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

	barrierTs, err := c.handleBarrier(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	allTableInListened := c.scheduler.Tick(c.state, c.schema.AllPhysicalTables())
	if allTableInListened {
		c.updateStatus(barrierTs)
	}
	return nil
}

func (c *changefeed) initialize(ctx context.Context) error {
	if c.initialized {
		return nil
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
		err := util.CheckSafetyOfStartTs(ctx, c.pdClient, c.state.ID, startTs)
		if err != nil {
			return errors.Trace(err)
		}
	}
	kvStore, err := util.KVStorageFromCtx(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	meta, err := kv.GetSnapshotMeta(kvStore, startTs)
	if err != nil {
		return errors.Trace(err)
	}
	schemaSnap, err := entry.NewSingleSchemaSnapshotFromMeta(meta, startTs, c.state.Info.Config.ForceReplicate)
	if err != nil {
		return errors.Trace(err)
	}

	filter, err := filter.NewFilter(c.state.Info.Config)
	if err != nil {
		return errors.Trace(err)
	}

	c.schema = newSchemaWrap4Owner(schemaSnap, filter, c.state.Info.Config)
	cancelCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.sink, err = sink.NewSink(cancelCtx, c.state.ID, c.state.Info.SinkURI, filter, c.state.Info.Config, c.state.Info.Opts, c.errCh)
	err = c.sink.Initialize(ctx, c.schema.SinkTableInfos())
	if err != nil {
		return errors.Trace(err)
	}
	c.ddlPuller = newDDLPuller(cancelCtx, c.pdClient, c.credential, kvStore, startTs)
	go func() {
		err := c.ddlPuller.Run(cancelCtx)
		if err != nil {
			log.Warn("ddhHandler returned error", zap.Error(err))
			c.errCh <- err
		}
	}()
	return nil
}

func (c *changefeed) releaseResources() error {
	if !c.initialized {
		return nil
	}
	c.cancel()
	c.cancel = func() {}
	c.ddlPuller.Close()
	c.schema = nil
	c.initialized = false
	// TODO wait ddlpuller and sink exited
	return errors.Trace(c.sink.Close())
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
	err := c.releaseResources()
	if err != nil {
		log.Warn("Sink closed with error", zap.Error(err))
	}
}
