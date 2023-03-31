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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/pdutil"
	redoCfg "github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/sink/observer"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// newSchedulerFromCtx creates a new scheduler from context.
// This function is factored out to facilitate unit testing.
func newSchedulerFromCtx(
	ctx cdcContext.Context, up *upstream.Upstream, epoch uint64, cfg *config.SchedulerConfig,
) (ret scheduler.Scheduler, err error) {
	changeFeedID := ctx.ChangefeedVars().ID
	messageServer := ctx.GlobalVars().MessageServer
	messageRouter := ctx.GlobalVars().MessageRouter
	ownerRev := ctx.GlobalVars().OwnerRevision
	captureID := ctx.GlobalVars().CaptureInfo.ID
	ret, err = scheduler.NewScheduler(
		ctx, captureID, changeFeedID, messageServer, messageRouter, ownerRev, epoch, up, cfg)
	return ret, errors.Trace(err)
}

func newScheduler(
	ctx cdcContext.Context, up *upstream.Upstream, epoch uint64, cfg *config.SchedulerConfig,
) (scheduler.Scheduler, error) {
	return newSchedulerFromCtx(ctx, up, epoch, cfg)
}

type changefeed struct {
	id model.ChangeFeedID
	// state is read-only during the Tick, should only be updated by patch the etcd.
	state *orchestrator.ChangefeedReactorState

	upstream  *upstream.Upstream
	cfg       *config.SchedulerConfig
	scheduler scheduler.Scheduler
	// barriers will be created when a changefeed is initialized
	// and will be destroyed when a changefeed is closed.
	barriers         *barriers
	feedStateManager *feedStateManager

	// ddl related fields
	ddlManager  *ddlManager
	redoDDLMgr  redo.DDLManager
	redoMetaMgr redo.MetaManager

	schema    *schemaWrap4Owner
	ddlSink   DDLSink
	ddlPuller puller.DDLPuller
	// The changefeed will start a backend goroutine in the function `initialize`
	// for DDLPuller and redo manager. `wg` is used to manage this backend goroutine.
	wg sync.WaitGroup

	// state related fields
	initialized bool
	// isRemoved is true if the changefeed is removed,
	// which means it will be removed from memory forever
	isRemoved bool
	// isReleased is true if the changefeed's resources were released,
	// but it will still be kept in the memory, and it will be check
	// in every tick. Such as the changefeed that is stopped or encountered an error.
	isReleased bool
	errCh      chan error
	// cancel the running goroutine start by `DDLPuller`
	cancel context.CancelFunc

	metricsChangefeedCheckpointTsGauge     prometheus.Gauge
	metricsChangefeedCheckpointTsLagGauge  prometheus.Gauge
	metricsChangefeedCheckpointLagDuration prometheus.Observer

	metricsChangefeedResolvedTsGauge       prometheus.Gauge
	metricsChangefeedResolvedTsLagGauge    prometheus.Gauge
	metricsChangefeedResolvedTsLagDuration prometheus.Observer
	metricsCurrentPDTsGauge                prometheus.Gauge

	metricsChangefeedBarrierTsGauge prometheus.Gauge
	metricsChangefeedTickDuration   prometheus.Observer

	downstreamObserver observer.Observer
	observerLastTick   *atomic.Time

	newDDLPuller func(ctx context.Context,
		replicaConfig *config.ReplicaConfig,
		up *upstream.Upstream,
		startTs uint64,
		changefeed model.ChangeFeedID,
	) (puller.DDLPuller, error)

	newSink      func(changefeedID model.ChangeFeedID, info *model.ChangeFeedInfo, reportErr func(error)) DDLSink
	newScheduler func(
		ctx cdcContext.Context, up *upstream.Upstream, epoch uint64, cfg *config.SchedulerConfig,
	) (scheduler.Scheduler, error)

	newDownstreamObserver func(
		ctx context.Context, sinkURIStr string, replCfg *config.ReplicaConfig,
		opts ...observer.NewObserverOption,
	) (observer.Observer, error)

	lastDDLTs uint64 // Timestamp of the last executed DDL. Only used for tests.
}

func newChangefeed(
	id model.ChangeFeedID,
	state *orchestrator.ChangefeedReactorState,
	up *upstream.Upstream,
	cfg *config.SchedulerConfig,
) *changefeed {
	c := &changefeed{
		id:    id,
		state: state,
		// The scheduler will be created lazily.
		scheduler:        nil,
		barriers:         newBarriers(),
		feedStateManager: newFeedStateManager(up),
		upstream:         up,

		errCh:  make(chan error, defaultErrChSize),
		cancel: func() {},

		newDDLPuller:          puller.NewDDLPuller,
		newSink:               newDDLSink,
		newDownstreamObserver: observer.NewObserver,
	}
	c.newScheduler = newScheduler
	c.cfg = cfg
	return c
}

func newChangefeed4Test(
	id model.ChangeFeedID, state *orchestrator.ChangefeedReactorState, up *upstream.Upstream,
	newDDLPuller func(ctx context.Context,
		replicaConfig *config.ReplicaConfig,
		up *upstream.Upstream,
		startTs uint64,
		changefeed model.ChangeFeedID,
	) (puller.DDLPuller, error),
	newSink func(changefeedID model.ChangeFeedID, info *model.ChangeFeedInfo, reportErr func(err error)) DDLSink,
	newScheduler func(
		ctx cdcContext.Context, up *upstream.Upstream, epoch uint64, cfg *config.SchedulerConfig,
	) (scheduler.Scheduler, error),
	newDownstreamObserver func(
		ctx context.Context, sinkURIStr string, replCfg *config.ReplicaConfig,
		opts ...observer.NewObserverOption,
	) (observer.Observer, error),
) *changefeed {
	cfg := config.NewDefaultSchedulerConfig()
	c := newChangefeed(id, state, up, cfg)
	c.newDDLPuller = newDDLPuller
	c.newSink = newSink
	c.newScheduler = newScheduler
	c.newDownstreamObserver = newDownstreamObserver
	return c
}

func (c *changefeed) Tick(ctx cdcContext.Context, captures map[model.CaptureID]*model.CaptureInfo) {
	startTime := time.Now()

	if skip, err := c.checkUpstream(); skip {
		if err != nil {
			c.handleErr(ctx, err)
		}
		return
	}

	ctx = cdcContext.WithErrorHandler(ctx, func(err error) error {
		c.errCh <- errors.Trace(err)
		return nil
	})
	c.state.CheckCaptureAlive(ctx.GlobalVars().CaptureInfo.ID)
	err := c.tick(ctx, captures)

	// The tick duration is recorded only if changefeed has completed initialization
	if c.initialized {
		costTime := time.Since(startTime)
		if costTime > changefeedLogsWarnDuration {
			log.Warn("changefeed tick took too long",
				zap.String("namespace", c.id.Namespace),
				zap.String("changefeed", c.id.ID),
				zap.Duration("duration", costTime))
		}
		c.metricsChangefeedTickDuration.Observe(costTime.Seconds())
	}

	if err != nil {
		log.Error("changefeed tick failed", zap.Error(err))
		c.handleErr(ctx, err)
	}
}

func (c *changefeed) handleErr(ctx cdcContext.Context, err error) {
	log.Error("an error occurred in Owner",
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID), zap.Error(err))
	var code string
	if rfcCode, ok := cerror.RFCCode(err); ok {
		code = string(rfcCode)
	} else {
		code = string(cerror.ErrOwnerUnknown.RFCCode())
	}
	c.feedStateManager.handleError(&model.RunningError{
		Addr:    contextutil.CaptureAddrFromCtx(ctx),
		Code:    code,
		Message: err.Error(),
	})
	c.releaseResources(ctx)
}

func (c *changefeed) checkStaleCheckpointTs(ctx cdcContext.Context, checkpointTs uint64) error {
	state := c.state.Info.State
	if state == model.StateNormal || state == model.StateStopped || state == model.StateError {
		failpoint.Inject("InjectChangefeedFastFailError", func() error {
			return cerror.ErrGCTTLExceeded.FastGen("InjectChangefeedFastFailError")
		})
		if err := c.upstream.GCManager.CheckStaleCheckpointTs(ctx, c.id, checkpointTs); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *changefeed) tick(ctx cdcContext.Context, captures map[model.CaptureID]*model.CaptureInfo) error {
	adminJobPending := c.feedStateManager.Tick(c.state)
	checkpointTs := c.state.Info.GetCheckpointTs(c.state.Status)
	// checkStaleCheckpointTs must be called before `feedStateManager.ShouldRunning()`
	// to ensure all changefeeds, no matter whether they are running or not, will be checked.
	if err := c.checkStaleCheckpointTs(ctx, checkpointTs); err != nil {
		return errors.Trace(err)
	}

	if !c.feedStateManager.ShouldRunning() {
		c.isRemoved = c.feedStateManager.ShouldRemoved()
		c.releaseResources(ctx)
		return nil
	}

	if adminJobPending {
		return nil
	}

	if !c.preflightCheck(captures) {
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
	// we need to wait ddl ddlSink to be ready before we do the other things
	// otherwise, we may cause a nil pointer panic when we try to write to the ddl ddlSink.
	if !c.ddlSink.isInitialized() {
		return nil
	}
	// TODO: pass table checkpointTs when we support concurrent process ddl
	allPhysicalTables, minTableBarrierTs, barrier, err := c.ddlManager.tick(ctx, checkpointTs, nil)
	if err != nil {
		return errors.Trace(err)
	}

	otherBarrierTs, err := c.handleBarrier(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// If there are other barriers less than ddl barrier,
	// we should wait for them.
	// Note: There may be some tableBarrierTs larger than otherBarrierTs,
	// but we can ignore them because they will be handled in the processor.
	if barrier.GlobalBarrierTs > otherBarrierTs {
		barrier.GlobalBarrierTs = otherBarrierTs
		minTableBarrierTs = otherBarrierTs
	}

	log.Debug("owner handles barrier",
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Uint64("resolvedTs", c.state.Status.ResolvedTs),
		zap.Uint64("globalBarrierTs", barrier.GlobalBarrierTs),
		zap.Uint64("minTableBarrierTs", minTableBarrierTs),
		zap.Any("tableBarrier", barrier.TableBarriers))

	if barrier.GlobalBarrierTs < checkpointTs {
		// This condition implies that the DDL resolved-ts has not yet reached checkpointTs,
		// which implies that it would be premature to schedule tables or to update status.
		// So we return here.
		return nil
	}

	startTime := time.Now()
	newCheckpointTs, newResolvedTs, err := c.scheduler.Tick(
		ctx, checkpointTs, allPhysicalTables, captures, barrier)
	// metricsResolvedTs to store the min resolved ts among all tables and show it in metrics
	metricsResolvedTs := newResolvedTs
	costTime := time.Since(startTime)
	if costTime > schedulerLogsWarnDuration {
		log.Warn("scheduler tick took too long",
			zap.String("namespace", c.id.Namespace),
			zap.String("changefeed", c.id.ID), zap.Duration("duration", costTime))
	}
	if err != nil {
		return errors.Trace(err)
	}

	pdTime, _ := c.upstream.PDClock.CurrentTime()
	currentTs := oracle.GetPhysical(pdTime)

	// CheckpointCannotProceed implies that not all tables are being replicated normally,
	// so in that case there is no need to advance the global watermarks.
	if newCheckpointTs == scheduler.CheckpointCannotProceed {
		if c.state.Status != nil {
			// We should keep the metrics updated even if the scheduler cannot
			// advance the watermarks for now.
			c.updateMetrics(currentTs, c.state.Status.CheckpointTs, c.state.Status.ResolvedTs)
		}
		return nil
	}

	// If the owner is just initialized, the newResolvedTs may be max uint64.
	// In this case, we should not update the resolved ts.
	if newResolvedTs > barrier.GlobalBarrierTs {
		newResolvedTs = barrier.GlobalBarrierTs
	}

	// If the owner is just initialized, minTableBarrierTs can be `checkpointTs-1`.
	// In such case the `newCheckpointTs` may be larger than the minTableBarrierTs,
	// but it shouldn't be, so we need to handle it here.
	if newCheckpointTs > minTableBarrierTs {
		newCheckpointTs = minTableBarrierTs
	}

	prevResolvedTs := c.state.Status.ResolvedTs
	if c.redoMetaMgr.Enabled() {
		// newResolvedTs can never exceed the barrier timestamp boundary. If redo is enabled,
		// we can only upload it to etcd after it has been flushed into redo meta.
		// NOTE: `UpdateMeta` handles regressed checkpointTs and resolvedTs internally.
		c.redoMetaMgr.UpdateMeta(newCheckpointTs, newResolvedTs)
		flushedMeta := c.redoMetaMgr.GetFlushedMeta()
		flushedCheckpointTs, flushedResolvedTs := flushedMeta.CheckpointTs, flushedMeta.ResolvedTs
		log.Debug("owner gets flushed meta",
			zap.Uint64("flushedResolvedTs", flushedResolvedTs),
			zap.Uint64("flushedCheckpointTs", flushedCheckpointTs),
			zap.Uint64("newResolvedTs", newResolvedTs),
			zap.Uint64("newCheckpointTs", newCheckpointTs),
			zap.String("namespace", c.id.Namespace),
			zap.String("changefeed", c.id.ID))
		if flushedResolvedTs != 0 {
			// It's not necessary to replace newCheckpointTs with flushedResolvedTs,
			// as cdc can ensure newCheckpointTs can never exceed prevResolvedTs.
			newResolvedTs = flushedResolvedTs
		} else {
			newResolvedTs = prevResolvedTs
		}
		metricsResolvedTs = newResolvedTs
	}
	log.Debug("owner prepares to update status",
		zap.Uint64("prevResolvedTs", prevResolvedTs),
		zap.Uint64("newResolvedTs", newResolvedTs),
		zap.Uint64("newCheckpointTs", newCheckpointTs),
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID))
	// resolvedTs should never regress but checkpointTs can, as checkpointTs has already
	// been decreased when the owner is initialized.
	if newResolvedTs < prevResolvedTs {
		newResolvedTs = prevResolvedTs
		metricsResolvedTs = newResolvedTs
	}

	// MinTableBarrierTs should never regress
	if minTableBarrierTs < c.state.Status.MinTableBarrierTs {
		minTableBarrierTs = c.state.Status.MinTableBarrierTs
	}

	failpoint.Inject("ChangefeedOwnerDontUpdateCheckpoint", func() {
		if c.lastDDLTs != 0 && c.state.Status.CheckpointTs >= c.lastDDLTs {
			log.Info("owner won't update checkpoint because of failpoint",
				zap.String("namespace", c.id.Namespace),
				zap.String("changefeed", c.id.ID),
				zap.Uint64("keepCheckpoint", c.state.Status.CheckpointTs),
				zap.Uint64("skipCheckpoint", newCheckpointTs))
			newCheckpointTs = c.state.Status.CheckpointTs
		}
	})

	c.updateStatus(newCheckpointTs, newResolvedTs, minTableBarrierTs)
	c.updateMetrics(currentTs, newCheckpointTs, metricsResolvedTs)
	c.tickDownstreamObserver(ctx)

	return nil
}

func (c *changefeed) initialize(ctx cdcContext.Context) (err error) {
	if c.initialized || c.state.Status == nil {
		// If `c.state.Status` is nil it means the changefeed struct is just created, it needs to
		//  1. use startTs as checkpointTs and resolvedTs, if it's a new created changefeed; or
		//  2. load checkpointTs and resolvedTs from etcd, if it's an existing changefeed.
		// And then it can continue to initialize.
		return nil
	}
	c.isReleased = false
	// clean the errCh
	// When the changefeed is resumed after being stopped, the changefeed instance will be reused,
	// So we should make sure that the errCh is empty when the changefeed is restarting
LOOP:
	for {
		select {
		case <-c.errCh:
		default:
			break LOOP
		}
	}

	checkpointTs := c.state.Status.CheckpointTs
	resolvedTs := c.state.Status.ResolvedTs
	minTableBarrierTs := c.state.Status.MinTableBarrierTs

	failpoint.Inject("NewChangefeedNoRetryError", func() {
		failpoint.Return(cerror.ErrStartTsBeforeGC.GenWithStackByArgs(checkpointTs-300, checkpointTs))
	})
	failpoint.Inject("NewChangefeedRetryError", func() {
		failpoint.Return(errors.New("failpoint injected retriable error"))
	})

	if c.state.Info.Config.CheckGCSafePoint {
		// Check TiDB GC safepoint does not exceed the checkpoint.
		//
		// We update TTL to 10 minutes,
		//  1. to delete the service GC safepoint effectively,
		//  2. in case owner update TiCDC service GC safepoint fails.
		//
		// Also, it unblocks TiDB GC, because the service GC safepoint is set to
		// 1 hour TTL during creating changefeed.
		//
		// See more gc doc.
		ensureTTL := int64(10 * 60)
		err = gc.EnsureChangefeedStartTsSafety(
			ctx, c.upstream.PDClient,
			ctx.GlobalVars().EtcdClient.GetEnsureGCServiceID(gc.EnsureGCServiceInitializing),
			c.id, ensureTTL, checkpointTs)
		if err != nil {
			return errors.Trace(err)
		}
		// clean service GC safepoint '-creating-' and '-resuming-' if there are any.
		err = gc.UndoEnsureChangefeedStartTsSafety(
			ctx, c.upstream.PDClient,
			ctx.GlobalVars().EtcdClient.GetEnsureGCServiceID(gc.EnsureGCServiceCreating),
			c.id,
		)
		if err != nil {
			return errors.Trace(err)
		}
		err = gc.UndoEnsureChangefeedStartTsSafety(
			ctx, c.upstream.PDClient,
			ctx.GlobalVars().EtcdClient.GetEnsureGCServiceID(gc.EnsureGCServiceResuming),
			c.id,
		)
		if err != nil {
			return errors.Trace(err)
		}
	}

	var ddlStartTs model.Ts
	// This means there was a ddl job when the changefeed was paused.
	// We don't know whether the ddl job is finished or not, so we need to
	// start the ddl puller from the `checkpointTs-1` to execute the ddl job
	// again.
	// FIXME: TiCDC can't handle some ddl jobs correctly in this situation.
	// For example, if the ddl job is `add index`, TiCDC will execute the ddl
	// job again and cause the index to be added twice. We need to fix this
	// problem in the future. See:https://github.com/pingcap/tiflow/issues/2543
	if checkpointTs == minTableBarrierTs {
		ddlStartTs = checkpointTs - 1
	} else {
		ddlStartTs = checkpointTs
	}

	c.barriers = newBarriers()
	if c.state.Info.Config.EnableSyncPoint { // preResolvedTs model.Ts

		c.barriers.Update(syncPointBarrier, resolvedTs)
	}
	c.barriers.Update(finishBarrier, c.state.Info.GetTargetTs())

	c.schema, err = newSchemaWrap4Owner(c.upstream.KVStorage, ddlStartTs, c.state.Info.Config, c.id)
	if err != nil {
		return errors.Trace(err)
	}

	cancelCtx, cancel := cdcContext.WithCancel(ctx)
	c.cancel = cancel

	sourceID, err := pdutil.GetSourceID(ctx, c.upstream.PDClient)
	if err != nil {
		return errors.Trace(err)
	}
	c.state.Info.Config.Sink.TiDBSourceID = sourceID
	log.Info("set source id",
		zap.Uint64("sourceID", sourceID),
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID),
	)

	c.ddlSink = c.newSink(c.id, c.state.Info, ctx.Throw)
	c.ddlSink.run(cancelCtx)

	c.ddlPuller, err = c.newDDLPuller(cancelCtx, c.state.Info.Config, c.upstream, ddlStartTs, c.id)
	if err != nil {
		return errors.Trace(err)
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ctx.Throw(c.ddlPuller.Run(cancelCtx))
	}()

	c.downstreamObserver, err = c.newDownstreamObserver(
		ctx, c.state.Info.SinkURI, c.state.Info.Config)
	if err != nil {
		return err
	}
	c.observerLastTick = atomic.NewTime(time.Time{})

	stdCtx := contextutil.PutChangefeedIDInCtx(cancelCtx, c.id)
	c.redoDDLMgr, err = redo.NewDDLManager(stdCtx, c.state.Info.Config.Consistent, ddlStartTs)
	failpoint.Inject("ChangefeedNewRedoManagerError", func() {
		err = errors.New("changefeed new redo manager injected error")
	})
	if err != nil {
		return err
	}
	if c.redoDDLMgr.Enabled() {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			ctx.Throw(c.redoDDLMgr.Run(stdCtx))
		}()
	}

	c.redoMetaMgr, err = redo.NewMetaManagerWithInit(stdCtx,
		c.state.Info.Config.Consistent, checkpointTs)
	if err != nil {
		return err
	}
	if c.redoMetaMgr.Enabled() {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			ctx.Throw(c.redoMetaMgr.Run(stdCtx))
		}()
	}
	log.Info("owner creates redo manager",
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID))

	downstreamType, err := c.state.Info.DownstreamType()
	if err != nil {
		return errors.Trace(err)
	}
	c.ddlManager = newDDLManager(
		c.id,
		ddlStartTs,
		c.state.Status.CheckpointTs,
		c.ddlSink,
		c.ddlPuller,
		c.schema,
		c.redoDDLMgr,
		c.redoMetaMgr,
		downstreamType,
		c.state.Info.Config.BDRMode)

	// create scheduler
	cfg := *c.cfg
	cfg.ChangefeedSettings = c.state.Info.Config.Scheduler
	epoch := c.state.Info.Epoch
	c.scheduler, err = c.newScheduler(ctx, c.upstream, epoch, &cfg)
	if err != nil {
		return errors.Trace(err)
	}

	c.initMetrics()

	c.initialized = true
	log.Info("changefeed initialized",
		zap.String("namespace", c.state.ID.Namespace),
		zap.String("changefeed", c.state.ID.ID),
		zap.Uint64("changefeedEpoch", epoch),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Uint64("resolvedTs", resolvedTs),
		zap.Stringer("info", c.state.Info))

	return nil
}

func (c *changefeed) initMetrics() {
	c.metricsChangefeedCheckpointTsGauge = changefeedCheckpointTsGauge.
		WithLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedCheckpointTsLagGauge = changefeedCheckpointTsLagGauge.
		WithLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedCheckpointLagDuration = changefeedCheckpointLagDuration.
		WithLabelValues(c.id.Namespace, c.id.ID)

	c.metricsChangefeedResolvedTsGauge = changefeedResolvedTsGauge.
		WithLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedResolvedTsLagGauge = changefeedResolvedTsLagGauge.
		WithLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedResolvedTsLagDuration = changefeedResolvedTsLagDuration.
		WithLabelValues(c.id.Namespace, c.id.ID)
	c.metricsCurrentPDTsGauge = currentPDTsGauge.WithLabelValues(c.id.Namespace, c.id.ID)

	c.metricsChangefeedBarrierTsGauge = changefeedBarrierTsGauge.
		WithLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedTickDuration = changefeedTickDuration.
		WithLabelValues(c.id.Namespace, c.id.ID)
}

// releaseResources is idempotent.
func (c *changefeed) releaseResources(ctx cdcContext.Context) {
	if c.isReleased {
		return
	}
	// Must clean redo manager before calling cancel, otherwise
	// the manager can be closed internally.
	c.cleanupRedoManager(ctx)
	c.cleanupChangefeedServiceGCSafePoints(ctx)

	c.cancel()
	c.cancel = func() {}

	if c.ddlPuller != nil {
		c.ddlPuller.Close()
	}
	c.wg.Wait()

	if c.ddlSink != nil {
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		// TODO(dongmen): remove ctx from func ddlSink.close(), it is useless.
		// We don't need to wait ddlSink Close, pass a canceled context is ok
		if err := c.ddlSink.close(canceledCtx); err != nil {
			log.Warn("owner close ddlSink failed",
				zap.String("namespace", c.id.Namespace),
				zap.String("changefeed", c.id.ID),
				zap.Error(err))
		}
	}

	if c.scheduler != nil {
		c.scheduler.Close(ctx)
		c.scheduler = nil
	}
	if c.downstreamObserver != nil {
		_ = c.downstreamObserver.Close()
	}

	c.cleanupMetrics()
	c.schema = nil
	c.barriers = nil
	c.initialized = false
	c.isReleased = true

	log.Info("changefeed closed",
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID),
		zap.Any("status", c.state.Status),
		zap.Stringer("info", c.state.Info),
		zap.Bool("isRemoved", c.isRemoved))
}

func (c *changefeed) cleanupMetrics() {
	changefeedCheckpointTsGauge.DeleteLabelValues(c.id.Namespace, c.id.ID)
	changefeedCheckpointTsLagGauge.DeleteLabelValues(c.id.Namespace, c.id.ID)
	changefeedCheckpointLagDuration.DeleteLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedCheckpointTsGauge = nil
	c.metricsChangefeedCheckpointTsLagGauge = nil
	c.metricsChangefeedCheckpointLagDuration = nil

	changefeedResolvedTsGauge.DeleteLabelValues(c.id.Namespace, c.id.ID)
	changefeedResolvedTsLagGauge.DeleteLabelValues(c.id.Namespace, c.id.ID)
	changefeedResolvedTsLagDuration.DeleteLabelValues(c.id.Namespace, c.id.ID)
	currentPDTsGauge.DeleteLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedResolvedTsGauge = nil
	c.metricsChangefeedResolvedTsLagGauge = nil
	c.metricsChangefeedResolvedTsLagDuration = nil
	c.metricsCurrentPDTsGauge = nil

	changefeedTickDuration.DeleteLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedTickDuration = nil

	changefeedBarrierTsGauge.DeleteLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedBarrierTsGauge = nil
}

// cleanup redo logs if changefeed is removed and redo log is enabled
func (c *changefeed) cleanupRedoManager(ctx context.Context) {
	if c.isRemoved {
		if c.state == nil || c.state.Info == nil || c.state.Info.Config == nil ||
			c.state.Info.Config.Consistent == nil {
			log.Warn("changefeed is removed, but state is not complete", zap.Any("state", c.state))
			return
		}
		if !redoCfg.IsConsistentEnabled(c.state.Info.Config.Consistent.Level) {
			return
		}
		// when removing a paused changefeed, the redo manager is nil, create a new one
		if c.redoMetaMgr == nil {
			redoMetaMgr, err := redo.NewMetaManager(ctx, c.state.Info.Config.Consistent)
			if err != nil {
				log.Info("owner creates redo manager for clean fail",
					zap.String("namespace", c.id.Namespace),
					zap.String("changefeed", c.id.ID),
					zap.Error(err))
				return
			}
			c.redoMetaMgr = redoMetaMgr
		}
		err := c.redoMetaMgr.Cleanup(ctx)
		if err != nil {
			log.Error("cleanup redo logs failed", zap.String("changefeed", c.id.ID), zap.Error(err))
		}
	}
}

func (c *changefeed) cleanupChangefeedServiceGCSafePoints(ctx cdcContext.Context) {
	if !c.isRemoved {
		return
	}

	serviceIDs := []string{
		ctx.GlobalVars().EtcdClient.GetEnsureGCServiceID(gc.EnsureGCServiceCreating),
		ctx.GlobalVars().EtcdClient.GetEnsureGCServiceID(gc.EnsureGCServiceResuming),
		ctx.GlobalVars().EtcdClient.GetEnsureGCServiceID(gc.EnsureGCServiceInitializing),
	}

	for _, serviceID := range serviceIDs {
		err := gc.UndoEnsureChangefeedStartTsSafety(
			ctx,
			c.upstream.PDClient,
			serviceID,
			c.id)
		if err != nil {
			log.Error("failed to remove gc safepoint",
				zap.String("namespace", c.id.Namespace),
				zap.String("changefeed", c.id.ID),
				zap.String("serviceID", serviceID))
		}
	}
}

// preflightCheck makes sure that the metadata in Etcd is complete enough to run the tick.
// If the metadata is not complete, such as when the ChangeFeedStatus is nil,
// this function will reconstruct the lost metadata and skip this tick.
func (c *changefeed) preflightCheck(captures map[model.CaptureID]*model.CaptureInfo) (ok bool) {
	ok = true
	if c.state.Status == nil {
		// complete the changefeed status when it is just created.
		c.state.PatchStatus(
			func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
				if status == nil {
					status = &model.ChangeFeedStatus{
						// changefeed status is nil when the changefeed has just created.
						ResolvedTs:        c.state.Info.StartTs,
						CheckpointTs:      c.state.Info.StartTs,
						MinTableBarrierTs: c.state.Info.StartTs,
						AdminJobType:      model.AdminNone,
					}
					return status, true, nil
				}
				return status, false, nil
			})
		ok = false
	} else if c.state.Status.MinTableBarrierTs == 0 {
		// complete the changefeed status when the TiCDC cluster is
		// upgraded from an old version(less than v6.7.0).
		c.state.PatchStatus(
			func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
				if status != nil {
					if status.MinTableBarrierTs == 0 {
						status.MinTableBarrierTs = status.CheckpointTs
					}
					return status, true, nil
				}
				return status, false, nil
			})
		ok = false
	}

	// clean stale capture task positions
	for captureID := range c.state.TaskPositions {
		if _, exist := captures[captureID]; !exist {
			c.state.PatchTaskPosition(captureID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
				return nil, position != nil, nil
			})
			ok = false
		}
	}
	if !ok {
		log.Info("changefeed preflight check failed, will skip this tick",
			zap.String("namespace", c.id.Namespace),
			zap.String("changefeed", c.id.ID),
			zap.Any("status", c.state.Status), zap.Bool("ok", ok),
		)
	}

	return
}

// handleBarrier calculates the barrierTs of the changefeed.
// barrierTs is used to control the data that can be flush to downstream.
func (c *changefeed) handleBarrier(ctx cdcContext.Context) (uint64, error) {
	barrierTp, barrierTs := c.barriers.Min()

	c.metricsChangefeedBarrierTsGauge.Set(float64(oracle.ExtractPhysical(barrierTs)))

	// It means:
	//   1. All data before the barrierTs was sent to downstream.
	//   2. No more data after barrierTs was sent to downstream.
	checkpointReachBarrier := barrierTs == c.state.Status.CheckpointTs

	// TODO: To check if we can remove the `barrierTs == c.state.Status.ResolvedTs` condition.
	fullyBlocked := checkpointReachBarrier && barrierTs == c.state.Status.ResolvedTs

	switch barrierTp {
	case syncPointBarrier:
		if !fullyBlocked {
			return barrierTs, nil
		}
		nextSyncPointTs := oracle.GoTimeToTS(oracle.GetTimeFromTS(barrierTs).Add(c.state.Info.Config.SyncPointInterval))
		if err := c.ddlSink.emitSyncPoint(ctx, barrierTs); err != nil {
			return 0, errors.Trace(err)
		}
		c.barriers.Update(syncPointBarrier, nextSyncPointTs)
	case finishBarrier:
		if fullyBlocked {
			c.feedStateManager.MarkFinished()
		}
		return barrierTs, nil
	default:
		log.Panic("Unknown barrier type", zap.Int("barrierType", int(barrierTp)))
	}
	return barrierTs, nil
}

func (c *changefeed) updateMetrics(currentTs int64, checkpointTs, resolvedTs model.Ts) {
	phyCkpTs := oracle.ExtractPhysical(checkpointTs)
	c.metricsChangefeedCheckpointTsGauge.Set(float64(phyCkpTs))

	checkpointLag := float64(currentTs-phyCkpTs) / 1e3
	c.metricsChangefeedCheckpointTsLagGauge.Set(checkpointLag)
	c.metricsChangefeedCheckpointLagDuration.Observe(checkpointLag)

	phyRTs := oracle.ExtractPhysical(resolvedTs)
	c.metricsChangefeedResolvedTsGauge.Set(float64(phyRTs))

	resolvedLag := float64(currentTs-phyRTs) / 1e3
	c.metricsChangefeedResolvedTsLagGauge.Set(resolvedLag)
	c.metricsChangefeedResolvedTsLagDuration.Observe(resolvedLag)

	c.metricsCurrentPDTsGauge.Set(float64(currentTs))
}

func (c *changefeed) updateStatus(checkpointTs, resolvedTs, minTableBarrierTs model.Ts) {
	c.state.PatchStatus(
		func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			changed := false
			if status == nil {
				return nil, changed, nil
			}
			if status.ResolvedTs != resolvedTs {
				status.ResolvedTs = resolvedTs
				changed = true
			}
			if status.CheckpointTs != checkpointTs {
				status.CheckpointTs = checkpointTs
				changed = true
			}
			if status.MinTableBarrierTs != minTableBarrierTs {
				status.MinTableBarrierTs = minTableBarrierTs
				changed = true
			}
			return status, changed, nil
		})
}

func (c *changefeed) Close(ctx cdcContext.Context) {
	startTime := time.Now()
	c.releaseResources(ctx)

	costTime := time.Since(startTime)
	if costTime > changefeedLogsWarnDuration {
		log.Warn("changefeed close took too long",
			zap.String("changefeed", c.id.ID),
			zap.Duration("duration", costTime))
	}
	changefeedCloseDuration.Observe(costTime.Seconds())
}

// GetInfoProvider returns an InfoProvider if one is available.
func (c *changefeed) GetInfoProvider() scheduler.InfoProvider {
	if provider, ok := c.scheduler.(scheduler.InfoProvider); ok {
		return provider
	}
	return nil
}

// checkUpstream returns skip = true if the upstream is still in initializing phase,
// and returns an error if the upstream is unavailable.
func (c *changefeed) checkUpstream() (skip bool, err error) {
	if err = c.upstream.Error(); err != nil {
		return true, err
	}
	if c.upstream.IsClosed() {
		log.Warn("upstream is closed",
			zap.Uint64("upstreamID", c.upstream.ID),
			zap.String("namespace", c.id.Namespace),
			zap.String("changefeed", c.id.ID))
		return true, cerror.
			WrapChangefeedUnretryableErr(
				cerror.ErrUpstreamClosed.GenWithStackByArgs())
	}
	// upstream is still in initializing phase
	// skip this changefeed tick
	if !c.upstream.IsNormal() {
		return true, nil
	}
	return
}

// tickDownstreamObserver checks whether needs to trigger tick of downstream
// observer, if needed run it in an independent goroutine with 5s timeout.
func (c *changefeed) tickDownstreamObserver(ctx context.Context) {
	if time.Since(c.observerLastTick.Load()) > downstreamObserverTickDuration {
		c.observerLastTick.Store(time.Now())
		select {
		case <-ctx.Done():
			return
		default:
		}
		go func() {
			cctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			if err := c.downstreamObserver.Tick(cctx); err != nil {
				// Prometheus is not deployed, it happens in non production env.
				if strings.Contains(err.Error(),
					fmt.Sprintf(":%d", errno.ErrPrometheusAddrIsNotSet)) {
					return
				}
				log.Warn("backend observer tick error", zap.Error(err))
			}
		}()
	}
}
