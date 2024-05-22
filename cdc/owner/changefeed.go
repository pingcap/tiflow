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
	"math"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tiflow/cdc/async"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pfilter "github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/pdutil"
	redoCfg "github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/sink/observer"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// newScheduler creates a new scheduler from context.
// This function is factored out to facilitate unit testing.
func newScheduler(
	ctx cdcContext.Context, up *upstream.Upstream, epoch uint64, cfg *config.SchedulerConfig, redoMetaManager redo.MetaManager,
) (scheduler.Scheduler, error) {
	changeFeedID := ctx.ChangefeedVars().ID
	messageServer := ctx.GlobalVars().MessageServer
	messageRouter := ctx.GlobalVars().MessageRouter
	ownerRev := ctx.GlobalVars().OwnerRevision
	captureID := ctx.GlobalVars().CaptureInfo.ID
	ret, err := scheduler.NewScheduler(
		ctx, captureID, changeFeedID, messageServer, messageRouter, ownerRev, epoch, up, cfg, redoMetaManager)
	return ret, errors.Trace(err)
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
	resolvedTs       model.Ts

	// lastSyncedTs is the lastest resolvedTs that has been synced to downstream.
	// pullerResolvedTs is the minimum resolvedTs of all pullers.
	// we don't need to initialize lastSyncedTs and pullerResolvedTs specially
	// because it will be updated in tick.
	lastSyncedTs     model.Ts
	pullerResolvedTs model.Ts

	// ddl related fields
	ddlManager  *ddlManager
	redoDDLMgr  redo.DDLManager
	redoMetaMgr redo.MetaManager

	schema    entry.SchemaStorage
	ddlSink   DDLSink
	ddlPuller puller.DDLPuller
	// The changefeed will start a backend goroutine in the function `initialize`
	// for DDLPuller and redo manager. `wg` is used to manage this backend goroutine.
	wg sync.WaitGroup

	// state related fields
	initialized *atomic.Bool
	initializer *async.Initializer

	// isRemoved is true if the changefeed is removed,
	// which means it will be removed from memory forever
	isRemoved bool
	// isReleased is true if the changefeed's resources were released,
	// but it will still be kept in the memory, and it will be check
	// in every tick. Such as the changefeed that is stopped or encountered an error.
	isReleased bool
	errCh      chan error
	warningCh  chan error
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

	metricsChangefeedCreateTimeGuage  prometheus.Gauge
	metricsChangefeedRestartTimeGauge prometheus.Gauge

	downstreamObserver observer.Observer
	observerLastTick   *atomic.Time

	newDDLPuller func(ctx context.Context,
		replicaConfig *config.ReplicaConfig,
		up *upstream.Upstream,
		startTs uint64,
		changefeed model.ChangeFeedID,
		schemaStorage entry.SchemaStorage,
		filter pfilter.Filter,
	) (puller.DDLPuller, error)

	newSink func(
		changefeedID model.ChangeFeedID, info *model.ChangeFeedInfo,
		reportError func(err error), reportWarning func(err error),
	) DDLSink

	newScheduler func(
		ctx cdcContext.Context, up *upstream.Upstream, epoch uint64, cfg *config.SchedulerConfig, redoMetaManager redo.MetaManager,
	) (scheduler.Scheduler, error)

	newDownstreamObserver func(
		ctx context.Context,
		changefeedID model.ChangeFeedID,
		sinkURIStr string, replCfg *config.ReplicaConfig,
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
		feedStateManager: newFeedStateManager(up, state.Info.Config),
		upstream:         up,

		errCh:     make(chan error, defaultErrChSize),
		warningCh: make(chan error, defaultErrChSize),
		cancel:    func() {},

		newDDLPuller:          puller.NewDDLPuller,
		newSink:               newDDLSink,
		newDownstreamObserver: observer.NewObserver,
		initialized:           atomic.NewBool(false),
	}
	c.newScheduler = newScheduler
	c.cfg = cfg
	c.initializer = async.NewInitializer()
	return c
}

func newChangefeed4Test(
	id model.ChangeFeedID, state *orchestrator.ChangefeedReactorState, up *upstream.Upstream,
	newDDLPuller func(ctx context.Context,
		replicaConfig *config.ReplicaConfig,
		up *upstream.Upstream,
		startTs uint64,
		changefeed model.ChangeFeedID,
		schemaStorage entry.SchemaStorage,
		filter pfilter.Filter,
	) (puller.DDLPuller, error),
	newSink func(
		changefeedID model.ChangeFeedID, info *model.ChangeFeedInfo,
		reportError func(err error), reportWarning func(err error),
	) DDLSink,
	newScheduler func(
		ctx cdcContext.Context, up *upstream.Upstream, epoch uint64, cfg *config.SchedulerConfig, redoMetaManager redo.MetaManager,
	) (scheduler.Scheduler, error),
	newDownstreamObserver func(
		ctx context.Context,
		changefeedID model.ChangeFeedID,
		sinkURIStr string, replCfg *config.ReplicaConfig,
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

	// Handle all internal warnings.
	noMoreWarnings := false
	for !noMoreWarnings {
		select {
		case err := <-c.warningCh:
			c.handleWarning(err)
		default:
			noMoreWarnings = true
		}
	}

	if skip, err := c.checkUpstream(); skip {
		if err != nil {
			c.handleErr(ctx, err)
		}
		return
	}

	ctx = cdcContext.WithErrorHandler(ctx, func(err error) error {
		select {
		case <-ctx.Done():
		case c.errCh <- errors.Trace(err):
		}
		return nil
	})
	c.state.CheckCaptureAlive(ctx.GlobalVars().CaptureInfo.ID)
	err := c.tick(ctx, captures)

	// The tick duration is recorded only if changefeed has completed initialization
	if c.initialized.Load() {
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
		Time:    time.Now(),
		Addr:    config.GetGlobalServerConfig().AdvertiseAddr,
		Code:    code,
		Message: err.Error(),
	})
	c.releaseResources(ctx)
}

func (c *changefeed) handleWarning(err error) {
	log.Warn("an warning occurred in Owner",
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID), zap.Error(err))
	var code string
	if rfcCode, ok := cerror.RFCCode(err); ok {
		code = string(rfcCode)
	} else {
		code = string(cerror.ErrOwnerUnknown.RFCCode())
	}

	c.feedStateManager.handleWarning(&model.RunningError{
		Time:    time.Now(),
		Addr:    config.GetGlobalServerConfig().AdvertiseAddr,
		Code:    code,
		Message: err.Error(),
	})
}

func (c *changefeed) checkStaleCheckpointTs(ctx cdcContext.Context, checkpointTs uint64) error {
	cfInfo := c.state.Info
	if cfInfo.NeedBlockGC() {
		failpoint.Inject("InjectChangefeedFastFailError", func() error {
			return cerror.ErrStartTsBeforeGC.FastGen("InjectChangefeedFastFailError")
		})
		if err := c.upstream.GCManager.CheckStaleCheckpointTs(ctx, c.id, checkpointTs); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *changefeed) tick(ctx cdcContext.Context, captures map[model.CaptureID]*model.CaptureInfo) error {
	adminJobPending := c.feedStateManager.Tick(c.state, c.resolvedTs)
	preCheckpointTs := c.state.Info.GetCheckpointTs(c.state.Status)
	// checkStaleCheckpointTs must be called before `feedStateManager.ShouldRunning()`
	// to ensure all changefeeds, no matter whether they are running or not, will be checked.
	if err := c.checkStaleCheckpointTs(ctx, preCheckpointTs); err != nil {
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
	if c.state.Status == nil {
		// If `c.state.Status` is nil it means the changefeed struct is just created, it needs to
		//  1. use startTs as checkpointTs and resolvedTs, if it's a new created changefeed; or
		//  2. load checkpointTs and resolvedTs from etcd, if it's an existing changefeed.
		// And then it can continue to initialize.
		return nil
	}

	if !c.initialized.Load() {
		info, err := c.state.Info.Clone()
		if err != nil {
			return errors.Trace(err)
		}
		checkpointTs := c.state.Status.CheckpointTs
		minTableBarrierTs := c.state.Status.MinTableBarrierTs
		initialized, err := c.initializer.TryInitialize(ctx,
			func(ctx cdcContext.Context) error {
				return c.initialize(ctx, info, checkpointTs, minTableBarrierTs)
			},
			ctx.GlobalVars().ChangefeedThreadPool)
		if err != nil {
			return errors.Trace(err)
		}
		if !initialized {
			return nil
		}
	}

	select {
	case err := <-c.errCh:
		return errors.Trace(err)
	default:
	}

	if c.redoMetaMgr.Enabled() {
		if !c.redoMetaMgr.Running() {
			return nil
		}
	}

	// TODO: pass table checkpointTs when we support concurrent process ddl
	allPhysicalTables, barrier, err := c.ddlManager.tick(ctx, preCheckpointTs, nil)
	if err != nil {
		return errors.Trace(err)
	}

	err = c.handleBarrier(ctx, barrier)
	if err != nil {
		return errors.Trace(err)
	}

	log.Debug("owner handles barrier",
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID),
		zap.Uint64("preCheckpointTs", preCheckpointTs),
		zap.Uint64("preResolvedTs", c.resolvedTs),
		zap.Uint64("globalBarrierTs", barrier.GlobalBarrierTs),
		zap.Uint64("minTableBarrierTs", barrier.MinTableBarrierTs),
		zap.Any("tableBarrier", barrier.TableBarriers))

	if barrier.GlobalBarrierTs < preCheckpointTs {
		// This condition implies that the DDL resolved-ts has not yet reached checkpointTs,
		// which implies that it would be premature to schedule tables or to update status.
		// So we return here.
		return nil
	}

	watermark, err := c.scheduler.Tick(
		ctx, preCheckpointTs, allPhysicalTables, captures,
		barrier)
	if err != nil {
		return errors.Trace(err)
	}

	if watermark.LastSyncedTs != scheduler.CheckpointCannotProceed {
		if c.lastSyncedTs < watermark.LastSyncedTs {
			c.lastSyncedTs = watermark.LastSyncedTs
		} else if c.lastSyncedTs > watermark.LastSyncedTs {
			log.Warn("LastSyncedTs should not be greater than newLastSyncedTs",
				zap.Uint64("c.LastSyncedTs", c.lastSyncedTs),
				zap.Uint64("newLastSyncedTs", watermark.LastSyncedTs))
		}
	}

	if watermark.PullerResolvedTs != scheduler.CheckpointCannotProceed && watermark.PullerResolvedTs != math.MaxUint64 {
		if watermark.PullerResolvedTs > c.pullerResolvedTs {
			c.pullerResolvedTs = watermark.PullerResolvedTs
		} else if watermark.PullerResolvedTs < c.pullerResolvedTs {
			log.Warn("the newPullerResolvedTs should not be smaller than c.pullerResolvedTs",
				zap.Uint64("c.pullerResolvedTs", c.pullerResolvedTs),
				zap.Uint64("newPullerResolvedTs", watermark.PullerResolvedTs))
		}
	}

	pdTime := c.upstream.PDClock.CurrentTime()
	currentTs := oracle.GetPhysical(pdTime)

	// CheckpointCannotProceed implies that not all tables are being replicated normally,
	// so in that case there is no need to advance the global watermarks.
	if watermark.CheckpointTs == scheduler.CheckpointCannotProceed {
		if c.state.Status != nil {
			// We should keep the metrics updated even if the scheduler cannot
			// advance the watermarks for now.
			c.updateMetrics(currentTs, c.state.Status.CheckpointTs, c.resolvedTs)
		}
		return nil
	}

	log.Debug("owner prepares to update status",
		zap.Uint64("prevResolvedTs", c.resolvedTs),
		zap.Uint64("newResolvedTs", watermark.ResolvedTs),
		zap.Uint64("newCheckpointTs", watermark.CheckpointTs),
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID))
	// resolvedTs should never regress.
	if watermark.ResolvedTs > c.resolvedTs {
		c.resolvedTs = watermark.ResolvedTs
	}

	// MinTableBarrierTs should never regress
	if barrier.MinTableBarrierTs < c.state.Status.MinTableBarrierTs {
		barrier.MinTableBarrierTs = c.state.Status.MinTableBarrierTs
	}

	failpoint.Inject("ChangefeedOwnerDontUpdateCheckpoint", func() {
		if c.lastDDLTs != 0 && c.state.Status.CheckpointTs >= c.lastDDLTs {
			log.Info("owner won't update checkpoint because of failpoint",
				zap.String("namespace", c.id.Namespace),
				zap.String("changefeed", c.id.ID),
				zap.Uint64("keepCheckpoint", c.state.Status.CheckpointTs),
				zap.Uint64("skipCheckpoint", watermark.CheckpointTs))
			watermark.CheckpointTs = c.state.Status.CheckpointTs
		}
	})

	failpoint.Inject("ChangefeedOwnerNotUpdateCheckpoint", func() {
		watermark.CheckpointTs = c.state.Status.CheckpointTs
	})

	c.updateStatus(watermark.CheckpointTs, barrier.MinTableBarrierTs)
	c.updateMetrics(currentTs, watermark.CheckpointTs, c.resolvedTs)
	c.tickDownstreamObserver(ctx)

	return nil
}

func (c *changefeed) initialize(ctx cdcContext.Context,
	info *model.ChangeFeedInfo,
	checkpointTs uint64,
	minTableBarrierTs uint64,
) (err error) {
	if c.initialized.Load() {
		return nil
	}
	c.isReleased = false

	// clean the errCh
	// When the changefeed is resumed after being stopped, the changefeed instance will be reused,
	// So we should make sure that the errCh is empty when the changefeed is restarting
LOOP1:
	for {
		select {
		case <-c.errCh:
		default:
			break LOOP1
		}
	}
LOOP2:
	for {
		select {
		case <-c.warningCh:
		default:
			break LOOP2
		}
	}

	if c.resolvedTs == 0 {
		c.resolvedTs = checkpointTs
	}

	failpoint.Inject("NewChangefeedNoRetryError", func() {
		failpoint.Return(cerror.ErrStartTsBeforeGC.GenWithStackByArgs(checkpointTs-300, checkpointTs))
	})
	failpoint.Inject("NewChangefeedRetryError", func() {
		failpoint.Return(errors.New("failpoint injected retriable error"))
	})

	if info.Config.CheckGCSafePoint {
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
	if util.GetOrZero(info.Config.EnableSyncPoint) {
		c.barriers.Update(syncPointBarrier, c.resolvedTs)
	}
	c.barriers.Update(finishBarrier, info.GetTargetTs())

	filter, err := pfilter.NewFilter(info.Config, "")
	if err != nil {
		return errors.Trace(err)
	}
	c.schema, err = entry.NewSchemaStorage(
		c.upstream.KVStorage,
		ddlStartTs,
		info.Config.ForceReplicate,
		c.id,
		util.RoleOwner,
		filter)
	if err != nil {
		return errors.Trace(err)
	}

	cancelCtx, cancel := cdcContext.WithCancel(ctx)
	c.cancel = cancel

	sourceID, err := pdutil.GetSourceID(ctx, c.upstream.PDClient)
	if err != nil {
		return errors.Trace(err)
	}
	info.Config.Sink.TiDBSourceID = sourceID
	log.Info("set source id",
		zap.Uint64("sourceID", sourceID),
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID),
	)

	c.ddlSink = c.newSink(c.id, info, ctx.Throw, func(err error) {
		select {
		case <-ctx.Done():
		case c.warningCh <- err:
		}
	})
	c.ddlSink.run(cancelCtx)

	c.ddlPuller, err = c.newDDLPuller(cancelCtx,
		info.Config,
		c.upstream, ddlStartTs,
		c.id,
		c.schema,
		filter)
	if err != nil {
		return errors.Trace(err)
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ctx.Throw(c.ddlPuller.Run(cancelCtx))
	}()

	c.downstreamObserver, err = c.newDownstreamObserver(ctx, c.id, info.SinkURI, info.Config)
	if err != nil {
		return err
	}
	c.observerLastTick = atomic.NewTime(time.Time{})

	c.redoDDLMgr = redo.NewDDLManager(c.id, info.Config.Consistent, ddlStartTs)
	if c.redoDDLMgr.Enabled() {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			ctx.Throw(c.redoDDLMgr.Run(cancelCtx))
		}()
	}

	c.redoMetaMgr = redo.NewMetaManager(c.id, info.Config.Consistent, checkpointTs)
	if c.redoMetaMgr.Enabled() {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			ctx.Throw(c.redoMetaMgr.Run(cancelCtx))
		}()
	}
	log.Info("owner creates redo manager",
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID))

	downstreamType, err := info.DownstreamType()
	if err != nil {
		return errors.Trace(err)
	}

	c.ddlManager = newDDLManager(
		c.id,
		ddlStartTs,
		checkpointTs,
		c.ddlSink,
		filter,
		c.ddlPuller,
		c.schema,
		c.redoDDLMgr,
		c.redoMetaMgr,
		downstreamType,
		util.GetOrZero(info.Config.BDRMode),
	)

	// create scheduler
	cfg := *c.cfg
	cfg.ChangefeedSettings = info.Config.Scheduler
	epoch := info.Epoch
	c.scheduler, err = c.newScheduler(ctx, c.upstream, epoch, &cfg, c.redoMetaMgr)
	if err != nil {
		return errors.Trace(err)
	}

	c.initMetrics()

	c.initialized.Store(true)
	c.metricsChangefeedCreateTimeGuage.Set(float64(oracle.GetPhysical(info.CreateTime)))
	c.metricsChangefeedRestartTimeGauge.Set(float64(oracle.GetPhysical(time.Now())))
	log.Info("changefeed initialized",
		zap.String("namespace", c.state.ID.Namespace),
		zap.String("changefeed", c.state.ID.ID),
		zap.Uint64("changefeedEpoch", epoch),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Uint64("resolvedTs", c.resolvedTs),
		zap.String("info", info.String()))

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

	c.metricsChangefeedCreateTimeGuage = changefeedStartTimeGauge.
		WithLabelValues(c.id.Namespace, c.id.ID, "create")
	c.metricsChangefeedRestartTimeGauge = changefeedStartTimeGauge.
		WithLabelValues(c.id.Namespace, c.id.ID, "restart")
}

// releaseResources is idempotent.
func (c *changefeed) releaseResources(ctx cdcContext.Context) {
	c.initializer.Terminate()
	c.cleanupMetrics()
	if c.isReleased {
		return
	}
	// Must clean redo manager before calling cancel, otherwise
	// the manager can be closed internally.
	c.cleanupRedoManager(ctx)
	c.cleanupChangefeedServiceGCSafePoints(ctx)

	if c.cancel != nil {
		c.cancel()
	}
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

	c.schema = nil
	c.barriers = nil
	c.initialized.Store(false)
	c.isReleased = true

	log.Info("changefeed closed",
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID),
		zap.Any("status", c.state.Status),
		zap.String("info", c.state.Info.String()),
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

	if c.isRemoved {
		changefeedStatusGauge.DeleteLabelValues(c.id.Namespace, c.id.ID)
		changefeedCheckpointTsGauge.DeleteLabelValues(c.id.Namespace, c.id.ID, "create")
		changefeedCheckpointTsLagGauge.DeleteLabelValues(c.id.Namespace, c.id.ID, "restart")
	}
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
			c.redoMetaMgr = redo.NewMetaManager(c.id, c.state.Info.Config.Consistent, 0)
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
func (c *changefeed) handleBarrier(ctx cdcContext.Context, barrier *schedulepb.BarrierWithMinTs) error {
	barrierTp, barrierTs := c.barriers.Min()
	c.metricsChangefeedBarrierTsGauge.Set(float64(oracle.ExtractPhysical(barrierTs)))

	// It means:
	//   1. All data before the barrierTs was sent to downstream.
	//   2. No more data after barrierTs was sent to downstream.
	checkpointReachBarrier := barrierTs == c.state.Status.CheckpointTs
	if checkpointReachBarrier {
		switch barrierTp {
		case syncPointBarrier:
			nextSyncPointTs := oracle.GoTimeToTS(
				oracle.GetTimeFromTS(barrierTs).
					Add(util.GetOrZero(c.state.Info.Config.SyncPointInterval)),
			)
			if err := c.ddlSink.emitSyncPoint(ctx, barrierTs); err != nil {
				return errors.Trace(err)
			}
			c.barriers.Update(syncPointBarrier, nextSyncPointTs)
		case finishBarrier:
			c.feedStateManager.MarkFinished()
		default:
			log.Panic("Unknown barrier type", zap.Int("barrierType", int(barrierTp)))
		}
	}

	// If there are other barriers less than ddl barrier,
	// we should wait for them.
	// Note: There may be some tableBarrierTs larger than otherBarrierTs,
	// but we can ignore them because they will be handled in the processor.
	if barrier.GlobalBarrierTs > barrierTs {
		log.Debug("There are other barriers less than ddl barrier, wait for them",
			zap.Uint64("otherBarrierTs", barrierTs),
			zap.Uint64("globalBarrierTs", barrier.GlobalBarrierTs))
		barrier.GlobalBarrierTs = barrierTs
	}

	if barrier.MinTableBarrierTs > barrierTs {
		log.Debug("There are other barriers less than min table barrier, wait for them",
			zap.Uint64("otherBarrierTs", barrierTs),
			zap.Uint64("minTableBarrierTs", barrier.GlobalBarrierTs))
		barrier.MinTableBarrierTs = barrierTs
	}

	return nil
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

func (c *changefeed) updateStatus(checkpointTs, minTableBarrierTs model.Ts) {
	c.state.PatchStatus(
		func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			changed := false
			if status == nil {
				return nil, changed, nil
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
				noPrometheusMsg := fmt.Sprintf(":%d", errno.ErrPrometheusAddrIsNotSet)
				if strings.Contains(err.Error(), noPrometheusMsg) {
					return
				}
				log.Warn("backend observer tick error", zap.Error(err))
			}
		}()
	}
}
