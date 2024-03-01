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

// Changefeed is the tick logic of changefeed.
type Changefeed interface {
	// Tick is called periodically to drive the changefeed's internal logic.
	// The main logic of changefeed is in this function, including the calculation of many kinds of ts,
	// maintain table components, error handling, etc.
	//
	// It can be called in etcd ticks, so it should never be blocked.
	// Tick Returns:  checkpointTs, minTableBarrierTs
	Tick(cdcContext.Context, *model.ChangeFeedInfo,
		*model.ChangeFeedStatus,
		map[model.CaptureID]*model.CaptureInfo) (model.Ts, model.Ts)

	// Close closes the changefeed.
	Close(ctx cdcContext.Context)

	// GetScheduler returns the scheduler of this changefeed.
	GetScheduler() scheduler.Scheduler
}

var _ Changefeed = (*changefeed)(nil)

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

	upstream  *upstream.Upstream
	cfg       *config.SchedulerConfig
	scheduler scheduler.Scheduler
	// barriers will be created when a changefeed is initialized
	// and will be destroyed when a changefeed is closed.
	barriers         *barriers
	feedStateManager FeedStateManager
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
	initialized bool
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
		up *upstream.Upstream,
		startTs uint64,
		changefeed model.ChangeFeedID,
		schemaStorage entry.SchemaStorage,
		filter pfilter.Filter,
	) puller.DDLPuller

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

	// The latest changefeed info and status from meta storage. they are updated in every Tick.
	latestInfo   *model.ChangeFeedInfo
	latestStatus *model.ChangeFeedStatus
}

func (c *changefeed) GetScheduler() scheduler.Scheduler {
	return c.scheduler
}

// NewChangefeed creates a new changefeed.
func NewChangefeed(
	id model.ChangeFeedID,
	cfInfo *model.ChangeFeedInfo,
	cfStatus *model.ChangeFeedStatus,
	feedStateManager FeedStateManager,
	up *upstream.Upstream,
	cfg *config.SchedulerConfig,
) *changefeed {
	c := &changefeed{
		id:           id,
		latestInfo:   cfInfo,
		latestStatus: cfStatus,
		// The scheduler will be created lazily.
		scheduler:        nil,
		barriers:         newBarriers(),
		feedStateManager: feedStateManager,
		upstream:         up,

		errCh:     make(chan error, defaultErrChSize),
		warningCh: make(chan error, defaultErrChSize),
		cancel:    func() {},

		newDDLPuller:          puller.NewDDLPuller,
		newSink:               newDDLSink,
		newDownstreamObserver: observer.NewObserver,
	}
	c.newScheduler = newScheduler
	c.cfg = cfg
	return c
}

func newChangefeed4Test(
	id model.ChangeFeedID,
	cfInfo *model.ChangeFeedInfo,
	cfStatus *model.ChangeFeedStatus,
	cfstateManager FeedStateManager, up *upstream.Upstream,
	newDDLPuller func(ctx context.Context,
		up *upstream.Upstream,
		startTs uint64,
		changefeed model.ChangeFeedID,
		schemaStorage entry.SchemaStorage,
		filter pfilter.Filter,
	) puller.DDLPuller,
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
	c := NewChangefeed(id, cfInfo, cfStatus, cfstateManager, up, cfg)
	c.newDDLPuller = newDDLPuller
	c.newSink = newSink
	c.newScheduler = newScheduler
	c.newDownstreamObserver = newDownstreamObserver
	return c
}

func (c *changefeed) Tick(ctx cdcContext.Context,
	cfInfo *model.ChangeFeedInfo,
	cfStatus *model.ChangeFeedStatus,
	captures map[model.CaptureID]*model.CaptureInfo,
) (model.Ts, model.Ts) {
	startTime := time.Now()
	c.latestInfo = cfInfo
	c.latestStatus = cfStatus
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
		return 0, 0
	}

	ctx = cdcContext.WithErrorHandler(ctx, func(err error) error {
		select {
		case <-ctx.Done():
		case c.errCh <- errors.Trace(err):
		}
		return nil
	})
	checkpointTs, minTableBarrierTs, err := c.tick(ctx, captures)

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
	return checkpointTs, minTableBarrierTs
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
	c.feedStateManager.HandleError(&model.RunningError{
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

	c.feedStateManager.HandleWarning(&model.RunningError{
		Time:    time.Now(),
		Addr:    config.GetGlobalServerConfig().AdvertiseAddr,
		Code:    code,
		Message: err.Error(),
	})
}

func (c *changefeed) checkStaleCheckpointTs(
	ctx cdcContext.Context, checkpointTs uint64,
) error {
	if c.latestInfo.NeedBlockGC() {
		failpoint.Inject("InjectChangefeedFastFailError", func() error {
			return cerror.ErrStartTsBeforeGC.FastGen("InjectChangefeedFastFailError")
		})
		if err := c.upstream.GCManager.CheckStaleCheckpointTs(ctx, c.id, checkpointTs); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// tick is the main logic of changefeed.
// tick returns the checkpointTs and minTableBarrierTs.
func (c *changefeed) tick(ctx cdcContext.Context,
	captures map[model.CaptureID]*model.CaptureInfo,
) (model.Ts, model.Ts, error) {
	adminJobPending := c.feedStateManager.Tick(c.resolvedTs, c.latestStatus, c.latestInfo)
	preCheckpointTs := c.latestInfo.GetCheckpointTs(c.latestStatus)
	// checkStaleCheckpointTs must be called before `feedStateManager.ShouldRunning()`
	// to ensure all changefeeds, no matter whether they are running or not, will be checked.
	if err := c.checkStaleCheckpointTs(ctx, preCheckpointTs); err != nil {
		return 0, 0, errors.Trace(err)
	}

	if !c.feedStateManager.ShouldRunning() {
		c.isRemoved = c.feedStateManager.ShouldRemoved()
		c.releaseResources(ctx)
		return 0, 0, nil
	}

	if adminJobPending {
		return 0, 0, nil
	}

	if err := c.initialize(ctx); err != nil {
		return 0, 0, errors.Trace(err)
	}

	select {
	case err := <-c.errCh:
		return 0, 0, errors.Trace(err)
	default:
	}

	if c.redoMetaMgr.Enabled() {
		if !c.redoMetaMgr.Running() {
			return 0, 0, nil
		}
	}

	allPhysicalTables, barrier, err := c.ddlManager.tick(ctx, preCheckpointTs)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	err = c.handleBarrier(ctx, c.latestInfo, c.latestStatus, barrier)
	if err != nil {
		return 0, 0, errors.Trace(err)
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
		return 0, 0, nil
	}

	watermark, err := c.scheduler.Tick(
		ctx, preCheckpointTs, allPhysicalTables, captures,
		barrier)
	if err != nil {
		return 0, 0, errors.Trace(err)
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
		if c.latestStatus != nil {
			// We should keep the metrics updated even if the scheduler cannot
			// advance the watermarks for now.
			c.updateMetrics(currentTs, c.latestStatus.CheckpointTs, c.resolvedTs)
		}
		return 0, 0, nil
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
	if barrier.MinTableBarrierTs < c.latestStatus.MinTableBarrierTs {
		barrier.MinTableBarrierTs = c.latestStatus.MinTableBarrierTs
	}

	failpoint.Inject("ChangefeedOwnerDontUpdateCheckpoint", func() {
		if c.lastDDLTs != 0 && c.latestStatus.CheckpointTs >= c.lastDDLTs {
			log.Info("owner won't update checkpoint because of failpoint",
				zap.String("namespace", c.id.Namespace),
				zap.String("changefeed", c.id.ID),
				zap.Uint64("keepCheckpoint", c.latestStatus.CheckpointTs),
				zap.Uint64("skipCheckpoint", watermark.CheckpointTs))
			watermark.CheckpointTs = c.latestStatus.CheckpointTs
		}
	})

	failpoint.Inject("ChangefeedOwnerNotUpdateCheckpoint", func() {
		watermark.CheckpointTs = c.latestStatus.CheckpointTs
	})

	c.updateMetrics(currentTs, watermark.CheckpointTs, c.resolvedTs)
	c.tickDownstreamObserver(ctx)

	return watermark.CheckpointTs, barrier.MinTableBarrierTs, nil
}

func (c *changefeed) initialize(ctx cdcContext.Context) (err error) {
	if c.initialized || c.latestStatus == nil {
		// If `c.latestStatus` is nil it means the changefeed struct is just created, it needs to
		//  1. use startTs as checkpointTs and resolvedTs, if it's a new created changefeed; or
		//  2. load checkpointTs and resolvedTs from etcd, if it's an existing changefeed.
		// And then it can continue to initialize.
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

	checkpointTs := c.latestStatus.CheckpointTs
	if c.resolvedTs == 0 {
		c.resolvedTs = checkpointTs
	}

	minTableBarrierTs := c.latestStatus.MinTableBarrierTs

	failpoint.Inject("NewChangefeedNoRetryError", func() {
		failpoint.Return(cerror.ErrStartTsBeforeGC.GenWithStackByArgs(checkpointTs-300, checkpointTs))
	})
	failpoint.Inject("NewChangefeedRetryError", func() {
		failpoint.Return(errors.New("failpoint injected retriable error"))
	})

	if c.latestInfo.Config.CheckGCSafePoint {
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
	if util.GetOrZero(c.latestInfo.Config.EnableSyncPoint) {
		c.barriers.Update(syncPointBarrier, c.resolvedTs)
	}
	c.barriers.Update(finishBarrier, c.latestInfo.GetTargetTs())

	filter, err := pfilter.NewFilter(c.latestInfo.Config, "")
	if err != nil {
		return errors.Trace(err)
	}
	c.schema, err = entry.NewSchemaStorage(
		c.upstream.KVStorage, ddlStartTs,
		c.latestInfo.Config.ForceReplicate, c.id, util.RoleOwner, filter)
	if err != nil {
		return errors.Trace(err)
	}

	cancelCtx, cancel := cdcContext.WithCancel(ctx)
	c.cancel = cancel

	sourceID, err := pdutil.GetSourceID(ctx, c.upstream.PDClient)
	if err != nil {
		return errors.Trace(err)
	}
	c.latestInfo.Config.Sink.TiDBSourceID = sourceID
	log.Info("set source id",
		zap.Uint64("sourceID", sourceID),
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID),
	)

	c.ddlSink = c.newSink(c.id, c.latestInfo, ctx.Throw, func(err error) {
		select {
		case <-ctx.Done():
		case c.warningCh <- err:
		}
	})
	c.ddlSink.run(cancelCtx)

	c.ddlPuller = c.newDDLPuller(cancelCtx, c.upstream, ddlStartTs, c.id, c.schema, filter)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ctx.Throw(c.ddlPuller.Run(cancelCtx))
	}()

	c.downstreamObserver, err = c.newDownstreamObserver(ctx, c.id, c.latestInfo.SinkURI, c.latestInfo.Config)
	if err != nil {
		return err
	}
	c.observerLastTick = atomic.NewTime(time.Time{})

	c.redoDDLMgr = redo.NewDDLManager(c.id, c.latestInfo.Config.Consistent, ddlStartTs)
	if c.redoDDLMgr.Enabled() {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			ctx.Throw(c.redoDDLMgr.Run(cancelCtx))
		}()
	}

	c.redoMetaMgr = redo.NewMetaManager(c.id, c.latestInfo.Config.Consistent, checkpointTs)
	if c.redoMetaMgr.Enabled() {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			ctx.Throw(c.redoMetaMgr.Run(cancelCtx))
		}()
		log.Info("owner creates redo manager",
			zap.String("namespace", c.id.Namespace),
			zap.String("changefeed", c.id.ID))
	}

	c.ddlManager = newDDLManager(
		c.id,
		ddlStartTs,
		c.latestStatus.CheckpointTs,
		c.ddlSink,
		filter,
		c.ddlPuller,
		c.schema,
		c.redoDDLMgr,
		c.redoMetaMgr,
		util.GetOrZero(c.latestInfo.Config.BDRMode))

	// create scheduler
	cfg := *c.cfg
	cfg.ChangefeedSettings = c.latestInfo.Config.Scheduler
	epoch := c.latestInfo.Epoch
	c.scheduler, err = c.newScheduler(ctx, c.upstream, epoch, &cfg, c.redoMetaMgr)
	if err != nil {
		return errors.Trace(err)
	}

	c.initMetrics()

	c.initialized = true
	c.metricsChangefeedCreateTimeGuage.Set(float64(oracle.GetPhysical(c.latestInfo.CreateTime)))
	c.metricsChangefeedRestartTimeGauge.Set(float64(oracle.GetPhysical(time.Now())))
	log.Info("changefeed initialized",
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID),
		zap.Uint64("changefeedEpoch", epoch),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Uint64("resolvedTs", c.resolvedTs),
		zap.String("info", c.latestInfo.String()))

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
	c.cleanupMetrics()
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

	c.schema = nil
	c.barriers = nil
	c.resolvedTs = 0
	c.initialized = false
	c.isReleased = true

	log.Info("changefeed closed",
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID),
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
	cfInfo := c.latestInfo
	if c.isRemoved {
		if cfInfo == nil || cfInfo.Config == nil ||
			cfInfo.Config.Consistent == nil {
			log.Warn("changefeed is removed, but state is not complete", zap.Any("info", cfInfo))
			return
		}
		if !redoCfg.IsConsistentEnabled(cfInfo.Config.Consistent.Level) {
			return
		}
		// when removing a paused changefeed, the redo manager is nil, create a new one
		if c.redoMetaMgr == nil {
			c.redoMetaMgr = redo.NewMetaManager(c.id, cfInfo.Config.Consistent, 0)
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

// handleBarrier calculates the barrierTs of the changefeed.
// barrierTs is used to control the data that can be flush to downstream.
func (c *changefeed) handleBarrier(ctx cdcContext.Context,
	cfInfo *model.ChangeFeedInfo,
	cfStatus *model.ChangeFeedStatus,
	barrier *schedulepb.BarrierWithMinTs,
) error {
	barrierTp, barrierTs := c.barriers.Min()
	c.metricsChangefeedBarrierTsGauge.Set(float64(oracle.ExtractPhysical(barrierTs)))

	// It means:
	//   1. All data before the barrierTs was sent to downstream.
	//   2. No more data after barrierTs was sent to downstream.
	checkpointReachBarrier := barrierTs == cfStatus.CheckpointTs
	if checkpointReachBarrier {
		switch barrierTp {
		case syncPointBarrier:
			nextSyncPointTs := oracle.GoTimeToTS(
				oracle.GetTimeFromTS(barrierTs).
					Add(util.GetOrZero(cfInfo.Config.SyncPointInterval)),
			)
			if err := c.ddlSink.emitSyncPoint(ctx, barrierTs); err != nil {
				return errors.Trace(err)
			}
			c.barriers.Update(syncPointBarrier, nextSyncPointTs)
		case finishBarrier:
			c.feedStateManager.MarkFinished()
		default:
			log.Error("Unknown barrier type", zap.Int("barrierType", int(barrierTp)))
			return cerror.ErrUnexpected.FastGenByArgs("Unknown barrier type")
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
