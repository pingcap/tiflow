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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
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
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// newSchedulerFromCtx creates a new scheduler from context.
// This function is factored out to facilitate unit testing.
func newSchedulerFromCtx(
	ctx cdcContext.Context, up *upstream.Upstream, cfg *config.SchedulerConfig,
) (ret scheduler.Scheduler, err error) {
	changeFeedID := ctx.ChangefeedVars().ID
	messageServer := ctx.GlobalVars().MessageServer
	messageRouter := ctx.GlobalVars().MessageRouter
	ownerRev := ctx.GlobalVars().OwnerRevision
	captureID := ctx.GlobalVars().CaptureInfo.ID
	ret, err = scheduler.NewScheduler(
		ctx, captureID, changeFeedID,
		messageServer, messageRouter, ownerRev, up.RegionCache, up.PDClock, cfg)
	return ret, errors.Trace(err)
}

func newScheduler(
	ctx cdcContext.Context, up *upstream.Upstream, cfg *config.SchedulerConfig,
) (scheduler.Scheduler, error) {
	return newSchedulerFromCtx(ctx, up, cfg)
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
	redoManager      redo.LogManager

	schema      *schemaWrap4Owner
	sink        DDLSink
	ddlPuller   puller.DDLPuller
	initialized bool
	// isRemoved is true if the changefeed is removed,
	// which means it will be removed from memory forever
	isRemoved bool
	// isReleased is true if the changefeed's resources were released,
	// but it will still be kept in the memory, and it will be check
	// in every tick. Such as the changefeed that is stopped or encountered an error.
	isReleased bool

	// only used for asyncExecDDL function
	// ddlEventCache is not nil when the changefeed is executing
	// a DDL job asynchronously. After the DDL job has been executed,
	// ddlEventCache will be set to nil. ddlEventCache contains more than
	// one event for a rename tables DDL job.
	ddlEventCache []*model.DDLEvent
	// currentTables is the tables that the changefeed is watching.
	// And it contains only the tables of the ddl that have been processed.
	// The ones that have not been executed yet do not have.
	currentTables []*model.TableInfo

	errCh chan error
	// cancel the running goroutine start by `DDLPuller`
	cancel context.CancelFunc

	// The changefeed will start a backend goroutine in the function `initialize` for DDLPuller
	// `ddlWg` is used to manage this backend goroutine.
	ddlWg sync.WaitGroup

	metricsChangefeedCheckpointTsGauge     prometheus.Gauge
	metricsChangefeedCheckpointTsLagGauge  prometheus.Gauge
	metricsChangefeedCheckpointLagDuration prometheus.Observer

	metricsChangefeedResolvedTsGauge       prometheus.Gauge
	metricsChangefeedResolvedTsLagGauge    prometheus.Gauge
	metricsChangefeedResolvedTsLagDuration prometheus.Observer
	metricsCurrentPDTsGauge                prometheus.Gauge

	metricsChangefeedBarrierTsGauge prometheus.Gauge
	metricsChangefeedTickDuration   prometheus.Observer

	newDDLPuller func(ctx context.Context,
		replicaConfig *config.ReplicaConfig,
		up *upstream.Upstream,
		startTs uint64,
		changefeed model.ChangeFeedID,
	) (puller.DDLPuller, error)

	newSink      func(changefeedID model.ChangeFeedID, info *model.ChangeFeedInfo, reportErr func(error)) DDLSink
	newScheduler func(
		ctx cdcContext.Context, up *upstream.Upstream, cfg *config.SchedulerConfig,
	) (scheduler.Scheduler, error)

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
		feedStateManager: newFeedStateManager(),
		upstream:         up,

		errCh:  make(chan error, defaultErrChSize),
		cancel: func() {},

		newDDLPuller: puller.NewDDLPuller,
		newSink:      newDDLSink,
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
		ctx cdcContext.Context, up *upstream.Upstream, cfg *config.SchedulerConfig,
	) (scheduler.Scheduler, error),
) *changefeed {
	cfg := config.NewDefaultSchedulerConfig()
	c := newChangefeed(id, state, up, cfg)
	c.newDDLPuller = newDDLPuller
	c.newSink = newSink
	c.newScheduler = newScheduler
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
	// we need to wait ddl sink to be ready before we do the other things
	// otherwise, we may cause a nil pointer panic when we try to write to the ddl sink.
	if !c.sink.isInitialized() {
		return nil
	}
	// This means that the cached DDL has been executed,
	// and we need to use the latest tables.
	if c.currentTables == nil {
		c.currentTables = c.schema.AllTables()
		log.Debug("changefeed current tables updated",
			zap.String("namespace", c.id.Namespace),
			zap.String("changefeed", c.id.ID),
			zap.Any("tables", c.currentTables),
		)
	}
	c.sink.emitCheckpointTs(checkpointTs, c.currentTables)

	barrierTs, err := c.handleBarrier(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("owner handles barrier",
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID),
		zap.Uint64("barrierTs", barrierTs),
		zap.Uint64("checkpointTs", checkpointTs))

	if barrierTs < checkpointTs {
		// This condition implies that the DDL resolved-ts has not yet reached checkpointTs,
		// which implies that it would be premature to schedule tables or to update status.
		// So we return here.
		return nil
	}

	startTime := time.Now()
	newCheckpointTs, newResolvedTs, err := c.scheduler.Tick(
		ctx, c.state.Status.CheckpointTs, c.schema.AllPhysicalTables(), captures)
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

	// If the owner is just initialized, barrierTs can be `checkpoint-1`.
	// In such case the `newResolvedTs` and `newCheckpointTs` may be larger
	// than the barrierTs, but it shouldn't be, so we need to handle it here.
	if newResolvedTs > barrierTs {
		newResolvedTs = barrierTs
	}
	if newCheckpointTs > barrierTs {
		newCheckpointTs = barrierTs
	}
	prevResolvedTs := c.state.Status.ResolvedTs
	if c.redoManager.Enabled() {
		var flushedCheckpointTs, flushedResolvedTs model.Ts
		// newResolvedTs can never exceed the barrier timestamp boundary. If redo is enabled,
		// we can only upload it to etcd after it has been flushed into redo meta.
		// NOTE: `UpdateMeta` handles regressed checkpointTs and resolvedTs internally.
		c.redoManager.UpdateMeta(newCheckpointTs, newResolvedTs)
		c.redoManager.GetFlushedMeta(&flushedCheckpointTs, &flushedResolvedTs)
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
	} else {
		// If redo is not enabled, there is no need to wait the slowest table
		// progress, we can just use `barrierTs` as  `newResolvedTs` to make
		// the checkpointTs as close as possible to the barrierTs.
		newResolvedTs = barrierTs
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

	c.updateStatus(newCheckpointTs, newResolvedTs)
	c.updateMetrics(currentTs, newCheckpointTs, metricsResolvedTs)

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

	// if resolvedTs == checkpointTs it means owner can't tell whether the DDL on checkpointTs has
	// been executed or not. So the DDL puller must start at checkpointTs-1.
	var ddlStartTs uint64
	if resolvedTs > checkpointTs {
		ddlStartTs = checkpointTs
	} else {
		ddlStartTs = checkpointTs - 1
	}

	c.barriers = newBarriers()
	if c.state.Info.Config.EnableSyncPoint {
		c.barriers.Update(syncPointBarrier, resolvedTs)
	}
	c.barriers.Update(ddlJobBarrier, ddlStartTs)
	c.barriers.Update(finishBarrier, c.state.Info.GetTargetTs())

	c.schema, err = newSchemaWrap4Owner(c.upstream.KVStorage, ddlStartTs, c.state.Info.Config, c.id)
	if err != nil {
		return errors.Trace(err)
	}

	// we must clean cached ddl and tables in changefeed initialization
	// otherwise, the changefeed will loss tables that are needed to be replicated
	// ref: https://github.com/pingcap/tiflow/issues/7682
	c.ddlEventCache = nil
	c.currentTables = nil

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

	c.sink = c.newSink(c.id, c.state.Info, ctx.Throw)
	c.sink.run(cancelCtx)

	c.ddlPuller, err = c.newDDLPuller(cancelCtx, c.state.Info.Config, c.upstream, ddlStartTs, c.id)
	if err != nil {
		return errors.Trace(err)
	}

	c.ddlWg.Add(1)
	go func() {
		defer c.ddlWg.Done()
		ctx.Throw(c.ddlPuller.Run(cancelCtx))
	}()

	stdCtx := contextutil.PutChangefeedIDInCtx(cancelCtx, c.id)
	redoManagerOpts := redo.NewOwnerManagerOptions(c.errCh)
	mgr, err := redo.NewManager(stdCtx, c.state.Info.Config.Consistent, redoManagerOpts)
	c.redoManager = mgr
	failpoint.Inject("ChangefeedNewRedoManagerError", func() {
		err = errors.New("changefeed new redo manager injected error")
	})
	if err != nil {
		return err
	}
	log.Info("owner creates redo manager",
		zap.String("namespace", c.id.Namespace),
		zap.String("changefeed", c.id.ID))

	// create scheduler
	cfg := *c.cfg
	cfg.ChangefeedSettings = c.state.Info.Config.Scheduler
	c.scheduler, err = c.newScheduler(ctx, c.upstream, &cfg)
	if err != nil {
		return errors.Trace(err)
	}

	c.initMetrics()

	c.initialized = true
	log.Info("changefeed initialized",
		zap.String("namespace", c.state.ID.Namespace),
		zap.String("changefeed", c.state.ID.ID),
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
	c.ddlWg.Wait()

	if c.sink != nil {
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()
		// TODO(dongmen): remove ctx from func sink.close(), it is useless.
		// We don't need to wait sink Close, pass a canceled context is ok
		if err := c.sink.close(canceledCtx); err != nil {
			log.Warn("owner close sink failed",
				zap.String("namespace", c.id.Namespace),
				zap.String("changefeed", c.id.ID),
				zap.Error(err))
		}
	}

	if c.scheduler != nil {
		c.scheduler.Close(ctx)
		c.scheduler = nil
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

// redoManagerCleanup cleanups redo logs if changefeed is removed and redo log is enabled
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
		if c.redoManager == nil {
			redoManagerOpts := redo.NewManagerOptionsForClean()
			redoManager, err := redo.NewManager(ctx, c.state.Info.Config.Consistent, redoManagerOpts)
			if err != nil {
				log.Info("owner creates redo manager for clean fail",
					zap.String("namespace", c.id.Namespace),
					zap.String("changefeed", c.id.ID),
					zap.Error(err))
				return
			}
			c.redoManager = redoManager
		}
		err := c.redoManager.Cleanup(ctx)
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
		c.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			if status == nil {
				status = &model.ChangeFeedStatus{
					// changefeed status is nil when the changefeed has just created.
					ResolvedTs:   c.state.Info.StartTs,
					CheckpointTs: c.state.Info.StartTs,
					AdminJobType: model.AdminNone,
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
	// So we can execute the DDL job at the barrierTs.
	checkpointReachBarrier := barrierTs == c.state.Status.CheckpointTs

	// TODO: To check if we can remove the `barrierTs == c.state.Status.ResolvedTs` condition.
	fullyBlocked := checkpointReachBarrier && barrierTs == c.state.Status.ResolvedTs

	switch barrierTp {
	case ddlJobBarrier:
		ddlResolvedTs, ddlJob := c.ddlPuller.FrontDDL()
		// ddlJob is nil means there is no ddl job in the queue
		// and the ddlResolvedTs is updated by resolvedTs event.
		if ddlJob == nil || ddlResolvedTs != barrierTs {
			// This situation should only happen when the changefeed release
			// but the `c.barriers` is not cleaned.
			// In this case, ddlPuller would restart at `checkpointTs - 1`,
			// so ddlResolvedTs would be less than barrierTs for a short time.
			// TODO: To check if we can remove it, since the `c.barriers` is cleaned now.
			if ddlResolvedTs < barrierTs {
				return barrierTs, nil
			}
			// If the ddlResolvedTs is greater than barrierTs, we should not execute
			// the DDL job, because the changefeed is not blocked by the DDL job yet,
			// which also means not all data before the DDL job is sent to downstream.
			// For example, let say barrierTs(ts=10) and there are some ddl jobs in
			// the queue: [ddl-1(ts=11), ddl-2(ts=12), ddl-3(ts=13)] => ddlResolvedTs(ts=11)
			// If ddlResolvedTs(ts=11) > barrierTs(ts=10), it means the last barrier was sent
			// to sink is barrierTs(ts=10), so the data have been sent ware at most ts=10 not ts=11.
			c.barriers.Update(ddlJobBarrier, ddlResolvedTs)
			_, barrierTs = c.barriers.Min()
			return barrierTs, nil
		}

		// TiCDC guarantees all dml(s) that happen before a ddl was sent to
		// downstream when this ddl is sent. So, we need to wait checkpointTs is
		// fullyBlocked at ddl resolvedTs (equivalent to ddl barrierTs here) before we
		// execute the next ddl.
		// For example, let say there are some events are replicated by cdc:
		// [dml-1(ts=5), dml-2(ts=8), ddl-1(ts=11), ddl-2(ts=12)].
		// We need to wait `checkpointTs == ddlResolvedTs(ts=11)` before execute ddl-1.
		if !checkpointReachBarrier {
			return barrierTs, nil
		}

		done, err := c.asyncExecDDLJob(ctx, ddlJob)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if !done {
			return barrierTs, nil
		}

		// If the last ddl was executed successfully, we can pop it
		// from ddlPuller and update the ddl barrierTs.
		c.lastDDLTs = ddlResolvedTs
		c.ddlPuller.PopFrontDDL()
		newDDLResolvedTs, _ := c.ddlPuller.FrontDDL()
		c.barriers.Update(ddlJobBarrier, newDDLResolvedTs)
	case syncPointBarrier:
		if !fullyBlocked {
			return barrierTs, nil
		}
		nextSyncPointTs := oracle.GoTimeToTS(oracle.GetTimeFromTS(barrierTs).Add(c.state.Info.Config.SyncPointInterval))
		if err := c.sink.emitSyncPoint(ctx, barrierTs); err != nil {
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

// asyncExecDDLJob execute ddl job asynchronously, it returns true if the jod is done.
// 0. Build ddl events from job.
// 1. Apply ddl job to c.schema.
// 2. Emit ddl event to redo manager.
// 3. Emit ddl event to ddl sink.
func (c *changefeed) asyncExecDDLJob(ctx cdcContext.Context,
	job *timodel.Job,
) (bool, error) {
	if job.BinlogInfo == nil {
		log.Warn("ignore the invalid DDL job", zap.String("changefeed", c.id.ID),
			zap.Any("job", job))
		return true, nil
	}

	if c.ddlEventCache == nil {
		// We must build ddl events from job before we call c.schema.HandleDDL(job).
		ddlEvents, err := c.schema.BuildDDLEvents(job)
		if err != nil {
			log.Error("build DDL event fail", zap.String("changefeed", c.id.ID),
				zap.Any("job", job), zap.Error(err))
			return false, errors.Trace(err)
		}
		c.ddlEventCache = ddlEvents
		// We can't use the latest schema directly,
		// we need to make sure we receive the ddl before we start or stop broadcasting checkpoint ts.
		// So let's remember the tables before processing and cache the DDL.
		c.currentTables = c.schema.AllTables()
		checkpointTs := c.state.Status.CheckpointTs
		// refresh checkpointTs and currentTables when a ddl job is received
		c.sink.emitCheckpointTs(checkpointTs, c.currentTables)
		// we apply ddl to update changefeed schema here.
		err = c.schema.HandleDDL(job)
		if err != nil {
			return false, errors.Trace(err)
		}
		if c.redoManager.Enabled() {
			for _, ddlEvent := range c.ddlEventCache {
				// FIXME: seems it's not necessary to emit DDL to redo storage,
				// because for a given redo meta with range (checkpointTs, resolvedTs],
				// there must be no pending DDLs not flushed into DDL sink.
				err = c.redoManager.EmitDDLEvent(ctx, ddlEvent)
				if err != nil {
					return false, err
				}
			}
		}
	}

	jobDone := true
	for _, event := range c.ddlEventCache {
		eventDone, err := c.asyncExecDDLEvent(ctx, event)
		if err != nil {
			return false, err
		}
		jobDone = jobDone && eventDone
	}

	if jobDone {
		c.ddlEventCache = nil
		// It has expired.
		// We should use the latest table names now.
		c.currentTables = nil
	}

	return jobDone, nil
}

func (c *changefeed) asyncExecDDLEvent(ctx cdcContext.Context,
	ddlEvent *model.DDLEvent,
) (done bool, err error) {
	if ddlEvent.TableInfo != nil &&
		c.schema.IsIneligibleTableID(ddlEvent.TableInfo.TableName.TableID) {
		log.Warn("ignore the DDL event of ineligible table",
			zap.String("changefeed", c.id.ID), zap.Any("event", ddlEvent))
		return true, nil
	}

	// check whether in bdr mode, if so, we need to skip all DDLs
	if c.state.Info.Config.BDRMode {
		log.Info("ignore the DDL event in BDR mode",
			zap.String("changefeed", c.id.ID),
			zap.Any("ddl", ddlEvent.Query))
		return true, nil
	}

	done, err = c.sink.emitDDLEvent(ctx, ddlEvent)
	if err != nil {
		return false, err
	}

	return done, nil
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

func (c *changefeed) updateStatus(checkpointTs, resolvedTs model.Ts) {
	c.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
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
