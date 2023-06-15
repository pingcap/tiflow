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
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/format"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	redoCfg "github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// newSchedulerV2FromCtx creates a new schedulerV2 from context.
// This function is factored out to facilitate unit testing.
func newSchedulerV2FromCtx(
	ctx cdcContext.Context, startTs uint64,
) (scheduler.Scheduler, error) {
	changeFeedID := ctx.ChangefeedVars().ID
	messageServer := ctx.GlobalVars().MessageServer
	messageRouter := ctx.GlobalVars().MessageRouter
	ownerRev := ctx.GlobalVars().OwnerRevision
	ret, err := scheduler.NewScheduler(
		ctx, changeFeedID, startTs, messageServer, messageRouter, ownerRev)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

func newScheduler(ctx cdcContext.Context, startTs uint64) (scheduler.Scheduler, error) {
	return newSchedulerV2FromCtx(ctx, startTs)
}

type changefeed struct {
	id    model.ChangeFeedID
	state *orchestrator.ChangefeedReactorState

	upStream         *upstream.Upstream
	scheduler        scheduler.Scheduler
	barriers         *barriers
	feedStateManager *feedStateManager
	redoManager      redo.LogManager

	schema      *schemaWrap4Owner
	sink        DDLSink
	ddlPuller   DDLPuller
	initialized bool
	// isRemoved is true if the changefeed is removed,
	// which means it will be removed from memory forever
	isRemoved bool
	// isReleased is true if the changefeed's resource is released
	// but it will still be kept in the memory and it will be check
	// in every tick
	isReleased bool

	// only used for asyncExecDDL function
	// ddlEventCache is not nil when the changefeed is executing
	// a DDL job asynchronously. After the DDL job has been executed,
	// ddlEventCache will be set to nil. ddlEventCache contains more than
	// one event for a rename tables DDL job.
	ddlEventCache []*model.DDLEvent
	// currentTableNames is the table names that the changefeed is watching.
	// And it contains only the tables of the ddl that have been processed.
	// The ones that have not been executed yet do not have.
	currentTableNames []model.TableName

	errCh chan error
	// cancel the running goroutine start by `DDLPuller`
	cancel context.CancelFunc

	// The changefeed will start a backend goroutine in the function `initialize` for DDLPuller
	// `ddlWg` is used to manage this backend goroutine.
	ddlWg sync.WaitGroup

	metricsChangefeedBarrierTsGauge       prometheus.Gauge
	metricsChangefeedCheckpointTsGauge    prometheus.Gauge
	metricsChangefeedCheckpointTsLagGauge prometheus.Gauge
	metricsChangefeedResolvedTsGauge      prometheus.Gauge
	metricsChangefeedResolvedTsLagGauge   prometheus.Gauge
	metricsChangefeedTickDuration         prometheus.Observer

	newDDLPuller func(ctx cdcContext.Context, upStream *upstream.Upstream, startTs uint64) (DDLPuller, error)
	newSink      func() DDLSink
	newScheduler func(ctx cdcContext.Context, startTs uint64) (scheduler.Scheduler, error)

	lastDDLTs uint64 // Timestamp of the last executed DDL. Only used for tests.
}

func newChangefeed(id model.ChangeFeedID, upStream *upstream.Upstream) *changefeed {
	c := &changefeed{
		id: id,
		// The scheduler will be created lazily.
		scheduler:        nil,
		barriers:         newBarriers(),
		feedStateManager: newFeedStateManager(),
		upStream:         upStream,

		errCh:  make(chan error, defaultErrChSize),
		cancel: func() {},

		newDDLPuller: newDDLPuller,
		newSink:      newDDLSink,
	}
	c.newScheduler = newScheduler
	return c
}

func newChangefeed4Test(
	id model.ChangeFeedID, upStream *upstream.Upstream,
	newDDLPuller func(ctx cdcContext.Context, upStream *upstream.Upstream, startTs uint64) (DDLPuller, error),
	newSink func() DDLSink,
) *changefeed {
	c := newChangefeed(id, upStream)
	c.newDDLPuller = newDDLPuller
	c.newSink = newSink
	return c
}

func (c *changefeed) Tick(ctx cdcContext.Context, state *orchestrator.ChangefeedReactorState, captures map[model.CaptureID]*model.CaptureInfo) {
	startTime := time.Now()
	// skip this tick
	if !c.upStream.IsNormal() {
		return
	}
	ctx = cdcContext.WithErrorHandler(ctx, func(err error) error {
		c.errCh <- errors.Trace(err)
		return nil
	})
	state.CheckCaptureAlive(ctx.GlobalVars().CaptureInfo.ID)
	err := c.tick(ctx, state, captures)

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
}

func (c *changefeed) checkStaleCheckpointTs(ctx cdcContext.Context, checkpointTs uint64) error {
	state := c.state.Info.State
	if state == model.StateNormal || state == model.StateStopped || state == model.StateError {
		failpoint.Inject("InjectChangefeedFastFailError", func() error {
			return cerror.ErrGCTTLExceeded.FastGen("InjectChangefeedFastFailError")
		})
		if err := c.upStream.GCManager.CheckStaleCheckpointTs(ctx, c.id, checkpointTs); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *changefeed) tick(ctx cdcContext.Context, state *orchestrator.ChangefeedReactorState, captures map[model.CaptureID]*model.CaptureInfo) error {
	c.state = state
	c.feedStateManager.Tick(state)

	checkpointTs := c.state.Info.GetCheckpointTs(c.state.Status)
	// check stale checkPointTs must be called before `feedStateManager.ShouldRunning()`
	// to ensure an error or stopped changefeed also be checked
	if err := c.checkStaleCheckpointTs(ctx, checkpointTs); err != nil {
		return errors.Trace(err)
	}

	if !c.feedStateManager.ShouldRunning() {
		c.isRemoved = c.feedStateManager.ShouldRemoved()
		c.releaseResources(ctx)
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
	// we need to wait sink to be ready before we do the other things
	// otherwise, we may cause a nil pointer panic
	if !c.sink.isInitialized() {
		return nil
	}
	// This means that the cached DDL has been executed,
	// and we need to use the latest table names.
	if c.currentTableNames == nil {
		c.currentTableNames = c.schema.AllTableNames()
		log.Debug("changefeed current table names updated",
			zap.String("namespace", c.id.Namespace),
			zap.String("changefeed", c.id.ID),
			zap.Any("tables", c.currentTableNames),
		)
	}
	c.sink.emitCheckpointTs(checkpointTs, c.currentTableNames)

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
	costTime := time.Since(startTime)
	if costTime > schedulerLogsWarnDuration {
		log.Warn("scheduler tick took too long",
			zap.String("namespace", c.id.Namespace),
			zap.String("changefeed", c.id.ID), zap.Duration("duration", costTime))
	}
	if err != nil {
		return errors.Trace(err)
	}

	pdTime, _ := c.upStream.PDClock.CurrentTime()
	currentTs := oracle.GetPhysical(pdTime)

	// CheckpointCannotProceed implies that not all tables are being replicated normally,
	// so in that case there is no need to advance the global watermarks.
	if newCheckpointTs != scheduler.CheckpointCannotProceed {
		// If the owner is just initialized, barrierTs can be `checkpoint-1`. To avoid
		// global resolvedTs and checkpointTs regression, we need to handle the case.
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
		c.updateMetrics(currentTs, newCheckpointTs, newResolvedTs)
	} else if c.state.Status != nil {
		// We should keep the metrics updated even if the scheduler cannot
		// advance the watermarks for now.
		c.updateMetrics(currentTs, c.state.Status.CheckpointTs, c.state.Status.ResolvedTs)
	}
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
		// Also it unblocks TiDB GC, because the service GC safepoint is set to
		// 1 hour TTL during creating changefeed.
		//
		// See more gc doc.
		ensureTTL := int64(10 * 60)
		err := gc.EnsureChangefeedStartTsSafety(
			ctx, c.upStream.PDClient,
			gc.EnsureGCServiceInitializing,
			c.state.ID, ensureTTL, checkpointTs)
		if err != nil {
			return errors.Trace(err)
		}
		// clean service GC safepoint '-creating-' if there are any.
		err = gc.UndoEnsureChangefeedStartTsSafety(
			ctx, c.upStream.PDClient,
			gc.EnsureGCServiceCreating,
			ctx.ChangefeedVars().ID,
		)
		if err != nil {
			return errors.Trace(err)
		}
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

	if c.state.Info.SyncPointEnabled {
		c.barriers.Update(syncPointBarrier, resolvedTs)
	}
	c.barriers.Update(ddlJobBarrier, ddlStartTs)
	c.barriers.Update(finishBarrier, c.state.Info.GetTargetTs())

	c.schema, err = newSchemaWrap4Owner(c.upStream.KVStorage, ddlStartTs, c.state.Info.Config, c.id)
	if err != nil {
		return errors.Trace(err)
	}

	// we must clean cached ddl and tables in changefeed initialization
	// otherwise, the changefeed will loss tables that are needed to be replicated
	// ref: https://github.com/pingcap/tiflow/issues/7682
	c.ddlEventCache = nil
	c.currentTableNames = nil

	cancelCtx, cancel := cdcContext.WithCancel(ctx)
	c.cancel = cancel

	c.sink = c.newSink()
	c.sink.run(cancelCtx, cancelCtx.ChangefeedVars().ID, cancelCtx.ChangefeedVars().Info)

	c.ddlPuller, err = c.newDDLPuller(cancelCtx, c.upStream, ddlStartTs)
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

	// init metrics
	c.metricsChangefeedBarrierTsGauge = changefeedBarrierTsGauge.
		WithLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedCheckpointTsGauge = changefeedCheckpointTsGauge.
		WithLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedCheckpointTsLagGauge = changefeedCheckpointTsLagGauge.
		WithLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedResolvedTsGauge = changefeedResolvedTsGauge.
		WithLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedResolvedTsLagGauge = changefeedResolvedTsLagGauge.
		WithLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedTickDuration = changefeedTickDuration.
		WithLabelValues(c.id.Namespace, c.id.ID)

	// create scheduler
	c.scheduler, err = c.newScheduler(ctx, checkpointTs)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("initialize changefeed",
		zap.String("namespace", c.state.ID.Namespace),
		zap.String("changefeed", c.state.ID.ID),
		zap.Stringer("info", c.state.Info),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Uint64("resolvedTs", resolvedTs))

	c.initialized = true
	return nil
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
	}

	c.schema = nil

	changefeedCheckpointTsGauge.DeleteLabelValues(c.id.Namespace, c.id.ID)
	changefeedCheckpointTsLagGauge.DeleteLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedCheckpointTsGauge = nil
	c.metricsChangefeedCheckpointTsLagGauge = nil

	changefeedResolvedTsGauge.DeleteLabelValues(c.id.Namespace, c.id.ID)
	changefeedResolvedTsLagGauge.DeleteLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedResolvedTsGauge = nil
	c.metricsChangefeedResolvedTsLagGauge = nil

	changefeedTickDuration.DeleteLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedTickDuration = nil

	changefeedBarrierTsGauge.DeleteLabelValues(c.id.Namespace, c.id.ID)
	c.metricsChangefeedBarrierTsGauge = nil

	if c.isRemoved {
		changefeedStatusGauge.DeleteLabelValues(c.id.Namespace, c.id.ID)
	}
	c.isReleased = true
	c.initialized = false
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
		gc.EnsureGCServiceCreating,
		gc.EnsureGCServiceInitializing,
	}

	for _, serviceID := range serviceIDs {
		err := gc.UndoEnsureChangefeedStartTsSafety(
			ctx,
			c.upStream.PDClient,
			serviceID,
			ctx.ChangefeedVars().ID)
		if err != nil {
			log.Error("failed to remove gc safepoint",
				zap.String("namespace", c.state.ID.Namespace),
				zap.String("changefeed", c.state.ID.ID),
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
					// the changefeed status is nil when the changefeed is just created.
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

func (c *changefeed) handleBarrier(ctx cdcContext.Context) (uint64, error) {
	barrierTp, barrierTs := c.barriers.Min()
	phyBarrierTs := oracle.ExtractPhysical(barrierTs)
	c.metricsChangefeedBarrierTsGauge.Set(float64(phyBarrierTs))
	ddlBlocked := barrierTs == c.state.Status.CheckpointTs
	blocked := ddlBlocked && barrierTs == c.state.Status.ResolvedTs
	switch barrierTp {
	case ddlJobBarrier:
		ddlResolvedTs, ddlJob := c.ddlPuller.FrontDDL()
		if ddlJob == nil || ddlResolvedTs != barrierTs {
			if ddlResolvedTs < barrierTs {
				return barrierTs, nil
			}
			c.barriers.Update(ddlJobBarrier, ddlResolvedTs)
			return barrierTs, nil
		}
		if !ddlBlocked {
			// DDL shouldn't be blocked by resolvedTs because DDL puller is created
			// with checkpointTs instead of resolvedTs.
			return barrierTs, nil
		}
		done, err := c.asyncExecDDLJob(ctx, ddlJob)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if !done {
			return barrierTs, nil
		}
		c.lastDDLTs = ddlResolvedTs
		c.ddlPuller.PopFrontDDL()
		newDDLResolvedTs, _ := c.ddlPuller.FrontDDL()
		c.barriers.Update(ddlJobBarrier, newDDLResolvedTs)
	case syncPointBarrier:
		if !blocked {
			return barrierTs, nil
		}
		nextSyncPointTs := oracle.GoTimeToTS(oracle.GetTimeFromTS(barrierTs).Add(c.state.Info.SyncPointInterval))
		if err := c.sink.emitSyncPoint(ctx, barrierTs); err != nil {
			return 0, errors.Trace(err)
		}
		c.barriers.Update(syncPointBarrier, nextSyncPointTs)
	case finishBarrier:
		if blocked {
			c.feedStateManager.MarkFinished()
		}
		return barrierTs, nil
	default:
		log.Panic("Unknown barrier type", zap.Int("barrierType", int(barrierTp)))
	}
	return barrierTs, nil
}

func (c *changefeed) asyncExecDDLJob(ctx cdcContext.Context,
	job *timodel.Job,
) (bool, error) {
	if job.BinlogInfo == nil {
		log.Warn("ignore the invalid DDL job", zap.String("changefeed", c.id.ID),
			zap.Reflect("job", job))
		return true, nil
	}

	cyclicConfig := c.state.Info.Config.Cyclic
	if cyclicConfig.IsEnabled() && !cyclicConfig.SyncDDL {
		log.Info("ignore the DDL job because cyclic config is enabled and syncDDL is false",
			zap.String("changefeed", c.id.ID), zap.Reflect("job", job))
		return true, nil
	}

	if c.ddlEventCache == nil {
		ddlEvents, err := c.schema.BuildDDLEvents(job)
		if err != nil {
			log.Error("build DDL event fail", zap.String("changefeed", c.id.ID),
				zap.Reflect("job", job), zap.Error(err))
			return false, errors.Trace(err)
		}
		c.ddlEventCache = ddlEvents

		// We can't use the latest schema directly,
		// we need to make sure we receive the ddl before we start or stop broadcasting checkpoint ts.
		// So let's remember the name of the table before processing and cache the DDL.
		c.currentTableNames = c.schema.AllTableNames()
		checkpointTs := c.state.Status.CheckpointTs
		// refresh checkpointTs and currentTableNames when a ddl job is received
		c.sink.emitCheckpointTs(checkpointTs, c.currentTableNames)
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
		c.currentTableNames = nil
	}

	return jobDone, nil
}

func (c *changefeed) asyncExecDDLEvent(ctx cdcContext.Context,
	ddlEvent *model.DDLEvent,
) (done bool, err error) {
	ddlEvent.Query, err = addSpecialComment(ddlEvent.Query)
	if ddlEvent.TableInfo != nil &&
		c.schema.IsIneligibleTableID(ddlEvent.TableInfo.TableID) {
		log.Warn("ignore the DDL event of ineligible table",
			zap.String("changefeed", c.id.ID), zap.Reflect("event", ddlEvent))
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
	c.metricsChangefeedCheckpointTsLagGauge.Set(float64(currentTs-phyCkpTs) / 1e3)

	phyRTs := oracle.ExtractPhysical(resolvedTs)
	c.metricsChangefeedResolvedTsGauge.Set(float64(phyRTs))
	c.metricsChangefeedResolvedTsLagGauge.Set(float64(currentTs-phyRTs) / 1e3)
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
	c.upStream.Release()
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

// addSpecialComment translate tidb feature to comment
func addSpecialComment(ddlQuery string) (string, error) {
	stms, _, err := parser.New().ParseSQL(ddlQuery)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(stms) != 1 {
		log.Panic("invalid ddlQuery statement size", zap.String("ddlQuery", ddlQuery))
	}
	var sb strings.Builder
	// translate TiDB feature to special comment
	restoreFlags := format.RestoreTiDBSpecialComment
	// escape the keyword
	restoreFlags |= format.RestoreNameBackQuotes
	// upper case keyword
	restoreFlags |= format.RestoreKeyWordUppercase
	// wrap string with single quote
	restoreFlags |= format.RestoreStringSingleQuotes
	// remove placement rule
	restoreFlags |= format.SkipPlacementRuleForRestore
	if err = stms[0].Restore(format.NewRestoreCtx(restoreFlags, &sb)); err != nil {
		return "", errors.Trace(err)
	}
	return sb.String(), nil
}
