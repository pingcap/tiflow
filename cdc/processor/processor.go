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

package processor

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sinkmanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	backoffBaseDelayInMs = 5
	maxTries             = 3
)

type processor struct {
	changefeedID model.ChangeFeedID
	captureInfo  *model.CaptureInfo
	globalVars   *cdcContext.GlobalVars
	changefeed   *orchestrator.ChangefeedReactorState

	upstream     *upstream.Upstream
	lastSchemaTs model.Ts

	filter filter.Filter

	// To manager DDL events and schema storage.
	ddlHandler component[*ddlHandler]
	// To manage MounterGroup.
	mg component[entry.MounterGroup]
	// To manage redo.DMLManager.
	redo component[redo.DMLManager]

	sourceManager component[*sourcemanager.SourceManager]

	sinkManager component[*sinkmanager.SinkManager]

	initialized bool

	lazyInit func(ctx cdcContext.Context) error
	newAgent func(
		context.Context, *model.Liveness, uint64, *config.SchedulerConfig,
	) (scheduler.Agent, error)
	cfg *config.SchedulerConfig

	liveness        *model.Liveness
	agent           scheduler.Agent
	changefeedEpoch uint64

	metricSyncTableNumGauge      prometheus.Gauge
	metricSchemaStorageGcTsGauge prometheus.Gauge
	metricProcessorErrorCounter  prometheus.Counter
	metricProcessorTickDuration  prometheus.Observer
	metricsProcessorMemoryGauge  prometheus.Gauge
}

// checkReadyForMessages checks whether all necessary Etcd keys have been established.
func (p *processor) checkReadyForMessages() bool {
	return p.changefeed != nil && p.changefeed.Status != nil
}

var _ scheduler.TableExecutor = (*processor)(nil)

// AddTableSpan implements TableExecutor interface.
// AddTableSpan may cause by the following scenario
// 1. `Create Table`, a new table dispatched to the processor, `isPrepare` should be false
// 2. Prepare phase for 2 phase scheduling, `isPrepare` should be true.
// 3. Replicating phase for 2 phase scheduling, `isPrepare` should be false
func (p *processor) AddTableSpan(
	ctx context.Context, span tablepb.Span, checkpoint tablepb.Checkpoint, isPrepare bool,
) (bool, error) {
	if !p.checkReadyForMessages() {
		return false, nil
	}

	startTs := checkpoint.CheckpointTs
	if startTs == 0 {
		log.Panic("table start ts must not be 0",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Uint64("checkpointTs", startTs),
			zap.Bool("isPrepare", isPrepare))
	}

	state, alreadyExist := p.sinkManager.r.GetTableState(span)

	if alreadyExist {
		switch state {
		// table is still `preparing`, which means the table is `replicating` on other captures.
		// no matter `isPrepare` or not, just ignore it should be ok.
		case tablepb.TableStatePreparing:
			log.Warn("table is still preparing, ignore the request",
				zap.String("captureID", p.captureInfo.ID),
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Stringer("span", &span),
				zap.Uint64("checkpointTs", startTs),
				zap.Bool("isPrepare", isPrepare))
			return true, nil
		case tablepb.TableStatePrepared:
			// table is `prepared`, and a `isPrepare = false` request indicate that old table should
			// be stopped on original capture already, it's safe to start replicating data now.
			if !isPrepare {
				if p.redo.r.Enabled() {
					// ResolvedTs is store in external storage when redo log is enabled, so we need to
					// start table with ResolvedTs in redoDMLManager.
					p.redo.r.StartTable(span, checkpoint.ResolvedTs)
				}
				if err := p.sinkManager.r.StartTable(span, startTs); err != nil {
					return false, errors.Trace(err)
				}
			}
			return true, nil
		case tablepb.TableStateReplicating:
			log.Warn("Ignore existing table",
				zap.String("captureID", p.captureInfo.ID),
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Stringer("span", &span),
				zap.Uint64("checkpointTs", startTs),
				zap.Bool("isPrepare", isPrepare))
			return true, nil
		case tablepb.TableStateStopped:
			log.Warn("The same table exists but is stopped. Cancel it and continue.",
				zap.String("captureID", p.captureInfo.ID),
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Stringer("span", &span),
				zap.Uint64("checkpointTs", startTs),
				zap.Bool("isPrepare", isPrepare))
			p.removeTable(span)
		}
	}

	// table not found, can happen in 2 cases
	// 1. this is a new table scheduling request, create the table and make it `replicating`
	// 2. `prepare` phase for 2 phase scheduling, create the table and make it `preparing`
	globalCheckpointTs := p.changefeed.Status.CheckpointTs
	if startTs < globalCheckpointTs {
		log.Warn("addTable: startTs < checkpoint",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Uint64("checkpointTs", startTs),
			zap.Bool("isPrepare", isPrepare))
	}

	p.sinkManager.r.AddTable(
		span, startTs, p.changefeed.Info.TargetTs)
	if p.redo.r.Enabled() {
		p.redo.r.AddTable(span, startTs)
	}
	p.sourceManager.r.AddTable(span, p.getTableName(ctx, span.TableID), startTs)

	return true, nil
}

// RemoveTableSpan implements TableExecutor interface.
func (p *processor) RemoveTableSpan(span tablepb.Span) bool {
	if !p.checkReadyForMessages() {
		return false
	}

	_, exist := p.sinkManager.r.GetTableState(span)
	if !exist {
		log.Warn("Table which will be deleted is not found",
			zap.String("capture", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Stringer("span", &span))
		return true
	}
	return p.sinkManager.r.AsyncStopTable(span)
}

// IsAddTableSpanFinished implements TableExecutor interface.
func (p *processor) IsAddTableSpanFinished(span tablepb.Span, isPrepare bool) bool {
	if !p.checkReadyForMessages() {
		return false
	}

	globalCheckpointTs := p.changefeed.Status.CheckpointTs

	var tableResolvedTs, tableCheckpointTs uint64
	var state tablepb.TableState
	done := func() bool {
		var alreadyExist bool
		state, alreadyExist = p.sinkManager.r.GetTableState(span)
		if alreadyExist {
			stats := p.sinkManager.r.GetTableStats(span)
			tableResolvedTs = stats.ResolvedTs
			tableCheckpointTs = stats.CheckpointTs
		} else {
			log.Panic("table which was added is not found",
				zap.String("captureID", p.captureInfo.ID),
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Stringer("span", &span),
				zap.Bool("isPrepare", isPrepare))
		}

		if isPrepare {
			return state == tablepb.TableStatePrepared
		}

		// The table is `replicating`, it's indicating that the `add table` must be finished.
		return state == tablepb.TableStateReplicating
	}
	if !done() {
		log.Debug("Add Table not finished",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Uint64("tableResolvedTs", tableResolvedTs),
			zap.Uint64("tableCheckpointTs", tableCheckpointTs),
			zap.Uint64("globalCheckpointTs", globalCheckpointTs),
			zap.Any("state", state),
			zap.Bool("isPrepare", isPrepare))
		return false
	}

	log.Info("Add Table finished",
		zap.String("captureID", p.captureInfo.ID),
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID),
		zap.Stringer("span", &span),
		zap.Uint64("tableResolvedTs", tableResolvedTs),
		zap.Uint64("tableCheckpointTs", tableCheckpointTs),
		zap.Uint64("globalCheckpointTs", globalCheckpointTs),
		zap.Any("state", state),
		zap.Bool("isPrepare", isPrepare))
	return true
}

// IsRemoveTableSpanFinished implements TableExecutor interface.
func (p *processor) IsRemoveTableSpanFinished(span tablepb.Span) (model.Ts, bool) {
	if !p.checkReadyForMessages() {
		return 0, false
	}

	state, ok := p.sinkManager.r.GetTableState(span)
	if !ok {
		log.Warn("table has been stopped",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Stringer("span", &span))
		return 0, true
	}

	stats := p.sinkManager.r.GetTableStats(span)
	if state != tablepb.TableStateStopped {
		log.Debug("table is still not stopped",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Uint64("checkpointTs", stats.CheckpointTs),
			zap.Stringer("span", &span),
			zap.Any("tableStatus", state))
		return 0, false
	}

	if p.redo.r.Enabled() {
		p.redo.r.RemoveTable(span)
	}
	p.sinkManager.r.RemoveTable(span)
	p.sourceManager.r.RemoveTable(span)
	log.Info("table removed",
		zap.String("captureID", p.captureInfo.ID),
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID),
		zap.Stringer("span", &span),
		zap.Uint64("checkpointTs", stats.CheckpointTs))

	return stats.CheckpointTs, true
}

// GetTableSpanStatus implements TableExecutor interface
func (p *processor) GetTableSpanStatus(span tablepb.Span, collectStat bool) tablepb.TableStatus {
	state, exist := p.sinkManager.r.GetTableState(span)
	if !exist {
		return tablepb.TableStatus{
			TableID: span.TableID,
			Span:    span,
			State:   tablepb.TableStateAbsent,
		}
	}
	sinkStats := p.sinkManager.r.GetTableStats(span)
	stats := tablepb.Stats{}
	if collectStat {
		stats = p.getStatsFromSourceManagerAndSinkManager(span, sinkStats)
	}
	return tablepb.TableStatus{
		TableID: span.TableID,
		Span:    span,
		Checkpoint: tablepb.Checkpoint{
			CheckpointTs: sinkStats.CheckpointTs,
			ResolvedTs:   sinkStats.ResolvedTs,
			LastSyncedTs: sinkStats.LastSyncedTs,
		},
		State: state,
		Stats: stats,
	}
}

func (p *processor) getStatsFromSourceManagerAndSinkManager(
	span tablepb.Span, sinkStats sinkmanager.TableStats,
) tablepb.Stats {
	pullerStats := p.sourceManager.r.GetTablePullerStats(span)
	now, _ := p.upstream.PDClock.CurrentTime()

	stats := tablepb.Stats{
		RegionCount: pullerStats.RegionCount,
		CurrentTs:   oracle.ComposeTS(oracle.GetPhysical(now), 0),
		BarrierTs:   sinkStats.BarrierTs,
		StageCheckpoints: map[string]tablepb.Checkpoint{
			"puller-ingress": {
				CheckpointTs: pullerStats.CheckpointTsIngress,
				ResolvedTs:   pullerStats.ResolvedTsIngress,
			},
			"puller-egress": {
				CheckpointTs: pullerStats.CheckpointTsEgress,
				ResolvedTs:   pullerStats.ResolvedTsEgress,
			},
			"sink": {
				CheckpointTs: sinkStats.CheckpointTs,
				ResolvedTs:   sinkStats.ResolvedTs,
			},
		},
	}

	sortStats := p.sourceManager.r.GetTableSorterStats(span)
	stats.StageCheckpoints["sorter-ingress"] = tablepb.Checkpoint{
		CheckpointTs: sortStats.ReceivedMaxCommitTs,
		ResolvedTs:   sortStats.ReceivedMaxResolvedTs,
	}
	stats.StageCheckpoints["sorter-egress"] = tablepb.Checkpoint{
		CheckpointTs: sinkStats.ResolvedTs,
		ResolvedTs:   sinkStats.ResolvedTs,
	}

	return stats
}

// newProcessor creates a new processor
func newProcessor(
	state *orchestrator.ChangefeedReactorState,
	captureInfo *model.CaptureInfo,
	changefeedID model.ChangeFeedID,
	up *upstream.Upstream,
	liveness *model.Liveness,
	changefeedEpoch uint64,
	cfg *config.SchedulerConfig,
) *processor {
	p := &processor{
		changefeed:      state,
		upstream:        up,
		changefeedID:    changefeedID,
		captureInfo:     captureInfo,
		liveness:        liveness,
		changefeedEpoch: changefeedEpoch,

		metricSyncTableNumGauge: syncTableNumGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricProcessorErrorCounter: processorErrorCounter.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricSchemaStorageGcTsGauge: processorSchemaStorageGcTsGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricProcessorTickDuration: processorTickDuration.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricsProcessorMemoryGauge: processorMemoryGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}
	p.lazyInit = p.lazyInitImpl
	p.newAgent = p.newAgentImpl
	p.cfg = cfg
	return p
}

var processorIgnorableError = []*errors.Error{
	cerror.ErrAdminStopProcessor,
	cerror.ErrReactorFinished,
}

// isProcessorIgnorableError returns true if the error means the processor exits
// normally, caused by changefeed pause, remove, etc.
func isProcessorIgnorableError(err error) bool {
	if err == nil {
		return true
	}
	if errors.Cause(err) == context.Canceled {
		return true
	}
	for _, e := range processorIgnorableError {
		if e.Equal(err) {
			return true
		}
	}
	return false
}

// Tick implements the `orchestrator.State` interface
// the `state` parameter is sent by the etcd worker, the `state` must be a snapshot of KVs in etcd
// The main logic of processor is in this function, including the calculation of many kinds of ts,
// maintain table pipeline, error handling, etc.
//
// It can be called in etcd ticks, so it should never be blocked.
func (p *processor) Tick(ctx cdcContext.Context) error {
	// check upstream error first
	if err := p.upstream.Error(); err != nil {
		return p.handleErr(err)
	}
	if p.upstream.IsClosed() {
		log.Panic("upstream is closed",
			zap.Uint64("upstreamID", p.upstream.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID))
	}
	// skip this tick
	if !p.upstream.IsNormal() {
		log.Warn("upstream is not ready, skip",
			zap.Uint64("id", p.upstream.ID),
			zap.Strings("pd", p.upstream.PdEndpoints),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID))
		return nil
	}
	startTime := time.Now()
	p.changefeed.CheckCaptureAlive(p.captureInfo.ID)
	err := p.tick(ctx)
	costTime := time.Since(startTime)
	if costTime > processorLogsWarnDuration {
		log.Warn("processor tick took too long",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.String("capture", p.captureInfo.ID),
			zap.Duration("duration", costTime))
	}

	p.metricProcessorTickDuration.Observe(costTime.Seconds())

	// we should check if this error is nil,
	// otherwise the function called below may panic.
	if err == nil {
		p.refreshMetrics()
	}

	return p.handleErr(err)
}

func (p *processor) handleErr(err error) error {
	if err == nil {
		return nil
	}
	if isProcessorIgnorableError(err) {
		log.Info("processor exited",
			zap.String("capture", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Error(err))
		return cerror.ErrReactorFinished.GenWithStackByArgs()
	}
	p.metricProcessorErrorCounter.Inc()
	// record error information in etcd
	var code string
	if rfcCode, ok := cerror.RFCCode(err); ok {
		code = string(rfcCode)
	} else {
		code = string(cerror.ErrProcessorUnknown.RFCCode())
	}
	p.changefeed.PatchTaskPosition(p.captureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			if position == nil {
				position = &model.TaskPosition{}
			}
			position.Error = &model.RunningError{
				Time:    time.Now(),
				Addr:    p.captureInfo.AdvertiseAddr,
				Code:    code,
				Message: err.Error(),
			}
			return position, true, nil
		})
	log.Error("run processor failed",
		zap.String("capture", p.captureInfo.ID),
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID),
		zap.Error(err))
	return err
}

func (p *processor) handleWarnings() {
	var err error
	select {
	case err = <-p.ddlHandler.warnings:
	case err = <-p.mg.warnings:
	case err = <-p.redo.warnings:
	case err = <-p.sourceManager.warnings:
	case err = <-p.sinkManager.warnings:
	default:
		return
	}
	var code string
	if rfcCode, ok := cerror.RFCCode(err); ok {
		code = string(rfcCode)
	} else {
		code = string(cerror.ErrProcessorUnknown.RFCCode())
	}
	p.changefeed.PatchTaskPosition(p.captureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			if position == nil {
				position = &model.TaskPosition{}
			}
			position.Warning = &model.RunningError{
				Time:    time.Now(),
				Addr:    p.captureInfo.AdvertiseAddr,
				Code:    code,
				Message: err.Error(),
			}
			return position, true, nil
		})
}

func (p *processor) tick(ctx cdcContext.Context) error {
	if !p.checkChangefeedNormal() {
		return cerror.ErrAdminStopProcessor.GenWithStackByArgs()
	}
	// we should skip this tick after create a task position
	if p.createTaskPosition() {
		return nil
	}

	p.handleWarnings()

	if err := p.handleErrorCh(); err != nil {
		return errors.Trace(err)
	}
	if err := p.lazyInit(ctx); err != nil {
		return errors.Trace(err)
	}

	barrier, err := p.agent.Tick(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	if barrier != nil && barrier.GlobalBarrierTs != 0 {
		p.updateBarrierTs(barrier)
	}
	p.doGCSchemaStorage()

	return nil
}

// checkChangefeedNormal checks if the changefeed is runnable.
func (p *processor) checkChangefeedNormal() bool {
	// check the state in this tick, make sure that the admin job type of the changefeed is not stopped
	if p.changefeed.Info.AdminJobType.IsStopState() || p.changefeed.Status.AdminJobType.IsStopState() {
		return false
	}
	// add a patch to check the changefeed is runnable when applying the patches in the etcd worker.
	p.changefeed.CheckChangefeedNormal()
	return true
}

// createTaskPosition will create a new task position if a task position does not exist.
// task position not exist only when the processor is running first in the first tick.
func (p *processor) createTaskPosition() (skipThisTick bool) {
	if _, exist := p.changefeed.TaskPositions[p.captureInfo.ID]; exist {
		return false
	}
	if p.initialized {
		log.Warn("position is nil, maybe position info is removed unexpected", zap.Any("state", p.changefeed))
	}
	p.changefeed.PatchTaskPosition(p.captureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			if position == nil {
				return &model.TaskPosition{}, true, nil
			}
			return position, false, nil
		})
	return true
}

// lazyInitImpl create Filter, SchemaStorage, Mounter instances at the first tick.
func (p *processor) lazyInitImpl(etcdCtx cdcContext.Context) (err error) {
	if p.initialized {
		return nil
	}

	// Here we use a separated context for sub-components, so we can custom the
	// order of stopping all sub-components when closing the processor.
	prcCtx := cdcContext.NewContext(context.Background(), etcdCtx.GlobalVars())
	prcCtx = cdcContext.WithChangefeedVars(prcCtx, etcdCtx.ChangefeedVars())
	p.globalVars = prcCtx.GlobalVars()

	// NOTE: We must call contextutil.Put* to put some variables into the new context.
	// Maybe it's better to put all things into global vars or changefeed vars.
	stdCtx := contextutil.PutTimezoneInCtx(prcCtx, contextutil.TimezoneFromCtx(etcdCtx))
	stdCtx = contextutil.PutChangefeedIDInCtx(stdCtx, p.changefeedID)
	stdCtx = contextutil.PutRoleInCtx(stdCtx, util.RoleProcessor)
	stdCtx = contextutil.PutCaptureAddrInCtx(stdCtx, p.globalVars.CaptureInfo.AdvertiseAddr)

	tz := contextutil.TimezoneFromCtx(stdCtx)
	p.filter, err = filter.NewFilter(p.changefeed.Info.Config, util.GetTimeZoneName(tz))
	if err != nil {
		return errors.Trace(err)
	}

	if err = p.initDDLHandler(stdCtx); err != nil {
		return err
	}
	p.ddlHandler.name = "ddlHandler"
	p.ddlHandler.spawn(stdCtx)

	p.mg.r = entry.NewMounterGroup(p.ddlHandler.r.schemaStorage,
		p.changefeed.Info.Config.Mounter.WorkerNum,
		p.filter, tz, p.changefeedID, p.changefeed.Info.Config.Integrity)
	p.mg.name = "MounterGroup"
	p.mg.spawn(stdCtx)

	sourceID, err := pdutil.GetSourceID(stdCtx, p.upstream.PDClient)
	if err != nil {
		return errors.Trace(err)
	}
	p.changefeed.Info.Config.Sink.TiDBSourceID = sourceID

	p.redo.r = redo.NewDMLManager(p.changefeedID, p.changefeed.Info.Config.Consistent)
	p.redo.name = "RedoManager"
	p.redo.spawn(stdCtx)

	sortEngine, err := p.globalVars.SortEngineFactory.Create(p.changefeedID)
	log.Info("Processor creates sort engine",
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID),
		zap.Error(err))
	if err != nil {
		return errors.Trace(err)
	}

	p.sourceManager.r = sourcemanager.New(
		p.changefeedID, p.upstream, p.mg.r,
		sortEngine, p.changefeed.Info.Config.BDRMode)
	p.sourceManager.name = "SourceManager"
	p.sourceManager.spawn(stdCtx)

	p.sinkManager.r = sinkmanager.New(
		p.changefeedID, p.changefeed.Info, p.upstream,
		p.ddlHandler.r.schemaStorage, p.redo.r, p.sourceManager.r)
	p.sinkManager.name = "SinkManager"
	p.sinkManager.spawn(stdCtx)

	// Bind them so that sourceManager can notify sinkManager.r.
	p.sourceManager.r.OnResolve(p.sinkManager.r.UpdateReceivedSorterResolvedTs)

	p.agent, err = p.newAgent(stdCtx, p.liveness, p.changefeedEpoch, p.cfg)
	if err != nil {
		return err
	}

	p.initialized = true
	log.Info("processor initialized",
		zap.String("capture", p.captureInfo.ID),
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID),
		zap.Uint64("changefeedEpoch", p.changefeedEpoch))
	return nil
}

func (p *processor) newAgentImpl(
	ctx context.Context,
	liveness *model.Liveness,
	changefeedEpoch uint64,
	cfg *config.SchedulerConfig,
) (ret scheduler.Agent, err error) {
	messageServer := p.globalVars.MessageServer
	messageRouter := p.globalVars.MessageRouter
	etcdClient := p.globalVars.EtcdClient
	captureID := p.globalVars.CaptureInfo.ID
	ret, err = scheduler.NewAgent(
		ctx, captureID, liveness,
		messageServer, messageRouter, etcdClient, p, p.changefeedID,
		changefeedEpoch, cfg)
	return ret, errors.Trace(err)
}

// handleErrorCh listen the error channel and throw the error if it is not expected.
func (p *processor) handleErrorCh() (err error) {
	// TODO(qupeng): handle different errors in different ways.
	select {
	case err = <-p.ddlHandler.errors:
	case err = <-p.mg.errors:
	case err = <-p.redo.errors:
	case err = <-p.sourceManager.errors:
	case err = <-p.sinkManager.errors:
	default:
		return nil
	}
	if !isProcessorIgnorableError(err) {
		log.Error("error on running processor",
			zap.String("capture", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Error(err))
		return err
	}
	log.Info("processor exited",
		zap.String("capture", p.captureInfo.ID),
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID))
	return cerror.ErrReactorFinished
}

func (p *processor) initDDLHandler(ctx context.Context) error {
	checkpointTs := p.changefeed.Info.GetCheckpointTs(p.changefeed.Status)
	minTableBarrierTs := p.changefeed.Status.MinTableBarrierTs
	forceReplicate := p.changefeed.Info.Config.ForceReplicate

	// if minTableBarrierTs == checkpointTs it means owner can't tell whether the DDL on checkpointTs has
	// been executed or not. So the DDL puller must start at checkpointTs-1.
	var ddlStartTs uint64
	if minTableBarrierTs > checkpointTs {
		ddlStartTs = checkpointTs
	} else {
		ddlStartTs = checkpointTs - 1
	}

	meta, err := kv.GetSnapshotMeta(p.upstream.KVStorage, ddlStartTs)
	if err != nil {
		return errors.Trace(err)
	}
	f, err := filter.NewFilter(p.changefeed.Info.Config, "")
	if err != nil {
		return errors.Trace(err)
	}
	schemaStorage, err := entry.NewSchemaStorage(meta, ddlStartTs,
		forceReplicate, p.changefeedID, util.RoleProcessor, f)
	if err != nil {
		return errors.Trace(err)
	}

	serverCfg := config.GetGlobalServerConfig()
	ctx = contextutil.PutTableInfoInCtx(ctx, -1, puller.DDLPullerTableName)
	ddlPuller, err := puller.NewDDLJobPuller(
		ctx,
		p.upstream.PDClient,
		p.upstream.GrpcPool,
		p.upstream.RegionCache,
		p.upstream.KVStorage,
		p.upstream.PDClock,
		ddlStartTs,
		serverCfg,
		p.changefeedID,
		schemaStorage,
		f,
	)
	if err != nil {
		return errors.Trace(err)
	}
	p.ddlHandler.r = &ddlHandler{puller: ddlPuller, schemaStorage: schemaStorage}
	return nil
}

// updateBarrierTs updates barrierTs for all tables.
func (p *processor) updateBarrierTs(barrier *schedulepb.Barrier) {
	tableBarrier := p.calculateTableBarrierTs(barrier)
	globalBarrierTs := barrier.GetGlobalBarrierTs()
	schemaResolvedTs := p.ddlHandler.r.schemaStorage.ResolvedTs()
	if schemaResolvedTs < globalBarrierTs {
		// Do not update barrier ts that is larger than
		// DDL puller's resolved ts.
		// When DDL puller stall, resolved events that outputted by sorter
		// may pile up in memory, as they have to wait DDL.
		globalBarrierTs = schemaResolvedTs
	}
	log.Debug("update barrierTs",
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID),
		zap.Any("tableBarriers", barrier.GetTableBarriers()),
		zap.Uint64("globalBarrierTs", globalBarrierTs))

	p.sinkManager.r.UpdateBarrierTs(globalBarrierTs, tableBarrier)
}

func (p *processor) getTableName(ctx context.Context, tableID model.TableID) string {
	// FIXME: using GetLastSnapshot here would be confused and get the wrong table name
	// after `rename table` DDL, since `rename table` keeps the tableID unchanged
	var tableName *model.TableName
	retry.Do(ctx, func() error { //nolint:errcheck
		if x, ok := p.ddlHandler.r.schemaStorage.GetLastSnapshot().PhysicalTableByID(tableID); ok {
			tableName = &x.TableName
			return nil
		}
		return errors.Errorf("failed to get table name, fallback to use table id: %d",
			tableID)
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs),
		retry.WithMaxTries(maxTries),
		retry.WithIsRetryableErr(cerror.IsRetryableError))

	if tableName == nil {
		log.Warn("failed to get table name for metric",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Any("tableID", tableID))
		return strconv.Itoa(int(tableID))
	}

	return tableName.QuoteString()
}

func (p *processor) removeTable(span tablepb.Span) {
	if p.redo.r.Enabled() {
		p.redo.r.RemoveTable(span)
	}
	p.sinkManager.r.RemoveTable(span)
	p.sourceManager.r.RemoveTable(span)
}

// doGCSchemaStorage trigger the schema storage GC
func (p *processor) doGCSchemaStorage() {
	if p.ddlHandler.r.schemaStorage == nil {
		// schemaStorage is nil only in test
		return
	}

	if p.changefeed.Status == nil {
		// This could happen if Etcd data is not complete.
		return
	}

	// Please refer to `unmarshalAndMountRowChanged` in cdc/entry/mounter.go
	// for why we need -1.
	lastSchemaTs := p.ddlHandler.r.schemaStorage.DoGC(p.changefeed.Status.CheckpointTs - 1)
	if p.lastSchemaTs == lastSchemaTs {
		return
	}
	p.lastSchemaTs = lastSchemaTs

	log.Debug("finished gc in schema storage",
		zap.Uint64("gcTs", lastSchemaTs),
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID))
	lastSchemaPhysicalTs := oracle.ExtractPhysical(lastSchemaTs)
	p.metricSchemaStorageGcTsGauge.Set(float64(lastSchemaPhysicalTs))
}

func (p *processor) refreshMetrics() {
	// Before the processor is initialized, we should not refresh metrics.
	// Otherwise, it will cause panic.
	if !p.initialized {
		return
	}
	p.metricSyncTableNumGauge.Set(float64(p.sinkManager.r.GetAllCurrentTableSpansCount()))
}

// Close closes the processor. It must be called explicitly to stop all sub-components.
func (p *processor) Close() error {
	log.Info("processor closing ...",
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID))

	// clean up metrics first to avoid some metrics are not cleaned up
	// when error occurs during closing the processor
	p.cleanupMetrics()

	p.sinkManager.stop(p.changefeedID)
	p.sinkManager.r = nil
	p.sourceManager.stop(p.changefeedID)
	p.sourceManager.r = nil
	p.redo.stop(p.changefeedID)
	p.mg.stop(p.changefeedID)
	p.ddlHandler.stop(p.changefeedID)

	if p.globalVars != nil && p.globalVars.SortEngineFactory != nil {
		if err := p.globalVars.SortEngineFactory.Drop(p.changefeedID); err != nil {
			log.Error("Processor drop event sort engine fail",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Error(err))
			return errors.Trace(err)
		}
		log.Info("Processor drop sort engine successfully",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID))
	}

	if p.agent != nil {
		log.Info("Processor try to close agent",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID))
		if err := p.agent.Close(); err != nil {
			log.Warn("close agent meet error",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Error(err))
		}
		log.Info("Processor closed agent successfully",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID))
		p.agent = nil
	}

	// mark tables share the same cdcContext with its original table, don't need to cancel
	failpoint.Inject("processorStopDelay", nil)

	log.Info("processor closed",
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID))

	return nil
}

func (p *processor) cleanupMetrics() {
	syncTableNumGauge.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	processorErrorCounter.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	processorSchemaStorageGcTsGauge.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	processorTickDuration.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	processorMemoryGauge.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
}

// WriteDebugInfo write the debug info to Writer
func (p *processor) WriteDebugInfo(w io.Writer) error {
	fmt.Fprintf(w, "%+v\n", *p.changefeed)
	spans := p.sinkManager.r.GetAllCurrentTableSpans()
	for _, span := range spans {
		state, _ := p.sinkManager.r.GetTableState(span)
		stats := p.sinkManager.r.GetTableStats(span)
		// TODO: add table name.
		fmt.Fprintf(w, "span: %s, resolvedTs: %d, checkpointTs: %d, state: %s\n",
			&span, stats.ResolvedTs, stats.CheckpointTs, state)
	}

	return nil
}

func (p *processor) calculateTableBarrierTs(
	barrier *schedulepb.Barrier,
) map[model.TableID]model.Ts {
	tableBarrierTs := make(map[model.TableID]model.Ts)
	for _, tb := range barrier.TableBarriers {
		tableBarrierTs[tb.TableID] = tb.BarrierTs
	}
	return tableBarrierTs
}

type component[R util.Runnable] struct {
	r        R
	name     string
	ctx      context.Context
	cancel   context.CancelFunc
	errors   chan error
	warnings chan error
	wg       sync.WaitGroup
}

func (c *component[R]) spawn(ctx context.Context) {
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.errors = make(chan error, 16)
	c.warnings = make(chan error, 16)

	changefeedID := contextutil.ChangefeedIDFromCtx(c.ctx)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		err := c.r.Run(c.ctx, c.warnings)
		if err != nil && errors.Cause(err) != context.Canceled {
			log.Error("processor sub-component fails",
				zap.String("namespace", changefeedID.Namespace),
				zap.String("changefeed", changefeedID.ID),
				zap.String("name", c.name),
				zap.Error(err))
			select {
			case <-c.ctx.Done():
			case c.errors <- err:
			}
		}
	}()
	c.r.WaitForReady(ctx)
	log.Info("processor sub-component starts",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID),
		zap.String("name", c.name))
}

func (c *component[R]) stop(changefeedID model.ChangeFeedID) {
	if c.cancel == nil {
		log.Info("processor sub-component isn't started",
			zap.String("namespace", changefeedID.Namespace),
			zap.String("changefeed", changefeedID.ID),
			zap.String("name", c.name))
		return
	}
	log.Info("processor sub-component is in stopping",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID),
		zap.String("name", c.name))
	c.cancel()
	c.wg.Wait()
	c.r.Close()
}

type ddlHandler struct {
	puller        puller.DDLJobPuller
	schemaStorage entry.SchemaStorage
}

func (d *ddlHandler) Run(ctx context.Context, _ ...chan<- error) error {
	g, ctx := errgroup.WithContext(ctx)
	// d.puller will update the schemaStorage.
	g.Go(func() error { return d.puller.Run(ctx) })
	g.Go(func() error {
		for {
			var jobEntry *model.DDLJobEntry
			select {
			case <-ctx.Done():
				return nil
			case jobEntry = <-d.puller.Output():
			}
			failpoint.Inject("processorDDLResolved", nil)
			if jobEntry.OpType == model.OpTypeResolved {
				d.schemaStorage.AdvanceResolvedTs(jobEntry.CRTs)
			}
			err := jobEntry.Err
			if err != nil {
				return errors.Trace(err)
			}
		}
	})
	return g.Wait()
}

func (d *ddlHandler) WaitForReady(_ context.Context) {}

func (d *ddlHandler) Close() {}
