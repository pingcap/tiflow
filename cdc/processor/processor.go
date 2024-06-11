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
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/async"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sinkmanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/cdc/vars"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	backoffBaseDelayInMs = 5
	maxTries             = 3
)

// Processor is the processor of changefeed data.
type Processor interface {
	// Tick is called periodically to drive the Processor's internal logic.
	// The main logic of processor is in this function, including the calculation of many kinds of ts,
	// maintain table components, error handling, etc.
	//
	// It can be called in etcd ticks, so it should never be blocked.
	// Tick Returns: error and warnings. error will be propagated to the owner, and warnings will be record.
	Tick(context.Context, *model.ChangeFeedInfo, *model.ChangeFeedStatus) (error, error)

	// Close closes the processor.
	Close() error
}

var _ Processor = (*processor)(nil)

type processor struct {
	changefeedID model.ChangeFeedID
	captureInfo  *model.CaptureInfo
	globalVars   *vars.GlobalVars

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

	initialized *atomic.Bool
	initializer *async.Initializer

	lazyInit func(ctx context.Context) error
	newAgent func(
		context.Context, *model.Liveness, uint64, *config.SchedulerConfig,
		etcd.OwnerCaptureInfoClient,
	) (scheduler.Agent, error)
	cfg *config.SchedulerConfig

	liveness        *model.Liveness
	agent           scheduler.Agent
	changefeedEpoch uint64

	// The latest changefeed info and status from meta storage. they are updated in every Tick.
	// processor implements TableExecutor interface, so we need to add these two fields here to use them
	// in `AddTableSpan` and `RemoveTableSpan`, otherwise we need to adjust the interface.
	// we can refactor this step by step.
	latestInfo   *model.ChangeFeedInfo
	latestStatus *model.ChangeFeedStatus

	ownerCaptureInfoClient etcd.OwnerCaptureInfoClient

	metricSyncTableNumGauge      prometheus.Gauge
	metricSchemaStorageGcTsGauge prometheus.Gauge
	metricProcessorErrorCounter  prometheus.Counter
	metricProcessorTickDuration  prometheus.Observer
	metricsProcessorMemoryGauge  prometheus.Gauge
}

// checkReadyForMessages checks whether all necessary Etcd keys have been established.
func (p *processor) checkReadyForMessages() bool {
	return p.latestStatus != nil
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

	failpoint.Inject("ProcessorAddTableError", func() {
		failpoint.Return(false, cerror.New("processor add table injected error"))
	})

	startTs := checkpoint.CheckpointTs
	if startTs == 0 {
		log.Error("table start ts must not be 0",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Uint64("checkpointTs", startTs),
			zap.Bool("isPrepare", isPrepare))
		return false, cerror.ErrUnexpected.FastGenByArgs("table start ts must not be 0")
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
	globalCheckpointTs := p.latestStatus.CheckpointTs
	if startTs < globalCheckpointTs {
		log.Warn("addTable: startTs < checkpoint",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Uint64("checkpointTs", startTs),
			zap.Bool("isPrepare", isPrepare))
	}

	table := p.sinkManager.r.AddTable(
		span, startTs, p.latestInfo.TargetTs)
	if p.redo.r.Enabled() {
		p.redo.r.AddTable(span, startTs)
	}

	p.sourceManager.r.AddTable(span, p.getTableName(ctx, span.TableID), startTs, table.GetReplicaTs)
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

	globalCheckpointTs := p.latestStatus.CheckpointTs

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
	now := p.upstream.PDClock.CurrentTime()

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

// NewProcessor creates a new processor
func NewProcessor(
	info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus,
	captureInfo *model.CaptureInfo,
	changefeedID model.ChangeFeedID,
	up *upstream.Upstream,
	liveness *model.Liveness,
	changefeedEpoch uint64,
	cfg *config.SchedulerConfig,
	ownerCaptureInfoClient etcd.OwnerCaptureInfoClient,
	globalVars *vars.GlobalVars,
) *processor {
	p := &processor{
		upstream:        up,
		changefeedID:    changefeedID,
		captureInfo:     captureInfo,
		liveness:        liveness,
		changefeedEpoch: changefeedEpoch,
		latestInfo:      info,
		latestStatus:    status,

		initialized: atomic.NewBool(false),

		ownerCaptureInfoClient: ownerCaptureInfoClient,
		globalVars:             globalVars,

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
	p.initializer = async.NewInitializer()
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
// the `info` parameter is sent by metadata store, the `info` must be the latest value snapshot.
// the `status` parameter is sent by metadata store, the `status` must be the latest value snapshot.
// The main logic of processor is in this function, including the calculation of many kinds of ts,
// maintain table components, error handling, etc.
//
// It can be called in etcd ticks, so it should never be blocked.
func (p *processor) Tick(
	ctx context.Context,
	info *model.ChangeFeedInfo, status *model.ChangeFeedStatus,
) (error, error) {
	if !p.initialized.Load() {
		initialized, err := p.initializer.TryInitialize(ctx, p.lazyInit, p.globalVars.ChangefeedThreadPool)
		if err != nil {
			return errors.Trace(err), nil
		}
		if !initialized {
			return nil, nil
		}
	}

	p.latestInfo = info
	p.latestStatus = status

	// check upstream error first
	if err := p.upstream.Error(); err != nil {
		p.metricProcessorErrorCounter.Inc()
		return err, nil
	}
	if p.upstream.IsClosed() {
		log.Error("upstream is closed",
			zap.Uint64("upstreamID", p.upstream.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID))
		return cerror.ErrUnexpected.FastGenByArgs("upstream is closed"), nil
	}
	// skip this tick
	if !p.upstream.IsNormal() {
		log.Warn("upstream is not ready, skip",
			zap.Uint64("id", p.upstream.ID),
			zap.Strings("pd", p.upstream.PdEndpoints),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID))
		return nil, nil
	}
	startTime := time.Now()
	err, warning := p.tick(ctx)
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
	} else {
		p.metricProcessorErrorCounter.Inc()
	}
	return err, warning
}

func (p *processor) handleWarnings() error {
	var err error
	select {
	case err = <-p.ddlHandler.warnings:
	case err = <-p.mg.warnings:
	case err = <-p.redo.warnings:
	case err = <-p.sourceManager.warnings:
	case err = <-p.sinkManager.warnings:
	default:
	}
	return err
}

func (p *processor) tick(ctx context.Context) (error, error) {
	warning := p.handleWarnings()
	if err := p.handleErrorCh(); err != nil {
		return errors.Trace(err), warning
	}

	barrier, err := p.agent.Tick(ctx)
	if err != nil {
		return errors.Trace(err), warning
	}

	if barrier != nil && barrier.GlobalBarrierTs != 0 {
		p.updateBarrierTs(barrier)
	}
	p.doGCSchemaStorage()

	return nil, warning
}

// isMysqlCompatibleBackend returns true if the sinkURIStr is mysql compatible.
func isMysqlCompatibleBackend(sinkURIStr string) (bool, error) {
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return false, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	scheme := sink.GetScheme(sinkURI)
	return sink.IsMySQLCompatibleScheme(scheme), nil
}

// lazyInitImpl create Filter, SchemaStorage, Mounter instances at the first tick.
func (p *processor) lazyInitImpl(etcdCtx context.Context) (err error) {
	if p.initialized.Load() {
		return nil
	}
	// Here we use a separated context for sub-components, so we can custom the
	// order of stopping all sub-components when closing the processor.
	prcCtx := context.Background()

	var tz *time.Location
	// todo: get the timezone from the global config or the changefeed config?
	tz, err = util.GetTimezone(config.GetGlobalServerConfig().TZ)
	if err != nil {
		return errors.Trace(err)
	}

	// Clone the config to avoid data race
	cfConfig := p.latestInfo.Config.Clone()

	p.filter, err = filter.NewFilter(cfConfig, util.GetTimeZoneName(tz))
	if err != nil {
		return errors.Trace(err)
	}

	if err = p.initDDLHandler(prcCtx); err != nil {
		return err
	}
	p.ddlHandler.name = "ddlHandler"
	p.ddlHandler.changefeedID = p.changefeedID
	p.ddlHandler.spawn(prcCtx)

	p.mg.r = entry.NewMounterGroup(p.ddlHandler.r.schemaStorage,
		cfConfig.Mounter.WorkerNum,
		p.filter, tz, p.changefeedID, cfConfig.Integrity)
	p.mg.name = "MounterGroup"
	p.mg.changefeedID = p.changefeedID
	p.mg.spawn(prcCtx)

	sourceID, err := pdutil.GetSourceID(prcCtx, p.upstream.PDClient)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("get sourceID from PD", zap.Uint64("sourceID", sourceID), zap.Stringer("changefeedID", p.changefeedID))
	cfConfig.Sink.TiDBSourceID = sourceID

	p.redo.r = redo.NewDMLManager(p.changefeedID, cfConfig.Consistent)
	p.redo.name = "RedoManager"
	p.redo.changefeedID = p.changefeedID
	p.redo.spawn(prcCtx)

	sortEngine, err := p.globalVars.SortEngineFactory.Create(p.changefeedID)
	log.Info("Processor creates sort engine",
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID),
		zap.Error(err))
	if err != nil {
		return errors.Trace(err)
	}

	isMysqlBackend, err := isMysqlCompatibleBackend(p.latestInfo.SinkURI)
	if err != nil {
		return errors.Trace(err)
	}
	p.sourceManager.r = sourcemanager.New(
		p.changefeedID, p.upstream, p.mg.r,
		sortEngine, util.GetOrZero(cfConfig.BDRMode),
		util.GetOrZero(cfConfig.EnableTableMonitor),
		isMysqlBackend)
	p.sourceManager.name = "SourceManager"
	p.sourceManager.changefeedID = p.changefeedID
	p.sourceManager.spawn(prcCtx)

	p.sinkManager.r = sinkmanager.New(
		p.changefeedID, p.latestInfo.SinkURI, cfConfig, p.upstream,
		p.ddlHandler.r.schemaStorage, p.redo.r, p.sourceManager.r, isMysqlBackend)
	p.sinkManager.name = "SinkManager"
	p.sinkManager.changefeedID = p.changefeedID
	p.sinkManager.spawn(prcCtx)

	// Bind them so that sourceManager can notify sinkManager.r.
	p.sourceManager.r.OnResolve(p.sinkManager.r.UpdateReceivedSorterResolvedTs)
	p.agent, err = p.newAgent(prcCtx, p.liveness, p.changefeedEpoch, p.cfg, p.ownerCaptureInfoClient)
	if err != nil {
		return err
	}

	p.initialized.Store(true)
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
	client etcd.OwnerCaptureInfoClient,
) (ret scheduler.Agent, err error) {
	messageServer := p.globalVars.MessageServer
	messageRouter := p.globalVars.MessageRouter
	captureID := p.globalVars.CaptureInfo.ID
	ret, err = scheduler.NewAgent(
		ctx, captureID, liveness,
		messageServer, messageRouter, client, p, p.changefeedID,
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
	checkpointTs := p.latestInfo.GetCheckpointTs(p.latestStatus)
	minTableBarrierTs := p.latestStatus.MinTableBarrierTs
	forceReplicate := p.latestInfo.Config.ForceReplicate

	// if minTableBarrierTs == checkpointTs it means owner can't tell whether the DDL on checkpointTs has
	// been executed or not. So the DDL puller must start at checkpointTs-1.
	var ddlStartTs uint64
	if minTableBarrierTs > checkpointTs {
		ddlStartTs = checkpointTs
	} else {
		ddlStartTs = checkpointTs - 1
	}

	f, err := filter.NewFilter(p.latestInfo.Config, "")
	if err != nil {
		return errors.Trace(err)
	}
	schemaStorage, err := entry.NewSchemaStorage(p.upstream.KVStorage, ddlStartTs,
		forceReplicate, p.changefeedID, util.RoleProcessor, f)
	if err != nil {
		return errors.Trace(err)
	}

	serverCfg := config.GetGlobalServerConfig()
	changefeedID := model.DefaultChangeFeedID(p.changefeedID.ID + "_processor_ddl_puller")
	ddlPuller := puller.NewDDLJobPuller(
		ctx, p.upstream, ddlStartTs, serverCfg, changefeedID, schemaStorage, p.filter,
	)
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

	if p.latestStatus == nil {
		// This could happen if Etcd data is not complete.
		return
	}

	// Please refer to `unmarshalAndMountRowChanged` in cdc/entry/mounter.go
	// for why we need -1.
	lastSchemaTs := p.ddlHandler.r.schemaStorage.DoGC(p.latestStatus.CheckpointTs - 1)
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
	if !p.initialized.Load() {
		return
	}
	p.metricSyncTableNumGauge.Set(float64(p.sinkManager.r.GetAllCurrentTableSpansCount()))
}

// Close closes the processor. It must be called explicitly to stop all sub-components.
func (p *processor) Close() error {
	log.Info("processor closing ...",
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID))
	p.initializer.Terminate()
	// clean up metrics first to avoid some metrics are not cleaned up
	// when error occurs during closing the processor
	p.cleanupMetrics()

	p.sinkManager.stop()
	p.sinkManager.r = nil
	p.sourceManager.stop()
	p.sourceManager.r = nil
	p.redo.stop()
	p.mg.stop()
	p.ddlHandler.stop()

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

	// mark tables share the same ctx with its original table, don't need to cancel
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

	ok := puller.PullerEventCounter.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID, "kv")
	if !ok {
		log.Warn("delete puller event counter metrics failed",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.String("type", "kv"))
	}
	ok = puller.PullerEventCounter.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID, "resolved")
	if !ok {
		log.Warn("delete puller event counter metrics failed",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.String("type", "resolved"))
	}
}

// WriteDebugInfo write the debug info to Writer
func (p *processor) WriteDebugInfo(w io.Writer) error {
	if !p.initialized.Load() {
		fmt.Fprintln(w, "processor is not initialized")
		return nil
	}
	fmt.Fprintf(w, "%+v\n%+v\n", *p.latestInfo, *p.latestStatus)
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
	r            R
	name         string
	ctx          context.Context
	cancel       context.CancelFunc
	errors       chan error
	warnings     chan error
	wg           sync.WaitGroup
	changefeedID model.ChangeFeedID
}

func (c *component[R]) spawn(ctx context.Context) {
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.errors = make(chan error, 16)
	c.warnings = make(chan error, 16)

	changefeedID := c.changefeedID
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

func (c *component[R]) stop() {
	if c.cancel == nil {
		log.Info("processor sub-component isn't started",
			zap.String("namespace", c.changefeedID.Namespace),
			zap.String("changefeed", c.changefeedID.ID),
			zap.String("name", c.name))
		return
	}
	log.Info("processor sub-component is in stopping",
		zap.String("namespace", c.changefeedID.Namespace),
		zap.String("changefeed", c.changefeedID.ID),
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
		}
	})
	return g.Wait()
}

func (d *ddlHandler) WaitForReady(_ context.Context) {}

func (d *ddlHandler) Close() {}
