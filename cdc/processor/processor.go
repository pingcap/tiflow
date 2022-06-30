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
	"math"
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
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/sink"
	sinkmetric "github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sorter/memory"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	backoffBaseDelayInMs = 5
	maxTries             = 3
)

type processor struct {
	changefeedID model.ChangeFeedID
	captureInfo  *model.CaptureInfo
	changefeed   *orchestrator.ChangefeedReactorState

	upstream *upstream.Upstream

	tables map[model.TableID]pipeline.TablePipeline

	schemaStorage entry.SchemaStorage
	lastSchemaTs  model.Ts

	filter        *filter.Filter
	mounter       entry.Mounter
	sink          sink.Sink
	redoManager   redo.LogManager
	lastRedoFlush time.Time

	initialized bool
	errCh       chan error
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	lazyInit            func(ctx cdcContext.Context) error
	createTablePipeline func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (pipeline.TablePipeline, error)
	newAgent            func(ctx cdcContext.Context) (scheduler.Agent, error)

	agent        scheduler.Agent
	checkpointTs model.Ts
	resolvedTs   model.Ts

	metricResolvedTsGauge           prometheus.Gauge
	metricResolvedTsLagGauge        prometheus.Gauge
	metricMinResolvedTableIDGauge   prometheus.Gauge
	metricCheckpointTsGauge         prometheus.Gauge
	metricCheckpointTsLagGauge      prometheus.Gauge
	metricMinCheckpointTableIDGauge prometheus.Gauge
	metricSyncTableNumGauge         prometheus.Gauge
	metricSchemaStorageGcTsGauge    prometheus.Gauge
	metricProcessorErrorCounter     prometheus.Counter
	metricProcessorTickDuration     prometheus.Observer
	metricsTableSinkTotalRows       prometheus.Counter

	metricsTableMemoryHistogram prometheus.Observer
	metricsProcessorMemoryGauge prometheus.Gauge
}

// checkReadyForMessages checks whether all necessary Etcd keys have been established.
func (p *processor) checkReadyForMessages() bool {
	return p.changefeed != nil && p.changefeed.Status != nil
}

// AddTable implements TableExecutor interface.
// AddTable may cause by the following scenario
// 1. `Create Table`, a new table dispatched to the processor, `isPrepare` should be false
// 2. Prepare phase for 2 phase scheduling, `isPrepare` should be true.
// 3. Replicating phase for 2 phase scheduling, `isPrepare` should be false
func (p *processor) AddTable(
	ctx context.Context, tableID model.TableID, startTs model.Ts, isPrepare bool,
) (bool, error) {
	if !p.checkReadyForMessages() {
		return false, nil
	}

	if startTs == 0 {
		log.Panic("table start ts must not be 0",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.Uint64("checkpointTs", startTs),
			zap.Bool("isPrepare", isPrepare))
	}

	table, ok := p.tables[tableID]
	if ok {
		switch table.State() {
		// table is still `preparing`, which means the table is `replicating` on other captures.
		// no matter `isPrepare` or not, just ignore it should be ok.
		case pipeline.TableStatePreparing:
			log.Warn("table is still preparing, ignore the request",
				zap.String("captureID", p.captureInfo.ID),
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Int64("tableID", tableID),
				zap.Uint64("checkpointTs", startTs),
				zap.Bool("isPrepare", isPrepare))
			return true, nil
		case pipeline.TableStatePrepared:
			// table is `prepared`, and a `isPrepare = false` request indicate that old table should
			// be stopped on original capture already, it's safe to start replicating data now.
			if !isPrepare {
				table.Start(startTs)
			}
			return true, nil
		case pipeline.TableStateReplicating:
			log.Warn("Ignore existing table",
				zap.String("captureID", p.captureInfo.ID),
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Int64("tableID", tableID),
				zap.Uint64("checkpointTs", startTs),
				zap.Bool("isPrepare", isPrepare))
			return true, nil
		case pipeline.TableStateStopped:
			log.Warn("The same table exists but is stopped. Cancel it and continue.",
				zap.String("captureID", p.captureInfo.ID),
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Int64("tableID", tableID),
				zap.Uint64("checkpointTs", startTs),
				zap.Bool("isPrepare", isPrepare))
			p.removeTable(table, tableID)
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
			zap.Int64("tableID", tableID),
			zap.Uint64("checkpointTs", startTs),
			zap.Bool("isPrepare", isPrepare))
	}

	log.Info("adding table",
		zap.String("captureID", p.captureInfo.ID),
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID),
		zap.Int64("tableID", tableID),
		zap.Uint64("checkpointTs", startTs),
		zap.Bool("isPrepare", isPrepare))

	table, err := p.createTablePipeline(
		ctx.(cdcContext.Context), tableID, &model.TableReplicaInfo{StartTs: startTs})
	if err != nil {
		return false, errors.Trace(err)
	}
	p.tables[tableID] = table
	if !isPrepare {
		table.Start(startTs)
		log.Info("start table",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.Uint64("startTs", startTs))
	}

	return true, nil
}

// RemoveTable implements TableExecutor interface.
func (p *processor) RemoveTable(ctx context.Context, tableID model.TableID) bool {
	if !p.checkReadyForMessages() {
		return false
	}

	table, ok := p.tables[tableID]
	if !ok {
		log.Warn("table which will be deleted is not found",
			zap.String("capture", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Int64("tableID", tableID))
		return true
	}

	boundaryTs := p.changefeed.Status.CheckpointTs
	if !table.AsyncStop(boundaryTs) {
		// We use a Debug log because it is conceivable for the pipeline to block for a legitimate reason,
		// and we do not want to alarm the user.
		log.Debug("AsyncStop has failed, possible due to a full pipeline",
			zap.String("capture", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Uint64("checkpointTs", table.CheckpointTs()),
			zap.Int64("tableID", tableID))
		return false
	}
	return true
}

// IsAddTableFinished implements TableExecutor interface.
func (p *processor) IsAddTableFinished(ctx context.Context, tableID model.TableID, isPrepare bool) bool {
	if !p.checkReadyForMessages() {
		return false
	}

	table, exist := p.tables[tableID]
	if !exist {
		log.Panic("table which was added is not found",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.Bool("isPrepare", isPrepare))
	}

	localResolvedTs := p.resolvedTs
	globalResolvedTs := p.changefeed.Status.ResolvedTs
	localCheckpointTs := p.agent.GetLastSentCheckpointTs()
	globalCheckpointTs := p.changefeed.Status.CheckpointTs

	done := func() bool {
		if isPrepare {
			return table.State() == pipeline.TableStatePrepared
		}

		if config.GetGlobalServerConfig().Debug.EnableTwoPhaseScheduler {
			// The table is `replicating`, it's indicating that the `add table` must be finished.
			return table.State() == pipeline.TableStateReplicating
		}

		// TODO: this should be removed, after SchedulerV3 become the first choice.
		// The following only work for SchedulerV2.
		// These two conditions are used to determine if the table's pipeline has finished
		// initializing and all invariants have been preserved.
		//
		// The processor needs to make sure all reasonable invariants about the checkpoint-ts and
		// the resolved-ts are preserved before communicating with the Owner.
		//
		// These conditions are similar to those in the legacy implementation of
		// the Owner/Processor.
		// the adding table is considered into the calculation of checkpoint ts and resolved ts
		if table.CheckpointTs() < localCheckpointTs || localCheckpointTs < globalCheckpointTs {
			return false
		}
		if table.ResolvedTs() < localResolvedTs || localResolvedTs < globalResolvedTs {
			return false
		}

		return true
	}
	if !done() {
		log.Debug("Add Table not finished",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.Uint64("tableResolvedTs", table.ResolvedTs()),
			zap.Uint64("localResolvedTs", localResolvedTs),
			zap.Uint64("globalResolvedTs", globalResolvedTs),
			zap.Uint64("tableCheckpointTs", table.CheckpointTs()),
			zap.Uint64("localCheckpointTs", localCheckpointTs),
			zap.Uint64("globalCheckpointTs", globalCheckpointTs),
			zap.Any("state", table.State()), zap.Bool("isPrepare", isPrepare))
		return false
	}

	log.Info("Add Table finished",
		zap.String("captureID", p.captureInfo.ID),
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID),
		zap.Int64("tableID", tableID),
		zap.Uint64("tableResolvedTs", table.ResolvedTs()),
		zap.Uint64("localResolvedTs", localResolvedTs),
		zap.Uint64("globalResolvedTs", globalResolvedTs),
		zap.Uint64("tableCheckpointTs", table.CheckpointTs()),
		zap.Uint64("localCheckpointTs", localCheckpointTs),
		zap.Uint64("globalCheckpointTs", globalCheckpointTs),
		zap.Any("state", table.State()), zap.Bool("isPrepare", isPrepare))
	return true
}

// IsRemoveTableFinished implements TableExecutor interface.
func (p *processor) IsRemoveTableFinished(ctx context.Context, tableID model.TableID) (model.Ts, bool) {
	if !p.checkReadyForMessages() {
		return 0, false
	}

	table, exist := p.tables[tableID]
	if !exist {
		log.Warn("table should be removing but not found",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Int64("tableID", tableID))
		return 0, true
	}
	status := table.State()
	if status != pipeline.TableStateStopped {
		log.Debug("table is still not stopped",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Uint64("checkpointTs", table.CheckpointTs()),
			zap.Int64("tableID", tableID),
			zap.Any("tableStatus", status))
		return 0, false
	}

	table.Cancel()
	table.Wait()
	delete(p.tables, tableID)

	checkpointTs := table.CheckpointTs()
	log.Info("Remove Table finished",
		zap.String("captureID", p.captureInfo.ID),
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID),
		zap.Int64("tableID", tableID),
		zap.Uint64("checkpointTs", checkpointTs))
	return checkpointTs, true
}

// GetAllCurrentTables implements TableExecutor interface.
func (p *processor) GetAllCurrentTables() []model.TableID {
	ret := make([]model.TableID, 0, len(p.tables))
	for tableID := range p.tables {
		ret = append(ret, tableID)
	}
	return ret
}

// GetCheckpoint implements TableExecutor interface.
func (p *processor) GetCheckpoint() (checkpointTs, resolvedTs model.Ts) {
	return p.checkpointTs, p.resolvedTs
}

// GetTableMeta implements TableExecutor interface
func (p *processor) GetTableMeta(tableID model.TableID) pipeline.TableMeta {
	table, ok := p.tables[tableID]
	if !ok {
		return pipeline.TableMeta{
			TableID:      tableID,
			CheckpointTs: 0,
			ResolvedTs:   0,
			State:        pipeline.TableStateAbsent,
		}
	}
	return pipeline.TableMeta{
		TableID:      tableID,
		CheckpointTs: table.CheckpointTs(),
		ResolvedTs:   table.ResolvedTs(),
		State:        table.State(),
	}
}

// newProcessor creates a new processor
func newProcessor(ctx cdcContext.Context, up *upstream.Upstream) *processor {
	changefeedID := ctx.ChangefeedVars().ID
	p := &processor{
		upstream:      up,
		tables:        make(map[model.TableID]pipeline.TablePipeline),
		errCh:         make(chan error, 1),
		changefeedID:  changefeedID,
		captureInfo:   ctx.GlobalVars().CaptureInfo,
		cancel:        func() {},
		lastRedoFlush: time.Now(),

		metricResolvedTsGauge: resolvedTsGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricResolvedTsLagGauge: resolvedTsLagGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricMinResolvedTableIDGauge: resolvedTsMinTableIDGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricCheckpointTsGauge: checkpointTsGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricCheckpointTsLagGauge: checkpointTsLagGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricMinCheckpointTableIDGauge: checkpointTsMinTableIDGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricSyncTableNumGauge: syncTableNumGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricProcessorErrorCounter: processorErrorCounter.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricSchemaStorageGcTsGauge: processorSchemaStorageGcTsGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricProcessorTickDuration: processorTickDuration.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricsTableSinkTotalRows: sinkmetric.TableSinkTotalRowsCountCounter.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricsTableMemoryHistogram: tableMemoryHistogram.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricsProcessorMemoryGauge: processorMemoryGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}
	p.createTablePipeline = p.createTablePipelineImpl
	p.lazyInit = p.lazyInitImpl
	p.newAgent = p.newAgentImpl
	return p
}

var processorIgnorableError = []*errors.Error{
	cerror.ErrAdminStopProcessor,
	cerror.ErrReactorFinished,
	cerror.ErrRedoWriterStopped,
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
// The main logic of processor is in this function, including the calculation of many kinds of ts, maintain table pipeline, error handling, etc.
func (p *processor) Tick(ctx cdcContext.Context, state *orchestrator.ChangefeedReactorState) (orchestrator.ReactorState, error) {
	// skip this tick
	if !p.upstream.IsNormal() {
		return state, nil
	}
	startTime := time.Now()
	p.changefeed = state
	state.CheckCaptureAlive(ctx.GlobalVars().CaptureInfo.ID)
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID:   state.ID,
		Info: state.Info,
	})
	err := p.tick(ctx, state)

	costTime := time.Since(startTime)
	if costTime > processorLogsWarnDuration {
		log.Warn("processor tick took too long", zap.String("changefeed", p.changefeedID.ID),
			zap.String("capture", ctx.GlobalVars().CaptureInfo.ID), zap.Duration("duration", costTime))
	}

	p.metricProcessorTickDuration.Observe(costTime.Seconds())
	p.refreshMetrics()

	if err == nil {
		return state, nil
	}
	if isProcessorIgnorableError(err) {
		log.Info("processor exited", cdcContext.ZapFieldCapture(ctx), cdcContext.ZapFieldChangefeed(ctx))
		return state, cerror.ErrReactorFinished.GenWithStackByArgs()
	}
	p.metricProcessorErrorCounter.Inc()
	// record error information in etcd
	var code string
	if rfcCode, ok := cerror.RFCCode(err); ok {
		code = string(rfcCode)
	} else {
		code = string(cerror.ErrProcessorUnknown.RFCCode())
	}
	state.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		if position == nil {
			position = &model.TaskPosition{}
		}
		position.Error = &model.RunningError{
			Addr:    ctx.GlobalVars().CaptureInfo.AdvertiseAddr,
			Code:    code,
			Message: err.Error(),
		}
		return position, true, nil
	})
	log.Error("run processor failed",
		cdcContext.ZapFieldChangefeed(ctx),
		cdcContext.ZapFieldCapture(ctx),
		zap.Error(err))
	return state, cerror.ErrReactorFinished.GenWithStackByArgs()
}

func (p *processor) tick(ctx cdcContext.Context, state *orchestrator.ChangefeedReactorState) error {
	p.changefeed = state
	if !p.checkChangefeedNormal() {
		return cerror.ErrAdminStopProcessor.GenWithStackByArgs()
	}
	// we should skip this tick after create a task position
	if p.createTaskPosition() {
		return nil
	}
	if err := p.handleErrorCh(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := p.lazyInit(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := p.flushRedoLogMeta(ctx); err != nil {
		return err
	}
	// it is no need to check the error here, because we will use
	// local time when an error return, which is acceptable
	pdTime, _ := p.upstream.PDClock.CurrentTime()

	p.handlePosition(oracle.GetPhysical(pdTime))
	p.pushResolvedTs2Table()

	p.doGCSchemaStorage(ctx)

	if err := p.agent.Tick(ctx); err != nil {
		return errors.Trace(err)
	}
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
	p.changefeed.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		if position == nil {
			return &model.TaskPosition{}, true, nil
		}
		return position, false, nil
	})
	return true
}

// lazyInitImpl create Filter, SchemaStorage, Mounter instances at the first tick.
func (p *processor) lazyInitImpl(ctx cdcContext.Context) error {
	if p.initialized {
		return nil
	}
	ctx, cancel := cdcContext.WithCancel(ctx)
	p.cancel = cancel
	// We don't close this error channel, since it is only safe to close channel
	// in sender, and this channel will be used in many modules including sink,
	// redo log manager, etc. Let runtime GC to recycle it.
	errCh := make(chan error, 16)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		// there are some other objects need errCh, such as sink and sink manager
		// but we can't ensure that all the producer of errCh are non-blocking
		// It's very tricky that create a goroutine to receive the local errCh
		// TODO(leoppro): we should using `pkg/cdcContext.Context` instead of standard cdcContext and handle error by `pkg/cdcContext.Context.Throw`
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errCh:
				if err == nil {
					return
				}
				p.sendError(err)
			}
		}
	}()

	var err error
	p.filter, err = filter.NewFilter(p.changefeed.Info.Config)
	if err != nil {
		return errors.Trace(err)
	}

	p.schemaStorage, err = p.createAndDriveSchemaStorage(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	stdCtx := contextutil.PutChangefeedIDInCtx(ctx, p.changefeed.ID)
	stdCtx = contextutil.PutRoleInCtx(stdCtx, util.RoleProcessor)

	p.mounter = entry.NewMounter(p.schemaStorage,
		p.changefeedID,
		contextutil.TimezoneFromCtx(ctx),
		p.filter,
		p.changefeed.Info.Config.EnableOldValue,
	)

	log.Info("processor try new sink",
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeed.ID.ID))

	start := time.Now()
	p.sink, err = sink.New(
		stdCtx,
		p.changefeed.ID,
		p.changefeed.Info.SinkURI,
		p.changefeed.Info.Config,
		errCh,
	)
	if err != nil {
		log.Info("processor new sink failed",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeed.ID.ID),
			zap.Duration("duration", time.Since(start)))
		return errors.Trace(err)
	}
	log.Info("processor try new sink success",
		zap.Duration("duration", time.Since(start)))

	redoManagerOpts := &redo.ManagerOptions{EnableBgRunner: true, ErrCh: errCh}
	p.redoManager, err = redo.NewManager(stdCtx, p.changefeed.Info.Config.Consistent, redoManagerOpts)
	if err != nil {
		return err
	}

	p.agent, err = p.newAgent(ctx)
	if err != nil {
		return err
	}

	p.initialized = true
	log.Info("run processor", cdcContext.ZapFieldCapture(ctx), cdcContext.ZapFieldChangefeed(ctx))
	return nil
}

func (p *processor) newAgentImpl(ctx cdcContext.Context) (ret scheduler.Agent, err error) {
	messageServer := ctx.GlobalVars().MessageServer
	messageRouter := ctx.GlobalVars().MessageRouter
	etcdClient := ctx.GlobalVars().EtcdClient
	captureID := ctx.GlobalVars().CaptureInfo.ID
	cfg := config.GetGlobalServerConfig().Debug
	if cfg.EnableTwoPhaseScheduler {
		ret, err = scheduler.NewTpAgent(
			ctx, captureID, messageServer, messageRouter, etcdClient, p, p.changefeedID)
	} else {
		ret, err = scheduler.NewAgent(
			ctx, captureID, messageServer, messageRouter, etcdClient, p, p.changefeedID)
	}
	return ret, errors.Trace(err)
}

// handleErrorCh listen the error channel and throw the error if it is not expected.
func (p *processor) handleErrorCh(ctx cdcContext.Context) error {
	var err error
	select {
	case err = <-p.errCh:
	default:
		return nil
	}
	if !isProcessorIgnorableError(err) {
		log.Error("error on running processor",
			cdcContext.ZapFieldCapture(ctx),
			cdcContext.ZapFieldChangefeed(ctx),
			zap.Error(err))
		return err
	}
	log.Info("processor exited", cdcContext.ZapFieldCapture(ctx), cdcContext.ZapFieldChangefeed(ctx))
	return cerror.ErrReactorFinished
}

func (p *processor) createAndDriveSchemaStorage(ctx cdcContext.Context) (entry.SchemaStorage, error) {
	kvStorage := p.upstream.KVStorage
	ddlspans := []regionspan.Span{regionspan.GetDDLSpan(), regionspan.GetAddIndexDDLSpan()}
	checkpointTs := p.changefeed.Info.GetCheckpointTs(p.changefeed.Status)
	kvCfg := config.GetGlobalServerConfig().KVClient
	stdCtx := contextutil.PutTableInfoInCtx(ctx, -1, puller.DDLPullerTableName)
	stdCtx = contextutil.PutChangefeedIDInCtx(stdCtx, ctx.ChangefeedVars().ID)
	stdCtx = contextutil.PutRoleInCtx(stdCtx, util.RoleProcessor)
	ddlPuller := puller.NewPuller(
		stdCtx,
		p.upstream.PDClient,
		p.upstream.GrpcPool,
		p.upstream.RegionCache,
		p.upstream.KVStorage,
		p.upstream.PDClock,
		ctx.ChangefeedVars().ID,
		checkpointTs,
		ddlspans,
		kvCfg,
	)
	meta, err := kv.GetSnapshotMeta(kvStorage, checkpointTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaStorage, err := entry.NewSchemaStorage(meta, checkpointTs, p.filter,
		p.changefeed.Info.Config.ForceReplicate, ctx.ChangefeedVars().ID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.sendError(ddlPuller.Run(stdCtx))
	}()
	ddlRawKVCh := memory.SortOutput(ctx, ddlPuller.Output())
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		var ddlRawKV *model.RawKVEntry
		for {
			select {
			case <-ctx.Done():
				return
			case ddlRawKV = <-ddlRawKVCh:
			}
			if ddlRawKV == nil {
				continue
			}
			failpoint.Inject("processorDDLResolved", nil)
			if ddlRawKV.OpType == model.OpTypeResolved {
				schemaStorage.AdvanceResolvedTs(ddlRawKV.CRTs)
			}
			job, err := entry.UnmarshalDDL(ddlRawKV)
			if err != nil {
				p.sendError(errors.Trace(err))
				return
			}
			if job == nil {
				continue
			}
			if err := schemaStorage.HandleDDLJob(job); err != nil {
				p.sendError(errors.Trace(err))
				return
			}
		}
	}()
	return schemaStorage, nil
}

func (p *processor) sendError(err error) {
	if err == nil {
		return
	}
	select {
	case p.errCh <- err:
	default:
		if !isProcessorIgnorableError(err) {
			log.Error("processor receives redundant error", zap.Error(err))
		}
	}
}

// handlePosition calculates the local resolved ts and local checkpoint ts
func (p *processor) handlePosition(currentTs int64) {
	minResolvedTs := uint64(math.MaxUint64)
	minResolvedTableID := int64(0)
	if p.schemaStorage != nil {
		minResolvedTs = p.schemaStorage.ResolvedTs()
	}
	for _, table := range p.tables {
		ts := table.ResolvedTs()
		if ts < minResolvedTs {
			minResolvedTs = ts
			minResolvedTableID, _ = table.ID()
		}
	}

	minCheckpointTs := minResolvedTs
	minCheckpointTableID := int64(0)
	for _, table := range p.tables {
		ts := table.CheckpointTs()
		if ts < minCheckpointTs {
			minCheckpointTs = ts
			minCheckpointTableID, _ = table.ID()
		}
	}

	resolvedPhyTs := oracle.ExtractPhysical(minResolvedTs)
	p.metricResolvedTsLagGauge.Set(float64(currentTs-resolvedPhyTs) / 1e3)
	p.metricResolvedTsGauge.Set(float64(resolvedPhyTs))
	p.metricMinResolvedTableIDGauge.Set(float64(minResolvedTableID))

	checkpointPhyTs := oracle.ExtractPhysical(minCheckpointTs)
	p.metricCheckpointTsLagGauge.Set(float64(currentTs-checkpointPhyTs) / 1e3)
	p.metricCheckpointTsGauge.Set(float64(checkpointPhyTs))
	p.metricMinCheckpointTableIDGauge.Set(float64(minCheckpointTableID))

	p.checkpointTs = minCheckpointTs
	p.resolvedTs = minResolvedTs
}

// pushResolvedTs2Table sends global resolved ts to all the table pipelines.
func (p *processor) pushResolvedTs2Table() {
	resolvedTs := p.changefeed.Status.ResolvedTs
	schemaResolvedTs := p.schemaStorage.ResolvedTs()
	if schemaResolvedTs < resolvedTs {
		// Do not update barrier ts that is larger than
		// DDL puller's resolved ts.
		// When DDL puller stall, resolved events that outputted by sorter
		// may pile up in memory, as they have to wait DDL.
		resolvedTs = schemaResolvedTs
	}
	for _, table := range p.tables {
		table.UpdateBarrierTs(resolvedTs)
	}
}

func (p *processor) getTableName(ctx cdcContext.Context, tableID model.TableID) string {
	// FIXME: using GetLastSnapshot here would be confused and get the wrong table name
	// after `rename table` DDL, since `rename table` keeps the tableID unchanged
	var tableName *model.TableName
	retry.Do(ctx, func() error { //nolint:errcheck
		if x, ok := p.schemaStorage.GetLastSnapshot().PhysicalTableByID(tableID); ok {
			tableName = &x.TableName
			return nil
		}
		return errors.Errorf("failed to get table name, fallback to use table id: %d",
			tableID)
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs),
		retry.WithMaxTries(maxTries),
		retry.WithIsRetryableErr(cerror.IsRetryableError))

	if tableName == nil {
		log.Warn("failed to get table name for metric")
		return strconv.Itoa(int(tableID))
	}

	return tableName.QuoteString()
}

func (p *processor) createTablePipelineImpl(
	ctx cdcContext.Context,
	tableID model.TableID,
	replicaInfo *model.TableReplicaInfo,
) (pipeline.TablePipeline, error) {
	ctx = cdcContext.WithErrorHandler(ctx, func(err error) error {
		if cerror.ErrTableProcessorStoppedSafely.Equal(err) ||
			errors.Cause(errors.Cause(err)) == context.Canceled {
			return nil
		}
		p.sendError(err)
		return nil
	})

	tableName := p.getTableName(ctx, tableID)

	s, err := sink.NewTableSink(p.sink, tableID, p.metricsTableSinkTotalRows)
	if err != nil {
		return nil, errors.Trace(err)
	}
	table, err := pipeline.NewTableActor(
		ctx,
		p.upstream,
		p.mounter,
		tableID,
		tableName,
		replicaInfo,
		s,
		p.redoManager,
		p.changefeed.Info.GetTargetTs())
	if err != nil {
		return nil, errors.Trace(err)
	}

	if p.redoManager.Enabled() {
		p.redoManager.AddTable(tableID, replicaInfo.StartTs)
	}

	log.Info("Add table pipeline", zap.Int64("tableID", tableID),
		cdcContext.ZapFieldChangefeed(ctx),
		zap.String("name", table.Name()),
		zap.Any("replicaInfo", replicaInfo),
		zap.Uint64("globalResolvedTs", p.changefeed.Status.ResolvedTs))

	return table, nil
}

func (p *processor) removeTable(table pipeline.TablePipeline, tableID model.TableID) {
	table.Cancel()
	table.Wait()
	delete(p.tables, tableID)
	if p.redoManager.Enabled() {
		p.redoManager.RemoveTable(tableID)
	}
}

// doGCSchemaStorage trigger the schema storage GC
func (p *processor) doGCSchemaStorage(ctx cdcContext.Context) {
	if p.schemaStorage == nil {
		// schemaStorage is nil only in test
		return
	}

	if p.changefeed.Status == nil {
		// This could happen if Etcd data is not complete.
		return
	}

	// Please refer to `unmarshalAndMountRowChanged` in cdc/entry/mounter.go
	// for why we need -1.
	lastSchemaTs := p.schemaStorage.DoGC(p.changefeed.Status.CheckpointTs - 1)
	if p.lastSchemaTs == lastSchemaTs {
		return
	}
	p.lastSchemaTs = lastSchemaTs

	log.Debug("finished gc in schema storage",
		zap.Uint64("gcTs", lastSchemaTs),
		cdcContext.ZapFieldChangefeed(ctx))
	lastSchemaPhysicalTs := oracle.ExtractPhysical(lastSchemaTs)
	p.metricSchemaStorageGcTsGauge.Set(float64(lastSchemaPhysicalTs))
}

// flushRedoLogMeta flushes redo log meta, including resolved-ts and checkpoint-ts
func (p *processor) flushRedoLogMeta(ctx context.Context) error {
	if p.redoManager.Enabled() &&
		time.Since(p.lastRedoFlush).Milliseconds() > p.changefeed.Info.Config.Consistent.FlushIntervalInMs {
		st := p.changefeed.Status
		err := p.redoManager.FlushResolvedAndCheckpointTs(ctx, st.ResolvedTs, st.CheckpointTs)
		if err != nil {
			return err
		}
		p.lastRedoFlush = time.Now()
	}
	return nil
}

func (p *processor) refreshMetrics() {
	var total uint64
	for _, table := range p.tables {
		consumed := table.MemoryConsumption()
		p.metricsTableMemoryHistogram.Observe(float64(consumed))
		total += consumed
	}
	p.metricsProcessorMemoryGauge.Set(float64(total))
	p.metricSyncTableNumGauge.Set(float64(len(p.tables)))
}

func (p *processor) Close() error {
	log.Info("processor closing ...",
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeed.ID.ID))
	for _, tbl := range p.tables {
		tbl.Cancel()
	}
	for _, tbl := range p.tables {
		tbl.Wait()
	}
	p.cancel()
	p.wg.Wait()
	p.upstream.Release()

	if p.agent == nil {
		return nil
	}
	if err := p.agent.Close(); err != nil {
		return errors.Trace(err)
	}
	p.agent = nil

	// sink close might be time-consuming, do it the last.
	if p.sink != nil {
		// pass a canceled context is ok here, since we don't need to wait Close
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		log.Info("processor try to close the sink",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID))
		start := time.Now()
		if err := p.sink.Close(ctx); err != nil && errors.Cause(err) != context.Canceled {
			log.Info("processor close sink failed",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Duration("duration", time.Since(start)))
			return errors.Trace(err)
		}
		log.Info("processor close sink success",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Duration("duration", time.Since(start)))
	}
	// mark tables share the same cdcContext with its original table, don't need to cancel
	failpoint.Inject("processorStopDelay", nil)
	resolvedTsGauge.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	resolvedTsLagGauge.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	checkpointTsGauge.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	checkpointTsLagGauge.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	syncTableNumGauge.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	processorErrorCounter.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	processorSchemaStorageGcTsGauge.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	sinkmetric.TableSinkTotalRowsCountCounter.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	tableMemoryHistogram.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	processorMemoryGauge.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	return nil
}

// WriteDebugInfo write the debug info to Writer
func (p *processor) WriteDebugInfo(w io.Writer) {
	fmt.Fprintf(w, "%+v\n", *p.changefeed)
	for tableID, tablePipeline := range p.tables {
		fmt.Fprintf(w, "tableID: %d, tableName: %s, resolvedTs: %d, checkpointTs: %d, state: %s\n",
			tableID, tablePipeline.Name(), tablePipeline.ResolvedTs(), tablePipeline.CheckpointTs(), tablePipeline.State())
	}
}
