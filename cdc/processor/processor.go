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
	"github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/processor/sinkmanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	sinkv1 "github.com/pingcap/tiflow/cdc/sink"
	sinkmetric "github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/factory"
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
)

const (
	backoffBaseDelayInMs = 5
	maxTries             = 3
)

type processor struct {
	changefeedID model.ChangeFeedID
	captureInfo  *model.CaptureInfo
	changefeed   *orchestrator.ChangefeedReactorState

	upstream      *upstream.Upstream
	schemaStorage entry.SchemaStorage
	lastSchemaTs  model.Ts

	filter filter.Filter
	mg     entry.MounterGroup

	pullBasedSinking bool

	// These fields are used to sinking data in non-pull-based mode.
	tables        map[model.TableID]tablepb.TablePipeline
	sinkV1        sinkv1.Sink
	sinkV2Factory *factory.SinkFactory

	// These fields are used to sinking data in pull-based mode.
	sourceManager *sourcemanager.SourceManager
	sinkManager   *sinkmanager.SinkManager

	redoDMLMgr redo.DMLManager

	initialized bool
	errCh       chan error
	warnCh      chan error
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	lazyInit            func(ctx cdcContext.Context) error
	createTablePipeline func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepb.TablePipeline, error)
	newAgent            func(cdcContext.Context, *model.Liveness, uint64) (scheduler.Agent, error)

	liveness        *model.Liveness
	agent           scheduler.Agent
	changefeedEpoch uint64

	checkpointTs model.Ts
	resolvedTs   model.Ts

	metricSyncTableNumGauge      prometheus.Gauge
	metricSchemaStorageGcTsGauge prometheus.Gauge
	metricProcessorErrorCounter  prometheus.Counter
	metricProcessorTickDuration  prometheus.Observer
	metricsTableSinkTotalRows    prometheus.Counter
	metricsTableMemoryHistogram  prometheus.Observer
	metricsProcessorMemoryGauge  prometheus.Gauge
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
	ctx context.Context, tableID model.TableID, checkpoint tablepb.Checkpoint, isPrepare bool,
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
			zap.Int64("tableID", tableID),
			zap.Uint64("checkpointTs", startTs),
			zap.Bool("isPrepare", isPrepare))
	}

	var alreadyExist bool
	var state tablepb.TableState
	if p.pullBasedSinking {
		state, alreadyExist = p.sinkManager.GetTableState(tableID)
	} else {
		table, ok := p.tables[tableID]
		if ok {
			alreadyExist = true
			state = table.State()
		}
	}

	if alreadyExist {
		switch state {
		// table is still `preparing`, which means the table is `replicating` on other captures.
		// no matter `isPrepare` or not, just ignore it should be ok.
		case tablepb.TableStatePreparing:
			log.Warn("table is still preparing, ignore the request",
				zap.String("captureID", p.captureInfo.ID),
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Int64("tableID", tableID),
				zap.Uint64("checkpointTs", startTs),
				zap.Bool("isPrepare", isPrepare))
			return true, nil
		case tablepb.TableStatePrepared:
			// table is `prepared`, and a `isPrepare = false` request indicate that old table should
			// be stopped on original capture already, it's safe to start replicating data now.
			if !isPrepare {
				if p.pullBasedSinking {
					if p.redoDMLMgr.Enabled() {
						// ResolvedTs is store in external storage when redo log is enabled, so we need to
						// start table with ResolvedTs in redoDMLManager.
						p.redoDMLMgr.StartTable(tableID, checkpoint.ResolvedTs)
					}
					if err := p.sinkManager.StartTable(tableID, startTs); err != nil {
						return false, errors.Trace(err)
					}
				} else {
					p.tables[tableID].Start(startTs)
				}
			}
			return true, nil
		case tablepb.TableStateReplicating:
			log.Warn("Ignore existing table",
				zap.String("captureID", p.captureInfo.ID),
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Int64("tableID", tableID),
				zap.Uint64("checkpointTs", startTs),
				zap.Bool("isPrepare", isPrepare))
			return true, nil
		case tablepb.TableStateStopped:
			log.Warn("The same table exists but is stopped. Cancel it and continue.",
				zap.String("captureID", p.captureInfo.ID),
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Int64("tableID", tableID),
				zap.Uint64("checkpointTs", startTs),
				zap.Bool("isPrepare", isPrepare))
			p.removeTable(p.tables[tableID], tableID)
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

	if p.pullBasedSinking {
		p.sinkManager.AddTable(tableID, startTs, p.changefeed.Info.TargetTs)
		if p.redoDMLMgr.Enabled() {
			p.redoDMLMgr.AddTable(tableID, startTs)
		}
		p.sourceManager.AddTable(ctx.(cdcContext.Context), tableID, p.getTableName(ctx, tableID), startTs)
	} else {
		table, err := p.createTablePipeline(
			ctx.(cdcContext.Context), tableID, &model.TableReplicaInfo{StartTs: startTs})
		if err != nil {
			return false, errors.Trace(err)
		}
		p.tables[tableID] = table
	}

	return true, nil
}

// RemoveTable implements TableExecutor interface.
func (p *processor) RemoveTable(tableID model.TableID) bool {
	if !p.checkReadyForMessages() {
		return false
	}

	if p.pullBasedSinking {
		_, exist := p.sinkManager.GetTableState(tableID)
		if !exist {
			log.Warn("Table which will be deleted is not found",
				zap.String("capture", p.captureInfo.ID),
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Int64("tableID", tableID))
			return true
		}
		return p.sinkManager.AsyncStopTable(tableID)
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
	return table.AsyncStop()
}

// IsAddTableFinished implements TableExecutor interface.
func (p *processor) IsAddTableFinished(tableID model.TableID, isPrepare bool) bool {
	if !p.checkReadyForMessages() {
		return false
	}

	globalCheckpointTs := p.changefeed.Status.CheckpointTs

	var tableResolvedTs, tableCheckpointTs uint64
	var state tablepb.TableState
	done := func() bool {
		var alreadyExist bool
		if p.pullBasedSinking {
			state, alreadyExist = p.sinkManager.GetTableState(tableID)
			if alreadyExist {
				stats := p.sinkManager.GetTableStats(tableID)
				tableResolvedTs = stats.ResolvedTs
				tableCheckpointTs = stats.CheckpointTs
			}
		} else {
			table, ok := p.tables[tableID]
			if ok {
				alreadyExist = true
				state = table.State()
				tableResolvedTs = table.ResolvedTs()
				tableCheckpointTs = table.CheckpointTs()
			}
		}

		if !alreadyExist {
			log.Panic("table which was added is not found",
				zap.String("captureID", p.captureInfo.ID),
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Int64("tableID", tableID),
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
			zap.Int64("tableID", tableID),
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
		zap.Int64("tableID", tableID),
		zap.Uint64("tableResolvedTs", tableResolvedTs),
		zap.Uint64("tableCheckpointTs", tableCheckpointTs),
		zap.Uint64("globalCheckpointTs", globalCheckpointTs),
		zap.Any("state", state),
		zap.Bool("isPrepare", isPrepare))
	return true
}

// IsRemoveTableFinished implements TableExecutor interface.
func (p *processor) IsRemoveTableFinished(tableID model.TableID) (model.Ts, bool) {
	if !p.checkReadyForMessages() {
		return 0, false
	}

	var alreadyExist bool
	var state tablepb.TableState
	var tableCheckpointTs uint64
	if p.pullBasedSinking {
		state, alreadyExist = p.sinkManager.GetTableState(tableID)
		if alreadyExist {
			stats := p.sinkManager.GetTableStats(tableID)
			tableCheckpointTs = stats.CheckpointTs
		}
	} else {
		table, ok := p.tables[tableID]
		if ok {
			alreadyExist = true
			state = table.State()
			tableCheckpointTs = table.CheckpointTs()
		}
	}

	if !alreadyExist {
		log.Warn("table has been stopped",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Int64("tableID", tableID))
		return 0, true
	}

	if state != tablepb.TableStateStopped {
		log.Debug("table is still not stopped",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Uint64("checkpointTs", tableCheckpointTs),
			zap.Int64("tableID", tableID),
			zap.Any("tableStatus", state))
		return 0, false
	}

	if p.pullBasedSinking {
		stats := p.sinkManager.GetTableStats(tableID)
		if p.redoDMLMgr.Enabled() {
			p.redoDMLMgr.RemoveTable(tableID)
		}
		p.sinkManager.RemoveTable(tableID)
		p.sourceManager.RemoveTable(tableID)
		log.Info("table removed",
			zap.String("captureID", p.captureInfo.ID),
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.Uint64("checkpointTs", stats.CheckpointTs))

		return stats.CheckpointTs, true
	}
	table := p.tables[tableID]
	table.Cancel()
	table.Wait()
	delete(p.tables, tableID)

	checkpointTs := table.CheckpointTs()
	log.Info("table removed",
		zap.String("captureID", p.captureInfo.ID),
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID),
		zap.Int64("tableID", tableID),
		zap.Uint64("checkpointTs", checkpointTs))
	return checkpointTs, true
}

// GetCheckpoint implements TableExecutor interface.
func (p *processor) GetCheckpoint() (checkpointTs, resolvedTs model.Ts) {
	return p.checkpointTs, p.resolvedTs
}

// GetTableStatus implements TableExecutor interface
func (p *processor) GetTableStatus(tableID model.TableID, collectStat bool) tablepb.TableStatus {
	if p.pullBasedSinking {
		state, exist := p.sinkManager.GetTableState(tableID)
		if !exist {
			return tablepb.TableStatus{
				TableID: tableID,
				State:   tablepb.TableStateAbsent,
			}
		}
		sinkStats := p.sinkManager.GetTableStats(tableID)
		stats := tablepb.Stats{}
		if collectStat {
			stats = p.getStatsFromSourceManagerAndSinkManager(tableID, sinkStats)
		}
		return tablepb.TableStatus{
			TableID: tableID,
			Checkpoint: tablepb.Checkpoint{
				CheckpointTs: sinkStats.CheckpointTs,
				ResolvedTs:   sinkStats.ResolvedTs,
			},
			State: state,
			Stats: stats,
		}
	}
	table, ok := p.tables[tableID]
	if !ok {
		return tablepb.TableStatus{
			TableID: tableID,
			State:   tablepb.TableStateAbsent,
		}
	}
	stats := tablepb.Stats{}
	if collectStat {
		stats = table.Stats()
	}
	return tablepb.TableStatus{
		TableID: tableID,
		Checkpoint: tablepb.Checkpoint{
			CheckpointTs: table.CheckpointTs(),
			ResolvedTs:   table.ResolvedTs(),
		},
		State: table.State(),
		Stats: stats,
	}
}

func (p *processor) getStatsFromSourceManagerAndSinkManager(tableID model.TableID, sinkStats sinkmanager.TableStats) tablepb.Stats {
	pullerStats := p.sourceManager.GetTablePullerStats(tableID)
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

	sortStats := p.sourceManager.GetTableSorterStats(tableID)
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
) *processor {
	p := &processor{
		changefeed:      state,
		upstream:        up,
		tables:          make(map[model.TableID]tablepb.TablePipeline),
		errCh:           make(chan error, 1),
		warnCh:          make(chan error, 1),
		changefeedID:    changefeedID,
		captureInfo:     captureInfo,
		cancel:          func() {},
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

func (p *processor) handleWarn(err error) {
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

	select {
	case err := <-p.warnCh:
		p.handleWarn(err)
	default:
	}

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

	tz := contextutil.TimezoneFromCtx(ctx)
	var err error
	p.filter, err = filter.NewFilter(p.changefeed.Info.Config,
		util.GetTimeZoneName(tz))
	if err != nil {
		return errors.Trace(err)
	}

	p.schemaStorage, err = p.createAndDriveSchemaStorage(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	stdCtx := contextutil.PutChangefeedIDInCtx(ctx, p.changefeedID)
	stdCtx = contextutil.PutRoleInCtx(stdCtx, util.RoleProcessor)

	p.mg = entry.NewMounterGroup(p.schemaStorage,
		p.changefeed.Info.Config.Mounter.WorkerNum,
		p.filter, tz, p.changefeedID)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.sendError(p.mg.Run(ctx))
	}()

	sourceID, err := pdutil.GetSourceID(ctx, p.upstream.PDClient)
	if err != nil {
		return errors.Trace(err)
	}
	p.changefeed.Info.Config.Sink.TiDBSourceID = sourceID

	start := time.Now()
	conf := config.GetGlobalServerConfig()
	p.pullBasedSinking = conf.Debug.EnablePullBasedSink

	p.redoDMLMgr = redo.NewDMLManager(p.changefeedID, p.changefeed.Info.Config.Consistent)
	if p.redoDMLMgr.Enabled() {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.sendError(p.redoDMLMgr.Run(stdCtx))
		}()
	}
	log.Info("processor creates redo manager",
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID))

	if p.pullBasedSinking {
		engineFactory := ctx.GlobalVars().SortEngineFactory
		sortEngine, err := engineFactory.Create(p.changefeedID)
		if err != nil {
			log.Info("Processor creates sort engine",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Error(err),
				zap.Duration("duration", time.Since(start)))
			return errors.Trace(err)
		}
		p.sourceManager = sourcemanager.New(p.changefeedID, p.upstream, p.mg,
			sortEngine, p.errCh, p.changefeed.Info.Config.BDRMode)
		p.sinkManager, err = sinkmanager.New(stdCtx, p.changefeedID,
			p.changefeed.Info, p.upstream, p.schemaStorage,
			p.redoDMLMgr, p.sourceManager,
			p.errCh, p.warnCh, p.metricsTableSinkTotalRows)
		if err != nil {
			log.Info("Processor creates sink manager fail",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Error(err),
				zap.Duration("duration", time.Since(start)))
			p.sourceManager = nil
			_ = engineFactory.Drop(p.changefeedID)
			return errors.Trace(err)
		}
		// Bind them so that sourceManager can notify sinkManager.
		p.sourceManager.OnResolve(p.sinkManager.UpdateReceivedSorterResolvedTs)
	} else {
		if !conf.Debug.EnableNewSink {
			log.Info("Try to create sinkV1")
			s, err := sinkv1.New(
				stdCtx,
				p.changefeedID,
				p.changefeed.Info.SinkURI,
				p.changefeed.Info.Config,
				errCh,
			)
			if err != nil {
				log.Error("processor creates sink failed",
					zap.String("namespace", p.changefeedID.Namespace),
					zap.String("changefeed", p.changefeedID.ID),
					zap.Error(err),
					zap.Duration("duration", time.Since(start)))
				return errors.Trace(err)
			}
			// Make sure `s` is not nil before assigning it to the `sinkV1`, which is an interface.
			// See: https://go.dev/play/p/sDlHncxO3Nz
			if s != nil {
				p.sinkV1 = s
			}
		} else {
			log.Info("Try to create sinkV2")
			sinkV2Factory, err := factory.New(stdCtx, p.changefeed.Info.SinkURI,
				p.changefeed.Info.Config,
				errCh)
			if err != nil {
				log.Error("processor creates sink failed",
					zap.String("namespace", p.changefeedID.Namespace),
					zap.String("changefeed", p.changefeedID.ID),
					zap.Error(err),
					zap.Duration("duration", time.Since(start)))
				return errors.Trace(err)
			}
			p.sinkV2Factory = sinkV2Factory
		}
		log.Info("processor creates sink",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeed.ID.ID),
			zap.Duration("duration", time.Since(start)))
	}

	p.agent, err = p.newAgent(ctx, p.liveness, p.changefeedEpoch)
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
	ctx cdcContext.Context, liveness *model.Liveness, changefeedEpoch uint64,
) (ret scheduler.Agent, err error) {
	messageServer := ctx.GlobalVars().MessageServer
	messageRouter := ctx.GlobalVars().MessageRouter
	etcdClient := ctx.GlobalVars().EtcdClient
	captureID := ctx.GlobalVars().CaptureInfo.ID
	ret, err = scheduler.NewAgent(
		ctx, captureID, liveness,
		messageServer, messageRouter, etcdClient, p, p.changefeedID, changefeedEpoch)
	return ret, errors.Trace(err)
}

// handleErrorCh listen the error channel and throw the error if it is not expected.
func (p *processor) handleErrorCh() error {
	var err error
	select {
	case err = <-p.errCh:
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

func (p *processor) createAndDriveSchemaStorage(ctx cdcContext.Context) (entry.SchemaStorage, error) {
	kvStorage := p.upstream.KVStorage
	checkpointTs := p.changefeed.Info.GetCheckpointTs(p.changefeed.Status)
	minTableBarrierTs := p.changefeed.Status.MinTableBarrierTs
	// if minTableBarrierTs == checkpointTs it means owner can't tell whether the DDL on checkpointTs has
	// been executed or not. So the DDL puller must start at checkpointTs-1.
	var ddlStartTs uint64
	if minTableBarrierTs > checkpointTs {
		ddlStartTs = checkpointTs
	} else {
		ddlStartTs = checkpointTs - 1
	}

	serverCfg := config.GetGlobalServerConfig()
	stdCtx := contextutil.PutTableInfoInCtx(ctx, -1, puller.DDLPullerTableName)
	stdCtx = contextutil.PutChangefeedIDInCtx(stdCtx, p.changefeedID)
	stdCtx = contextutil.PutRoleInCtx(stdCtx, util.RoleProcessor)
	meta, err := kv.GetSnapshotMeta(kvStorage, ddlStartTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f, err := filter.NewFilter(p.changefeed.Info.Config, "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaStorage, err := entry.NewSchemaStorage(meta,
		ddlStartTs,
		p.changefeed.Info.Config.ForceReplicate,
		p.changefeedID, util.RoleProcessor, f)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ddlPuller, err := puller.NewDDLJobPuller(
		stdCtx,
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
		return nil, errors.Trace(err)
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		// ddlPuller will update the schemaStorage.
		p.sendError(ddlPuller.Run(stdCtx))
	}()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		var jobEntry *model.DDLJobEntry
		for {
			select {
			case <-ctx.Done():
				return
			case jobEntry = <-ddlPuller.Output():
			}
			failpoint.Inject("processorDDLResolved", nil)
			if jobEntry.OpType == model.OpTypeResolved {
				schemaStorage.AdvanceResolvedTs(jobEntry.CRTs)
			}
			err := jobEntry.Err
			if err != nil {
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

// updateBarrierTs updates barrierTs for all tables.
func (p *processor) updateBarrierTs(barrier *schedulepb.Barrier) {
	tableBarrier := p.calculateTableBarrierTs(barrier)
	globalBarrierTs := barrier.GetGlobalBarrierTs()
	schemaResolvedTs := p.schemaStorage.ResolvedTs()
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
	if p.pullBasedSinking {
		p.sinkManager.UpdateBarrierTs(globalBarrierTs, tableBarrier)
	} else {
		for id, table := range p.tables {
			if _, ok := tableBarrier[id]; ok {
				table.UpdateBarrierTs(tableBarrier[id])
				continue
			}
			table.UpdateBarrierTs(globalBarrierTs)
		}
	}
}

func (p *processor) getTableName(ctx context.Context, tableID model.TableID) string {
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
		log.Warn("failed to get table name for metric",
			zap.String("namespace", p.changefeedID.Namespace),
			zap.String("changefeed", p.changefeedID.ID),
			zap.Any("tableID", tableID))
		return strconv.Itoa(int(tableID))
	}

	return tableName.QuoteString()
}

func (p *processor) createTablePipelineImpl(
	ctx cdcContext.Context,
	tableID model.TableID,
	replicaInfo *model.TableReplicaInfo,
) (table tablepb.TablePipeline, err error) {
	ctx = cdcContext.WithErrorHandler(ctx, func(err error) error {
		if cerror.ErrTableProcessorStoppedSafely.Equal(err) ||
			errors.Cause(errors.Cause(err)) == context.Canceled {
			return nil
		}
		p.sendError(err)
		return nil
	})

	if p.redoDMLMgr.Enabled() {
		p.redoDMLMgr.AddTable(tableID, replicaInfo.StartTs)
	}

	tableName := p.getTableName(ctx, tableID)

	if p.sinkV1 != nil {
		s, err := sinkv1.NewTableSink(p.sinkV1, tableID, p.metricsTableSinkTotalRows)
		if err != nil {
			return nil, errors.Trace(err)
		}
		table, err = pipeline.NewTableActor(
			ctx,
			p.upstream,
			p.mg,
			tableID,
			tableName,
			replicaInfo,
			s,
			nil,
			p.redoDMLMgr,
			p.changefeed.Info.GetTargetTs())
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		s := p.sinkV2Factory.CreateTableSink(p.changefeedID, tableID,
			replicaInfo.StartTs, p.metricsTableSinkTotalRows)
		table, err = pipeline.NewTableActor(
			ctx,
			p.upstream,
			p.mg,
			tableID,
			tableName,
			replicaInfo,
			nil,
			s,
			p.redoDMLMgr,
			p.changefeed.Info.GetTargetTs())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return table, nil
}

func (p *processor) removeTable(table tablepb.TablePipeline, tableID model.TableID) {
	if p.redoDMLMgr.Enabled() {
		p.redoDMLMgr.RemoveTable(tableID)
	}
	if p.pullBasedSinking {
		p.sinkManager.RemoveTable(tableID)
		p.sourceManager.RemoveTable(tableID)
	} else {
		table.Cancel()
		table.Wait()
		delete(p.tables, tableID)
	}
}

// doGCSchemaStorage trigger the schema storage GC
func (p *processor) doGCSchemaStorage() {
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
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID))
	lastSchemaPhysicalTs := oracle.ExtractPhysical(lastSchemaTs)
	p.metricSchemaStorageGcTsGauge.Set(float64(lastSchemaPhysicalTs))
}

func (p *processor) refreshMetrics() {
	if p.pullBasedSinking {
		p.metricSyncTableNumGauge.Set(float64(p.sinkManager.GetAllTableCount()))
	} else {
		var totalConsumed uint64
		var totalEvents int64
		for _, table := range p.tables {
			consumed := table.MemoryConsumption()
			p.metricsTableMemoryHistogram.Observe(float64(consumed))
			totalConsumed += consumed
			events := table.RemainEvents()
			if events > 0 {
				totalEvents += events
			}
		}
		p.metricsProcessorMemoryGauge.Set(float64(totalConsumed))
		p.metricSyncTableNumGauge.Set(float64(len(p.tables)))
	}
}

func (p *processor) Close(ctx cdcContext.Context) error {
	log.Info("processor closing ...",
		zap.String("namespace", p.changefeedID.Namespace),
		zap.String("changefeed", p.changefeedID.ID))
	// clean up metrics first to avoid some metrics are not cleaned up
	// when error occurs during closing the processor
	p.cleanupMetrics()

	p.cancel()
	if p.pullBasedSinking {
		if p.sinkManager != nil {
			log.Info("Processor try to close sink manager",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID))
			p.sinkManager.Close()
			log.Info("Processor closed sink manager successfully",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID))
			p.sinkManager = nil
		}
		if p.sourceManager != nil {
			log.Info("Processor try to close source manager",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID))
			if err := p.sourceManager.Close(); err != nil {
				log.Error("Failed to close source manager",
					zap.String("namespace", p.changefeedID.Namespace),
					zap.String("changefeed", p.changefeedID.ID),
					zap.Error(err))
				return errors.Trace(err)
			}
			log.Info("Processor closed source manager successfully",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID))
			p.sourceManager = nil
		}
		engineFactory := ctx.GlobalVars().SortEngineFactory
		if engineFactory != nil {
			if err := engineFactory.Drop(p.changefeedID); err != nil {
				log.Error("drop event sort engine fail",
					zap.String("namespace", p.changefeedID.Namespace),
					zap.String("changefeed", p.changefeedID.ID),
					zap.Error(err))
				return errors.Trace(err)
			}
			log.Info("Processor drop sort engine successfully",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID))
		}
	} else {
		for _, tbl := range p.tables {
			tbl.Cancel()
		}
		for _, tbl := range p.tables {
			tbl.Wait()
		}
		// sink close might be time-consuming, do it the last.
		if p.sinkV1 != nil {
			// pass a canceled context is ok here, since we don't need to wait Close
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			log.Info("processor try to close the sinkV1",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID))
			start := time.Now()
			if err := p.sinkV1.Close(ctx); err != nil && errors.Cause(err) != context.Canceled {
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
		} else if p.sinkV2Factory != nil { // maybe nil in test
			log.Info("processor try to close the sinkV2",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID))
			start := time.Now()
			p.sinkV2Factory.Close()
			log.Info("processor close sink success",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Duration("duration", time.Since(start)))
		}
	}
	p.wg.Wait()

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

	tableMemoryHistogram.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	processorMemoryGauge.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)

	sinkmetric.TableSinkTotalRowsCountCounter.
		DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)

	pipeline.SorterBatchReadSize.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
	pipeline.SorterBatchReadDuration.DeleteLabelValues(p.changefeedID.Namespace, p.changefeedID.ID)
}

// WriteDebugInfo write the debug info to Writer
func (p *processor) WriteDebugInfo(w io.Writer) error {
	fmt.Fprintf(w, "%+v\n", *p.changefeed)
	if p.pullBasedSinking {
		tables := p.sinkManager.GetAllCurrentTableIDs()
		for _, tableID := range tables {
			state, _ := p.sinkManager.GetTableState(tableID)
			stats := p.sinkManager.GetTableStats(tableID)
			// TODO: add table name.
			fmt.Fprintf(w, "tableID: %d, resolvedTs: %d, checkpointTs: %d, state: %s\n",
				tableID, stats.ResolvedTs, stats.CheckpointTs, state)
		}
	} else {
		for tableID, tablePipeline := range p.tables {
			fmt.Fprintf(w, "tableID: %d, tableName: %s, resolvedTs: %d, checkpointTs: %d, state: %s\n",
				tableID, tablePipeline.Name(), tablePipeline.ResolvedTs(), tablePipeline.CheckpointTs(), tablePipeline.State())
		}
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
