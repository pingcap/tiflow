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
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	tablepipeline "github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/cdc/sink"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	backoffBaseDelayInMs = 5
	maxTries             = 3
)

type processor struct {
	changefeedID model.ChangeFeedID
	captureInfo  *model.CaptureInfo
	changefeed   *model.ChangefeedReactorState

	tables map[model.TableID]tablepipeline.TablePipeline

	schemaStorage entry.SchemaStorage
	lastSchemaTs  model.Ts

	filter      *filter.Filter
	mounter     entry.Mounter
	sinkManager *sink.Manager

	initialized bool
	errCh       chan error
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	lazyInit            func(ctx cdcContext.Context) error
	createTablePipeline func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error)

	metricResolvedTsGauge           prometheus.Gauge
	metricResolvedTsLagGauge        prometheus.Gauge
	metricMinResolvedTableIDGuage   prometheus.Gauge
	metricCheckpointTsGauge         prometheus.Gauge
	metricCheckpointTsLagGauge      prometheus.Gauge
	metricMinCheckpointTableIDGuage prometheus.Gauge
	metricSyncTableNumGauge         prometheus.Gauge
	metricSchemaStorageGcTsGauge    prometheus.Gauge
	metricProcessorErrorCounter     prometheus.Counter
}

// newProcessor creates a new processor
func newProcessor(ctx cdcContext.Context) *processor {
	changefeedID := ctx.ChangefeedVars().ID
	advertiseAddr := ctx.GlobalVars().CaptureInfo.AdvertiseAddr
	p := &processor{
		tables:       make(map[model.TableID]tablepipeline.TablePipeline),
		errCh:        make(chan error, 1),
		changefeedID: changefeedID,
		captureInfo:  ctx.GlobalVars().CaptureInfo,
		cancel:       func() {},

		metricResolvedTsGauge:           resolvedTsGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricResolvedTsLagGauge:        resolvedTsLagGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricMinResolvedTableIDGuage:   resolvedTsMinTableIDGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricCheckpointTsGauge:         checkpointTsGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricCheckpointTsLagGauge:      checkpointTsLagGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricMinCheckpointTableIDGuage: checkpointTsMinTableIDGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricSyncTableNumGauge:         syncTableNumGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricProcessorErrorCounter:     processorErrorCounter.WithLabelValues(changefeedID, advertiseAddr),
		metricSchemaStorageGcTsGauge:    processorSchemaStorageGcTsGauge.WithLabelValues(changefeedID, advertiseAddr),
	}
	p.createTablePipeline = p.createTablePipelineImpl
	p.lazyInit = p.lazyInitImpl
	return p
}

// Tick implements the `orchestrator.State` interface
// the `state` parameter is sent by the etcd worker, the `state` must be a snapshot of KVs in etcd
// The main logic of processor is in this function, including the calculation of many kinds of ts, maintain table pipeline, error handling, etc.
func (p *processor) Tick(ctx cdcContext.Context, state *model.ChangefeedReactorState) (orchestrator.ReactorState, error) {
	p.changefeed = state
	state.CheckCaptureAlive(ctx.GlobalVars().CaptureInfo.ID)
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID:   state.ID,
		Info: state.Info,
	})
	_, err := p.tick(ctx, state)
	if err == nil {
		return state, nil
	}
	cause := errors.Cause(err)
	if cause == context.Canceled || cerror.ErrAdminStopProcessor.Equal(cause) || cerror.ErrReactorFinished.Equal(cause) {
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

func (p *processor) tick(ctx cdcContext.Context, state *model.ChangefeedReactorState) (nextState orchestrator.ReactorState, err error) {
	p.changefeed = state
	if !p.checkChangefeedNormal() {
		return nil, cerror.ErrAdminStopProcessor.GenWithStackByArgs()
	}
	// we should skip this tick after create a task position
	if p.createTaskPosition() {
		return p.changefeed, nil
	}
	if err := p.handleErrorCh(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	if err := p.lazyInit(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	// sink manager will return this checkpointTs to sink node if sink node resolvedTs flush failed
	p.sinkManager.UpdateChangeFeedCheckpointTs(state.Info.GetCheckpointTs(state.Status))
	if err := p.handleTableOperation(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	if err := p.checkTablesNum(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	// it is no need to check the err here, because we will use
	// local time when an error return, which is acceptable
	pdTime, _ := ctx.GlobalVars().TimeAcquirer.CurrentTimeFromCached()

	p.handlePosition(oracle.GetPhysical(pdTime))
	p.pushResolvedTs2Table()
	p.handleWorkload()
	p.doGCSchemaStorage(ctx)
	return p.changefeed, nil
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
	checkpointTs := p.changefeed.Info.GetCheckpointTs(p.changefeed.Status)
	p.changefeed.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		if position == nil {
			return &model.TaskPosition{
				CheckPointTs: checkpointTs,
				ResolvedTs:   checkpointTs,
			}, true, nil
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

	stdCtx := util.PutChangefeedIDInCtx(ctx, p.changefeed.ID)

	p.mounter = entry.NewMounter(p.schemaStorage, p.changefeed.Info.Config.Mounter.WorkerNum, p.changefeed.Info.Config.EnableOldValue)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.sendError(p.mounter.Run(stdCtx))
	}()

	opts := make(map[string]string, len(p.changefeed.Info.Opts)+2)
	for k, v := range p.changefeed.Info.Opts {
		opts[k] = v
	}

	// TODO(neil) find a better way to let sink know cyclic is enabled.
	if p.changefeed.Info.Config.Cyclic.IsEnabled() {
		cyclicCfg, err := p.changefeed.Info.Config.Cyclic.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		opts[mark.OptCyclicConfig] = cyclicCfg
	}
	opts[sink.OptChangefeedID] = p.changefeed.ID
	opts[sink.OptCaptureAddr] = ctx.GlobalVars().CaptureInfo.AdvertiseAddr
	log.Info("processor try new sink", zap.String("changefeed", p.changefeed.ID))

	start := time.Now()
	s, err := sink.NewSink(stdCtx, p.changefeed.ID, p.changefeed.Info.SinkURI, p.filter, p.changefeed.Info.Config, opts, errCh)
	if err != nil {
		log.Info("processor new sink failed",
			zap.String("changefeed", p.changefeed.ID),
			zap.Duration("duration", time.Since(start)))
		return errors.Trace(err)
	}
	log.Info("processor try new sink success",
		zap.Duration("duration", time.Since(start)))

	checkpointTs := p.changefeed.Info.GetCheckpointTs(p.changefeed.Status)
	captureAddr := ctx.GlobalVars().CaptureInfo.AdvertiseAddr
	p.sinkManager = sink.NewManager(stdCtx, s, errCh, checkpointTs, captureAddr, p.changefeedID)
	p.initialized = true
	log.Info("run processor", cdcContext.ZapFieldCapture(ctx), cdcContext.ZapFieldChangefeed(ctx))
	return nil
}

// handleErrorCh listen the error channel and throw the error if it is not expected.
func (p *processor) handleErrorCh(ctx cdcContext.Context) error {
	var err error
	select {
	case err = <-p.errCh:
	default:
		return nil
	}
	cause := errors.Cause(err)
	if cause != nil && cause != context.Canceled && cerror.ErrAdminStopProcessor.NotEqual(cause) {
		log.Error("error on running processor",
			cdcContext.ZapFieldCapture(ctx),
			cdcContext.ZapFieldChangefeed(ctx),
			zap.Error(err))
		return err
	}
	log.Info("processor exited", cdcContext.ZapFieldCapture(ctx), cdcContext.ZapFieldChangefeed(ctx))
	return cerror.ErrReactorFinished
}

// handleTableOperation handles the operation of `TaskStatus`(add table operation and remove table operation)
func (p *processor) handleTableOperation(ctx cdcContext.Context) error {
	patchOperation := func(tableID model.TableID, fn func(operation *model.TableOperation) error) {
		p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			if status == nil || status.Operation == nil {
				log.Error("Operation not found, may be remove by other patch", zap.Int64("tableID", tableID), zap.Any("status", status))
				return nil, false, cerror.ErrTaskStatusNotExists.GenWithStackByArgs()
			}
			opt := status.Operation[tableID]
			if opt == nil {
				log.Error("Operation not found, may be remove by other patch", zap.Int64("tableID", tableID), zap.Any("status", status))
				return nil, false, cerror.ErrTaskStatusNotExists.GenWithStackByArgs()
			}
			if err := fn(opt); err != nil {
				return nil, false, errors.Trace(err)
			}
			return status, true, nil
		})
	}
	taskStatus := p.changefeed.TaskStatuses[p.captureInfo.ID]
	for tableID, opt := range taskStatus.Operation {
		if opt.TableApplied() {
			continue
		}
		globalCheckpointTs := p.changefeed.Status.CheckpointTs
		if opt.Delete {
			table, exist := p.tables[tableID]
			if !exist {
				log.Warn("table which will be deleted is not found",
					cdcContext.ZapFieldChangefeed(ctx), zap.Int64("tableID", tableID))
				patchOperation(tableID, func(operation *model.TableOperation) error {
					operation.Status = model.OperFinished
					operation.Done = true
					return nil
				})
				continue
			}
			switch opt.Status {
			case model.OperDispatched:
				if opt.BoundaryTs < globalCheckpointTs {
					log.Warn("the BoundaryTs of remove table operation is smaller than global checkpoint ts", zap.Uint64("globalCheckpointTs", globalCheckpointTs), zap.Any("operation", opt))
				}
				if !table.AsyncStop(opt.BoundaryTs) {
					// We use a Debug log because it is conceivable for the pipeline to block for a legitimate reason,
					// and we do not want to alarm the user.
					log.Debug("AsyncStop has failed, possible due to a full pipeline",
						zap.Uint64("checkpointTs", table.CheckpointTs()), zap.Int64("tableID", tableID))
					continue
				}
				patchOperation(tableID, func(operation *model.TableOperation) error {
					operation.Status = model.OperProcessed
					return nil
				})
			case model.OperProcessed:
				if table.Status() != tablepipeline.TableStatusStopped {
					log.Debug("the table is still not stopped", zap.Uint64("checkpointTs", table.CheckpointTs()), zap.Int64("tableID", tableID))
					continue
				}
				patchOperation(tableID, func(operation *model.TableOperation) error {
					operation.BoundaryTs = table.CheckpointTs()
					operation.Status = model.OperFinished
					operation.Done = true
					return nil
				})
				table.Cancel()
				table.Wait()
				delete(p.tables, tableID)
				log.Debug("Operation done signal received",
					cdcContext.ZapFieldChangefeed(ctx),
					zap.Int64("tableID", tableID),
					zap.Reflect("operation", opt))
			default:
				log.Panic("unreachable")
			}
		} else {
			switch opt.Status {
			case model.OperDispatched:
				replicaInfo, exist := taskStatus.Tables[tableID]
				if !exist {
					return cerror.ErrProcessorTableNotFound.GenWithStack("replicaInfo of table(%d)", tableID)
				}
				if replicaInfo.StartTs != opt.BoundaryTs {
					log.Warn("the startTs and BoundaryTs of add table operation should be always equaled", zap.Any("replicaInfo", replicaInfo))
				}
				err := p.addTable(ctx, tableID, replicaInfo)
				if err != nil {
					return errors.Trace(err)
				}
				patchOperation(tableID, func(operation *model.TableOperation) error {
					operation.Status = model.OperProcessed
					return nil
				})
			case model.OperProcessed:
				table, exist := p.tables[tableID]
				if !exist {
					log.Warn("table which was added is not found",
						cdcContext.ZapFieldChangefeed(ctx), zap.Int64("tableID", tableID))
					patchOperation(tableID, func(operation *model.TableOperation) error {
						operation.Status = model.OperDispatched
						return nil
					})
					continue
				}
				localResolvedTs := p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs
				globalResolvedTs := p.changefeed.Status.ResolvedTs
				if table.ResolvedTs() >= localResolvedTs && localResolvedTs >= globalResolvedTs {
					patchOperation(tableID, func(operation *model.TableOperation) error {
						operation.Status = model.OperFinished
						operation.Done = true
						return nil
					})
					log.Debug("Operation done signal received",
						cdcContext.ZapFieldChangefeed(ctx),
						zap.Int64("tableID", tableID),
						zap.Reflect("operation", opt))
				}
			default:
				log.Panic("unreachable")
			}
		}
	}
	return nil
}

func (p *processor) createAndDriveSchemaStorage(ctx cdcContext.Context) (entry.SchemaStorage, error) {
	kvStorage := ctx.GlobalVars().KVStorage
	ddlspans := []regionspan.Span{regionspan.GetDDLSpan(), regionspan.GetAddIndexDDLSpan()}
	checkpointTs := p.changefeed.Info.GetCheckpointTs(p.changefeed.Status)
	stdCtx := util.PutTableInfoInCtx(ctx, -1, puller.DDLPullerTableName)
	stdCtx = util.PutChangefeedIDInCtx(stdCtx, ctx.ChangefeedVars().ID)
	ddlPuller := puller.NewPuller(
		stdCtx,
		ctx.GlobalVars().PDClient,
		ctx.GlobalVars().GrpcPool,
		ctx.GlobalVars().KVStorage,
		ctx.ChangefeedVars().ID,
		checkpointTs, ddlspans, false)
	meta, err := kv.GetSnapshotMeta(kvStorage, checkpointTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaStorage, err := entry.NewSchemaStorage(meta, checkpointTs, p.filter,
		p.changefeed.Info.Config.ForceReplicate, p.changefeedID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.sendError(ddlPuller.Run(stdCtx))
	}()
	ddlRawKVCh := puller.SortOutput(ctx, ddlPuller.Output())
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
		if errors.Cause(err) != context.Canceled {
			log.Error("processor receives redundant error", zap.Error(err))
		}
	}
}

// checkTablesNum if the number of table pipelines is equal to the number of TaskStatus in etcd state.
// if the table number is not right, create or remove the odd tables.
func (p *processor) checkTablesNum(ctx cdcContext.Context) error {
	taskStatus := p.changefeed.TaskStatuses[p.captureInfo.ID]
	if len(p.tables) == len(taskStatus.Tables) {
		return nil
	}
	// check if a table should be listen but not
	// this only could be happened in the first tick.
	for tableID, replicaInfo := range taskStatus.Tables {
		if _, exist := p.tables[tableID]; exist {
			continue
		}
		opt := taskStatus.Operation
		// TODO(leoppro): check if the operation is a undone add operation
		if opt != nil && opt[tableID] != nil {
			continue
		}
		log.Info("start to listen to the table immediately", zap.Int64("tableID", tableID), zap.Any("replicaInfo", replicaInfo))
		if replicaInfo.StartTs < p.changefeed.Status.CheckpointTs {
			replicaInfo.StartTs = p.changefeed.Status.CheckpointTs
		}
		err := p.addTable(ctx, tableID, replicaInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}
	// check if a table should be removed but still exist
	// this shouldn't be happened in any time.
	for tableID, tablePipeline := range p.tables {
		if _, exist := taskStatus.Tables[tableID]; exist {
			continue
		}
		opt := taskStatus.Operation
		if opt != nil && opt[tableID] != nil && opt[tableID].Delete {
			// table will be removed by normal logic
			continue
		}
		tablePipeline.Cancel()
		tablePipeline.Wait()
		delete(p.tables, tableID)
		log.Warn("the table was forcibly deleted", zap.Int64("tableID", tableID), zap.Any("taskStatus", taskStatus))
	}
	return nil
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
	p.metricMinResolvedTableIDGuage.Set(float64(minResolvedTableID))

	checkpointPhyTs := oracle.ExtractPhysical(minCheckpointTs)
	p.metricCheckpointTsLagGauge.Set(float64(currentTs-checkpointPhyTs) / 1e3)
	p.metricCheckpointTsGauge.Set(float64(checkpointPhyTs))
	p.metricMinCheckpointTableIDGuage.Set(float64(minCheckpointTableID))

	// minResolvedTs and minCheckpointTs may less than global resolved ts and global checkpoint ts when a new table added, the startTs of the new table is less than global checkpoint ts.
	if minResolvedTs != p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs ||
		minCheckpointTs != p.changefeed.TaskPositions[p.captureInfo.ID].CheckPointTs {
		p.changefeed.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			failpoint.Inject("ProcessorUpdatePositionDelaying", nil)
			if position == nil {
				// when the captureInfo is deleted, the old owner will delete task status, task position, task workload in non-atomic
				// so processor may see a intermediate state, for example the task status is exist but task position is deleted.
				log.Warn("task position is not exist, skip to update position", zap.String("changefeed", p.changefeed.ID))
				return nil, false, nil
			}
			position.CheckPointTs = minCheckpointTs
			position.ResolvedTs = minResolvedTs
			return position, true, nil
		})
	}
}

// handleWorkload calculates the workload of all tables
func (p *processor) handleWorkload() {
	p.changefeed.PatchTaskWorkload(p.captureInfo.ID, func(workloads model.TaskWorkload) (model.TaskWorkload, bool, error) {
		changed := false
		if workloads == nil {
			workloads = make(model.TaskWorkload)
		}
		for tableID := range workloads {
			if _, exist := p.tables[tableID]; !exist {
				delete(workloads, tableID)
				changed = true
			}
		}
		for tableID, table := range p.tables {
			if workloads[tableID] != table.Workload() {
				workloads[tableID] = table.Workload()
				changed = true
			}
		}
		return workloads, changed, nil
	})
}

// pushResolvedTs2Table sends global resolved ts to all the table pipelines.
func (p *processor) pushResolvedTs2Table() {
	resolvedTs := p.changefeed.Status.ResolvedTs
	for _, table := range p.tables {
		table.UpdateBarrierTs(resolvedTs)
	}
}

// addTable creates a new table pipeline and adds it to the `p.tables`
func (p *processor) addTable(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) error {
	if table, ok := p.tables[tableID]; ok {
		if table.Status() == tablepipeline.TableStatusStopped {
			log.Warn("The same table exists but is stopped. Cancel it and continue.", cdcContext.ZapFieldChangefeed(ctx), zap.Int64("ID", tableID))
			table.Cancel()
			table.Wait()
			delete(p.tables, tableID)
		} else {
			log.Warn("Ignore existing table", cdcContext.ZapFieldChangefeed(ctx), zap.Int64("ID", tableID))
			return nil
		}
	}

	globalCheckpointTs := p.changefeed.Status.CheckpointTs

	if replicaInfo.StartTs < globalCheckpointTs {
		log.Warn("addTable: startTs < checkpoint",
			cdcContext.ZapFieldChangefeed(ctx),
			zap.Int64("tableID", tableID),
			zap.Uint64("checkpoint", globalCheckpointTs),
			zap.Uint64("startTs", replicaInfo.StartTs))
	}
	table, err := p.createTablePipeline(ctx, tableID, replicaInfo)
	if err != nil {
		return errors.Trace(err)
	}
	p.tables[tableID] = table
	return nil
}

func (p *processor) createTablePipelineImpl(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error) {
	ctx = cdcContext.WithErrorHandler(ctx, func(err error) error {
		if cerror.ErrTableProcessorStoppedSafely.Equal(err) ||
			errors.Cause(errors.Cause(err)) == context.Canceled {
			return nil
		}
		p.sendError(err)
		return nil
	})
	var tableName *model.TableName
	retry.Do(ctx, func() error { //nolint:errcheck
		if table, ok := p.schemaStorage.GetLastSnapshot().PhysicalTableByID(tableID); ok {
			tableName = &table.TableName
			return nil
		}
		return errors.Errorf("failed to get table name, fallback to use table id: %d", tableID)
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs), retry.WithMaxTries(maxTries), retry.WithIsRetryableErr(cerror.IsRetryableError))
	if p.changefeed.Info.Config.Cyclic.IsEnabled() {
		// Retry to find mark table ID
		var markTableID model.TableID
		err := retry.Do(context.Background(), func() error {
			if tableName == nil {
				table, exist := p.schemaStorage.GetLastSnapshot().PhysicalTableByID(tableID)
				if !exist {
					return cerror.ErrProcessorTableNotFound.GenWithStack("normal table(%s)", tableID)
				}
				tableName = &table.TableName
			}
			markTableSchemaName, markTableTableName := mark.GetMarkTableName(tableName.Schema, tableName.Table)
			tableInfo, exist := p.schemaStorage.GetLastSnapshot().TableByName(markTableSchemaName, markTableTableName)
			if !exist {
				return cerror.ErrProcessorTableNotFound.GenWithStack("normal table(%s) and mark table not match", tableName.String())
			}
			markTableID = tableInfo.ID
			return nil
		}, retry.WithBackoffBaseDelay(50), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(20))
		if err != nil {
			return nil, errors.Trace(err)
		}
		replicaInfo.MarkTableID = markTableID
	}
	var tableNameStr string
	if tableName == nil {
		log.Warn("failed to get table name for metric")
		tableNameStr = strconv.Itoa(int(tableID))
	} else {
		tableNameStr = tableName.QuoteString()
	}

	sink := p.sinkManager.CreateTableSink(tableID, replicaInfo.StartTs)
	table := tablepipeline.NewTablePipeline(
		ctx,
		p.mounter,
		tableID,
		tableNameStr,
		replicaInfo,
		sink,
		p.changefeed.Info.GetTargetTs(),
	)
	p.wg.Add(1)
	p.metricSyncTableNumGauge.Inc()
	go func() {
		table.Wait()
		p.wg.Done()
		p.metricSyncTableNumGauge.Dec()
		log.Debug("Table pipeline exited", zap.Int64("tableID", tableID),
			cdcContext.ZapFieldChangefeed(ctx),
			zap.String("name", table.Name()),
			zap.Any("replicaInfo", replicaInfo))
	}()

	log.Info("Add table pipeline", zap.Int64("tableID", tableID),
		cdcContext.ZapFieldChangefeed(ctx),
		zap.String("name", table.Name()),
		zap.Any("replicaInfo", replicaInfo),
		zap.Uint64("globalResolvedTs", p.changefeed.Status.ResolvedTs))

	return table, nil
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

func (p *processor) Close() error {
	log.Info("processor closing ...", zap.String("changefeed", p.changefeedID))
	for _, tbl := range p.tables {
		tbl.Cancel()
	}
	for _, tbl := range p.tables {
		tbl.Wait()
	}
	p.cancel()
	p.wg.Wait()
	// mark tables share the same cdcContext with its original table, don't need to cancel
	failpoint.Inject("processorStopDelay", nil)
	resolvedTsGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	resolvedTsLagGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	checkpointTsGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	checkpointTsLagGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	syncTableNumGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	processorErrorCounter.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	processorSchemaStorageGcTsGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	if p.sinkManager != nil {
		// pass a canceled context is ok here, since we don't need to wait Close
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		log.Info("processor try to close the sinkManager",
			zap.String("changefeed", p.changefeedID))
		start := time.Now()
		if err := p.sinkManager.Close(ctx); err != nil {
			log.Info("processor close sinkManager failed",
				zap.String("changefeed", p.changefeedID),
				zap.Duration("duration", time.Since(start)))
			return errors.Trace(err)
		}
		log.Info("processor close sinkManager success",
			zap.String("changefeed", p.changefeedID),
			zap.Duration("duration", time.Since(start)))
	}
	return nil
}

// WriteDebugInfo write the debug info to Writer
func (p *processor) WriteDebugInfo(w io.Writer) {
	fmt.Fprintf(w, "%+v\n", *p.changefeed)
	for tableID, tablePipeline := range p.tables {
		fmt.Fprintf(w, "tableID: %d, tableName: %s, resolvedTs: %d, checkpointTs: %d, status: %s\n",
			tableID, tablePipeline.Name(), tablePipeline.ResolvedTs(), tablePipeline.CheckpointTs(), tablePipeline.Status())
	}
}
