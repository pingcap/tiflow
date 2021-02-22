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
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	tablepipeline "github.com/pingcap/ticdc/cdc/processor/pipeline"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/cdc/sink"
	cdccontext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/prometheus/client_golang/prometheus"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	// defaultMemBufferCapacity is the default memory buffer per change feed.
	defaultMemBufferCapacity int64 = 10 * 1024 * 1024 * 1024 // 10G

	waitTableCloseTimeout = 15 * time.Second

	schemaStorageGCLag = time.Minute * 20
)

type processor struct {
	changefeed *changefeedState

	tables map[model.TableID]*tablepipeline.TablePipeline

	pdCli         pd.Client
	limitter      *puller.BlurResourceLimitter
	credential    *security.Credential
	captureInfo   *model.CaptureInfo
	schemaStorage *entry.SchemaStorage
	filter        *filter.Filter
	mounter       entry.Mounter
	sinkManager   *sink.Manager

	firstTick bool
	errCh     chan error
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	metricResolvedTsGauge       prometheus.Gauge
	metricResolvedTsLagGauge    prometheus.Gauge
	metricCheckpointTsGauge     prometheus.Gauge
	metricCheckpointTsLagGauge  prometheus.Gauge
	metricSyncTableNumGauge     prometheus.Gauge
	metricProcessorErrorCounter prometheus.Counter
}

// newProcessor creates a new processor
func newProcessor(
	pdCli pd.Client,
	changefeedID model.ChangeFeedID,
	credential *security.Credential,
	captureInfo *model.CaptureInfo,
) *processor {
	return &processor{
		pdCli:       pdCli,
		credential:  credential,
		captureInfo: captureInfo,
		limitter:    puller.NewBlurResourceLimmter(defaultMemBufferCapacity),
		tables:      make(map[model.TableID]*tablepipeline.TablePipeline),
		errCh:       make(chan error, 1),
		firstTick:   true,

		metricResolvedTsGauge:       resolvedTsGauge.WithLabelValues(changefeedID, captureInfo.AdvertiseAddr),
		metricResolvedTsLagGauge:    resolvedTsLagGauge.WithLabelValues(changefeedID, captureInfo.AdvertiseAddr),
		metricCheckpointTsGauge:     checkpointTsGauge.WithLabelValues(changefeedID, captureInfo.AdvertiseAddr),
		metricCheckpointTsLagGauge:  checkpointTsLagGauge.WithLabelValues(changefeedID, captureInfo.AdvertiseAddr),
		metricSyncTableNumGauge:     syncTableNumGauge.WithLabelValues(changefeedID, captureInfo.AdvertiseAddr),
		metricProcessorErrorCounter: processorErrorCounter.WithLabelValues(changefeedID, captureInfo.AdvertiseAddr),
	}
}

func (p *processor) Tick(ctx context.Context, state *changefeedState) (orchestrator.ReactorState, error) {
	log.Debug("Processor tick")
	if _, err := p.tick(ctx, state); err != nil {
		cause := errors.Cause(err)
		if cause != context.Canceled && cerror.ErrAdminStopProcessor.NotEqual(cause) && cerror.ErrReactorFinished.NotEqual(cause) {
			p.metricProcessorErrorCounter.Inc()
			// record error information in etcd
			var code string
			if terror, ok := err.(*errors.Error); ok {
				code = string(terror.RFCCode())
			} else {
				code = string(cerror.ErrProcessorUnknown.RFCCode())
			}
			state.PatchTaskPosition(func(position *model.TaskPosition) (*model.TaskPosition, error) {
				if position == nil {
					position = &model.TaskPosition{}
				}
				position.Error = &model.RunningError{
					Addr:    p.captureInfo.AdvertiseAddr,
					Code:    code,
					Message: err.Error(),
				}
				return position, nil
			})
			log.Error("run processor failed",
				zap.String("changefeed", p.changefeed.ID),
				zap.String("capture-id", p.captureInfo.ID),
				util.ZapFieldCapture(ctx),
				zap.Error(err))
		}
		return state, cerrors.ErrReactorFinished
	}
	p.firstTick = false
	return state, nil
}

func (p *processor) tick(ctx context.Context, state *changefeedState) (nextState orchestrator.ReactorState, err error) {
	p.changefeed = state
	if err := p.handleErrorCh(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	if p.changefeed.TaskStatus.AdminJobType.IsStopState() {
		return nil, cerror.ErrAdminStopProcessor.GenWithStackByArgs()
	}
	if err := p.lazyInit(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	if skip := p.initPosition(); skip {
		return p.changefeed, nil
	}
	if err := p.handleTableOperation(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	if err := p.initTables(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	if err := p.handlePosition(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := p.pushResolvedTs2Table(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := p.handleWorkload(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := p.doGCSchemaStorage(); err != nil {
		return nil, errors.Trace(err)
	}
	return p.changefeed, nil
}

func (p *processor) initPosition() bool {
	if p.changefeed.TaskPosition == nil {
		if !p.firstTick {
			log.Warn("position is nil, maybe position info is removed unexpected", zap.Any("state", p.changefeed))
		}
		checkpointTs := p.changefeed.Info.GetCheckpointTs(p.changefeed.Status)
		p.changefeed.PatchTaskPosition(func(position *model.TaskPosition) (*model.TaskPosition, error) {
			if position == nil {
				return &model.TaskPosition{
					CheckPointTs: checkpointTs,
					ResolvedTs:   checkpointTs,
				}, nil
			}
			return position, nil
		})
		return true
	}
	return false
}

func (p *processor) lazyInit(ctx context.Context) error {
	if !p.firstTick {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	ctx = util.PutChangefeedIDInCtx(ctx, p.changefeed.ID)

	errCh := make(chan error, 16)
	go func() {
		// there are some other objects need errCh, such as sink and sink manager
		// but we can't ensure that all the producer of errCh are non-blocking
		// It's very tricky that create a goroutine to receive the local errCh
		// TODO(leoppro): we should using `pkg/context.Context` instead of standard context and handle error by `pkg/context.Context.Throw`
		for {
			select {
			case <-ctx.Done():
				close(errCh)
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

	p.mounter = entry.NewMounter(p.schemaStorage, p.changefeed.Info.Config.Mounter.WorkerNum, p.changefeed.Info.Config.EnableOldValue)
	go func() {
		p.sendError(p.mounter.Run(ctx))
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
		opts[mark.OptCyclicConfig] = string(cyclicCfg)
	}
	opts[sink.OptChangefeedID] = p.changefeed.ID
	opts[sink.OptCaptureAddr] = p.captureInfo.AdvertiseAddr
	s, err := sink.NewSink(ctx, p.changefeed.ID, p.changefeed.Info.SinkURI, p.filter, p.changefeed.Info.Config, opts, errCh)
	if err != nil {
		return errors.Trace(err)
	}
	checkpointTs := p.changefeed.Info.GetCheckpointTs(p.changefeed.Status)
	p.sinkManager = sink.NewManager(ctx, s, errCh, checkpointTs)

	// Clean up possible residual error states
	p.changefeed.PatchTaskPosition(func(position *model.TaskPosition) (*model.TaskPosition, error) {
		if position != nil && position.Error != nil {
			position.Error = nil
		}
		return position, nil
	})

	log.Info("run processor",
		zap.String("capture-id", p.captureInfo.ID), util.ZapFieldCapture(ctx),
		zap.String("changefeed-id", p.changefeed.ID))
	return nil
}

func (p *processor) handleErrorCh(ctx context.Context) error {
	var err error
	select {
	case err = <-p.errCh:
	default:
		return nil
	}
	p.cancel()
	if util.WaitTimeout(&p.wg, waitTableCloseTimeout) {
		log.Warn("timeout when waiting for all the tables exit", zap.String("changefeedID", p.changefeed.ID))
	}
	cause := errors.Cause(err)
	if cause != nil && cause != context.Canceled && cerror.ErrAdminStopProcessor.NotEqual(cause) {
		log.Error("error on running processor",
			util.ZapFieldCapture(ctx),
			zap.String("changefeed", p.changefeed.ID),
			zap.String("captureID", p.captureInfo.ID),
			zap.String("captureAddr", p.captureInfo.AdvertiseAddr),
			zap.Error(err))
		return err
	}
	log.Info("processor exited",
		util.ZapFieldCapture(ctx),
		zap.String("changefeed", p.changefeed.ID),
		zap.String("captureID", p.captureInfo.ID),
		zap.String("captureAddr", p.captureInfo.AdvertiseAddr))
	return cerrors.ErrReactorFinished
}

func (p *processor) handleTableOperation(ctx context.Context) error {
	patchOperation := func(tableID model.TableID, fn func(operation *model.TableOperation) error) {
		p.changefeed.PatchTaskStatus(func(status *model.TaskStatus) (*model.TaskStatus, error) {
			if status.Operation == nil {
				log.Panic("Operation not found, may be remove by other patch", zap.Int64("tableID", tableID), zap.Any("status", status))
			}
			opt := status.Operation[tableID]
			if opt == nil {
				log.Panic("Operation not found, may be remove by other patch", zap.Int64("tableID", tableID), zap.Any("status", status))
			}
			if err := fn(opt); err != nil {
				return nil, errors.Trace(err)
			}
			return status, nil
		})
	}
	removeTable := func(tableID model.TableID) {
		p.changefeed.PatchTaskStatus(func(status *model.TaskStatus) (*model.TaskStatus, error) {
			if status.Tables == nil {
				log.Panic("Tables not found, may be remove by other patch", zap.Int64("tableID", tableID), zap.Any("status", status))
			}
			delete(status.Tables, tableID)
			return status, nil
		})
	}
	for tableID, opt := range p.changefeed.TaskStatus.Operation {
		if opt.TableApplied() {
			continue
		}
		localCheckpointTs := p.changefeed.TaskPosition.CheckPointTs
		if opt.Delete {
			if opt.BoundaryTs <= localCheckpointTs {
				table, exist := p.tables[tableID]
				if !exist {
					log.Warn("table which will be deleted is not found",
						util.ZapFieldChangefeed(ctx), zap.Int64("tableID", tableID))
					patchOperation(tableID, func(operation *model.TableOperation) error {
						operation.Status = model.OperFinished
						operation.Done = true
						return nil
					})
					removeTable(tableID)
					continue
				}
				switch opt.Status {
				case model.OperDispatched:
					table.AsyncStop(opt.BoundaryTs)
					patchOperation(tableID, func(operation *model.TableOperation) error {
						operation.Status = model.OperProcessed
						return nil
					})
				case model.OperProcessed:
					if table.Status() == tablepipeline.TableStatusStopped {
						patchOperation(tableID, func(operation *model.TableOperation) error {
							operation.BoundaryTs = table.CheckpointTs()
							operation.Status = model.OperFinished
							operation.Done = true
							return nil
						})
						removeTable(tableID)
					}
					table.Cancel()
					delete(p.tables, tableID)
					log.Debug("Operation done signal received",
						util.ZapFieldChangefeed(ctx),
						zap.Int64("tableID", tableID),
						zap.Reflect("operation", opt))
				default:
					log.Panic("unreachable")
				}
			}
		} else {
			switch opt.Status {
			case model.OperDispatched:
				replicaInfo, exist := p.changefeed.TaskStatus.Tables[tableID]
				if !exist {
					return cerror.ErrProcessorTableNotFound.GenWithStack("replicaInfo of table(%d)", tableID)
				}
				if p.changefeed.Info.Config.Cyclic.IsEnabled() && replicaInfo.MarkTableID == 0 {
					return cerror.ErrProcessorTableNotFound.GenWithStack("normal table(%d) and mark table not match ", tableID)
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
					log.Panic("table which was added is not found",
						util.ZapFieldChangefeed(ctx), zap.Int64("tableID", tableID))
				}
				localResolvedTs := p.changefeed.TaskPosition.ResolvedTs
				globalResolvedTs := p.changefeed.Status.ResolvedTs
				if table.ResolvedTs() >= localResolvedTs && localResolvedTs >= globalResolvedTs {
					patchOperation(tableID, func(operation *model.TableOperation) error {
						operation.Status = model.OperFinished
						operation.Done = true
						return nil
					})
					log.Debug("Operation done signal received",
						util.ZapFieldChangefeed(ctx),
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

func (p *processor) createAndDriveSchemaStorage(ctx context.Context) (*entry.SchemaStorage, error) {
	kvStorage, err := util.KVStorageFromCtx(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddlspans := []regionspan.Span{regionspan.GetDDLSpan(), regionspan.GetAddIndexDDLSpan()}
	checkpointTs := p.changefeed.Info.GetCheckpointTs(p.changefeed.Status)
	ddlPuller := puller.NewPuller(ctx, p.pdCli, p.credential, kvStorage, checkpointTs, ddlspans, p.limitter, false)
	meta, err := kv.GetSnapshotMeta(kvStorage, checkpointTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaStorage, err := entry.NewSchemaStorage(meta, checkpointTs, p.filter, p.changefeed.Info.Config.ForceReplicate)
	if err != nil {
		return nil, errors.Trace(err)
	}
	go func() {
		p.sendError(ddlPuller.Run(ctx))
	}()
	ddlRawKVCh := puller.SortOutput(ctx, ddlPuller.Output())
	go func() {
		var ddlRawKV *model.RawKVEntry
		for {
			select {
			case <-ctx.Done():
				p.sendError(ctx.Err())
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
			log.Debug("Processor DDL received", zap.Reflect("job", job))
			if err := schemaStorage.HandleDDLJob(job); err != nil {
				p.sendError(errors.Trace(err))
				return
			}
		}
	}()
	return schemaStorage, nil
}

func (p *processor) sendError(err error) {
	select {
	case p.errCh <- err:
	default:
		log.Error("processor receives redundant error", zap.Error(err), zap.Stack("stack"))
	}
}

func (p *processor) initTables(ctx context.Context) error {
	for tableID, replicaInfo := range p.changefeed.TaskStatus.Tables {
		if _, exist := p.tables[tableID]; exist {
			continue
		}
		opt := p.changefeed.TaskStatus.Operation
		if opt != nil && opt[tableID] != nil {
			continue
		}
		if !p.firstTick {
			log.Warn("The table was left out", zap.Int64("tableID", tableID), zap.Any("replicaInfo", replicaInfo))
		}
		err := p.addTable(ctx, tableID, replicaInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (p *processor) handlePosition() error {
	minResolvedTs := p.schemaStorage.ResolvedTs()
	for _, table := range p.tables {
		ts := table.ResolvedTs()
		if ts < minResolvedTs {
			minResolvedTs = ts
		}
	}

	minCheckpointTs := minResolvedTs
	for _, table := range p.tables {
		ts := table.CheckpointTs()

		if ts < minCheckpointTs {
			minCheckpointTs = ts
		}
	}

	resolvedPhyTs := oracle.ExtractPhysical(minResolvedTs)
	// It is more accurate to get tso from PD, but in most cases we have
	// deployed NTP service, a little bias is acceptable here.
	p.metricResolvedTsLagGauge.Set(float64(oracle.GetPhysical(time.Now())-resolvedPhyTs) / 1e3)
	p.metricResolvedTsGauge.Set(float64(resolvedPhyTs))

	checkpointPhyTs := oracle.ExtractPhysical(minCheckpointTs)
	// It is more accurate to get tso from PD, but in most cases we have
	// deployed NTP service, a little bias is acceptable here.
	p.metricCheckpointTsLagGauge.Set(float64(oracle.GetPhysical(time.Now())-checkpointPhyTs) / 1e3)
	p.metricCheckpointTsGauge.Set(float64(checkpointPhyTs))

	if minResolvedTs > p.changefeed.TaskPosition.ResolvedTs ||
		minCheckpointTs > p.changefeed.TaskPosition.CheckPointTs {
		p.changefeed.PatchTaskPosition(func(position *model.TaskPosition) (*model.TaskPosition, error) {
			failpoint.Inject("ProcessorUpdatePositionDelaying", nil)
			position.CheckPointTs = minCheckpointTs
			position.ResolvedTs = minResolvedTs
			return position, nil
		})
	}
	return nil
}

func (p *processor) handleWorkload() error {
	p.changefeed.PatchTaskWorkload(func(_ model.TaskWorkload) (model.TaskWorkload, error) {
		workload := make(model.TaskWorkload, len(p.tables))
		for tableID, table := range p.tables {
			workload[tableID] = table.Workload()
		}
		return workload, nil
	})
	return nil
}

func (p *processor) pushResolvedTs2Table() error {
	resolvedTs := p.changefeed.Status.ResolvedTs
	for _, table := range p.tables {
		table.UpdateBarrierTs(resolvedTs)
	}
	return nil
}

func (p *processor) addTable(ctx context.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) error {
	if table, ok := p.tables[tableID]; ok {
		if table.Status() == tablepipeline.TableStatusStopped {
			log.Warn("The same table exists but is stopped. Cancel it and continue.", util.ZapFieldChangefeed(ctx), zap.Int64("ID", tableID))
			table.Cancel()
			delete(p.tables, tableID)
		} else {
			log.Warn("Ignore existing table", util.ZapFieldChangefeed(ctx), zap.Int64("ID", tableID))
			return nil
		}
	}

	globalCheckpointTs := p.changefeed.Status.CheckpointTs
	globalResolvedTs := p.changefeed.Status.ResolvedTs

	if replicaInfo.StartTs < globalCheckpointTs {
		log.Warn("addTable: startTs < checkpoint",
			util.ZapFieldChangefeed(ctx),
			zap.Int64("tableID", tableID),
			zap.Uint64("checkpoint", globalCheckpointTs),
			zap.Uint64("startTs", replicaInfo.StartTs))
	}

	cdcCtx := cdccontext.NewContext(ctx, &cdccontext.Vars{
		CaptureAddr:   "TODO: CaptureAddr",
		PDClient:      p.pdCli,
		SchemaStorage: p.schemaStorage,
		Config:        p.changefeed.Info.Config,
	})
	kvStorage, err := util.KVStorageFromCtx(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	var tableName string
	err = retry.Run(time.Millisecond*5, 3, func() error {
		if name, ok := p.schemaStorage.GetLastSnapshot().GetTableNameByID(tableID); ok {
			tableName = name.QuoteString()
			return nil
		}
		return errors.Errorf("failed to get table name, fallback to use table id: %d", tableID)
	})
	if err != nil {
		log.Warn("get table name for metric", zap.Error(err))
		tableName = strconv.Itoa(int(tableID))
	}
	sink := p.sinkManager.CreateTableSink(tableID, replicaInfo.StartTs)

	_, table := tablepipeline.NewTablePipeline(
		cdcCtx,
		p.changefeed.ID,
		p.credential,
		kvStorage,
		p.limitter,
		p.mounter,
		p.changefeed.Info.Engine,
		p.changefeed.Info.SortDir,
		tableID,
		tableName,
		replicaInfo,
		sink,
		p.changefeed.Info.GetTargetTs(),
	)
	p.wg.Add(1)
	p.metricSyncTableNumGauge.Inc()
	go func() {
		for _, err := range table.Wait() {
			if cerror.ErrTableProcessorStoppedSafely.Equal(err) || errors.Cause(err) == context.Canceled {
				continue
			}
			p.sendError(err)
		}
		p.wg.Done()
		p.metricSyncTableNumGauge.Dec()
		log.Debug("Table pipeline exited", zap.Int64("tableID", tableID),
			util.ZapFieldChangefeed(ctx),
			zap.String("name", table.Name()),
			zap.Any("replicaInfo", replicaInfo),
			zap.Uint64("globalResolvedTs", globalResolvedTs))
	}()

	log.Debug("Add table pipeline", zap.Int64("tableID", tableID),
		util.ZapFieldChangefeed(ctx),
		zap.String("name", table.Name()),
		zap.Any("replicaInfo", replicaInfo),
		zap.Uint64("globalResolvedTs", globalResolvedTs))

	p.tables[tableID] = table
	return nil
}

func (p *processor) doGCSchemaStorage() error {
	// Delay GC to accommodate pullers starting from a startTs that's too small
	// TODO fix startTs problem and remove GC delay, or use other mechanism that prevents the problem deterministically
	gcTime := oracle.GetTimeFromTS(p.changefeed.Status.CheckpointTs).Add(-schemaStorageGCLag)
	gcTs := oracle.ComposeTS(gcTime.Unix(), 0)
	p.schemaStorage.DoGC(gcTs)
	return nil
}

func (p *processor) Close() error {
	log.Info("stop processor", zap.String("capture", p.captureInfo.AdvertiseAddr), zap.String("changefeed", p.changefeed.ID))
	for _, tbl := range p.tables {
		tbl.Cancel()
	}
	p.cancel()
	// mark tables share the same context with its original table, don't need to cancel
	failpoint.Inject("processorStopDelay", nil)
	p.changefeed.PatchTaskPosition(func(position *model.TaskPosition) (*model.TaskPosition, error) {
		if position == nil {
			return nil, nil
		}
		if position.Error != nil {
			return position, nil
		}
		return nil, nil
	})
	p.changefeed.PatchTaskStatus(func(_ *model.TaskStatus) (*model.TaskStatus, error) {
		return nil, nil
	})
	p.changefeed.PatchTaskWorkload(func(_ model.TaskWorkload) (model.TaskWorkload, error) {
		return nil, nil
	})

	resolvedTsGauge.DeleteLabelValues(p.changefeed.ID, p.captureInfo.AdvertiseAddr)
	resolvedTsLagGauge.DeleteLabelValues(p.changefeed.ID, p.captureInfo.AdvertiseAddr)
	checkpointTsGauge.DeleteLabelValues(p.changefeed.ID, p.captureInfo.AdvertiseAddr)
	checkpointTsLagGauge.DeleteLabelValues(p.changefeed.ID, p.captureInfo.AdvertiseAddr)
	syncTableNumGauge.DeleteLabelValues(p.changefeed.ID, p.captureInfo.AdvertiseAddr)
	processorErrorCounter.DeleteLabelValues(p.changefeed.ID, p.captureInfo.AdvertiseAddr)

	return p.sinkManager.Close()
}

// WriteDebugInfo write the debug info to Writer
func (p *processor) WriteDebugInfo(w io.Writer) {
	fmt.Fprintf(w, "%+v\n", *p.changefeed)
	for tableID, tablePipeline := range p.tables {
		fmt.Fprintf(w, "tableID: %d, tableName: %s, resolvedTs: %d, checkpointTs: %d, status: %s\n",
			tableID, tablePipeline.Name(), tablePipeline.ResolvedTs(), tablePipeline.CheckpointTs(), tablePipeline.Status())
	}
}
