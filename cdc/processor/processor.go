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

	"github.com/edwingeng/deque"
	"github.com/pingcap/ticdc/pkg/p2p"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	tablepipeline "github.com/pingcap/ticdc/cdc/processor/pipeline"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/cdc/sink"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	schemaStorageGCLag = time.Minute * 20

	backoffBaseDelayInMs = 5
	maxTries             = 3
)

type processor struct {
	changefeedID model.ChangeFeedID
	captureInfo  *model.CaptureInfo
	changefeed   *model.ChangefeedReactorState

	tables map[model.TableID]tablepipeline.TablePipeline

	schemaStorage entry.SchemaStorage
	filter        *filter.Filter
	mounter       entry.Mounter
	sinkManager   *sink.Manager

	initialized bool
	errCh       chan error
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	lazyInit            func(ctx cdcContext.Context) error
	createTablePipeline func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error)

	metricResolvedTsGauge       prometheus.Gauge
	metricResolvedTsLagGauge    prometheus.Gauge
	metricCheckpointTsGauge     prometheus.Gauge
	metricCheckpointTsLagGauge  prometheus.Gauge
	metricSyncTableNumGauge     prometheus.Gauge
	metricProcessorErrorCounter prometheus.Counter

	pendingOpsMu sync.Mutex
	pendingOps   deque.Deque

	operations map[model.TableID]*model.DispatchTableMessage

	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	// ownerID and ownerRev are used to make sure we send peer messages
	// to the correct owner
	ownerInfoMu sync.RWMutex
	ownerID     model.CaptureID
	ownerRev    int64
	// next tick will happen after the owner has acknowledged the peer message
	// with sequence number `waitForAck`.
	waitForAck           p2p.Seq
	waitForSync          p2p.Seq
	needSyncWithNewOwner bool
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

		metricResolvedTsGauge:       resolvedTsGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricResolvedTsLagGauge:    resolvedTsLagGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricCheckpointTsGauge:     checkpointTsGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricCheckpointTsLagGauge:  checkpointTsLagGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricSyncTableNumGauge:     syncTableNumGauge.WithLabelValues(changefeedID, advertiseAddr),
		metricProcessorErrorCounter: processorErrorCounter.WithLabelValues(changefeedID, advertiseAddr),

		messageServer: ctx.GlobalVars().MessageServer,
		messageRouter: ctx.GlobalVars().MessageRouter,

		pendingOps: deque.NewDeque(),
		operations: make(map[model.TableID]*model.DispatchTableMessage),
	}
	p.createTablePipeline = p.createTablePipelineImpl
	p.lazyInit = p.lazyInitImpl
	return p
}

func newProcessor4Test(ctx cdcContext.Context,
	createTablePipeline func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error),
) *processor {
	p := newProcessor(ctx)
	p.lazyInit = func(ctx cdcContext.Context) error { return nil }
	p.createTablePipeline = createTablePipeline
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
	if skip := p.checkPosition(); skip {
		return p.changefeed, nil
	}
	if err := p.handleErrorCh(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	if err := p.lazyInit(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	if err := p.handleTableOperation(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	p.handlePosition()
	p.pushResolvedTs2Table()
	p.handleWorkload()
	p.doGCSchemaStorage()
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

// checkPosition create a new task position, and put it into the etcd state.
// task position maybe be not exist only when the processor is running first time.
func (p *processor) checkPosition() (skipThisTick bool) {
	if p.changefeed.TaskPositions[p.captureInfo.ID] != nil {
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

	if err := p.setupPeerMessageHandler(ctx); err != nil {
		return errors.Trace(err)
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
	s, err := sink.NewSink(stdCtx, p.changefeed.ID, p.changefeed.Info.SinkURI, p.filter, p.changefeed.Info.Config, opts, errCh)
	if err != nil {
		return errors.Trace(err)
	}
	checkpointTs := p.changefeed.Info.GetCheckpointTs(p.changefeed.Status)
	p.sinkManager = sink.NewManager(stdCtx, s, errCh, checkpointTs)
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

	// We use a special context to send the message because the main ctx might have been canceled.
	ctx1, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	err1 := p.sendProcessorFailed(ctx1)
	if err1 != nil {
		// TODO better error handling
		log.Panic("failed to report processor failure to owner. Panic for the protection of whole cluster", zap.Error(err))
	}

	err1 = p.removePeerMessageHandler(ctx1)
	if err1 != nil {
		// TODO better error handling
		log.Panic("failed to report processor failure to owner. Panic for the protection of whole cluster", zap.Error(err))
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
	ok, err := p.syncWithOwner(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		log.Info("processor waiting to sync current status with owner",
			zap.String("changefeed-id", p.changefeedID))
		return nil
	}

	p.pendingOpsMu.Lock()
	for !p.pendingOps.Empty() {
		// TODO add a constant for this value
		ops := p.pendingOps.PopManyFront(128)
		for _, op := range ops {
			op := op.(*model.DispatchTableMessage)
			if _, ok := p.operations[op.ID]; ok {
				log.Warn("skipped duplicate operation", zap.Any("op", op))
				// operation from the previous owner
				continue
			}
			p.operations[op.ID] = op
		}
	}
	// We unlock as early as possible to avoid contention with
	// the peer message handler. Blocking the message handler could
	// block the whole gRPC server.
	p.pendingOpsMu.Unlock()

	for tableID, op := range p.operations {
		globalCheckpointTs := p.changefeed.Status.CheckpointTs

		if !op.Processed {
			// TODO do we need to remove this?
			if op.BoundaryTs != globalCheckpointTs {
				log.Warn("boundary-ts does not match global checkpoint-ts",
					zap.Uint64("globalCheckpointTs", globalCheckpointTs),
					zap.Any("operation", op))
			}
			if op.IsDelete {
				table, ok := p.tables[tableID]
				if !ok {
					log.Warn("table which will be deleted is not found",
						cdcContext.ZapFieldChangefeed(ctx), zap.Int64("tableID", tableID))
				} else if !table.AsyncStop(op.BoundaryTs) {
					// We use a Debug log because it is conceivable for the pipeline to block for a legitimate reason,
					// and we do not want to alarm the user.
					log.Debug("AsyncStop has failed, possible due to a full pipeline",
						zap.Uint64("checkpointTs", table.CheckpointTs()), zap.Int64("tableID", tableID))
					continue
				}
			} else {
				log.Info("adding table", zap.Int64("table-id", tableID))
				err := p.addTable(ctx, tableID, &model.TableReplicaInfo{StartTs: op.BoundaryTs})
				if err != nil {
					return errors.Trace(err)
				}
			}
			op.Processed = true
		} else {
			if op.IsDelete {
				table, exist := p.tables[tableID]
				if !exist {
					log.Warn("table which was added is not found",
						cdcContext.ZapFieldChangefeed(ctx), zap.Int64("tableID", tableID))
					ok, err := p.ackDispatchTable(ctx, tableID)
					if err != nil {
						return errors.Trace(err)
					}
					if ok {
						delete(p.operations, tableID)
					}
					continue
				} else if table.Status() != tablepipeline.TableStatusStopped {
					log.Debug("the table is still not stopped", zap.Uint64("checkpointTs", table.CheckpointTs()), zap.Int64("tableID", tableID))
					continue
				}

				ok, err := p.ackDispatchTable(ctx, tableID)
				if err != nil {
					return errors.Trace(err)
				}
				if ok {
					delete(p.operations, tableID)
				}

				table.Cancel()
				table.Wait()
				delete(p.tables, tableID)
				log.Debug("Operation done signal received",
					cdcContext.ZapFieldChangefeed(ctx),
					zap.Int64("tableID", tableID),
					zap.Reflect("operation", op))
			} else {
				table, exist := p.tables[tableID]
				if !exist {
					log.Panic("table which was added is not found",
						cdcContext.ZapFieldChangefeed(ctx), zap.Int64("tableID", tableID))
				}
				localResolvedTs := p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs
				globalResolvedTs := p.changefeed.Status.ResolvedTs
				if table.ResolvedTs() >= localResolvedTs && localResolvedTs >= globalResolvedTs {
					ok, err := p.ackDispatchTable(ctx, tableID)
					if err != nil {
						return errors.Trace(err)
					}
					if ok {
						delete(p.operations, tableID)
					}

					log.Debug("Operation done signal received",
						cdcContext.ZapFieldChangefeed(ctx),
						zap.Int64("tableID", tableID),
						zap.Reflect("operation", op))
				}
			}
		}
	}

	return nil
}

func (p *processor) createAndDriveSchemaStorage(ctx cdcContext.Context) (entry.SchemaStorage, error) {
	kvStorage := ctx.GlobalVars().KVStorage
	ddlspans := []regionspan.Span{regionspan.GetDDLSpan(), regionspan.GetAddIndexDDLSpan()}
	checkpointTs := p.changefeed.Info.GetCheckpointTs(p.changefeed.Status)
	ddlPuller := puller.NewPuller(
		ctx,
		ctx.GlobalVars().PDClient,
		ctx.GlobalVars().GrpcPool,
		ctx.GlobalVars().KVStorage,
		checkpointTs, ddlspans, false)
	meta, err := kv.GetSnapshotMeta(kvStorage, checkpointTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaStorage, err := entry.NewSchemaStorage(meta, checkpointTs, p.filter, p.changefeed.Info.Config.ForceReplicate)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.sendError(ddlPuller.Run(ctx))
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
func (p *processor) handlePosition() {
	if !p.waitOwnerAck() {
		log.Debug("waiting for owner to ack pending dispatch responses")
		return
	}

	minResolvedTs := uint64(math.MaxUint64)
	if p.schemaStorage != nil {
		minResolvedTs = p.schemaStorage.ResolvedTs()
	}
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
		if name, ok := p.schemaStorage.GetLastSnapshot().GetTableNameByID(tableID); ok {
			tableName = &name
			return nil
		}
		return errors.Errorf("failed to get table name, fallback to use table id: %d", tableID)
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs), retry.WithMaxTries(maxTries), retry.WithIsRetryableErr(cerror.IsRetryableError))
	if p.changefeed.Info.Config.Cyclic.IsEnabled() {
		// Retry to find mark table ID
		var markTableID model.TableID
		err := retry.Do(context.Background(), func() error {
			if tableName == nil {
				name, exist := p.schemaStorage.GetLastSnapshot().GetTableNameByID(tableID)
				if !exist {
					return cerror.ErrProcessorTableNotFound.GenWithStack("normal table(%s)", tableID)
				}
				tableName = &name
			}
			markTableSchemaName, markTableTableName := mark.GetMarkTableName(tableName.Schema, tableName.Table)
			tableInfo, exist := p.schemaStorage.GetLastSnapshot().GetTableByName(markTableSchemaName, markTableTableName)
			if !exist {
				return cerror.ErrProcessorTableNotFound.GenWithStack("normal table(%s) and mark table not match", tableName.String())
			}
			markTableID = tableInfo.ID
			return nil
		}, retry.WithBackoffMaxDelay(50), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(20))
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
func (p *processor) doGCSchemaStorage() {
	if p.schemaStorage == nil {
		// schemaStorage is nil only in test
		return
	}
	// Delay GC to accommodate pullers starting from a startTs that's too small
	// TODO fix startTs problem and remove GC delay, or use other mechanism that prevents the problem deterministically
	gcTime := oracle.GetTimeFromTS(p.changefeed.Status.CheckpointTs).Add(-schemaStorageGCLag)
	gcTs := oracle.ComposeTS(gcTime.Unix(), 0)
	p.schemaStorage.DoGC(gcTs)
}

func (p *processor) Close() error {
	err1 := p.sendProcessorFailed(context.Background())
	if err1 != nil {
		// TODO better error handling
		log.Panic("failed to report proce ssor failure to owner. Panic for the protection of whole cluster", zap.Error(err1))
	}

	err1 = p.removePeerMessageHandler(context.Background())
	if err1 != nil {
		// TODO better error handling
		log.Panic("failed to report processor failure to owner. Panic for the protection of whole cluster", zap.Error(err1))
	}

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
	if p.sinkManager != nil {
		// pass a canceled context is ok here, since we don't need to wait Close
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return p.sinkManager.Close(ctx)
	}
	return nil
}

func (p *processor) setupPeerMessageHandler(ctx context.Context) error {
	// TODO refactor this function

	doneCh1, _, err := p.messageServer.AddHandler(ctx,
		string(model.DispatchTableTopic(p.changefeedID)),
		&model.DispatchTableMessage{},
		func(senderID string, data interface{}) error {
			msg := data.(*model.DispatchTableMessage)
			log.Debug("processor received message", zap.Any("msg", msg))
			// TODO factor these
			hasOwnerChanged := false
			p.ownerInfoMu.Lock()
			if p.ownerRev < msg.OwnerRev {
				p.ownerID = senderID
				p.ownerRev = msg.OwnerRev
				p.waitForAck = 0
				p.waitForSync = 0
				hasOwnerChanged = true
				log.Debug("processor updated owner ID",
					zap.String("owner-id", senderID),
					zap.Int64("owner-rev", msg.OwnerRev))
			} else if p.ownerRev > msg.OwnerRev {
				log.Info("stale message from old owner, ignore",
					zap.String("owner-id", senderID),
					zap.Int64("owner-rev", msg.OwnerRev))
				p.ownerInfoMu.Unlock()
				return nil
			}
			p.ownerInfoMu.Unlock()

			p.pendingOpsMu.Lock()
			defer p.pendingOpsMu.Unlock()
			if hasOwnerChanged {
				// clears stale ops
				p.pendingOps = deque.NewDeque()
			}
			p.pendingOps.PushBack(data)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}

	doneCh2, _, err := p.messageServer.AddHandler(ctx,
		string(model.RequestSendTableStatusTopic(p.changefeedID)),
		&model.RequestSendTableStatusMessage{},
		func(senderID string, data interface{}) error {
			msg := data.(*model.RequestSendTableStatusMessage)
			log.Debug("processor received message", zap.Any("msg", msg))
			p.ownerInfoMu.Lock()
			if p.ownerRev > msg.OwnerRev {
				log.Info("stale message from old owner, ignore",
					zap.String("owner-id", senderID),
					zap.Int64("owner-rev", msg.OwnerRev))
				p.ownerInfoMu.Unlock()
				return nil
			} else if p.ownerRev == msg.OwnerRev {
				log.Panic("duplicate sync message from owner",
					zap.String("owner-id", senderID),
					zap.Int64("owner-rev", msg.OwnerRev))
			}
			p.ownerID = senderID
			p.ownerRev = msg.OwnerRev
			p.waitForAck = 0
			p.waitForSync = 0
			p.ownerInfoMu.Unlock()

			p.pendingOpsMu.Lock()
			defer p.pendingOpsMu.Unlock()

			// clears the previous pending ops queue
			p.pendingOps = deque.NewDeque()
			p.needSyncWithNewOwner = true
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}

	select {
	case <-ctx.Done():
		return errors.Trace(err)
	case <-doneCh1:
	}
	select {
	case <-ctx.Done():
		return errors.Trace(err)
	case <-doneCh2:
	}

	log.Debug("processor registered message handler")
	return nil
}

func (p *processor) removePeerMessageHandler(ctx context.Context) error {
	// TODO refactor this logical so that it is clearer when the handlers are to be removed.
	doneCh1, err := p.messageServer.RemoveHandler(ctx, string(model.DispatchTableTopic(p.changefeedID)))
	if err != nil {
		return errors.Trace(err)
	}

	doneCh2, err := p.messageServer.RemoveHandler(ctx, string(model.RequestSendTableStatusTopic(p.changefeedID)))
	if err != nil {
		return errors.Trace(err)
	}

	select {
	case <-ctx.Done():
		return errors.Trace(err)
	case <-doneCh1:
	}
	select {
	case <-ctx.Done():
		return errors.Trace(err)
	case <-doneCh2:
	}
	return nil
}

func (p *processor) ackDispatchTable(ctx context.Context, tableID model.TableID) (bool, error) {
	p.ownerInfoMu.RLock()
	ownerID := p.ownerID
	p.ownerInfoMu.RUnlock()

	client := p.messageRouter.GetClient(p2p.SenderID(ownerID))
	if client == nil {
		log.Warn("Cannot find a gRPC client to the owner", zap.String("owner-id", p.ownerID))
		return false, nil
	}

	seq, err := client.TrySendMessage(ctx,
		model.DispatchTableResponseTopic(p.changefeedID), &model.DispatchTableResponseMessage{ID: tableID})
	if err != nil {
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}

	p.ownerInfoMu.Lock()
	if p.waitForAck < seq {
		p.waitForAck = seq
	}
	p.ownerInfoMu.Unlock()

	return true, nil
}

func (p *processor) syncWithOwner(ctx context.Context) (bool, error) {
	p.ownerInfoMu.Lock()
	defer p.ownerInfoMu.Unlock()

	if !p.needSyncWithNewOwner {
		return true, nil
	}

	client := p.messageRouter.GetClient(p2p.SenderID(p.ownerID))
	if client == nil {
		log.Warn("Cannot find a gRPC client to the owner", zap.String("owner-id", p.ownerID))
		return false, nil
	}

	resp := new(model.RequestSendTableStatusResponseMessage)

	// Adds the running tables
	for tableID := range p.tables {
		if _, ok := p.operations[tableID]; ok {
			// there is a pending operation, so this table does not belong here.
			continue
		}
		resp.Running = append(resp.Running, tableID)
	}

	// Adds the tables with currently pending operations
	for tableID, op := range p.operations {
		if op.IsDelete {
			resp.Removing = append(resp.Removing, tableID)
		} else {
			resp.Adding = append(resp.Adding, tableID)
		}
	}

	seq, err := client.TrySendMessage(ctx, model.RequestSendTableStatusResponseTopic(p.changefeedID), resp)
	if err != nil {
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	if p.waitForSync < seq {
		p.waitForSync = seq
	}
	p.needSyncWithNewOwner = false
	return true, nil
}

func (p *processor) waitOwnerAck() bool {
	p.ownerInfoMu.RLock()
	targetAck := p.waitForAck
	targetSync := p.waitForSync
	ownerID := p.ownerID
	p.ownerInfoMu.RUnlock()

	if targetAck == 0 && targetSync == 0 {
		return true
	}

	client := p.messageRouter.GetClient(p2p.SenderID(ownerID))
	if client == nil {
		log.Warn("Cannot find a gRPC client to the owner", zap.String("owner-id", p.ownerID))
		return false
	}

	curAck, ok := client.CurrentAck(model.DispatchTableResponseTopic(p.changefeedID))
	if !ok {
		if targetAck > 0 {
			return false
		}
	} else if curAck < targetAck {
		return false
	}

	curSync, ok := client.CurrentAck(model.RequestSendTableStatusResponseTopic(p.changefeedID))
	if !ok {
		if targetSync > 0 {
			return false
		}
	} else if curSync < targetSync {
		return false
	}
	return true
}

func (p *processor) sendProcessorFailed(ctx context.Context) error {
	p.ownerInfoMu.RLock()
	ownerID := p.ownerID
	p.ownerInfoMu.RUnlock()

	client := p.messageRouter.GetClient(p2p.SenderID(ownerID))
	if client == nil {
		log.Warn("Cannot find a gRPC client to the owner", zap.String("owner-id", p.ownerID))
		// TODO use a proper error type here
		return errors.Trace(errors.New("no gRPC client to owner"))
	}

	_, err := client.SendMessage(ctx, model.ProcessorFailedTopic(p.changefeedID),
		&model.ProcessorFailedMessage{ProcessorID: p.captureInfo.ID})
	return errors.Trace(err)
}

// WriteDebugInfo write the debug info to Writer
func (p *processor) WriteDebugInfo(w io.Writer) {
	fmt.Fprintf(w, "%+v\n", *p.changefeed)
	for tableID, tablePipeline := range p.tables {
		fmt.Fprintf(w, "tableID: %d, tableName: %s, resolvedTs: %d, checkpointTs: %d, status: %s\n",
			tableID, tablePipeline.Name(), tablePipeline.ResolvedTs(), tablePipeline.CheckpointTs(), tablePipeline.Status())
	}
}
