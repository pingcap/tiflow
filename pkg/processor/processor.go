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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cdccontext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	tablepipeline "github.com/pingcap/ticdc/pkg/processor/pipeline"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/tikv/client-go/txnkv/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	// TODO: processor output chan size, the accumulated data is determined by
	// the count of sorted data and unmounted data. In current benchmark a single
	// processor can reach 50k-100k QPS, and accumulated data is around
	// 200k-400k in most cases. We need a better chan cache mechanism.
	defaultOutputChanSize = 1280000

	// defaultMemBufferCapacity is the default memory buffer per change feed.
	defaultMemBufferCapacity int64 = 10 * 1024 * 1024 * 1024 // 10G

	defaultSyncResolvedBatch = 1024

	schemaStorageGCLag = time.Minute * 20
)

//resolvedTsGauge := resolvedTsGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
//metricResolvedTsLagGauge := resolvedTsLagGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
//checkpointTsGauge := checkpointTsGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
//metricCheckpointTsLagGauge := checkpointTsLagGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
type Processor struct {
	changefeed changefeedState

	tables map[model.TableID]*tablepipeline.TablePipeline

	pdCli         pd.Client
	limitter      *puller.BlurResourceLimitter
	credential    *security.Credential
	captureInfo   model.CaptureInfo
	schemaStorage *entry.SchemaStorage
	mounter       entry.Mounter

	errCh chan error
}

func NewProcessor(
	ctx context.Context,
	pdCli pd.Client,
	credential *security.Credential,
	captureInfo model.CaptureInfo,
	checkpointTs uint64,
) (*Processor, error) {

	log.Info("start processor with startts",
		zap.Uint64("startts", checkpointTs), util.ZapFieldChangefeed(ctx))
	return &Processor{
		pdCli:       pdCli,
		credential:  credential,
		captureInfo: captureInfo,
		limitter:    puller.NewBlurResourceLimmter(defaultMemBufferCapacity),
	}, nil
}

func (p *Processor) Tick(ctx context.Context, state changefeedState) (nextState orchestrator.ReactorState, err error) {
	p.changefeed = state
	if p.changefeed.taskStatus.AdminJobType.IsStopState() {
		return nil, cerror.ErrAdminStopProcessor.GenWithStackByArgs()
	}
	if p.schemaStorage == nil {
		var err error
		p.schemaStorage, err = p.createAndDriveSchemaStorage(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if p.mounter == nil {
		p.mounter = entry.NewMounter(p.schemaStorage, p.changefeed.info.Config.Mounter.WorkerNum, p.changefeed.info.Config.EnableOldValue)
	}
	if err := p.handleTableOperation(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	if err := p.initTables(ctx); err != nil {
		return nil, errors.Trace(err)
	}

	// TODO calculate position
	return p.changefeed, nil
}

func (p *Processor) handleTableOperation(ctx context.Context) error {
	patchOperation := func(tableID model.TableID, fn func(operation *model.TableOperation) error) {
		p.changefeed.PatchTaskStatus(func(status *model.TaskStatus) error {
			if status.Operation == nil {
				log.Panic("Operation not found, may be remove by other patch", zap.Int64("tableID", tableID), zap.Any("status", status))
			}
			opt := status.Operation[tableID]
			if opt == nil {
				log.Panic("Operation not found, may be remove by other patch", zap.Int64("tableID", tableID), zap.Any("status", status))
			}
			if err := fn(opt); err != nil {
				return errors.Trace(err)
			}
			return nil
		})
	}
	for tableID, opt := range p.changefeed.taskStatus.Operation {
		if opt.TableApplied() {
			continue
		}
		localCheckpointTs := p.changefeed.taskPosition.CheckPointTs
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
					continue
				}
				switch opt.Status {
				case model.OperDispatched:
					table.AsyncStop()
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
					}
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
				replicaInfo, exist := p.changefeed.taskStatus.Tables[tableID]
				if !exist {
					return cerror.ErrProcessorTableNotFound.GenWithStack("replicaInfo of table(%d)", tableID)
				}
				if p.changefeed.info.Config.Cyclic.IsEnabled() && replicaInfo.MarkTableID == 0 {
					return cerror.ErrProcessorTableNotFound.GenWithStack("normal table(%d) and mark table not match ", modification.TableID))
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
				localResolvedTs := p.changefeed.taskPosition.ResolvedTs
				globalResolvedTs := p.changefeed.status.ResolvedTs
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

func (p *Processor) createAndDriveSchemaStorage(ctx context.Context) (*entry.SchemaStorage, error) {
	kvStorage, err := util.KVStorageFromCtx(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddlspans := []regionspan.Span{regionspan.GetDDLSpan(), regionspan.GetAddIndexDDLSpan()}
	checkpointTs := p.changefeed.info.GetCheckpointTs(p.changefeed.status)
	ddlPuller := puller.NewPuller(ctx, p.pdCli, p.credential, kvStorage, checkpointTs, ddlspans, p.limitter, false)
	filter, err := filter.NewFilter(p.changefeed.info.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	meta, err := kv.GetSnapshotMeta(kvStorage, checkpointTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaStorage, err := entry.NewSchemaStorage(meta, checkpointTs, filter, p.changefeed.info.Config.ForceReplicate)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddlRawKVCh := puller.SortOutput(ctx, ddlPuller.Output())
	go func() {
		var ddlRawKV *model.RawKVEntry
		for {
			select {
			case <-ctx.Done():
				p.sendError(ctx.Err())
			case ddlRawKV = <-ddlRawKVCh:
			}
			if ddlRawKV == nil {
				continue
			}
			failpoint.Inject("processorDDLResolved", func() {})
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

func (p *Processor) sendError(err error) {
	select {
	case p.errCh <- err:
	default:
		log.Error("processor receives redundant error", zap.Error(err))
	}
}

func (p *Processor) initTables(ctx context.Context) error {
	for tableID, replicaInfo := range p.changefeed.taskStatus.Tables {
		if _, exist := p.tables[tableID]; !exist {
			err := p.addTable(ctx, tableID, replicaInfo)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (p *Processor) handlePosition(ctx context.Context, state changefeedState) error {
	minResolvedTs := p.schemaStorage.ResolvedTs()
	for _, table := range p.tables {
		ts := table.ResolvedTs()

		if ts < minResolvedTs {
			minResolvedTs = ts
		}
	}

	phyTs := oracle.ExtractPhysical(minResolvedTs)
	// It is more accurate to get tso from PD, but in most cases we have
	// deployed NTP service, a little bias is acceptable here.
	metricResolvedTsLagGauge.Set(float64(oracle.GetPhysical(time.Now())-phyTs) / 1e3)
	resolvedTsGauge.Set(float64(phyTs))

	// TODO calc checkpointTs
	minCheckpointTs := uint64(0)
	if minResolvedTs > p.changefeed.taskPosition.ResolvedTs ||
		minCheckpointTs > p.changefeed.taskPosition.CheckPointTs {
		p.changefeed.PatchTaskPosition(func(position *model.TaskPosition) error {
			position.CheckPointTs = minCheckpointTs
			position.ResolvedTs = minResolvedTs
			return nil
		})
	}
	return nil
}

func (p *Processor) handleWorkload(ctx context.Context, state changefeedState) error{

	state.PatchTaskWorkload(func(workload model.TaskWorkload) error {
		workload = make(model.TaskWorkload, len(p.tables))
		for tableID, table := range p.tables {
			workload[tableID] = table.Workload()
		}
		return nil
	})
	return nil
}

func (p *Processor) addTable(ctx context.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) error {
	if table, ok := p.tables[tableID]; ok {
		if table.Status() == tablepipeline.TableStatusStopping {
			log.Warn("The same table exists but is stopping. Cancel it and continue.", util.ZapFieldChangefeed(ctx), zap.Int64("ID", tableID))
			table.Cancel()
		} else if table.Status() == tablepipeline.TableStatusStopped {
			log.Warn("The same table exists but is stopped. Cancel it and continue.", util.ZapFieldChangefeed(ctx), zap.Int64("ID", tableID))
			table.Cancel()
		} else {
			log.Warn("Ignore existing table", util.ZapFieldChangefeed(ctx), zap.Int64("ID", tableID))
			return nil
		}
	}

	globalCheckpointTs := p.changefeed.status.CheckpointTs
	globalResolvedTs := p.changefeed.status.ResolvedTs

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
		Config:        p.changefeed.info.Config,
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
	resolvedTsGauge := tableResolvedTsGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr, tableName)

	resolvedTsListener := func(table *tablepipeline.TablePipeline, resolvedTs model.Ts) {
		p.localResolvedNotifier.Notify()
		resolvedTsGauge.Set(float64(oracle.ExtractPhysical(resolvedTs)))
	}

	_, table := tablepipeline.NewTablePipeline(
		cdcCtx,
		p.credential,
		kvStorage,
		p.limitter,
		p.mounter,
		p.changefeed.info.Engine,
		p.changefeed.info.SortDir,
		tableID,
		tableName,
		replicaInfo,
		p.changefeed.info.GetTargetTs(),
		p.outputFromTable,
		resolvedTsListener,
	)

	go func() {
		for _, err := range table.Wait() {
			if cerror.ErrTableProcessorStoppedSafely.Equal(err) || errors.Cause(err) == context.Canceled {
				continue
			}
			p.sendError(err)
		}
	}()

	log.Debug("Add table", zap.Int64("tableID", tableID),
		util.ZapFieldChangefeed(ctx),
		zap.String("name", table.Name()),
		zap.Any("replicaInfo", replicaInfo),
		zap.Uint64("globalResolvedTs", globalResolvedTs))

	p.tables[tableID] = table

	syncTableNumGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr).Inc()
	return nil
}

/*
// globalStatusWorker read global resolve ts from changefeed level info and forward `tableInputChans` regularly.
func (p *processor) globalStatusWorker(ctx context.Context) error {
	log.Info("Global status worker started", util.ZapFieldChangefeed(ctx))

	var (
		changefeedStatus         *model.ChangeFeedStatus
		statusRev                int64
		lastCheckPointTs         uint64
		lastResolvedTs           uint64
		watchKey                 = kv.GetEtcdKeyJob(p.changefeedID)
		globalResolvedTsNotifier = new(notify.Notifier)
	)
	defer globalResolvedTsNotifier.Close()
	globalResolvedTsReceiver, err := globalResolvedTsNotifier.NewReceiver(1 * time.Second)
	if err != nil {
		return err
	}

	updateStatus := func(changefeedStatus *model.ChangeFeedStatus) {
		atomic.StoreUint64(&p.globalcheckpointTs, changefeedStatus.CheckpointTs)
		if lastResolvedTs == changefeedStatus.ResolvedTs &&
			lastCheckPointTs == changefeedStatus.CheckpointTs {
			return
		}
		if lastCheckPointTs < changefeedStatus.CheckpointTs {
			// Delay GC to accommodate pullers starting from a startTs that's too small
			// TODO fix startTs problem and remove GC delay, or use other mechanism that prevents the problem deterministically
			gcTime := oracle.GetTimeFromTS(changefeedStatus.CheckpointTs).Add(-schemaStorageGCLag)
			gcTs := oracle.ComposeTS(gcTime.Unix(), 0)
			p.schemaStorage.DoGC(gcTs)
			lastCheckPointTs = changefeedStatus.CheckpointTs
		}
		if lastResolvedTs < changefeedStatus.ResolvedTs {
			lastResolvedTs = changefeedStatus.ResolvedTs
			atomic.StoreUint64(&p.globalResolvedTs, lastResolvedTs)
			log.Debug("Update globalResolvedTs",
				zap.Uint64("globalResolvedTs", lastResolvedTs), util.ZapFieldChangefeed(ctx))
			globalResolvedTsNotifier.Notify()
		}
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-p.outputFromTable:
				select {
				case <-ctx.Done():
					return
				case p.output2Sink <- event:
				}
			case <-globalResolvedTsReceiver.C:
				globalResolvedTs := atomic.LoadUint64(&p.globalResolvedTs)
				localResolvedTs := atomic.LoadUint64(&p.localResolvedTs)
				if globalResolvedTs > localResolvedTs {
					log.Warn("globalResolvedTs too large", zap.Uint64("globalResolvedTs", globalResolvedTs),
						zap.Uint64("localResolvedTs", localResolvedTs), util.ZapFieldChangefeed(ctx))
					// we do not issue resolved events if globalResolvedTs > localResolvedTs.
					continue
				}
				select {
				case <-ctx.Done():
					return
				case p.output2Sink <- model.NewResolvedPolymorphicEvent(0, globalResolvedTs):
					// regionID = 0 means the event is produced by TiCDC
				}
			}
		}
	}()

	retryCfg := backoff.WithMaxRetries(
		backoff.WithContext(
			backoff.NewExponentialBackOff(), ctx),
		5,
	)
	for {
		select {
		case <-ctx.Done():
			log.Info("Global resolved worker exited", util.ZapFieldChangefeed(ctx))
			return ctx.Err()
		default:
		}

		err := backoff.Retry(func() error {
			var err error
			changefeedStatus, statusRev, err = p.etcdCli.GetChangeFeedStatus(ctx, p.changefeedID)
			if err != nil {
				if errors.Cause(err) == context.Canceled {
					return backoff.Permanent(err)
				}
				log.Error("Global resolved worker: read global resolved ts failed",
					util.ZapFieldChangefeed(ctx), zap.Error(err))
			}
			return err
		}, retryCfg)
		if err != nil {
			return errors.Trace(err)
		}

		updateStatus(changefeedStatus)

		ch := p.etcdCli.Client.Watch(ctx, watchKey, clientv3.WithRev(statusRev+1), clientv3.WithFilterDelete())
		for resp := range ch {
			if resp.Err() == mvcc.ErrCompacted {
				break
			}
			if resp.Err() != nil {
				return cerror.WrapError(cerror.ErrProcessorEtcdWatch, err)
			}
			for _, ev := range resp.Events {
				var status model.ChangeFeedStatus
				if err := status.Unmarshal(ev.Kv.Value); err != nil {
					return err
				}
				updateStatus(&status)
			}
		}
	}
}
*/
