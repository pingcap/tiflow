// Copyright 2020 PingCAP, Inc.
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

package cdc

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/cdc/sink"
	cdccontext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/notify"
	cdcprocessor "github.com/pingcap/ticdc/pkg/processor"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

var fNewPDCli = pd.NewClientWithContext

type processor struct {
	id           string
	captureInfo  model.CaptureInfo
	changefeedID string
	changefeed   model.ChangeFeedInfo
	limitter     *puller.BlurResourceLimitter
	stopped      int32

	pdCli      pd.Client
	credential *security.Credential
	etcdCli    kv.CDCEtcdClient
	session    *concurrency.Session

	sink sink.Sink

	sinkEmittedResolvedTs   uint64
	globalResolvedTs        uint64
	localResolvedTs         uint64
	checkpointTs            uint64
	globalcheckpointTs      uint64
	flushCheckpointInterval time.Duration

	ddlPuller       puller.Puller
	ddlPullerCancel context.CancelFunc
	schemaStorage   *entry.SchemaStorage

	output  chan *model.PolymorphicEvent
	mounter entry.Mounter

	stateMu           sync.Mutex
	status            *model.TaskStatus
	position          *model.TaskPosition
	tables            map[int64]*cdcprocessor.TablePipeline
	markTableIDs      map[int64]struct{}
	statusModRevision int64

	sinkEmittedResolvedNotifier *notify.Notifier
	sinkEmittedResolvedReceiver *notify.Receiver
	localResolvedNotifier       *notify.Notifier
	localResolvedReceiver       *notify.Receiver
	localCheckpointTsNotifier   *notify.Notifier
	localCheckpointTsReceiver   *notify.Receiver

	wg    *errgroup.Group
	errCh chan<- error
}

// newProcessor creates and returns a processor for the specified change feed
func newProcessor(
	ctx context.Context,
	credential *security.Credential,
	session *concurrency.Session,
	changefeed model.ChangeFeedInfo,
	sink sink.Sink,
	changefeedID string,
	captureInfo model.CaptureInfo,
	checkpointTs uint64,
	errCh chan error,
	flushCheckpointInterval time.Duration,
) (*processor, error) {
	etcdCli := session.Client()
	endpoints := session.Client().Endpoints()
	pdCli, err := fNewPDCli(ctx, endpoints, credential.PDSecurityOption())
	if err != nil {
		return nil, errors.Annotatef(
			cerror.WrapError(cerror.ErrNewProcessorFailed, err), "create pd client failed, addr: %v", endpoints)
	}
	cdcEtcdCli := kv.NewCDCEtcdClient(ctx, etcdCli)
	limitter := puller.NewBlurResourceLimmter(defaultMemBufferCapacity)

	log.Info("start processor with startts",
		zap.Uint64("startts", checkpointTs), util.ZapFieldChangefeed(ctx))
	kvStorage, err := util.KVStorageFromCtx(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddlspans := []regionspan.Span{regionspan.GetDDLSpan(), regionspan.GetAddIndexDDLSpan()}
	ddlPuller := puller.NewPuller(ctx, pdCli, credential, kvStorage, checkpointTs, ddlspans, limitter, false)
	filter, err := filter.NewFilter(changefeed.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaStorage, err := createSchemaStorage(kvStorage, checkpointTs, filter, changefeed.Config.ForceReplicate)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sinkEmittedResolvedNotifier := new(notify.Notifier)
	localResolvedNotifier := new(notify.Notifier)
	localCheckpointTsNotifier := new(notify.Notifier)
	p := &processor{
		id:            uuid.New().String(),
		limitter:      limitter,
		captureInfo:   captureInfo,
		changefeedID:  changefeedID,
		changefeed:    changefeed,
		pdCli:         pdCli,
		credential:    credential,
		etcdCli:       cdcEtcdCli,
		session:       session,
		sink:          sink,
		ddlPuller:     ddlPuller,
		mounter:       entry.NewMounter(schemaStorage, changefeed.Config.Mounter.WorkerNum, changefeed.Config.EnableOldValue),
		schemaStorage: schemaStorage,
		errCh:         errCh,

		position: &model.TaskPosition{CheckPointTs: checkpointTs},
		output:   make(chan *model.PolymorphicEvent, defaultOutputChanSize),

		sinkEmittedResolvedNotifier: sinkEmittedResolvedNotifier,
		sinkEmittedResolvedReceiver: sinkEmittedResolvedNotifier.NewReceiver(50 * time.Millisecond),

		localResolvedNotifier: localResolvedNotifier,
		localResolvedReceiver: localResolvedNotifier.NewReceiver(50 * time.Millisecond),

		checkpointTs:              checkpointTs,
		localCheckpointTsNotifier: localCheckpointTsNotifier,
		localCheckpointTsReceiver: localCheckpointTsNotifier.NewReceiver(50 * time.Millisecond),

		tables:       make(map[int64]*cdcprocessor.TablePipeline),
		markTableIDs: make(map[int64]struct{}),
	}
	modRevision, status, err := p.etcdCli.GetTaskStatus(ctx, p.changefeedID, p.captureInfo.ID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.status = status
	p.statusModRevision = modRevision

	for tableID, replicaInfo := range p.status.Tables {
		p.addTable(ctx, tableID, replicaInfo)
	}
	return p, nil
}

func (p *processor) Run(ctx context.Context) {
	wg, cctx := errgroup.WithContext(ctx)
	p.wg = wg
	ddlPullerCtx, ddlPullerCancel :=
		context.WithCancel(util.PutTableInfoInCtx(cctx, 0, "ticdc-processor-ddl"))
	p.ddlPullerCancel = ddlPullerCancel

	wg.Go(func() error {
		return p.positionWorker(cctx)
	})

	wg.Go(func() error {
		return p.globalStatusWorker(cctx)
	})

	wg.Go(func() error {
		return p.sinkDriver(cctx)
	})

	wg.Go(func() error {
		return p.syncResolved(cctx)
	})

	wg.Go(func() error {
		return p.collectMetrics(cctx)
	})

	wg.Go(func() error {
		return p.ddlPuller.Run(ddlPullerCtx)
	})

	wg.Go(func() error {
		return p.ddlPullWorker(cctx)
	})

	wg.Go(func() error {
		return p.mounter.Run(cctx)
	})

	wg.Go(func() error {
		return p.workloadWorker(cctx)
	})

	go func() {
		if err := wg.Wait(); err != nil {
			select {
			case p.errCh <- err:
			default:
			}
		}
	}()
}

// wait blocks until all routines in processor are returned
func (p *processor) wait() {
	err := p.wg.Wait()
	if err != nil && errors.Cause(err) != context.Canceled {
		log.Error("processor wait error",
			zap.String("capture-id", p.captureInfo.ID),
			zap.String("capture", p.captureInfo.AdvertiseAddr),
			zap.String("changefeed", p.changefeedID),
			zap.Error(err),
		)
	}
}

func (p *processor) writeDebugInfo(w io.Writer) {
	fmt.Fprintf(w, "changefeedID: %s, info: %+v, status: %+v\n", p.changefeedID, p.changefeed, p.status)

	p.stateMu.Lock()
	for tableID, table := range p.tables {
		fmt.Fprintf(w, "\ttable id: %d, resolveTS: %d\n", tableID, table.ResolvedTs())
	}
	p.stateMu.Unlock()

	fmt.Fprintf(w, "\n")
}

// localResolvedWorker do the flowing works.
// 1, update resolve ts by scanning all table's resolve ts.
// 2, update checkpoint ts by consuming entry from p.executedTxns.
// 3, sync TaskStatus between in memory and storage.
// 4, check admin command in TaskStatus and apply corresponding command
func (p *processor) positionWorker(ctx context.Context) error {
	lastFlushTime := time.Now()
	retryFlushTaskStatusAndPosition := func() error {
		t0Update := time.Now()
		err := retry.Run(500*time.Millisecond, 3, func() error {
			inErr := p.flushTaskStatusAndPosition(ctx)
			if inErr != nil {
				if errors.Cause(inErr) != context.Canceled {
					logError := log.Error
					errField := zap.Error(inErr)
					if cerror.ErrAdminStopProcessor.Equal(inErr) {
						logError = log.Warn
						errField = zap.String("error", inErr.Error())
					}
					logError("update info failed", util.ZapFieldChangefeed(ctx), errField)
				}
				if p.isStopped() || cerror.ErrAdminStopProcessor.Equal(inErr) {
					return backoff.Permanent(cerror.ErrAdminStopProcessor.FastGenByArgs())
				}
			}
			return inErr
		})
		updateInfoDuration.
			WithLabelValues(p.captureInfo.AdvertiseAddr).
			Observe(time.Since(t0Update).Seconds())
		if err != nil {
			return errors.Annotate(err, "failed to update info")
		}
		return nil
	}

	defer func() {
		p.localResolvedReceiver.Stop()
		p.localCheckpointTsReceiver.Stop()

		if !p.isStopped() {
			err := retryFlushTaskStatusAndPosition()
			if err != nil && errors.Cause(err) != context.Canceled {
				log.Warn("failed to update info before exit", util.ZapFieldChangefeed(ctx), zap.Error(err))
			}
		}

		log.Info("Local resolved worker exited", util.ZapFieldChangefeed(ctx))
	}()

	resolvedTsGauge := resolvedTsGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	metricResolvedTsLagGauge := resolvedTsLagGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	checkpointTsGauge := checkpointTsGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	metricCheckpointTsLagGauge := checkpointTsLagGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.localResolvedReceiver.C:
			minResolvedTs := p.ddlPuller.GetResolvedTs()
			p.stateMu.Lock()
			for _, table := range p.tables {
				ts := table.ResolvedTs()

				if ts < minResolvedTs {
					minResolvedTs = ts
				}
			}
			p.stateMu.Unlock()
			atomic.StoreUint64(&p.localResolvedTs, minResolvedTs)

			phyTs := oracle.ExtractPhysical(minResolvedTs)
			// It is more accurate to get tso from PD, but in most cases we have
			// deployed NTP service, a little bias is acceptable here.
			metricResolvedTsLagGauge.Set(float64(oracle.GetPhysical(time.Now())-phyTs) / 1e3)
			resolvedTsGauge.Set(float64(phyTs))

			if p.position.ResolvedTs < minResolvedTs {
				p.position.ResolvedTs = minResolvedTs
				if err := retryFlushTaskStatusAndPosition(); err != nil {
					return errors.Trace(err)
				}
			}
		case <-p.localCheckpointTsReceiver.C:
			checkpointTs := atomic.LoadUint64(&p.checkpointTs)
			if checkpointTs == 0 {
				log.Warn("0 is not a valid checkpointTs", util.ZapFieldChangefeed(ctx))
				continue
			}
			phyTs := oracle.ExtractPhysical(checkpointTs)
			// It is more accurate to get tso from PD, but in most cases we have
			// deployed NTP service, a little bias is acceptable here.
			metricCheckpointTsLagGauge.Set(float64(oracle.GetPhysical(time.Now())-phyTs) / 1e3)

			if time.Since(lastFlushTime) < p.flushCheckpointInterval {
				continue
			}

			p.position.CheckPointTs = checkpointTs
			checkpointTsGauge.Set(float64(phyTs))
			if err := retryFlushTaskStatusAndPosition(); err != nil {
				return errors.Trace(err)
			}
			lastFlushTime = time.Now()
		}
	}
}

func (p *processor) ddlPullWorker(ctx context.Context) error {
	ddlRawKVCh := puller.SortOutput(ctx, p.ddlPuller.Output())
	var ddlRawKV *model.RawKVEntry
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case ddlRawKV = <-ddlRawKVCh:
		}
		if ddlRawKV == nil {
			continue
		}
		failpoint.Inject("processorDDLResolved", func() {})
		if ddlRawKV.OpType == model.OpTypeResolved {
			p.schemaStorage.AdvanceResolvedTs(ddlRawKV.CRTs)
			p.localResolvedNotifier.Notify()
		}
		job, err := entry.UnmarshalDDL(ddlRawKV)
		if err != nil {
			return errors.Trace(err)
		}
		if job == nil {
			continue
		}
		if err := p.schemaStorage.HandleDDLJob(job); err != nil {
			return errors.Trace(err)
		}
	}
}

func (p *processor) workloadWorker(ctx context.Context) error {
	t := time.NewTicker(10 * time.Second)
	err := p.etcdCli.PutTaskWorkload(ctx, p.changefeedID, p.captureInfo.ID, nil)
	if err != nil {
		return errors.Trace(err)
	}
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-t.C:
		}
		if p.isStopped() {
			continue
		}
		p.stateMu.Lock()
		workload := make(model.TaskWorkload, len(p.tables))
		for tableID, table := range p.tables {
			workload[tableID] = table.Workload()
		}
		p.stateMu.Unlock()
		err := p.etcdCli.PutTaskWorkload(ctx, p.changefeedID, p.captureInfo.ID, &workload)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

func (p *processor) flushTaskPosition(ctx context.Context) error {
	failpoint.Inject("ProcessorUpdatePositionDelaying", func() {
		time.Sleep(1 * time.Second)
	})
	if p.isStopped() {
		return cerror.ErrAdminStopProcessor.GenWithStackByArgs()
	}
	// p.position.Count = p.sink.Count()
	updated, err := p.etcdCli.PutTaskPositionOnChange(ctx, p.changefeedID, p.captureInfo.ID, p.position)
	if err != nil {
		if errors.Cause(err) != context.Canceled {
			log.Error("failed to flush task position", util.ZapFieldChangefeed(ctx), zap.Error(err))
			return errors.Trace(err)
		}
	}
	if updated {
		log.Debug("flushed task position", util.ZapFieldChangefeed(ctx), zap.Stringer("position", p.position))
	}
	return nil
}

// First try to synchronize task status from etcd.
// If local cached task status is outdated (caused by new table scheduling),
// update it to latest value, and force update task position, since add new
// tables may cause checkpoint ts fallback in processor.
func (p *processor) flushTaskStatusAndPosition(ctx context.Context) error {
	if p.isStopped() {
		return cerror.ErrAdminStopProcessor.GenWithStackByArgs()
	}
	var tablesToRemove []model.TableID
	newTaskStatus, newModRevision, err := p.etcdCli.AtomicPutTaskStatus(ctx, p.changefeedID, p.captureInfo.ID,
		func(modRevision int64, taskStatus *model.TaskStatus) (bool, error) {
			// if the task status is not changed and not operation to handle
			// we need not to change the task status
			if p.statusModRevision == modRevision && !taskStatus.SomeOperationsUnapplied() {
				return false, nil
			}
			// task will be stopped in capture task handler, do nothing
			if taskStatus.AdminJobType.IsStopState() {
				return false, backoff.Permanent(cerror.ErrAdminStopProcessor.GenWithStackByArgs())
			}
			toRemove, err := p.handleTables(ctx, taskStatus)
			tablesToRemove = append(tablesToRemove, toRemove...)
			if err != nil {
				return false, backoff.Permanent(errors.Trace(err))
			}
			err = p.flushTaskPosition(ctx)
			if err != nil {
				return true, errors.Trace(err)
			}
			return true, nil
		})
	if err != nil {
		// not need to check error
		//nolint:errcheck
		p.flushTaskPosition(ctx)
		return errors.Trace(err)
	}
	for _, tableID := range tablesToRemove {
		p.removeTable(tableID)
	}
	// newModRevision == 0 means status is not updated
	if newModRevision > 0 {
		p.statusModRevision = newModRevision
		p.status = newTaskStatus
	}
	syncTableNumGauge.
		WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr).
		Set(float64(len(p.status.Tables)))

	return p.flushTaskPosition(ctx)
}

func (p *processor) removeTable(tableID int64) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	log.Debug("remove table", zap.String("changefeed", p.changefeedID), zap.Int64("id", tableID))

	table, ok := p.tables[tableID]
	if !ok {
		log.Warn("table not found", zap.String("changefeed", p.changefeedID), zap.Int64("tableID", tableID))
		return
	}

	if table.Status() != cdcprocessor.TableStatusStoped {
		return
	}
	table.Cancel()
	delete(p.tables, tableID)
	_, markTableID := table.ID()
	if markTableID != 0 {
		delete(p.markTableIDs, markTableID)
	}
	tableResolvedTsGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr, table.Name())
	syncTableNumGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr).Dec()
}

// handleTables handles table scheduler on this processor, add or remove table puller
func (p *processor) handleTables(ctx context.Context, status *model.TaskStatus) (tablesToRemove []model.TableID, err error) {
	for tableID, opt := range status.Operation {
		if opt.TableApplied() {
			continue
		}
		if opt.Delete {
			if opt.BoundaryTs <= p.position.CheckPointTs {
				p.stateMu.Lock()
				table, exist := p.tables[tableID]
				p.stateMu.Unlock()
				if !exist {
					log.Warn("table which will be deleted is not found",
						util.ZapFieldChangefeed(ctx), zap.Int64("tableID", tableID))
					opt.Done = true
					opt.Status = model.OperFinished
					continue
				}
				switch opt.Status {
				case model.OperDispatched:
					table.AsyncStop()
					opt.Status = model.OperDispatched
				case model.OperProcessed:
					if table.Status() == cdcprocessor.TableStatusStoped && table.ResolvedTs() <= p.position.CheckPointTs {
						tablesToRemove = append(tablesToRemove, tableID)
						opt.Done = true
						opt.BoundaryTs = table.ResolvedTs()
						opt.Status = model.OperFinished
						log.Debug("safeStop table", zap.Int64("tableID", tableID),
							util.ZapFieldChangefeed(ctx),
							zap.Uint64("checkpointTs", table.ResolvedTs()))
					}
				default:
					log.Panic("unreachable")
				}

			}
		} else {
			switch opt.Status {
			case model.OperDispatched:
				replicaInfo, exist := status.Tables[tableID]
				if !exist {
					return tablesToRemove, cerror.ErrProcessorTableNotFound.GenWithStack("replicaInfo of table(%d)", tableID)
				}
				if p.changefeed.Config.Cyclic.IsEnabled() && replicaInfo.MarkTableID == 0 {
					return tablesToRemove, cerror.ErrProcessorTableNotFound.GenWithStack("normal table(%d) and mark table not match ", tableID)
				}
				p.addTable(ctx, tableID, replicaInfo)
				opt.Status = model.OperProcessed
			case model.OperProcessed:
				p.stateMu.Lock()
				table, exist := p.tables[tableID]
				p.stateMu.Unlock()
				if !exist {
					log.Panic("table which was added is not found",
						util.ZapFieldChangefeed(ctx), zap.Int64("tableID", tableID))
				}
				localResolvedTs := atomic.LoadUint64(&p.localResolvedTs)
				globalResolvedTs := atomic.LoadUint64(&p.globalResolvedTs)
				if table.ResolvedTs() >= localResolvedTs && localResolvedTs >= globalResolvedTs {
					opt.Done = true
					opt.Status = model.OperFinished
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

	if !status.SomeOperationsUnapplied() {
		status.Operation = nil
	}
	return tablesToRemove, nil
}

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
		globalResolvedTsReceiver = globalResolvedTsNotifier.NewReceiver(1 * time.Second)
	)
	defer globalResolvedTsNotifier.Close()

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
				case p.output <- model.NewResolvedPolymorphicEvent(0, globalResolvedTs):
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

func (p *processor) sinkDriver(ctx context.Context) error {
	metricFlushDuration := sinkFlushRowChangedDuration.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.sinkEmittedResolvedReceiver.C:
			sinkEmittedResolvedTs := atomic.LoadUint64(&p.sinkEmittedResolvedTs)
			globalResolvedTs := atomic.LoadUint64(&p.globalResolvedTs)
			var minTs uint64
			if sinkEmittedResolvedTs < globalResolvedTs {
				minTs = sinkEmittedResolvedTs
			} else {
				minTs = globalResolvedTs
			}
			if minTs == 0 || atomic.LoadUint64(&p.checkpointTs) == minTs {
				continue
			}
			start := time.Now()

			checkpointTs, err := p.sink.FlushRowChangedEvents(ctx, minTs)
			if err != nil {
				return errors.Trace(err)
			}
			if checkpointTs != 0 {
				atomic.StoreUint64(&p.checkpointTs, checkpointTs)
				p.localCheckpointTsNotifier.Notify()
			}

			dur := time.Since(start)
			metricFlushDuration.Observe(dur.Seconds())
			if dur > 3*time.Second {
				log.Warn("flush row changed events too slow",
					zap.Duration("duration", dur), util.ZapFieldChangefeed(ctx))
			}
		}
	}
}

// syncResolved handle `p.ddlJobsCh` and `p.resolvedTxns`
func (p *processor) syncResolved(ctx context.Context) error {
	defer func() {
		p.sinkEmittedResolvedReceiver.Stop()
		log.Info("syncResolved stopped", util.ZapFieldChangefeed(ctx))
	}()

	events := make([]*model.PolymorphicEvent, 0, defaultSyncResolvedBatch)
	rows := make([]*model.RowChangedEvent, 0, defaultSyncResolvedBatch)

	flushRowChangedEvents := func() error {
		for _, ev := range events {
			err := ev.WaitPrepare(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			if ev.Row == nil {
				continue
			}
			rows = append(rows, ev.Row)
		}
		failpoint.Inject("ProcessorSyncResolvedPreEmit", func() {
			log.Info("Prepare to panic for ProcessorSyncResolvedPreEmit")
			time.Sleep(10 * time.Second)
			panic("ProcessorSyncResolvedPreEmit")
		})
		err := p.sink.EmitRowChangedEvents(ctx, rows...)
		if err != nil {
			return errors.Trace(err)
		}
		events = events[:0]
		rows = rows[:0]
		return nil
	}

	processRowChangedEvent := func(row *model.PolymorphicEvent) error {
		events = append(events, row)

		if len(events) >= defaultSyncResolvedBatch {
			err := flushRowChangedEvents()
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}

	var resolvedTs uint64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case row := <-p.output:
			if row == nil {
				continue
			}
			failpoint.Inject("ProcessorSyncResolvedError", func() {
				failpoint.Return(errors.New("processor sync resolved injected error"))
			})
			if row.RawKV != nil && row.RawKV.OpType == model.OpTypeResolved {
				err := flushRowChangedEvents()
				if err != nil {
					return errors.Trace(err)
				}
				resolvedTs = row.CRTs
				atomic.StoreUint64(&p.sinkEmittedResolvedTs, row.CRTs)
				p.sinkEmittedResolvedNotifier.Notify()
				continue
			}
			// Global resolved ts should fallback in some table rebalance cases,
			// since the start-ts(from checkpoint ts) or a rebalanced table could
			// be less then the global resolved ts.
			localResolvedTs := atomic.LoadUint64(&p.localResolvedTs)
			if resolvedTs > localResolvedTs {
				log.Info("global resolved ts fallback",
					zap.String("changefeed", p.changefeedID),
					zap.Uint64("localResolvedTs", localResolvedTs),
					zap.Uint64("resolvedTs", resolvedTs),
				)
				resolvedTs = localResolvedTs
			}
			if row.CRTs <= resolvedTs {
				_ = row.WaitPrepare(ctx)
				log.Panic("The CRTs must be greater than the resolvedTs",
					zap.String("model", "processor"),
					zap.String("changefeed", p.changefeedID),
					zap.Uint64("resolvedTs", resolvedTs),
					zap.Any("row", row))
			}
			err := processRowChangedEvent(row)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (p *processor) collectMetrics(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(defaultMetricInterval):
			tableOutputChanSizeGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr).Set(float64(len(p.output)))
		}
	}
}

func createSchemaStorage(
	kvStorage tidbkv.Storage,
	checkpointTs uint64,
	filter *filter.Filter,
	forceReplicate bool,
) (*entry.SchemaStorage, error) {
	meta, err := kv.GetSnapshotMeta(kvStorage, checkpointTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return entry.NewSchemaStorage(meta, checkpointTs, filter, forceReplicate)
}

func (p *processor) addTable(ctx context.Context, tableID int64, replicaInfo *model.TableReplicaInfo) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	if table, ok := p.tables[tableID]; ok {
		if table.Status() == cdcprocessor.TableStatusStopping {
			log.Warn("The same table exists but is stopping. Cancel it and continue.", util.ZapFieldChangefeed(ctx), zap.Int64("ID", tableID))
			table.Cancel()
		} else {
			log.Warn("Ignore existing table", util.ZapFieldChangefeed(ctx), zap.Int64("ID", tableID))
			return
		}
	}

	globalcheckpointTs := atomic.LoadUint64(&p.globalcheckpointTs)

	if replicaInfo.StartTs < globalcheckpointTs {
		log.Warn("addTable: startTs < checkpoint",
			util.ZapFieldChangefeed(ctx),
			zap.Int64("tableID", tableID),
			zap.Uint64("checkpoint", globalcheckpointTs),
			zap.Uint64("startTs", replicaInfo.StartTs))
	}

	globalResolvedTs := atomic.LoadUint64(&p.sinkEmittedResolvedTs)
	cdcCtx, _ := cdccontext.NewContext(ctx, &cdccontext.Vars{
		CaptureAddr:   p.captureInfo.AdvertiseAddr,
		PDClient:      p.pdCli,
		SchemaStorage: p.schemaStorage,
		Config:        p.changefeed.Config,
	})
	kvStorage, err := util.KVStorageFromCtx(ctx)
	if err != nil {
		p.errCh <- err
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

	resolvedTsListener := func(table *cdcprocessor.TablePipeline, resolvedTs model.Ts) {
		p.localResolvedNotifier.Notify()
		resolvedTsGauge.Set(float64(oracle.ExtractPhysical(resolvedTs)))
	}

	_, table := cdcprocessor.NewTablePipeline(
		cdcCtx,
		p.credential,
		kvStorage,
		p.limitter,
		p.mounter,
		p.changefeed.Engine,
		p.changefeed.SortDir,
		tableID,
		tableName,
		replicaInfo,
		p.changefeed.GetTargetTs(),
		p.output,
		resolvedTsListener,
	)

	go func() {
		for _, err := range table.Wait() {
			p.errCh <- err
		}
	}()

	log.Debug("Add table", zap.Int64("tableID", tableID),
		util.ZapFieldChangefeed(ctx),
		zap.String("name", table.Name()),
		zap.Any("replicaInfo", replicaInfo),
		zap.Uint64("globalResolvedTs", globalResolvedTs))

	p.tables[tableID] = table
	if p.position.CheckPointTs > replicaInfo.StartTs {
		p.position.CheckPointTs = replicaInfo.StartTs
	}
	if p.position.ResolvedTs > replicaInfo.StartTs {
		p.position.ResolvedTs = replicaInfo.StartTs
	}

	atomic.StoreUint64(&p.localResolvedTs, p.position.ResolvedTs)
	syncTableNumGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr).Inc()
}

func (p *processor) stop(ctx context.Context) error {
	log.Info("stop processor", zap.String("id", p.id), zap.String("capture", p.captureInfo.AdvertiseAddr), zap.String("changefeed", p.changefeedID))
	p.stateMu.Lock()
	for _, tbl := range p.tables {
		tbl.Cancel()
	}
	p.ddlPullerCancel()
	// mark tables share the same context with its original table, don't need to cancel
	p.stateMu.Unlock()
	atomic.StoreInt32(&p.stopped, 1)
	if err := p.etcdCli.DeleteTaskPosition(ctx, p.changefeedID, p.captureInfo.ID); err != nil {
		return err
	}
	if err := p.etcdCli.DeleteTaskStatus(ctx, p.changefeedID, p.captureInfo.ID); err != nil {
		return err
	}
	if err := p.etcdCli.DeleteTaskWorkload(ctx, p.changefeedID, p.captureInfo.ID); err != nil {
		return err
	}
	return p.sink.Close()
}

func (p *processor) isStopped() bool {
	return atomic.LoadInt32(&p.stopped) == 1
}

// runProcessor creates a new processor then starts it.
func runProcessor(
	ctx context.Context,
	credential *security.Credential,
	session *concurrency.Session,
	info model.ChangeFeedInfo,
	changefeedID string,
	captureInfo model.CaptureInfo,
	checkpointTs uint64,
	flushCheckpointInterval time.Duration,
) (*processor, error) {
	opts := make(map[string]string, len(info.Opts)+2)
	for k, v := range info.Opts {
		opts[k] = v
	}
	opts[sink.OptChangefeedID] = changefeedID
	opts[sink.OptCaptureAddr] = captureInfo.AdvertiseAddr
	ctx = util.PutChangefeedIDInCtx(ctx, changefeedID)
	filter, err := filter.NewFilter(info.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	sink, err := sink.NewSink(ctx, changefeedID, info.SinkURI, filter, info.Config, opts, errCh)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	processor, err := newProcessor(ctx, credential, session, info, sink,
		changefeedID, captureInfo, checkpointTs, errCh, flushCheckpointInterval)
	if err != nil {
		cancel()
		return nil, err
	}
	log.Info("start to run processor", zap.String("changefeed", changefeedID))

	processorErrorCounter.WithLabelValues(changefeedID, captureInfo.AdvertiseAddr).Add(0)
	processor.Run(ctx)

	go func() {
		err := <-errCh
		cause := errors.Cause(err)
		if cause != nil && cause != context.Canceled && cerror.ErrAdminStopProcessor.NotEqual(cause) {
			processorErrorCounter.WithLabelValues(changefeedID, captureInfo.AdvertiseAddr).Inc()
			log.Error("error on running processor",
				util.ZapFieldCapture(ctx),
				zap.String("changefeed", changefeedID),
				zap.String("processor", processor.id),
				zap.Error(err))
			// record error information in etcd
			var code string
			if terror, ok := err.(*errors.Error); ok {
				code = string(terror.RFCCode())
			} else {
				code = string(cerror.ErrProcessorUnknown.RFCCode())
			}
			processor.position.Error = &model.RunningError{
				Addr:    captureInfo.AdvertiseAddr,
				Code:    code,
				Message: err.Error(),
			}
			_, err = processor.etcdCli.PutTaskPositionOnChange(ctx, processor.changefeedID, processor.captureInfo.ID, processor.position)
			if err != nil {
				log.Warn("upload processor error failed", util.ZapFieldChangefeed(ctx), zap.Error(err))
			}
		} else {
			log.Info("processor exited",
				util.ZapFieldCapture(ctx),
				zap.String("changefeed", changefeedID),
				zap.String("processor", processor.id))
		}
		cancel()
	}()

	return processor, nil
}
