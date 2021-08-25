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
	psorter "github.com/pingcap/ticdc/cdc/puller/sorter"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/cdc/sink/common"
	serverConfig "github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/retry"
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
	defaultSyncResolvedBatch = 1024

	schemaStorageGCLag = time.Minute * 20

	// for better sink performance under flow control
	resolvedTsInterpolateInterval = 200 * time.Millisecond
	flushMemoryMetricsDuration    = time.Second * 5
	flowControlOutChSize          = 128
	maxTries                      = 3
)

type oldProcessor struct {
	id           string
	captureInfo  model.CaptureInfo
	changefeedID string
	changefeed   model.ChangeFeedInfo
	stopped      int32

	pdCli    pd.Client
	etcdCli  kv.CDCEtcdClient
	grpcPool kv.GrpcPool
	session  *concurrency.Session

	sinkManager *sink.Manager

	globalResolvedTs         uint64
	localResolvedTs          uint64
	checkpointTs             uint64
	globalCheckpointTs       uint64
	appliedLocalCheckpointTs uint64
	flushCheckpointInterval  time.Duration

	ddlPuller       puller.Puller
	ddlPullerCancel context.CancelFunc
	schemaStorage   entry.SchemaStorage

	mounter entry.Mounter

	stateMu           sync.Mutex
	status            *model.TaskStatus
	position          *model.TaskPosition
	tables            map[int64]*tableInfo
	markTableIDs      map[int64]struct{}
	statusModRevision int64

	globalResolvedTsNotifier  *notify.Notifier
	localResolvedNotifier     *notify.Notifier
	localResolvedReceiver     *notify.Receiver
	localCheckpointTsNotifier *notify.Notifier
	localCheckpointTsReceiver *notify.Receiver

	wg       *errgroup.Group
	errCh    chan<- error
	opDoneCh chan int64
}

type tableInfo struct {
	id           int64
	name         string // quoted schema and table, used in metircs only
	resolvedTs   uint64
	checkpointTs uint64

	markTableID   int64
	mResolvedTs   uint64
	mCheckpointTs uint64
	workload      model.WorkloadInfo
	cancel        context.CancelFunc
}

func (t *tableInfo) loadResolvedTs() uint64 {
	tableRts := atomic.LoadUint64(&t.resolvedTs)
	if t.markTableID != 0 {
		mTableRts := atomic.LoadUint64(&t.mResolvedTs)
		if mTableRts < tableRts {
			return mTableRts
		}
	}
	return tableRts
}

func (t *tableInfo) loadCheckpointTs() uint64 {
	tableCkpt := atomic.LoadUint64(&t.checkpointTs)
	if t.markTableID != 0 {
		mTableCkpt := atomic.LoadUint64(&t.mCheckpointTs)
		if mTableCkpt < tableCkpt {
			return mTableCkpt
		}
	}
	return tableCkpt
}

// newProcessor creates and returns a processor for the specified change feed
func newProcessor(
	ctx context.Context,
	pdCli pd.Client,
	grpcPool kv.GrpcPool,
	session *concurrency.Session,
	changefeed model.ChangeFeedInfo,
	sinkManager *sink.Manager,
	changefeedID string,
	captureInfo model.CaptureInfo,
	checkpointTs uint64,
	errCh chan error,
	flushCheckpointInterval time.Duration,
) (*oldProcessor, error) {
	etcdCli := session.Client()
	cdcEtcdCli := kv.NewCDCEtcdClient(ctx, etcdCli)

	log.Info("start processor with startts",
		zap.Uint64("startts", checkpointTs), util.ZapFieldChangefeed(ctx))
	kvStorage := util.KVStorageFromCtx(ctx)
	ddlspans := []regionspan.Span{regionspan.GetDDLSpan(), regionspan.GetAddIndexDDLSpan()}
	ddlPuller := puller.NewPuller(ctx, pdCli, grpcPool, kvStorage, checkpointTs, ddlspans, false)
	filter, err := filter.NewFilter(changefeed.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaStorage, err := createSchemaStorage(kvStorage, checkpointTs, filter, changefeed.Config.ForceReplicate)
	if err != nil {
		return nil, errors.Trace(err)
	}

	localResolvedNotifier := new(notify.Notifier)
	localCheckpointTsNotifier := new(notify.Notifier)
	globalResolvedTsNotifier := new(notify.Notifier)
	localResolvedReceiver, err := localResolvedNotifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, err
	}
	localCheckpointTsReceiver, err := localCheckpointTsNotifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, err
	}

	p := &oldProcessor{
		id:            uuid.New().String(),
		captureInfo:   captureInfo,
		changefeedID:  changefeedID,
		changefeed:    changefeed,
		pdCli:         pdCli,
		grpcPool:      grpcPool,
		etcdCli:       cdcEtcdCli,
		session:       session,
		sinkManager:   sinkManager,
		ddlPuller:     ddlPuller,
		mounter:       entry.NewMounter(schemaStorage, changefeed.Config.Mounter.WorkerNum, changefeed.Config.EnableOldValue),
		schemaStorage: schemaStorage,
		errCh:         errCh,

		flushCheckpointInterval: flushCheckpointInterval,

		position: &model.TaskPosition{CheckPointTs: checkpointTs},

		globalResolvedTsNotifier: globalResolvedTsNotifier,
		localResolvedNotifier:    localResolvedNotifier,
		localResolvedReceiver:    localResolvedReceiver,

		checkpointTs:              checkpointTs,
		localCheckpointTsNotifier: localCheckpointTsNotifier,
		localCheckpointTsReceiver: localCheckpointTsReceiver,

		tables:       make(map[int64]*tableInfo),
		markTableIDs: make(map[int64]struct{}),

		opDoneCh: make(chan int64, 256),
	}
	modRevision, status, err := p.etcdCli.GetTaskStatus(ctx, p.changefeedID, p.captureInfo.ID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.status = status
	p.statusModRevision = modRevision

	info, _, err := p.etcdCli.GetChangeFeedStatus(ctx, p.changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return nil, errors.Trace(err)
	}

	if err == nil {
		p.globalCheckpointTs = info.CheckpointTs
	}

	for tableID, replicaInfo := range p.status.Tables {
		p.addTable(ctx, tableID, replicaInfo)
	}
	return p, nil
}

func (p *oldProcessor) Run(ctx context.Context) {
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
		err := p.ddlPuller.Run(ddlPullerCtx)
		failpoint.Inject("ProcessorDDLPullerExitDelaying", nil)
		return err
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
			p.sendError(err)
		}
	}()
}

// wait blocks until all routines in processor are returned
func (p *oldProcessor) wait() {
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

func (p *oldProcessor) writeDebugInfo(w io.Writer) {
	fmt.Fprintf(w, "changefeedID:\n\t%s\ninfo:\n\t%s\nstatus:\n\t%+v\nposition:\n\t%s\n",
		p.changefeedID, p.changefeed.String(), p.status, p.position.String())

	fmt.Fprintf(w, "tables:\n")
	p.stateMu.Lock()
	for _, table := range p.tables {
		fmt.Fprintf(w, "\ttable id: %d, resolveTS: %d\n", table.id, table.loadResolvedTs())
	}
	p.stateMu.Unlock()
}

// localResolvedWorker do the flowing works.
// 1, update resolve ts by scanning all table's resolve ts.
// 2, update checkpoint ts by consuming entry from p.executedTxns.
// 3, sync TaskStatus between in memory and storage.
// 4, check admin command in TaskStatus and apply corresponding command
func (p *oldProcessor) positionWorker(ctx context.Context) error {
	lastFlushTime := time.Now()
	retryFlushTaskStatusAndPosition := func() error {
		t0Update := time.Now()
		err := retry.Do(ctx, func() error {
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
					return cerror.ErrAdminStopProcessor.FastGenByArgs()
				}
			}
			return inErr
		}, retry.WithBackoffBaseDelay(500), retry.WithMaxTries(maxTries), retry.WithIsRetryableErr(isRetryable))
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
				ts := table.loadResolvedTs()

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
			checkpointTs := atomic.LoadUint64(&p.globalResolvedTs)
			p.stateMu.Lock()
			for _, table := range p.tables {
				ts := table.loadCheckpointTs()
				if ts < checkpointTs {
					checkpointTs = ts
				}
			}
			p.stateMu.Unlock()
			if checkpointTs == 0 {
				log.Debug("0 is not a valid checkpointTs", util.ZapFieldChangefeed(ctx))
				continue
			}
			atomic.StoreUint64(&p.checkpointTs, checkpointTs)
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
			atomic.StoreUint64(&p.appliedLocalCheckpointTs, checkpointTs)
			lastFlushTime = time.Now()
		}
	}
}

func isRetryable(err error) bool {
	return cerror.IsRetryableError(err) && cerror.ErrAdminStopProcessor.NotEqual(err)
}

func (p *oldProcessor) ddlPullWorker(ctx context.Context) error {
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

func (p *oldProcessor) workloadWorker(ctx context.Context) error {
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
		for _, table := range p.tables {
			workload[table.id] = table.workload
		}
		p.stateMu.Unlock()
		err := p.etcdCli.PutTaskWorkload(ctx, p.changefeedID, p.captureInfo.ID, &workload)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

func (p *oldProcessor) flushTaskPosition(ctx context.Context) error {
	if p.isStopped() {
		return cerror.ErrAdminStopProcessor.GenWithStackByArgs()
	}
	failpoint.Inject("ProcessorUpdatePositionDelaying", nil)
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
func (p *oldProcessor) flushTaskStatusAndPosition(ctx context.Context) error {
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
			// processor reads latest task status from etcd, analyzes operation
			// field and processes table add or delete. If operation is unapplied
			// but stays unchanged after processor handling tables, it means no
			// status is changed and we don't need to flush task status neigher.
			if !taskStatus.Dirty {
				return false, nil
			}
			err = p.flushTaskPosition(ctx)
			return true, err
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

func (p *oldProcessor) removeTable(tableID int64) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	log.Debug("remove table", zap.String("changefeed", p.changefeedID), zap.Int64("id", tableID))

	table, ok := p.tables[tableID]
	if !ok {
		log.Warn("table not found", zap.String("changefeed", p.changefeedID), zap.Int64("tableID", tableID))
		return
	}

	table.cancel()
	delete(p.tables, tableID)
	if table.markTableID != 0 {
		delete(p.markTableIDs, table.markTableID)
	}
	tableResolvedTsGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	syncTableNumGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr).Dec()
}

// handleTables handles table scheduler on this processor, add or remove table puller
func (p *oldProcessor) handleTables(ctx context.Context, status *model.TaskStatus) (tablesToRemove []model.TableID, err error) {
	for tableID, opt := range status.Operation {
		if opt.TableProcessed() {
			continue
		}
		if opt.Delete {
			if opt.BoundaryTs <= p.position.CheckPointTs {
				if opt.BoundaryTs != p.position.CheckPointTs {
					log.Warn("the replication progresses beyond the BoundaryTs and duplicate data may be received by downstream",
						zap.Uint64("local resolved TS", p.position.ResolvedTs), zap.Any("opt", opt))
				}
				table, exist := p.tables[tableID]
				if !exist {
					log.Warn("table which will be deleted is not found",
						util.ZapFieldChangefeed(ctx), zap.Int64("tableID", tableID))
					opt.Done = true
					opt.Status = model.OperFinished
					status.Dirty = true
					continue
				}
				table.cancel()
				checkpointTs := table.loadCheckpointTs()
				log.Debug("stop table", zap.Int64("tableID", tableID),
					util.ZapFieldChangefeed(ctx),
					zap.Any("opt", opt),
					zap.Uint64("checkpointTs", checkpointTs))
				opt.BoundaryTs = checkpointTs
				tablesToRemove = append(tablesToRemove, tableID)
				opt.Done = true
				opt.Status = model.OperFinished
				status.Dirty = true
				tableResolvedTsGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
			}
		} else {
			replicaInfo, exist := status.Tables[tableID]
			if !exist {
				return tablesToRemove, cerror.ErrProcessorTableNotFound.GenWithStack("replicaInfo of table(%d)", tableID)
			}
			if p.changefeed.Config.Cyclic.IsEnabled() && replicaInfo.MarkTableID == 0 {
				return tablesToRemove, cerror.ErrProcessorTableNotFound.GenWithStack("normal table(%d) and mark table not match ", tableID)
			}
			p.addTable(ctx, tableID, replicaInfo)
			opt.Status = model.OperProcessed
			status.Dirty = true
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case tableID := <-p.opDoneCh:
			log.Debug("Operation done signal received",
				util.ZapFieldChangefeed(ctx),
				zap.Int64("tableID", tableID),
				zap.Reflect("operation", status.Operation[tableID]))
			if status.Operation[tableID] == nil {
				log.Debug("TableID does not exist, probably a mark table, ignore",
					util.ZapFieldChangefeed(ctx), zap.Int64("tableID", tableID))
				continue
			}
			status.Operation[tableID].Done = true
			status.Operation[tableID].Status = model.OperFinished
			status.Dirty = true
		default:
			goto done
		}
	}
done:
	if !status.SomeOperationsUnapplied() {
		status.Operation = nil
		// status.Dirty must be true when status changes from `unapplied` to `applied`,
		// setting status.Dirty = true is not **must** here.
		status.Dirty = true
	}
	return tablesToRemove, nil
}

// globalStatusWorker read global resolve ts from changefeed level info and forward `tableInputChans` regularly.
func (p *oldProcessor) globalStatusWorker(ctx context.Context) error {
	log.Info("Global status worker started", util.ZapFieldChangefeed(ctx))

	var (
		changefeedStatus *model.ChangeFeedStatus
		statusRev        int64
		lastCheckPointTs uint64
		lastResolvedTs   uint64
		watchKey         = kv.GetEtcdKeyJob(p.changefeedID)
	)

	updateStatus := func(changefeedStatus *model.ChangeFeedStatus) {
		atomic.StoreUint64(&p.globalCheckpointTs, changefeedStatus.CheckpointTs)
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
			p.globalResolvedTsNotifier.Notify()
		}
	}

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

func createSchemaStorage(
	kvStorage tidbkv.Storage,
	checkpointTs uint64,
	filter *filter.Filter,
	forceReplicate bool,
) (entry.SchemaStorage, error) {
	meta, err := kv.GetSnapshotMeta(kvStorage, checkpointTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return entry.NewSchemaStorage(meta, checkpointTs, filter, forceReplicate)
}

func (p *oldProcessor) addTable(ctx context.Context, tableID int64, replicaInfo *model.TableReplicaInfo) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	var tableName string

	err := retry.Do(ctx, func() error {
		if name, ok := p.schemaStorage.GetLastSnapshot().GetTableNameByID(tableID); ok {
			tableName = name.QuoteString()
			return nil
		}
		return errors.Errorf("failed to get table name, fallback to use table id: %d", tableID)
	}, retry.WithBackoffBaseDelay(5), retry.WithMaxTries(maxTries), retry.WithIsRetryableErr(cerror.IsRetryableError))
	if err != nil {
		log.Warn("get table name for metric", util.ZapFieldChangefeed(ctx), zap.String("error", err.Error()))
		tableName = strconv.Itoa(int(tableID))
	}

	if _, ok := p.tables[tableID]; ok {
		log.Warn("Ignore existing table", util.ZapFieldChangefeed(ctx), zap.Int64("ID", tableID))
		return
	}

	globalCheckpointTs := atomic.LoadUint64(&p.globalCheckpointTs)

	if replicaInfo.StartTs < globalCheckpointTs {
		// use Warn instead of Panic in case that p.globalCheckpointTs has not been initialized.
		// The cdc_state_checker will catch a real inconsistency in integration tests.
		log.Warn("addTable: startTs < checkpoint",
			util.ZapFieldChangefeed(ctx),
			zap.Int64("tableID", tableID),
			zap.Uint64("checkpoint", globalCheckpointTs),
			zap.Uint64("startTs", replicaInfo.StartTs))
	}

	globalResolvedTs := atomic.LoadUint64(&p.globalResolvedTs)
	log.Debug("Add table", zap.Int64("tableID", tableID),
		util.ZapFieldChangefeed(ctx),
		zap.String("name", tableName),
		zap.Any("replicaInfo", replicaInfo),
		zap.Uint64("globalResolvedTs", globalResolvedTs))

	ctx = util.PutTableInfoInCtx(ctx, tableID, tableName)
	ctx, cancel := context.WithCancel(ctx)
	table := &tableInfo{
		id:         tableID,
		name:       tableName,
		resolvedTs: replicaInfo.StartTs,
	}
	// TODO(leoppro) calculate the workload of this table
	// We temporarily set the value to constant 1
	table.workload = model.WorkloadInfo{Workload: 1}

	startPuller := func(tableID model.TableID, pResolvedTs *uint64, pCheckpointTs *uint64) sink.Sink {
		// start table puller
		span := regionspan.GetTableSpan(tableID)
		kvStorage := util.KVStorageFromCtx(ctx)
		// NOTICE: always pull the old value internally
		// See also: TODO(hi-rustin): add issue link here.
		plr := puller.NewPuller(ctx, p.pdCli, p.grpcPool, kvStorage,
			replicaInfo.StartTs, []regionspan.Span{span}, true)
		go func() {
			err := plr.Run(ctx)
			if errors.Cause(err) != context.Canceled {
				p.sendError(err)
			}
		}()

		var sorter puller.EventSorter
		switch p.changefeed.Engine {
		case model.SortInMemory:
			sorter = puller.NewEntrySorter()
		case model.SortUnified, model.SortInFile /* `file` becomes an alias of `unified` for backward compatibility */ :
			if p.changefeed.Engine == model.SortInFile {
				log.Warn("File sorter is obsolete. Please revise your changefeed settings and use unified sorter",
					util.ZapFieldChangefeed(ctx))
			}
			err := psorter.UnifiedSorterCheckDir(p.changefeed.SortDir)
			if err != nil {
				p.sendError(errors.Trace(err))
				return nil
			}
			sorter, err = psorter.NewUnifiedSorter(p.changefeed.SortDir, p.changefeedID, tableName, tableID, util.CaptureAddrFromCtx(ctx))
			if err != nil {
				p.sendError(errors.Trace(err))
				return nil
			}
		default:
			p.sendError(cerror.ErrUnknownSortEngine.GenWithStackByArgs(p.changefeed.Engine))
			return nil
		}
		failpoint.Inject("ProcessorAddTableError", func() {
			p.sendError(errors.New("processor add table injected error"))
			failpoint.Return(nil)
		})
		go func() {
			err := sorter.Run(ctx)
			if errors.Cause(err) != context.Canceled {
				p.sendError(err)
			}
		}()

		go func() {
			p.pullerConsume(ctx, plr, sorter)
		}()

		tableSink := p.sinkManager.CreateTableSink(tableID, replicaInfo.StartTs)
		go func() {
			p.sorterConsume(ctx, tableID, sorter, pResolvedTs, pCheckpointTs, replicaInfo, tableSink)
		}()
		return tableSink
	}
	var tableSink, mTableSink sink.Sink
	if p.changefeed.Config.Cyclic.IsEnabled() && replicaInfo.MarkTableID != 0 {
		mTableID := replicaInfo.MarkTableID
		// we should to make sure a mark table is only listened once.
		if _, exist := p.markTableIDs[mTableID]; !exist {
			p.markTableIDs[mTableID] = struct{}{}
			table.markTableID = mTableID
			table.mResolvedTs = replicaInfo.StartTs

			mTableSink = startPuller(mTableID, &table.mResolvedTs, &table.mCheckpointTs)
		}
	}

	p.tables[tableID] = table
	if p.position.CheckPointTs > replicaInfo.StartTs {
		p.position.CheckPointTs = replicaInfo.StartTs
	}
	if p.position.ResolvedTs > replicaInfo.StartTs {
		p.position.ResolvedTs = replicaInfo.StartTs
	}

	atomic.StoreUint64(&p.localResolvedTs, p.position.ResolvedTs)
	tableSink = startPuller(tableID, &table.resolvedTs, &table.checkpointTs)
	table.cancel = func() {
		cancel()
		if tableSink != nil {
			tableSink.Close(ctx)
		}
		if mTableSink != nil {
			mTableSink.Close(ctx)
		}
	}
	syncTableNumGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr).Inc()
}

// runFlowControl controls the flow of events out of the sorter.
func (p *oldProcessor) runFlowControl(
	ctx context.Context,
	tableID model.TableID,
	flowController *common.TableFlowController,
	inCh <-chan *model.PolymorphicEvent,
	outCh chan<- *model.PolymorphicEvent) {
	var (
		lastSendResolvedTsTime       time.Time
		lastCRTs, lastSentResolvedTs uint64
	)

	for {
		select {
		case <-ctx.Done():
			// NOTE: This line is buggy, because `context.Canceled` may indicate an actual error.
			// TODO Will be resolved together with other similar problems.
			if errors.Cause(ctx.Err()) != context.Canceled {
				p.sendError(ctx.Err())
			}
			return
		case event, ok := <-inCh:
			if !ok {
				// sorter output channel has been closed.
				// The sorter must have exited and has a reportable exit reason,
				// so we don't need to worry about sending an error here.
				log.Info("sorter output channel closed",
					zap.Int64("tableID", tableID), util.ZapFieldChangefeed(ctx))
				return
			}

			if event == nil || event.RawKV == nil {
				// This is an invariant violation.
				log.Panic("unexpected empty event", zap.Reflect("event", event))
			}

			if event.RawKV.OpType != model.OpTypeResolved {
				size := uint64(event.RawKV.ApproximateSize())
				commitTs := event.CRTs
				// We interpolate a resolved-ts if none has been sent for some time.
				if time.Since(lastSendResolvedTsTime) > resolvedTsInterpolateInterval {
					// Refer to `cdc/processor/pipeline/sorter.go` for detailed explanation of the design.
					// This is a backport.
					if lastCRTs > lastSentResolvedTs && commitTs > lastCRTs {
						lastSentResolvedTs = lastCRTs
						lastSendResolvedTsTime = time.Now()
						interpolatedEvent := model.NewResolvedPolymorphicEvent(0, lastCRTs)

						select {
						case <-ctx.Done():
							// TODO fix me
							if errors.Cause(ctx.Err()) != context.Canceled {
								p.sendError(ctx.Err())
							}
							return
						case outCh <- interpolatedEvent:
						}
					}
				}
				// NOTE we allow the quota to be exceeded if blocking means interrupting a transaction.
				// Otherwise the pipeline would deadlock.
				err := flowController.Consume(commitTs, size, func() error {
					if lastCRTs > lastSentResolvedTs {
						// If we are blocking, we send a Resolved Event here to elicit a sink-flush.
						// Not sending a Resolved Event here will very likely deadlock the pipeline.
						// NOTE: This is NOT an optimization, but is for liveness.
						lastSentResolvedTs = lastCRTs
						lastSendResolvedTsTime = time.Now()

						msg := model.NewResolvedPolymorphicEvent(0, lastCRTs)
						select {
						case <-ctx.Done():
							return ctx.Err()
						case outCh <- msg:
						}
					}
					return nil
				})
				if err != nil {
					log.Error("flow control error", zap.Error(err))
					if cerror.ErrFlowControllerAborted.Equal(err) {
						log.Info("flow control cancelled for table",
							zap.Int64("tableID", tableID),
							util.ZapFieldChangefeed(ctx))
					} else {
						p.sendError(ctx.Err())
					}
					return
				}
				lastCRTs = commitTs
			} else {
				// handle OpTypeResolved
				if event.CRTs < lastSentResolvedTs {
					continue
				}
				lastSentResolvedTs = event.CRTs
				lastSendResolvedTsTime = time.Now()
			}

			select {
			case <-ctx.Done():
				// TODO fix me
				if errors.Cause(ctx.Err()) != context.Canceled {
					p.sendError(ctx.Err())
				}
				return
			case outCh <- event:
			}
		}
	}
}

// sorterConsume receives sorted PolymorphicEvent from sorter of each table and
// sends to processor's output chan
func (p *oldProcessor) sorterConsume(
	ctx context.Context,
	tableID int64,
	sorter puller.EventSorter,
	pResolvedTs *uint64,
	pCheckpointTs *uint64,
	replicaInfo *model.TableReplicaInfo,
	sink sink.Sink,
) {
	var lastResolvedTs uint64
	opDone := false
	resolvedGauge := tableResolvedTsGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	checkDoneTicker := time.NewTicker(1 * time.Second)
	checkDone := func() {
		localResolvedTs := atomic.LoadUint64(&p.localResolvedTs)
		globalResolvedTs := atomic.LoadUint64(&p.globalResolvedTs)
		tableCheckPointTs := atomic.LoadUint64(pCheckpointTs)
		localCheckpoint := atomic.LoadUint64(&p.appliedLocalCheckpointTs)

		if !opDone && lastResolvedTs >= localResolvedTs && localResolvedTs >= globalResolvedTs &&
			tableCheckPointTs >= localCheckpoint {

			log.Debug("localResolvedTs >= globalResolvedTs, sending operation done signal",
				zap.Uint64("localResolvedTs", localResolvedTs), zap.Uint64("globalResolvedTs", globalResolvedTs),
				zap.Int64("tableID", tableID), util.ZapFieldChangefeed(ctx))

			opDone = true
			checkDoneTicker.Stop()
			select {
			case <-ctx.Done():
				if errors.Cause(ctx.Err()) != context.Canceled {
					p.sendError(ctx.Err())
				}
				return
			case p.opDoneCh <- tableID:
			}
		}
		if !opDone {
			log.Debug("addTable not done",
				util.ZapFieldChangefeed(ctx),
				zap.Uint64("tableResolvedTs", lastResolvedTs),
				zap.Uint64("localResolvedTs", localResolvedTs),
				zap.Uint64("globalResolvedTs", globalResolvedTs),
				zap.Uint64("tableCheckpointTs", tableCheckPointTs),
				zap.Uint64("localCheckpointTs", localCheckpoint),
				zap.Int64("tableID", tableID))
		}
	}

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
			colLen := len(ev.Row.Columns)
			preColLen := len(ev.Row.PreColumns)

			// This indicates that it is an update event,
			// and after enable old value internally by default(but disable in the configuration).
			// We need to handle the update event to be compatible with the old format.
			if !p.changefeed.Config.EnableOldValue && colLen != 0 && preColLen != 0 && colLen == preColLen {
				if shouldSplitUpdateEventRow(ev.Row) {
					deleteEventRow, insertEventRow, err := splitUpdateEventRow(ev.Row)
					if err != nil {
						return errors.Trace(err)
					}
					// NOTICE: Please do not change the order, the delete event always comes before the insert event.
					rows = append(rows, deleteEventRow, insertEventRow)
				} else {
					// If the handle key columns are not updated, PreColumns is directly ignored.
					ev.Row.PreColumns = nil
					rows = append(rows, ev.Row)
				}
			} else {
				rows = append(rows, ev.Row)
			}
		}
		failpoint.Inject("ProcessorSyncResolvedPreEmit", func() {
			log.Info("Prepare to panic for ProcessorSyncResolvedPreEmit")
			time.Sleep(10 * time.Second)
			panic("ProcessorSyncResolvedPreEmit")
		})
		err := sink.EmitRowChangedEvents(ctx, rows...)
		if err != nil {
			return errors.Trace(err)
		}
		events = events[:0]
		rows = rows[:0]
		return nil
	}

	processRowChangedEvent := func(event *model.PolymorphicEvent) error {
		events = append(events, event)

		if len(events) >= defaultSyncResolvedBatch {
			err := flushRowChangedEvents()
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}

	globalResolvedTsReceiver, err := p.globalResolvedTsNotifier.NewReceiver(500 * time.Millisecond)
	if err != nil {
		if errors.Cause(err) != context.Canceled {
			p.errCh <- errors.Trace(err)
		}
		return
	}
	defer globalResolvedTsReceiver.Stop()

	perTableMemoryQuota := serverConfig.GetGlobalServerConfig().PerTableMemoryQuota
	log.Debug("creating table flow controller",
		zap.Int64("table-id", tableID),
		zap.Uint64("quota", perTableMemoryQuota),
		util.ZapFieldChangefeed(ctx))

	flowController := common.NewTableFlowController(perTableMemoryQuota)
	defer func() {
		flowController.Abort()
		tableMemoryGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	}()

	sendResolvedTs2Sink := func() error {
		localResolvedTs := atomic.LoadUint64(&p.localResolvedTs)
		globalResolvedTs := atomic.LoadUint64(&p.globalResolvedTs)
		var minTs uint64
		if localResolvedTs < globalResolvedTs {
			minTs = localResolvedTs
			log.Warn("the local resolved ts is less than the global resolved ts",
				zap.Uint64("localResolvedTs", localResolvedTs), zap.Uint64("globalResolvedTs", globalResolvedTs))
		} else {
			minTs = globalResolvedTs
		}
		if minTs == 0 {
			return nil
		}

		checkpointTs, err := sink.FlushRowChangedEvents(ctx, minTs)
		if err != nil {
			if errors.Cause(err) != context.Canceled {
				p.sendError(errors.Trace(err))
			}
			return err
		}

		if checkpointTs < replicaInfo.StartTs {
			checkpointTs = replicaInfo.StartTs
		}

		if checkpointTs != 0 {
			atomic.StoreUint64(pCheckpointTs, checkpointTs)
			flowController.Release(checkpointTs)
			p.localCheckpointTsNotifier.Notify()
		}
		return nil
	}

	flowControlOutCh := make(chan *model.PolymorphicEvent, flowControlOutChSize)
	go func() {
		p.runFlowControl(ctx, tableID, flowController, sorter.Output(), flowControlOutCh)
		close(flowControlOutCh)
	}()

	metricsTableMemoryGauge := tableMemoryGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	metricsTicker := time.NewTicker(flushMemoryMetricsDuration)
	defer metricsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Cause(ctx.Err()) != context.Canceled {
				p.sendError(ctx.Err())
			}
			return
		case <-metricsTicker.C:
			metricsTableMemoryGauge.Set(float64(flowController.GetConsumption()))
		case pEvent := <-flowControlOutCh:
			if pEvent == nil {
				continue
			}

			pEvent.SetUpFinishedChan()
			select {
			case <-ctx.Done():
				if errors.Cause(ctx.Err()) != context.Canceled {
					p.sendError(ctx.Err())
				}
				return
			case p.mounter.Input() <- pEvent:
			}

			if pEvent.RawKV != nil && pEvent.RawKV.OpType == model.OpTypeResolved {
				if pEvent.CRTs == 0 {
					continue
				}
				err := flushRowChangedEvents()
				if err != nil {
					if errors.Cause(err) != context.Canceled {
						p.errCh <- errors.Trace(err)
					}
					return
				}
				atomic.StoreUint64(pResolvedTs, pEvent.CRTs)
				lastResolvedTs = pEvent.CRTs
				p.localResolvedNotifier.Notify()
				resolvedGauge.Set(float64(oracle.ExtractPhysical(pEvent.CRTs)))
				if !opDone {
					checkDone()
				}
				continue
			}
			if pEvent.CRTs <= lastResolvedTs || pEvent.CRTs < replicaInfo.StartTs {
				log.Panic("The CRTs of event is not expected, please report a bug",
					util.ZapFieldChangefeed(ctx),
					zap.String("model", "sorter"),
					zap.Uint64("resolvedTs", lastResolvedTs),
					zap.Int64("tableID", tableID),
					zap.Any("replicaInfo", replicaInfo),
					zap.Any("row", pEvent))
			}
			failpoint.Inject("ProcessorSyncResolvedError", func() {
				p.errCh <- errors.New("processor sync resolved injected error")
				failpoint.Return()
			})
			err := processRowChangedEvent(pEvent)
			if err != nil {
				if errors.Cause(err) != context.Canceled {
					p.sendError(ctx.Err())
				}
				return
			}
		case <-globalResolvedTsReceiver.C:
			if err := sendResolvedTs2Sink(); err != nil {
				// error is already sent to processor, so we can just ignore it
				return
			}
		case <-checkDoneTicker.C:
			if !opDone {
				checkDone()
			}
		}
	}
}

// shouldSplitUpdateEventRow determines if the split event is needed to align the old format based on
// whether the handle key column has been modified.
// If the handle key column is modified,
// we need to use splitUpdateEventRow to split the row update event into a row delete and a row insert event.
func shouldSplitUpdateEventRow(rowUpdateChangeEvent *model.RowChangedEvent) bool {
	// nil row will never be split.
	if rowUpdateChangeEvent == nil {
		return false
	}

	handleKeyCount := 0
	equivalentHandleKeyCount := 0
	for i := range rowUpdateChangeEvent.Columns {
		if rowUpdateChangeEvent.Columns[i].Flag.IsHandleKey() && rowUpdateChangeEvent.PreColumns[i].Flag.IsHandleKey() {
			handleKeyCount++
			colValueString := model.ColumnValueString(rowUpdateChangeEvent.Columns[i].Value)
			preColValueString := model.ColumnValueString(rowUpdateChangeEvent.PreColumns[i].Value)
			if colValueString == preColValueString {
				equivalentHandleKeyCount++
			}
		}
	}

	// If the handle key columns are not updated, so we do **not** need to split the event row.
	return !(handleKeyCount == equivalentHandleKeyCount)
}

// splitUpdateEventRow splits a row update event into a row delete and a row insert event.
func splitUpdateEventRow(rowUpdateChangeEvent *model.RowChangedEvent) (*model.RowChangedEvent, *model.RowChangedEvent, error) {
	if rowUpdateChangeEvent == nil {
		return nil, nil, errors.New("nil row cannot be split")
	}

	// If there is an update to handle key columns,
	// we need to split the event into two events to be compatible with the old format.
	deleteEventRow := *rowUpdateChangeEvent

	deleteEventRow.Columns = nil
	for i := range deleteEventRow.PreColumns {
		// NOTICE: Only the handle key pre column is retained in the delete event.
		if !deleteEventRow.PreColumns[i].Flag.IsHandleKey() {
			deleteEventRow.PreColumns[i] = nil
		}
	}
	// Align with the old format if old value disabled.
	deleteEventRow.TableInfoVersion = 0

	insertEventRow := *rowUpdateChangeEvent

	// NOTICE: clean up pre cols for insert event.
	insertEventRow.PreColumns = nil

	return &deleteEventRow, &insertEventRow, nil
}

// pullerConsume receives RawKVEntry from a given puller and sends to sorter
// for data sorting and mounter for data encode
func (p *oldProcessor) pullerConsume(
	ctx context.Context,
	plr puller.Puller,
	sorter puller.EventSorter,
) {
	for {
		select {
		case <-ctx.Done():
			if errors.Cause(ctx.Err()) != context.Canceled {
				p.sendError(ctx.Err())
			}
			return
		case rawKV := <-plr.Output():
			if rawKV == nil {
				continue
			}
			pEvent := model.NewPolymorphicEvent(rawKV)
			sorter.AddEntry(ctx, pEvent)
		}
	}
}

func (p *oldProcessor) stop(ctx context.Context) error {
	log.Info("stop processor", zap.String("id", p.id), zap.String("capture", p.captureInfo.AdvertiseAddr), zap.String("changefeed", p.changefeedID))
	p.stateMu.Lock()
	for _, tbl := range p.tables {
		tbl.cancel()
		tableResolvedTsGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	}
	p.ddlPullerCancel()
	// mark tables share the same context with its original table, don't need to cancel
	p.stateMu.Unlock()
	p.globalResolvedTsNotifier.Close()
	p.localCheckpointTsNotifier.Close()
	p.localResolvedNotifier.Close()
	var errRes error
	if err := p.sinkManager.Close(ctx); err != nil {
		log.Warn("an error occurred when stopping the processor", zap.Error(err))
		errRes = err
	}
	failpoint.Inject("processorStopDelay", nil)
	// send an admin stop error to make goroutine created by processor.Run() exit
	p.sendError(cerror.ErrAdminStopProcessor.GenWithStackByArgs())
	atomic.StoreInt32(&p.stopped, 1)
	if p.wg != nil {
		p.wg.Wait() //nolint:errcheck
	}
	if err := p.etcdCli.DeleteTaskPosition(ctx, p.changefeedID, p.captureInfo.ID); err != nil {
		log.Warn("an error occurred when stopping the processor", zap.Error(err))
		errRes = err
	}
	if err := p.etcdCli.DeleteTaskStatus(ctx, p.changefeedID, p.captureInfo.ID); err != nil {
		log.Warn("an error occurred when stopping the processor", zap.Error(err))
		errRes = err
	}
	if err := p.etcdCli.DeleteTaskWorkload(ctx, p.changefeedID, p.captureInfo.ID); err != nil {
		log.Warn("an error occurred when stopping the processor", zap.Error(err))
		errRes = err
	}
	resolvedTsGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	resolvedTsLagGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	checkpointTsGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	checkpointTsLagGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	syncTableNumGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	processorErrorCounter.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	return errRes
}

func (p *oldProcessor) isStopped() bool {
	return atomic.LoadInt32(&p.stopped) == 1
}

var runProcessorImpl = runProcessor

// runProcessor creates a new processor then starts it.
func runProcessor(
	ctx context.Context,
	pdCli pd.Client,
	grpcPool kv.GrpcPool,
	session *concurrency.Session,
	info model.ChangeFeedInfo,
	changefeedID string,
	captureInfo model.CaptureInfo,
	checkpointTs uint64,
	flushCheckpointInterval time.Duration,
) (*oldProcessor, error) {
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
	// processor only receives one error from the channel, all producers to this
	// channel must use the non-blocking way to send error.
	errCh := make(chan error, 1)
	s, err := sink.NewSink(ctx, changefeedID, info.SinkURI, filter, info.Config, opts, errCh)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	sinkManager := sink.NewManager(ctx, s, errCh, checkpointTs, captureInfo.AdvertiseAddr, changefeedID)
	processor, err := newProcessor(ctx, pdCli, grpcPool, session, info, sinkManager,
		changefeedID, captureInfo, checkpointTs, errCh, flushCheckpointInterval)
	if err != nil {
		cancel()
		return nil, err
	}
	log.Info("start to run processor", zap.String("changefeed", changefeedID), zap.String("processor", processor.id))

	processorErrorCounter.WithLabelValues(changefeedID, captureInfo.AdvertiseAddr).Add(0)
	processor.Run(ctx)

	go func() {
		err := <-errCh
		cancel()
		processor.wait()
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
			timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err = processor.etcdCli.PutTaskPositionOnChange(timeoutCtx, processor.changefeedID, processor.captureInfo.ID, processor.position)
			if err != nil {
				log.Warn("upload processor error failed", util.ZapFieldChangefeed(ctx), zap.Error(err))
			}
			timeoutCancel()
		} else {
			log.Info("processor exited",
				util.ZapFieldCapture(ctx),
				zap.String("changefeed", changefeedID),
				zap.String("processor", processor.id))
		}
	}()

	return processor, nil
}

func (p *oldProcessor) sendError(err error) {
	select {
	case p.errCh <- err:
	default:
		log.Error("processor receives redundant error", zap.Error(err))
	}
}
