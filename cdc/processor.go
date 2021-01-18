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
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/notify"
	tablepipeline "github.com/pingcap/ticdc/pkg/processor/pipeline"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	tidbkv "github.com/pingcap/tidb/kv"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3/concurrency"
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

	outputFromTable chan *model.PolymorphicEvent
	output2Sink     chan *model.PolymorphicEvent
	mounter         entry.Mounter

	stateMu           sync.Mutex
	status            *model.TaskStatus
	position          *model.TaskPosition
	tables            map[int64]*tablepipeline.TablePipeline
	markTableIDs      map[int64]struct{}
	statusModRevision int64

	localResolvedNotifier     *notify.Notifier
	localResolvedReceiver     *notify.Receiver
	localCheckpointTsNotifier *notify.Notifier
	localCheckpointTsReceiver *notify.Receiver

	wg    *errgroup.Group
	errCh chan<- error
}

// newProcessor creates and returns a processor for the specified change feed
func newProcessor(
	ctx context.Context,
	pdCli pd.Client,
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

	localResolvedNotifier := new(notify.Notifier)
	localCheckpointTsNotifier := new(notify.Notifier)
	localResolvedReceiver, err := localResolvedNotifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, err
	}
	localCheckpointTsReceiver, err := localCheckpointTsNotifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, err
	}

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

		flushCheckpointInterval: flushCheckpointInterval,

		position:        &model.TaskPosition{CheckPointTs: checkpointTs},
		outputFromTable: make(chan *model.PolymorphicEvent),
		output2Sink:     make(chan *model.PolymorphicEvent, defaultOutputChanSize),

		localResolvedNotifier: localResolvedNotifier,
		localResolvedReceiver: localResolvedReceiver,

		checkpointTs:              checkpointTs,
		localCheckpointTsNotifier: localCheckpointTsNotifier,
		localCheckpointTsReceiver: localCheckpointTsReceiver,

		tables:       make(map[int64]*tablepipeline.TablePipeline),
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

func (p *processor) removeTable(tableID int64) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	log.Debug("remove table", zap.String("changefeed", p.changefeedID), zap.Int64("id", tableID))

	table, ok := p.tables[tableID]
	if !ok {
		log.Warn("table not found", zap.String("changefeed", p.changefeedID), zap.Int64("tableID", tableID))
		return
	}

	if table.Status() != tablepipeline.TableStatusStopped {
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

// syncResolved handle `p.ddlJobsCh` and `p.resolvedTxns`
func (p *processor) syncResolved(ctx context.Context) error {
	defer func() {
		log.Info("syncResolved stopped", util.ZapFieldChangefeed(ctx))
	}()

	events := make([]*model.PolymorphicEvent, 0, defaultSyncResolvedBatch)
	rows := make([]*model.RowChangedEvent, 0, defaultSyncResolvedBatch)

	flush2Sink := func() error {
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
		if len(rows) == 0 {
			return nil
		}
		for _, row := range rows {
			log.Info("LEOPPRO: show row", zap.Reflect("row", row))
		}
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
			err := flush2Sink()
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}

	metricFlushDuration := sinkFlushRowChangedDuration.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)

	flushSink := func(resolvedTs model.Ts) error {
		globalResolvedTs := atomic.LoadUint64(&p.globalResolvedTs)
		if resolvedTs > globalResolvedTs {
			resolvedTs = globalResolvedTs
		}
		if resolvedTs == 0 || atomic.LoadUint64(&p.checkpointTs) == resolvedTs {
			return nil
		}
		start := time.Now()

		checkpointTs, err := p.sink.FlushRowChangedEvents(ctx, resolvedTs)
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
		return nil
	}

	var resolvedTs uint64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case row := <-p.output2Sink:
			if row == nil {
				continue
			}
			failpoint.Inject("ProcessorSyncResolvedError", func() {
				failpoint.Return(errors.New("processor sync resolved injected error"))
			})
			if row.RawKV != nil && row.RawKV.OpType == model.OpTypeResolved {
				resolvedTs = row.CRTs
				if err := flush2Sink(); err != nil {
					return errors.Trace(err)
				}
				if err := flushSink(resolvedTs); err != nil {
					return errors.Trace(err)
				}
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
			tableOutputChanSizeGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr).Set(float64(len(p.output2Sink)))
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

func (p *processor) stop(ctx context.Context) error {
	log.Info("stop processor", zap.String("id", p.id), zap.String("capture", p.captureInfo.AdvertiseAddr), zap.String("changefeed", p.changefeedID))
	p.stateMu.Lock()
	for _, tbl := range p.tables {
		tbl.Cancel()
	}
	p.ddlPullerCancel()
	// mark tables share the same context with its original table, don't need to cancel
	p.stateMu.Unlock()
	failpoint.Inject("processorStopDelay", nil)
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

var runProcessorImpl = runProcessor

// runProcessor creates a new processor then starts it.
func runProcessor(
	ctx context.Context,
	pdCli pd.Client,
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
	processor, err := newProcessor(ctx, pdCli, credential, session, info, sink,
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
				zap.String("processor", processor.id),
				zap.Error(err))
		}
		cancel()
	}()

	return processor, nil
}
