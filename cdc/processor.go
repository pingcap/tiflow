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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/cdc/roles/storage"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	resolveTsInterval = time.Millisecond * 500

	// TODO: processor output chan size, the accumulated data is determined by
	// the count of sorted data and unmounted data. In current benchmark a single
	// processor can reach 50k-100k QPS, and accumulated data is around
	// 200k-400k in most cases. We need a better chan cache mechanism.
	defaultOutputChanSize = 1280000

	// defaultMemBufferCapacity is the default memory buffer per change feed.
	defaultMemBufferCapacity int64 = 10 * 1024 * 1024 * 1024 // 10G

	defaultSyncResolvedBatch = 1024
)

var (
	fNewPDCli     = pd.NewClientWithContext
	fNewTsRWriter = createTsRWriter
)

type processor struct {
	id           string
	captureInfo  model.CaptureInfo
	changefeedID string
	changefeed   model.ChangeFeedInfo
	limitter     *puller.BlurResourceLimitter
	stopped      int32

	pdCli     pd.Client
	kvStorage tidbkv.Storage
	etcdCli   kv.CDCEtcdClient
	session   *concurrency.Session

	sink sink.Sink

	sinkEmittedResolvedTs uint64
	globalResolvedTs      uint64
	checkpointTs          uint64

	ddlPuller       puller.Puller
	ddlPullerCancel context.CancelFunc
	schemaStorage   *entry.SchemaStorage

	tsRWriter storage.ProcessorTsRWriter
	output    chan *model.PolymorphicEvent
	mounter   entry.Mounter

	stateMu      sync.Mutex
	status       *model.TaskStatus
	position     *model.TaskPosition
	tables       map[int64]*tableInfo
	markTableIDs map[int64]struct{}

	sinkEmittedResolvedNotifier *notify.Notifier
	sinkEmittedResolvedReceiver *notify.Receiver
	localResolvedNotifier       *notify.Notifier
	localResolvedReceiver       *notify.Receiver

	wg    *errgroup.Group
	errCh chan<- error
}

type tableInfo struct {
	id          int64
	name        string
	resolvedTs  uint64
	markTableID int64
	mResolvedTs uint64
	workload    model.WorkloadInfo
	cancel      context.CancelFunc
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

// newProcessor creates and returns a processor for the specified change feed
func newProcessor(
	ctx context.Context,
	session *concurrency.Session,
	changefeed model.ChangeFeedInfo,
	sink sink.Sink,
	changefeedID string,
	captureInfo model.CaptureInfo,
	checkpointTs uint64,
	errCh chan error,
) (*processor, error) {
	etcdCli := session.Client()
	endpoints := session.Client().Endpoints()
	pdCli, err := fNewPDCli(ctx, endpoints, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Annotatef(err, "create pd client failed, addr: %v", endpoints)
	}
	kvStorage, err := kv.CreateTiStore(strings.Join(endpoints, ","))
	if err != nil {
		return nil, errors.Trace(err)
	}
	cdcEtcdCli := kv.NewCDCEtcdClient(etcdCli)

	tsRWriter, err := fNewTsRWriter(cdcEtcdCli, changefeedID, captureInfo.ID)
	if err != nil {
		return nil, errors.Annotate(err, "failed to create ts RWriter")
	}

	limitter := puller.NewBlurResourceLimmter(defaultMemBufferCapacity)

	// The key in DDL kv pair returned from TiKV is already memcompariable encoded,
	// so we set `needEncode` to false.
	log.Info("start processor with startts", zap.Uint64("startts", checkpointTs))
	ddlPuller := puller.NewPuller(pdCli, kvStorage, checkpointTs, []regionspan.Span{regionspan.GetDDLSpan(), regionspan.GetAddIndexDDLSpan()}, limitter)
	filter, err := filter.NewFilter(changefeed.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaStorage, err := createSchemaStorage(endpoints, checkpointTs, filter)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sinkEmittedResolvedNotifier := new(notify.Notifier)
	localResolvedNotifier := new(notify.Notifier)
	p := &processor{
		id:            uuid.New().String(),
		limitter:      limitter,
		captureInfo:   captureInfo,
		changefeedID:  changefeedID,
		changefeed:    changefeed,
		pdCli:         pdCli,
		kvStorage:     kvStorage,
		etcdCli:       cdcEtcdCli,
		session:       session,
		sink:          sink,
		ddlPuller:     ddlPuller,
		mounter:       entry.NewMounter(schemaStorage, changefeed.Config.Mounter.WorkerNum),
		schemaStorage: schemaStorage,
		errCh:         errCh,

		tsRWriter: tsRWriter,
		status:    tsRWriter.GetTaskStatus(),
		position:  &model.TaskPosition{CheckPointTs: checkpointTs},
		output:    make(chan *model.PolymorphicEvent, defaultOutputChanSize),

		sinkEmittedResolvedNotifier: sinkEmittedResolvedNotifier,
		sinkEmittedResolvedReceiver: sinkEmittedResolvedNotifier.NewReceiver(50 * time.Millisecond),

		localResolvedNotifier: localResolvedNotifier,
		localResolvedReceiver: localResolvedNotifier.NewReceiver(50 * time.Millisecond),

		tables:       make(map[int64]*tableInfo),
		markTableIDs: make(map[int64]struct{}),
	}

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
			zap.String("captureid", p.captureInfo.ID),
			zap.String("captureaddr", p.captureInfo.AdvertiseAddr),
			zap.String("changefeedID", p.changefeedID),
			zap.Error(err),
		)
	}
}

func (p *processor) writeDebugInfo(w io.Writer) {
	fmt.Fprintf(w, "changefeedID: %s, info: %+v, status: %+v\n", p.changefeedID, p.changefeed, p.status)

	p.stateMu.Lock()
	for _, table := range p.tables {
		fmt.Fprintf(w, "\ttable id: %d, resolveTS: %d\n", table.id, table.loadResolvedTs())
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
	checkpointTsTick := time.NewTicker(resolveTsInterval)

	updateInfo := func() error {
		t0Update := time.Now()
		err := retry.Run(500*time.Millisecond, 3, func() error {
			inErr := p.updateInfo(ctx)
			if inErr != nil {
				if errors.Cause(inErr) != context.Canceled {
					log.Error(
						"update info failed",
						zap.String("changefeed", p.changefeedID), zap.Error(inErr),
					)
				}
				if p.isStopped() || errors.Cause(inErr) == model.ErrAdminStopProcessor {
					return backoff.Permanent(errors.Trace(model.ErrAdminStopProcessor))
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
		checkpointTsTick.Stop()
		p.localResolvedReceiver.Stop()

		if !p.isStopped() {
			err := updateInfo()
			if err != nil && errors.Cause(err) != context.Canceled {
				log.Warn("failed to update info before exit", zap.Error(err))
			}
		}

		log.Info("Local resolved worker exited")
	}()

	resolvedTsGauge := resolvedTsGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)
	checkpointTsGauge := checkpointTsGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr)

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

			if minResolvedTs == p.position.ResolvedTs {
				continue
			}

			p.position.ResolvedTs = minResolvedTs
			resolvedTsGauge.Set(float64(oracle.ExtractPhysical(minResolvedTs)))
			if err := updateInfo(); err != nil {
				return errors.Trace(err)
			}

		case <-checkpointTsTick.C:
			checkpointTs := atomic.LoadUint64(&p.checkpointTs)
			if p.position.CheckPointTs >= checkpointTs {
				continue
			}
			p.position.CheckPointTs = checkpointTs
			checkpointTsGauge.Set(float64(oracle.ExtractPhysical(checkpointTs)))
			if err := updateInfo(); err != nil {
				return errors.Trace(err)
			}
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
		workload := make(model.TaskWorkload, len(p.tables))
		p.stateMu.Lock()
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

func (p *processor) updateInfo(ctx context.Context) error {
	updatePosition := func() error {
		//p.position.Count = p.sink.Count()
		err := p.tsRWriter.WritePosition(ctx, p.position)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("update task position", zap.Stringer("position", p.position))
		return nil
	}
	statusChanged, err := p.tsRWriter.UpdateInfo(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if !statusChanged && !p.tsRWriter.GetTaskStatus().SomeOperationsUnapplied() {
		return updatePosition()
	}
	p.status = p.tsRWriter.GetTaskStatus()
	if p.status.AdminJobType == model.AdminStop || p.status.AdminJobType == model.AdminRemove {
		err = p.stop(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		return errors.Trace(model.ErrAdminStopProcessor)
	}
	err = p.handleTables(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	syncTableNumGauge.
		WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr).
		Set(float64(len(p.status.Tables)))
	err = updatePosition()
	if err != nil {
		return errors.Trace(err)
	}
	err = retry.Run(500*time.Millisecond, 5, func() error {
		err = p.tsRWriter.WriteInfoIntoStorage(ctx)
		switch errors.Cause(err) {
		case model.ErrWriteTsConflict:
			return errors.Trace(err)
		case nil:
			log.Info("update task status", zap.Stringer("status", p.status), zap.Stringer("position", p.position))
			return nil
		default:
			return backoff.Permanent(errors.Trace(err))
		}
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (p *processor) removeTable(tableID int64) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	log.Debug("remove table", zap.Int64("id", tableID))

	table, ok := p.tables[tableID]
	if !ok {
		log.Warn("table not found", zap.Int64("tableID", tableID))
		return
	}

	table.cancel()
	delete(p.tables, tableID)
	if table.markTableID != 0 {
		delete(p.markTableIDs, table.markTableID)
	}
	tableResolvedTsGauge.DeleteLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr, table.name)
	syncTableNumGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr).Dec()
}

// handleTables handles table scheduler on this processor, add or remove table puller
func (p *processor) handleTables(ctx context.Context) error {
	for tableID, opt := range p.status.Operation {
		if opt.Delete {
			if opt.BoundaryTs <= p.position.CheckPointTs {
				p.removeTable(tableID)
				opt.Done = true
			}
		} else {
			replicaInfo, exist := p.status.Tables[tableID]
			if !exist {
				return errors.NotFoundf("replicaInfo of table(%d)", tableID)
			}
			if p.changefeed.Config.Cyclic.IsEnabled() && replicaInfo.MarkTableID == 0 {
				return errors.NotValidf("normal table(%d) and mark table not match ", tableID)
			}
			p.addTable(ctx, tableID, replicaInfo)
			opt.Done = true
		}
	}
	if !p.status.SomeOperationsUnapplied() {
		p.status.Operation = nil
	}
	return nil
}

// globalStatusWorker read global resolve ts from changefeed level info and forward `tableInputChans` regularly.
func (p *processor) globalStatusWorker(ctx context.Context) error {
	log.Info("Global status worker started")

	var (
		changefeedStatus *model.ChangeFeedStatus
		statusRev        int64
		lastCheckPointTs uint64
		lastResolvedTs   uint64
		watchKey         = kv.GetEtcdKeyJob(p.changefeedID)
	)
	updateStatus := func(changefeedStatus *model.ChangeFeedStatus) error {
		if lastResolvedTs == changefeedStatus.ResolvedTs &&
			lastCheckPointTs == changefeedStatus.CheckpointTs {
			return nil
		}
		if lastCheckPointTs < changefeedStatus.CheckpointTs {
			p.schemaStorage.DoGC(changefeedStatus.CheckpointTs)
			lastCheckPointTs = changefeedStatus.CheckpointTs
		}
		if lastResolvedTs < changefeedStatus.ResolvedTs {
			lastResolvedTs = changefeedStatus.ResolvedTs
			atomic.StoreUint64(&p.globalResolvedTs, lastResolvedTs)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case p.output <- model.NewResolvedPolymorphicEvent(lastResolvedTs):
			}
		}
		return nil
	}

	retryCfg := backoff.WithMaxRetries(
		backoff.WithContext(
			backoff.NewExponentialBackOff(), ctx),
		5,
	)
	for {
		select {
		case <-ctx.Done():
			log.Info("Global resolved worker exited")
			return ctx.Err()
		default:
		}

		err := backoff.Retry(func() error {
			var err error
			changefeedStatus, statusRev, err = p.tsRWriter.GetChangeFeedStatus(ctx)
			if err != nil {
				if errors.Cause(err) == context.Canceled {
					return backoff.Permanent(err)
				}
				log.Error("Global resolved worker: read global resolved ts failed", zap.Error(err))
			}
			return err
		}, retryCfg)
		if err != nil {
			return errors.Trace(err)
		}

		if err := updateStatus(changefeedStatus); err != nil {
			return err
		}

		ch := p.etcdCli.Client.Watch(ctx, watchKey, clientv3.WithRev(statusRev+1), clientv3.WithFilterDelete())
		for resp := range ch {
			if resp.Err() == mvcc.ErrCompacted {
				break
			}
			if resp.Err() != nil {
				return err
			}
			for _, ev := range resp.Events {
				var status model.ChangeFeedStatus
				if err := status.Unmarshal(ev.Kv.Value); err != nil {
					return err
				}
				if err := updateStatus(&status); err != nil {
					return err
				}
			}
		}
	}
}

func (p *processor) sinkDriver(ctx context.Context) error {
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
			if minTs == 0 {
				continue
			}
			err := p.sink.FlushRowChangedEvents(ctx, minTs)
			if err != nil {
				return errors.Trace(err)
			}
			atomic.StoreUint64(&p.checkpointTs, minTs)
		}
	}
}

// syncResolved handle `p.ddlJobsCh` and `p.resolvedTxns`
func (p *processor) syncResolved(ctx context.Context) error {
	defer func() {
		p.sinkEmittedResolvedReceiver.Stop()
		log.Info("syncResolved stopped")
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
				failpoint.Return(errors.New("processor sync resolvd injected error"))
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
			if row.CRTs <= resolvedTs {
				log.Fatal("The CRTs must be greater than the resolvedTs",
					zap.Uint64("CRTs", row.CRTs),
					zap.Uint64("resolvedTs", resolvedTs))
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

func createSchemaStorage(pdEndpoints []string, checkpointTs uint64, filter *filter.Filter) (*entry.SchemaStorage, error) {
	// TODO here we create another pb client,we should reuse them
	kvStore, err := kv.CreateTiStore(strings.Join(pdEndpoints, ","))
	if err != nil {
		return nil, errors.Trace(err)
	}
	meta, err := kv.GetSnapshotMeta(kvStore, checkpointTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return entry.NewSchemaStorage(meta, checkpointTs, filter)
}

func createTsRWriter(cli kv.CDCEtcdClient, changefeedID, captureID string) (storage.ProcessorTsRWriter, error) {
	return storage.NewProcessorTsEtcdRWriter(cli, changefeedID, captureID)
}

func (p *processor) addTable(ctx context.Context, tableID int64, replicaInfo *model.TableReplicaInfo) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	ctx = util.PutTableInfoInCtx(ctx, tableID, replicaInfo.Name)

	log.Debug("Add table", zap.Int64("tableID", tableID), zap.Any("replicaInfo", replicaInfo))
	if _, ok := p.tables[tableID]; ok {
		log.Warn("Ignore existing table", zap.Int64("ID", tableID))
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	table := &tableInfo{
		id:         tableID,
		name:       replicaInfo.Name,
		resolvedTs: replicaInfo.StartTs,
		cancel:     cancel,
	}
	// TODO(leoppro) calculate the workload of this table
	// We temporarily set the value to constant 1
	table.workload = model.WorkloadInfo{Workload: 1}

	startPuller := func(tableID model.TableID, pResolvedTs *uint64) {

		// start table puller
		// The key in DML kv pair returned from TiKV is not memcompariable encoded,
		// so we set `needEncode` to true.
		span := regionspan.GetTableSpan(tableID)
		plr := puller.NewPuller(p.pdCli, p.kvStorage, replicaInfo.StartTs, []regionspan.Span{span}, p.limitter)
		go func() {
			err := plr.Run(ctx)
			if errors.Cause(err) != context.Canceled {
				p.errCh <- err
			}
		}()

		var sorter puller.EventSorter
		switch p.changefeed.Engine {
		case model.SortInMemory:
			sorter = puller.NewEntrySorter()
		case model.SortInFile:
			sorter = puller.NewFileSorter(p.changefeed.SortDir)
		default:
			p.errCh <- errors.Errorf("unknown sort engine %s", p.changefeed.Engine)
			return
		}
		go func() {
			err := sorter.Run(ctx)
			if errors.Cause(err) != context.Canceled {
				p.errCh <- err
			}
		}()
		go func() {
			resolvedTsGauge := tableResolvedTsGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr, table.name)
			for {
				select {
				case <-ctx.Done():
					if errors.Cause(ctx.Err()) != context.Canceled {
						p.errCh <- ctx.Err()
					}
					return
				case rawKV := <-plr.Output():
					if rawKV == nil {
						continue
					}
					pEvent := model.NewPolymorphicEvent(rawKV)
					sorter.AddEntry(ctx, pEvent)
					select {
					case <-ctx.Done():
						if errors.Cause(ctx.Err()) != context.Canceled {
							p.errCh <- ctx.Err()
						}
						return
					case p.mounter.Input() <- pEvent:
					}
				case pEvent := <-sorter.Output():
					if pEvent == nil {
						continue
					}
					if pEvent.RawKV != nil && pEvent.RawKV.OpType == model.OpTypeResolved {
						atomic.StoreUint64(pResolvedTs, pEvent.CRTs)
						p.localResolvedNotifier.Notify()
						resolvedTsGauge.Set(float64(oracle.ExtractPhysical(pEvent.CRTs)))
						continue
					}
					select {
					case <-ctx.Done():
						if errors.Cause(ctx.Err()) != context.Canceled {
							p.errCh <- ctx.Err()
						}
						return
					case p.output <- pEvent:
					}
				}
			}
		}()
	}

	startPuller(tableID, &table.resolvedTs)

	if p.changefeed.Config.Cyclic.IsEnabled() && replicaInfo.MarkTableID != 0 {
		mTableID := replicaInfo.MarkTableID
		// we should to make sure a mark table is only listened once.
		if _, exist := p.markTableIDs[mTableID]; !exist {
			p.markTableIDs[mTableID] = struct{}{}
			startPuller(mTableID, &table.mResolvedTs)

			table.markTableID = mTableID
			table.mResolvedTs = replicaInfo.StartTs
		}
	}

	p.tables[tableID] = table
	if p.position.CheckPointTs > replicaInfo.StartTs {
		p.position.CheckPointTs = replicaInfo.StartTs
	}
	if p.position.ResolvedTs > replicaInfo.StartTs {
		p.position.ResolvedTs = replicaInfo.StartTs
	}
	syncTableNumGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr).Inc()
}

func (p *processor) stop(ctx context.Context) error {
	log.Info("stop processor", zap.String("id", p.id), zap.String("capture", p.captureInfo.AdvertiseAddr), zap.String("changefeed", p.changefeedID))
	p.stateMu.Lock()
	for _, tbl := range p.tables {
		tbl.cancel()
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
	session *concurrency.Session,
	info model.ChangeFeedInfo,
	changefeedID string,
	captureInfo model.CaptureInfo,
	checkpointTs uint64,
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
	sink, err := sink.NewSink(ctx, info.SinkURI, filter, info.Config, opts, errCh)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	processor, err := newProcessor(ctx, session, info, sink, changefeedID, captureInfo, checkpointTs, errCh)
	if err != nil {
		cancel()
		return nil, err
	}
	log.Info("start to run processor", zap.String("changefeed id", changefeedID))

	processorErrorCounter.WithLabelValues(changefeedID, captureInfo.AdvertiseAddr).Add(0)
	processor.Run(ctx)

	go func() {
		err := <-errCh
		cause := errors.Cause(err)
		if cause != nil && cause != context.Canceled && cause != model.ErrAdminStopProcessor {
			processorErrorCounter.WithLabelValues(changefeedID, captureInfo.AdvertiseAddr).Inc()
			log.Error("error on running processor",
				zap.String("captureid", captureInfo.ID),
				zap.String("captureaddr", captureInfo.AdvertiseAddr),
				zap.String("changefeedid", changefeedID),
				zap.String("processorid", processor.id),
				zap.Error(err))
			// record error information in etcd
			// TODO: design error codes for TiCDC
			processor.position.Error = &model.RunningError{
				Addr:    captureInfo.AdvertiseAddr,
				Code:    "CDC-processor-1000",
				Message: err.Error(),
			}
			err = processor.tsRWriter.WritePosition(ctx, processor.position)
			if err != nil {
				log.Warn("upload processor error failed", zap.Error(err))
			}
		} else {
			log.Info("processor exited",
				zap.String("captureid", captureInfo.ID),
				zap.String("captureaddr", captureInfo.AdvertiseAddr),
				zap.String("changefeedid", changefeedID),
				zap.String("processorid", processor.id))
		}
		cancel()
	}()

	return processor, nil
}
