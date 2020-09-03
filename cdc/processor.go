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
	"os"
	"strconv"
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
	"github.com/pingcap/ticdc/cdc/sink"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/security"
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
	// TODO: processor output chan size, the accumulated data is determined by
	// the count of sorted data and unmounted data. In current benchmark a single
	// processor can reach 50k-100k QPS, and accumulated data is around
	// 200k-400k in most cases. We need a better chan cache mechanism.
	defaultOutputChanSize = 1280000

	// defaultMemBufferCapacity is the default memory buffer per change feed.
	defaultMemBufferCapacity int64 = 10 * 1024 * 1024 * 1024 // 10G

	defaultSyncResolvedBatch = 1024

	defaultFlushTaskPositionInterval = 200 * time.Millisecond
)

var (
	fNewPDCli = pd.NewClientWithContext
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
	kvStorage  tidbkv.Storage
	etcdCli    kv.CDCEtcdClient
	session    *concurrency.Session

	sink sink.Sink

	sinkEmittedResolvedTs uint64
	globalResolvedTs      uint64
	localResolvedTs       uint64
	checkpointTs          uint64

	ddlPuller       puller.Puller
	ddlPullerCancel context.CancelFunc
	schemaStorage   *entry.SchemaStorage

	output  chan *model.PolymorphicEvent
	mounter entry.Mounter

	stateMu           sync.Mutex
	status            *model.TaskStatus
	position          *model.TaskPosition
	tables            map[int64]*tableInfo
	markTableIDs      map[int64]struct{}
	statusModRevision int64

	sinkEmittedResolvedNotifier *notify.Notifier
	sinkEmittedResolvedReceiver *notify.Receiver
	localResolvedNotifier       *notify.Notifier
	localResolvedReceiver       *notify.Receiver
	localCheckpointTsNotifier   *notify.Notifier
	localCheckpointTsReceiver   *notify.Receiver

	wg       *errgroup.Group
	errCh    chan<- error
	opDoneCh chan int64
}

type tableInfo struct {
	id          int64
	name        string // quoted schema and table, used in metircs only
	resolvedTs  uint64
	markTableID int64
	mResolvedTs uint64
	sorter      *puller.Rectifier
	workload    model.WorkloadInfo
	cancel      context.CancelFunc
	// isDying shows that the table is being removed.
	// In the case the same table is added back before safe removal is finished,
	// this flag is used to tell whether it's safe to kill the table.
	isDying uint32
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

// safeStop will stop the table change feed safety
func (t *tableInfo) safeStop() (stopped bool, checkpointTs model.Ts) {
	atomic.StoreUint32(&t.isDying, 1)
	t.sorter.SafeStop()
	status := t.sorter.GetStatus()
	if status != model.SorterStatusStopped && status != model.SorterStatusFinished {
		return false, 0
	}
	return true, t.sorter.GetMaxResolvedTs()
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
) (*processor, error) {
	etcdCli := session.Client()
	endpoints := session.Client().Endpoints()
	pdCli, err := fNewPDCli(ctx, endpoints, credential.PDSecurityOption())
	if err != nil {
		return nil, errors.Annotatef(err, "create pd client failed, addr: %v", endpoints)
	}
	kvStorage, err := kv.CreateTiStore(strings.Join(endpoints, ","), credential)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cdcEtcdCli := kv.NewCDCEtcdClient(etcdCli)
	limitter := puller.NewBlurResourceLimmter(defaultMemBufferCapacity)

	log.Info("start processor with startts", zap.Uint64("startts", checkpointTs))
	ddlspans := []regionspan.Span{regionspan.GetDDLSpan(), regionspan.GetAddIndexDDLSpan()}
	ddlPuller := puller.NewPuller(pdCli, credential, kvStorage, checkpointTs, ddlspans, limitter, false)
	filter, err := filter.NewFilter(changefeed.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schemaStorage, err := createSchemaStorage(endpoints, credential, checkpointTs, filter)
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
		kvStorage:     kvStorage,
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

		localCheckpointTsNotifier: localCheckpointTsNotifier,
		localCheckpointTsReceiver: localCheckpointTsNotifier.NewReceiver(50 * time.Millisecond),

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
	lastFlushTime := time.Now()
	retryFlushTaskStatusAndPosition := func() error {
		t0Update := time.Now()
		err := retry.Run(500*time.Millisecond, 3, func() error {
			inErr := p.flushTaskStatusAndPosition(ctx)
			if inErr != nil {
				if errors.Cause(inErr) != context.Canceled {
					logError := log.Error
					if cerror.ErrAdminStopProcessor.Equal(inErr) {
						logError = log.Warn
					}
					logError(
						"update info failed",
						zap.String("changefeed", p.changefeedID), zap.Error(inErr),
					)
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
				log.Warn("failed to update info before exit", zap.Error(err))
			}
		}

		log.Info("Local resolved worker exited")
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

			p.position.ResolvedTs = minResolvedTs
			resolvedTsGauge.Set(float64(phyTs))
			if err := retryFlushTaskStatusAndPosition(); err != nil {
				return errors.Trace(err)
			}
		case <-p.localCheckpointTsReceiver.C:
			checkpointTs := atomic.LoadUint64(&p.checkpointTs)
			phyTs := oracle.ExtractPhysical(checkpointTs)
			// It is more accurate to get tso from PD, but in most cases we have
			// deployed NTP service, a little bias is acceptable here.
			metricCheckpointTsLagGauge.Set(float64(oracle.GetPhysical(time.Now())-phyTs) / 1e3)

			if p.position.CheckPointTs >= checkpointTs {
				continue
			}
			if time.Since(lastFlushTime) < defaultFlushTaskPositionInterval {
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

func (p *processor) flushTaskPosition(ctx context.Context) error {
	failpoint.Inject("ProcessorUpdatePositionDelaying", func() {
		time.Sleep(1 * time.Second)
	})
	if p.isStopped() {
		return cerror.ErrAdminStopProcessor.FastGenByArgs()
	}
	//p.position.Count = p.sink.Count()
	err := p.etcdCli.PutTaskPosition(ctx, p.changefeedID, p.captureInfo.ID, p.position)
	if err == nil {
		log.Debug("flushed task position", zap.Stringer("position", p.position))
	} else if errors.Cause(err) != context.Canceled {
		log.Error("failed to flush task position", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// First try to synchronize task status from etcd.
// If local cached task status is outdated (caused by new table scheduling),
// update it to latest value, and force update task position, since add new
// tables may cause checkpoint ts fallback in processor.
func (p *processor) flushTaskStatusAndPosition(ctx context.Context) error {
	if p.isStopped() {
		return cerror.ErrAdminStopProcessor.FastGenByArgs()
	}
	var tablesToRemove []model.TableID
	newTaskStatus, newModRevision, err := p.etcdCli.AtomicPutTaskStatus(ctx, p.changefeedID, p.captureInfo.ID,
		func(modRevision int64, taskStatus *model.TaskStatus) (bool, error) {
			// if the task status is not changed and not operation to handle
			// we need not to change the task status
			if p.statusModRevision == modRevision && !taskStatus.SomeOperationsUnapplied() {
				return false, nil
			}
			if taskStatus.AdminJobType.IsStopState() {
				err := p.stop(ctx)
				if err != nil {
					return false, backoff.Permanent(errors.Trace(err))
				}
				return false, backoff.Permanent(cerror.ErrAdminStopProcessor.FastGenByArgs())
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
	p.statusModRevision = newModRevision
	p.status = newTaskStatus
	syncTableNumGauge.
		WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr).
		Set(float64(len(p.status.Tables)))

	return p.flushTaskPosition(ctx)
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

	if atomic.SwapUint32(&table.isDying, 0) == 0 {
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
func (p *processor) handleTables(ctx context.Context, status *model.TaskStatus) (tablesToRemove []model.TableID, err error) {
	for tableID, opt := range status.Operation {
		if opt.Done {
			continue
		}
		if opt.Delete {
			if opt.BoundaryTs <= p.position.CheckPointTs {
				table, exist := p.tables[tableID]
				if !exist {
					log.Warn("table which will be deleted is not found", zap.Int64("tableID", tableID))
					opt.Done = true
					continue
				}
				stopped, checkpointTs := table.safeStop()
				log.Debug("safeStop table", zap.Int64("tableID", tableID),
					zap.Bool("stopped", stopped), zap.Uint64("checkpointTs", checkpointTs))
				if stopped {
					opt.BoundaryTs = checkpointTs
					if checkpointTs <= p.position.CheckPointTs {
						tablesToRemove = append(tablesToRemove, tableID)
						opt.Done = true
					}
				}
			}
		} else {
			replicaInfo, exist := status.Tables[tableID]
			if !exist {
				return tablesToRemove, errors.NotFoundf("replicaInfo of table(%d)", tableID)
			}
			if p.changefeed.Config.Cyclic.IsEnabled() && replicaInfo.MarkTableID == 0 {
				return tablesToRemove, errors.NotValidf("normal table(%d) and mark table not match ", tableID)
			}
			p.addTable(ctx, tableID, replicaInfo)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case tableID := <-p.opDoneCh:
			log.Debug("Operation done signal received",
				zap.Int64("tableID", tableID),
				zap.Reflect("operation", status.Operation[tableID]))
			if status.Operation[tableID] == nil {
				log.Debug("TableID does not exist, probably a mark table, ignore", zap.Int64("tableID", tableID))
				continue
			}
			status.Operation[tableID].Done = true
		default:
			goto done
		}
	}
done:
	if !status.SomeOperationsUnapplied() {
		status.Operation = nil
	}
	return tablesToRemove, nil
}

// globalStatusWorker read global resolve ts from changefeed level info and forward `tableInputChans` regularly.
func (p *processor) globalStatusWorker(ctx context.Context) error {
	log.Info("Global status worker started")

	var (
		changefeedStatus         *model.ChangeFeedStatus
		statusRev                int64
		lastCheckPointTs         uint64
		lastResolvedTs           uint64
		watchKey                 = kv.GetEtcdKeyJob(p.changefeedID)
		globalResolvedTsNotifier = new(notify.Notifier)
		globalResolvedTsReceiver = globalResolvedTsNotifier.NewReceiver(1 * time.Second)
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
			log.Debug("Update globalResolvedTs", zap.Uint64("globalResolvedTs", lastResolvedTs))
			globalResolvedTsNotifier.Notify()
		}
		return nil
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
						zap.Uint64("localResolvedTs", localResolvedTs))
					// we do not issue resolved events if globalResolvedTs > localResolvedTs.
					continue
				}
				select {
				case <-ctx.Done():
					return
				case p.output <- model.NewResolvedPolymorphicEvent(lastResolvedTs):
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
			log.Info("Global resolved worker exited")
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
				log.Warn("flush row changed events too slow", zap.Duration("duration", dur))
			}
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
		failpoint.Inject("ProcessorSyncResolvedPreEmit", func() {})
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
					zap.String("model", "processor"),
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

func createSchemaStorage(pdEndpoints []string, credential *security.Credential, checkpointTs uint64, filter *filter.Filter) (*entry.SchemaStorage, error) {
	// TODO here we create another pb client,we should reuse them
	kvStore, err := kv.CreateTiStore(strings.Join(pdEndpoints, ","), credential)
	if err != nil {
		return nil, errors.Trace(err)
	}
	meta, err := kv.GetSnapshotMeta(kvStore, checkpointTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return entry.NewSchemaStorage(meta, checkpointTs, filter)
}

func (p *processor) addTable(ctx context.Context, tableID int64, replicaInfo *model.TableReplicaInfo) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	var tableName string
	if name, ok := p.schemaStorage.GetLastSnapshot().GetTableNameByID(tableID); ok {
		tableName = name.QuoteString()
	} else {
		log.Warn("failed to get table name, fallback to use table id", zap.Int64("table-id", tableID))
		tableName = strconv.Itoa(int(tableID))
	}

	if table, ok := p.tables[tableID]; ok {
		if atomic.SwapUint32(&table.isDying, 0) == 1 {
			log.Warn("The same table exists but is dying. Cancel it and continue.", zap.Int64("ID", tableID))
			table.cancel()
		} else {
			log.Warn("Ignore existing table", zap.Int64("ID", tableID))
			return
		}
	}
	globalResolvedTs := atomic.LoadUint64(&p.sinkEmittedResolvedTs)
	log.Debug("Add table", zap.Int64("tableID", tableID),
		zap.String("name", tableName),
		zap.Any("replicaInfo", replicaInfo),
		zap.Uint64("globalResolvedTs", globalResolvedTs))

	ctx = util.PutTableInfoInCtx(ctx, tableID, tableName)
	ctx, cancel := context.WithCancel(ctx)
	table := &tableInfo{
		id:         tableID,
		name:       tableName,
		resolvedTs: replicaInfo.StartTs,
		cancel:     cancel,
	}
	// TODO(leoppro) calculate the workload of this table
	// We temporarily set the value to constant 1
	table.workload = model.WorkloadInfo{Workload: 1}

	startPuller := func(tableID model.TableID, pResolvedTs *uint64) *puller.Rectifier {

		// start table puller
		enableOldValue := p.changefeed.Config.EnableOldValue
		span := regionspan.GetTableSpan(tableID, enableOldValue)
		plr := puller.NewPuller(p.pdCli, p.credential, p.kvStorage, replicaInfo.StartTs, []regionspan.Span{span}, p.limitter, enableOldValue)
		go func() {
			err := plr.Run(ctx)
			if errors.Cause(err) != context.Canceled {
				p.errCh <- err
			}
		}()

		var sorterImpl puller.EventSorter
		switch p.changefeed.Engine {
		case model.SortInMemory:
			sorterImpl = puller.NewEntrySorter()
		case model.SortInFile:
			err := util.IsDirAndWritable(p.changefeed.SortDir)
			if err != nil {
				if os.IsNotExist(errors.Cause(err)) {
					err = os.MkdirAll(p.changefeed.SortDir, 0755)
					if err != nil {
						p.errCh <- errors.Annotate(err, "create dir")
						return nil
					}
				} else {
					p.errCh <- errors.Annotate(err, "sort dir check")
					return nil
				}
			}
			sorterImpl = puller.NewFileSorter(p.changefeed.SortDir)
		default:
			p.errCh <- errors.Errorf("unknown sort engine %s", p.changefeed.Engine)
			return nil
		}
		sorter := puller.NewRectifier(sorterImpl, p.changefeed.GetTargetTs())

		go func() {
			err := sorter.Run(ctx)
			if errors.Cause(err) != context.Canceled {
				p.errCh <- err
			}
		}()

		var lastResolvedTs uint64
		go func() {
			opDone := false
			resolvedTsGauge := tableResolvedTsGauge.WithLabelValues(p.changefeedID, p.captureInfo.AdvertiseAddr, table.name)
			checkDoneTicker := time.NewTicker(1 * time.Second)
			checkDone := func() {
				localResolvedTs := atomic.LoadUint64(&p.localResolvedTs)
				globalResolvedTs := atomic.LoadUint64(&p.globalResolvedTs)
				if !opDone && lastResolvedTs >= localResolvedTs && localResolvedTs >= globalResolvedTs {
					log.Debug("localResolvedTs >= globalResolvedTs, sending operation done signal",
						zap.Uint64("localResolvedTs", localResolvedTs), zap.Uint64("globalResolvedTs", globalResolvedTs),
						zap.Int64("tableID", tableID))

					opDone = true
					checkDoneTicker.Stop()
					select {
					case <-ctx.Done():
						if errors.Cause(ctx.Err()) != context.Canceled {
							p.errCh <- ctx.Err()
						}
						return
					case p.opDoneCh <- tableID:
					}
				}
				if !opDone {
					log.Debug("addTable not done",
						zap.Uint64("tableResolvedTs", lastResolvedTs),
						zap.Uint64("localResolvedTs", localResolvedTs),
						zap.Uint64("globalResolvedTs", globalResolvedTs),
						zap.Int64("tableID", tableID))
				}
			}

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
						lastResolvedTs = pEvent.CRTs
						p.localResolvedNotifier.Notify()
						resolvedTsGauge.Set(float64(oracle.ExtractPhysical(pEvent.CRTs)))
						if !opDone {
							checkDone()
						}
						continue
					}
					sinkResolvedTs := atomic.LoadUint64(&p.sinkEmittedResolvedTs)
					if pEvent.CRTs <= sinkResolvedTs || pEvent.CRTs <= lastResolvedTs || pEvent.CRTs < replicaInfo.StartTs {
						log.Fatal("The CRTs of event is not expected, please report a bug",
							zap.String("model", "sorter"),
							zap.Uint64("globalResolvedTs", sinkResolvedTs),
							zap.Uint64("resolvedTs", lastResolvedTs),
							zap.Int64("tableID", tableID),
							zap.Any("replicaInfo", replicaInfo),
							zap.Any("row", pEvent))
					}
					select {
					case <-ctx.Done():
						if errors.Cause(ctx.Err()) != context.Canceled {
							p.errCh <- ctx.Err()
						}
						return
					case p.output <- pEvent:
					}
				case <-checkDoneTicker.C:
					if !opDone {
						checkDone()
					}
				}
			}
		}()
		return sorter
	}

	if p.changefeed.Config.Cyclic.IsEnabled() && replicaInfo.MarkTableID != 0 {
		mTableID := replicaInfo.MarkTableID
		// we should to make sure a mark table is only listened once.
		if _, exist := p.markTableIDs[mTableID]; !exist {
			p.markTableIDs[mTableID] = struct{}{}
			table.markTableID = mTableID
			table.mResolvedTs = replicaInfo.StartTs

			startPuller(mTableID, &table.mResolvedTs)
		}
	}

	p.tables[tableID] = table
	if p.position.CheckPointTs > replicaInfo.StartTs {
		p.position.CheckPointTs = replicaInfo.StartTs
	}
	if p.position.ResolvedTs > replicaInfo.StartTs {
		p.position.ResolvedTs = replicaInfo.StartTs
	}

	table.sorter = startPuller(tableID, &table.resolvedTs)

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
	credential *security.Credential,
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
	sink, err := sink.NewSink(ctx, changefeedID, info.SinkURI, filter, info.Config, opts, errCh)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}
	processor, err := newProcessor(ctx, credential, session, info, sink, changefeedID, captureInfo, checkpointTs, errCh)
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
		if cause != nil && cause != context.Canceled && cerror.ErrAdminStopProcessor.NotEqual(cause) {
			processorErrorCounter.WithLabelValues(changefeedID, captureInfo.AdvertiseAddr).Inc()
			log.Error("error on running processor",
				zap.String("captureid", captureInfo.ID),
				zap.String("captureaddr", captureInfo.AdvertiseAddr),
				zap.String("changefeedid", changefeedID),
				zap.String("processorid", processor.id),
				zap.Error(err))
			// record error information in etcd
			code := "CDC:server:ErrProcessorUnknown"
			if terror, ok := err.(*errors.Error); ok {
				code = string(terror.RFCCode())
			}
			processor.position.Error = &model.RunningError{
				Addr:    captureInfo.AdvertiseAddr,
				Code:    code,
				Message: err.Error(),
			}
			err = processor.etcdCli.PutTaskPosition(ctx, processor.changefeedID, processor.captureInfo.ID, processor.position)
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
