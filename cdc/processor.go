// Copyright 2019 PingCAP, Inc.
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/cdc/roles/storage"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/codec"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	pbackoff "google.golang.org/grpc/backoff"
)

const (
	updateInfoInterval        = time.Millisecond * 500
	resolveTsInterval         = time.Millisecond * 500
	waitGlobalResolvedTsDelay = time.Millisecond * 500
	flushDMLsInterval         = time.Millisecond * 10

	defaultInputTxnChanSize  = 128
	defaultOutputTxnChanSize = 128

	// defaultMemBufferCapacity is the default memory buffer per change feed.
	defaultMemBufferCapacity int64 = 10 * 1024 * 1024 * 1024 // 10G
)

var (
	fCreateSchema = createSchemaStore
	fNewPDCli     = pd.NewClient
	fNewTsRWriter = createTsRWriter
	fNewMounter   = newMounter
	fNewMySQLSink = sink.NewMySQLSink
)

type mounter interface {
	Mount(rawTxn model.RawTxn) (model.Txn, error)
}

type txnChannel struct {
	inputTxn   <-chan model.RawTxn
	outputTxn  chan model.RawTxn
	putBackTxn *model.RawTxn
}

// Forward push all txn with commit ts not greater than ts into targetC.
func (p *txnChannel) Forward(ctx context.Context, ts uint64, targetC chan<- model.RawTxn) {
	if p.putBackTxn != nil {
		t := *p.putBackTxn
		if t.Ts > ts {
			return
		}
		p.putBackTxn = nil
		pushTxn(ctx, targetC, t)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-p.outputTxn:
			if !ok {
				log.Info("Input channel of table closed")
				return
			}
			if t.Ts > ts {
				p.putBack(t)
				return
			}
			pushTxn(ctx, targetC, t)
		}
	}
}

func pushTxn(ctx context.Context, targetC chan<- model.RawTxn, t model.RawTxn) {
	select {
	case <-ctx.Done():
		log.Info("Dropped txn during canceling", zap.Any("txn", t))
		return
	case targetC <- t:
	}
}

func (p *txnChannel) putBack(t model.RawTxn) {
	if p.putBackTxn != nil {
		log.Fatal("can not put back raw txn continuously")
	}
	p.putBackTxn = &t
}

func newTxnChannel(inputTxn <-chan model.RawTxn, chanSize int, handleResolvedTs func(uint64)) *txnChannel {
	tc := &txnChannel{
		inputTxn:  inputTxn,
		outputTxn: make(chan model.RawTxn, chanSize),
	}
	go func() {
		defer close(tc.outputTxn)
		for {
			t, ok := <-tc.inputTxn
			if !ok {
				return
			}
			handleResolvedTs(t.Ts)
			tc.outputTxn <- t
		}
	}()
	return tc
}

type processor struct {
	captureID    string
	changefeedID string
	changefeed   model.ChangeFeedInfo
	limitter     *puller.BlurResourceLimitter
	filter       *txnFilter

	pdCli   pd.Client
	etcdCli kv.CDCEtcdClient

	mounter       mounter
	schemaStorage *schema.Storage
	sink          sink.Sink

	ddlPuller    puller.Puller
	ddlJobsCh    chan model.RawTxn
	ddlResolveTS uint64

	tsRWriter    storage.ProcessorTsRWriter
	resolvedTxns chan model.RawTxn
	executedTxns chan model.RawTxn

	status   *model.TaskStatus
	position *model.TaskPosition

	tablesMu sync.Mutex
	tables   map[int64]*tableInfo

	wg    *errgroup.Group
	errCh chan<- error
}

type tableInfo struct {
	id         int64
	puller     puller.CancellablePuller
	inputChan  *txnChannel
	inputTxn   chan model.RawTxn
	resolvedTS uint64
}

func (t *tableInfo) loadResolvedTS() uint64 {
	return atomic.LoadUint64(&t.resolvedTS)
}

func (t *tableInfo) storeResolvedTS(ts uint64) {
	atomic.StoreUint64(&t.resolvedTS, ts)
}

// NewProcessor creates and returns a processor for the specified change feed
func NewProcessor(ctx context.Context, pdEndpoints []string, changefeed model.ChangeFeedInfo, changefeedID, captureID string, checkpointTs uint64) (*processor, error) {
	pdCli, err := fNewPDCli(pdEndpoints, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Annotatef(err, "create pd client failed, addr: %v", pdEndpoints)
	}

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   pdEndpoints,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: pbackoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		},
	})
	if err != nil {
		return nil, errors.Annotate(err, "new etcd client")
	}
	cdcEtcdCli := kv.NewCDCEtcdClient(etcdCli)
	schemaStorage, err := fCreateSchema(pdEndpoints)
	if err != nil {
		return nil, err
	}

	tsRWriter, err := fNewTsRWriter(cdcEtcdCli, changefeedID, captureID)
	if err != nil {
		return nil, errors.Annotate(err, "failed to create ts RWriter")
	}

	limitter := puller.NewBlurResourceLimmter(defaultMemBufferCapacity)

	// The key in DDL kv pair returned from TiKV is already memcompariable encoded,
	// so we set `needEncode` to false.
	ddlPuller := puller.NewPuller(pdCli, checkpointTs, []util.Span{util.GetDDLSpan()}, false, limitter)

	mounter := fNewMounter(schemaStorage)

	sink, err := fNewMySQLSink(changefeed.SinkURI, schemaStorage, changefeed.Opts)
	if err != nil {
		return nil, err
	}

	filter, err := newTxnFilter(changefeed.GetConfig())
	if err != nil {
		return nil, errors.Trace(err)
	}

	p := &processor{
		limitter:      limitter,
		captureID:     captureID,
		changefeedID:  changefeedID,
		changefeed:    changefeed,
		pdCli:         pdCli,
		etcdCli:       cdcEtcdCli,
		mounter:       mounter,
		schemaStorage: schemaStorage,
		sink:          sink,
		ddlPuller:     ddlPuller,
		filter:        filter,

		tsRWriter:    tsRWriter,
		status:       tsRWriter.GetTaskStatus(),
		position:     &model.TaskPosition{},
		resolvedTxns: make(chan model.RawTxn, 1),
		executedTxns: make(chan model.RawTxn, 1),
		ddlJobsCh:    make(chan model.RawTxn, 16),

		tables: make(map[int64]*tableInfo),
	}

	for _, table := range p.status.TableInfos {
		p.addTable(ctx, int64(table.ID), table.StartTs)
	}

	return p, nil
}

func (p *processor) Run(ctx context.Context, errCh chan<- error) {
	wg, cctx := errgroup.WithContext(ctx)
	p.wg = wg
	p.errCh = errCh

	wg.Go(func() error {
		return p.localResolvedWorker(cctx)
	})

	wg.Go(func() error {
		return p.globalResolvedWorker(cctx)
	})

	wg.Go(func() error {
		return p.syncResolved(cctx)
	})

	wg.Go(func() error {
		return p.pullDDLJob(cctx)
	})

	go func() {
		if err := wg.Wait(); err != nil {
			errCh <- err
		}
	}()
}

// wait blocks until all routines in processor are returned
func (p *processor) wait() {
	err := p.wg.Wait()
	if err != nil && errors.Cause(err) != context.Canceled {
		log.Error("processor wait error",
			zap.String("captureID", p.captureID),
			zap.String("changefeedID", p.changefeedID),
			zap.Error(err),
		)
	}
}

func (p *processor) writeDebugInfo(w io.Writer) {
	fmt.Fprintf(w, "changefeedID: %s, info: %+v, status: %+v\n", p.changefeedID, p.changefeed, p.status)

	p.tablesMu.Lock()
	for _, table := range p.tables {
		fmt.Fprintf(w, "\ttable id: %d, resolveTS: %d\n", table.id, table.loadResolvedTS())
	}
	p.tablesMu.Unlock()

	fmt.Fprintf(w, "\n")
}

// localResolvedWorker do the flowing works.
// 1, update resolve ts by scaning all table's resolve ts.
// 2, update checkpoint ts by consuming entry from p.executedTxns.
// 3, sync TaskStatus between in memory and storage.
// 4, check admin command in TaskStatus and apply corresponding command
func (p *processor) localResolvedWorker(ctx context.Context) error {
	updateInfoTick := time.NewTicker(updateInfoInterval)
	defer updateInfoTick.Stop()

	resolveTsTick := time.NewTicker(resolveTsInterval)
	defer resolveTsTick.Stop()

	for {
		select {
		case <-ctx.Done():
			timedCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			err := p.updateInfo(timedCtx)
			if err != nil {
				log.Error("failed to update info", zap.Error(err))
			}
			cancel()

			log.Info("Local resolved worker exited")
			return ctx.Err()
		case <-resolveTsTick.C:
			p.tablesMu.Lock()
			// no table in this processor
			if len(p.tables) == 0 {
				p.tablesMu.Unlock()
				continue
			}

			minResolvedTs := atomic.LoadUint64(&p.ddlResolveTS)

			for _, table := range p.tables {
				ts := table.loadResolvedTS()
				tableResolvedTsGauge.WithLabelValues(p.changefeedID, p.captureID, strconv.FormatInt(table.id, 10)).Set(float64(oracle.ExtractPhysical(ts)))

				if ts < minResolvedTs {
					minResolvedTs = ts
				}
			}
			p.tablesMu.Unlock()
			p.position.ResolvedTs = minResolvedTs
			resolvedTsGauge.WithLabelValues(p.changefeedID, p.captureID).Set(float64(oracle.ExtractPhysical(minResolvedTs)))
		case e, ok := <-p.executedTxns:
			if !ok {
				log.Info("Resolved worker exited")
				return nil
			}
			if e.IsResolved {
				p.position.CheckPointTs = e.Ts
				checkpointTsGauge.WithLabelValues(p.changefeedID, p.captureID).Set(float64(oracle.ExtractPhysical(e.Ts)))
			}
		case <-updateInfoTick.C:
			t0Update := time.Now()
			err := retry.Run(func() error {
				inErr := p.updateInfo(ctx)
				if errors.Cause(inErr) == model.ErrAdminStopProcessor {
					return backoff.Permanent(inErr)
				}
				return inErr
			}, 3)
			updateInfoDuration.WithLabelValues(p.captureID).Observe(time.Since(t0Update).Seconds())
			if err != nil {
				return errors.Annotate(err, "failed to update info")
			}
		}
	}
}

func (p *processor) updateInfo(ctx context.Context) error {
	err := p.tsRWriter.WritePosition(ctx, p.position)
	if err != nil {
		return errors.Trace(err)
	}
	statusChanged, err := p.tsRWriter.UpdateInfo(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if !statusChanged {
		return nil
	}
	oldStatus := p.status
	p.status = p.tsRWriter.GetTaskStatus()
	if p.status.AdminJobType == model.AdminStop || p.status.AdminJobType == model.AdminRemove {
		err = p.stop(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		return errors.Trace(model.ErrAdminStopProcessor)
	}
	p.handleTables(ctx, oldStatus, p.status, p.position.CheckPointTs)
	syncTableNumGauge.WithLabelValues(p.changefeedID, p.captureID).Set(float64(len(p.status.TableInfos)))

	return retry.Run(func() error {
		err = p.tsRWriter.WriteInfoIntoStorage(ctx)
		switch errors.Cause(err) {
		case model.ErrWriteTsConflict:
			return errors.Trace(err)
		case nil:
			log.Info("update task status", zap.Stringer("status", p.status))
			return nil
		default:
			return backoff.Permanent(errors.Trace(err))
		}
	}, 5)
}

func diffProcessTableInfos(oldInfo, newInfo []*model.ProcessTableInfo) (removed, added []*model.ProcessTableInfo) {
	sort.Slice(oldInfo, func(i, j int) bool {
		return oldInfo[i].ID < oldInfo[j].ID
	})

	sort.Slice(newInfo, func(i, j int) bool {
		return newInfo[i].ID < newInfo[j].ID
	})

	i, j := 0, 0
	for i < len(oldInfo) && j < len(newInfo) {
		if oldInfo[i].ID == newInfo[j].ID {
			i++
			j++
		} else if oldInfo[i].ID < newInfo[j].ID {
			removed = append(removed, oldInfo[i])
			i++
		} else {
			added = append(added, newInfo[j])
			j++
		}
	}
	for ; i < len(oldInfo); i++ {
		removed = append(removed, oldInfo[i])
	}
	for ; j < len(newInfo); j++ {
		added = append(added, newInfo[j])
	}

	if len(removed) > 0 || len(added) > 0 {
		log.Debug("table diff", zap.Reflect("old", oldInfo),
			zap.Reflect("new", newInfo),
			zap.Reflect("add", added),
			zap.Reflect("remove", removed),
		)
	}

	return
}

func (p *processor) removeTable(tableID int64) {
	p.tablesMu.Lock()
	defer p.tablesMu.Unlock()

	log.Debug("remove table", zap.Int64("id", tableID))

	table, ok := p.tables[tableID]
	if !ok {
		log.Warn("table not found", zap.Int64("tableID", tableID))
		return
	}

	table.puller.Cancel()
	delete(p.tables, tableID)
	tableIDStr := strconv.FormatInt(tableID, 10)
	tableInputChanSizeGauge.DeleteLabelValues(p.changefeedID, p.captureID, tableIDStr)
	tableOutputChanSizeGauge.DeleteLabelValues(p.changefeedID, p.captureID, tableIDStr)
	tableResolvedTsGauge.DeleteLabelValues(p.changefeedID, p.captureID, tableIDStr)
	syncTableNumGauge.WithLabelValues(p.changefeedID, p.captureID).Dec()
}

// handleTables handles table scheduler on this processor, add or remove table puller
func (p *processor) handleTables(ctx context.Context, oldInfo, newInfo *model.TaskStatus, checkpointTs uint64) {
	removedTables, addedTables := diffProcessTableInfos(oldInfo.TableInfos, newInfo.TableInfos)

	// remove tables
	for _, pinfo := range removedTables {
		p.removeTable(int64(pinfo.ID))
	}

	// write clock if need
	if newInfo.TablePLock != nil && newInfo.TableCLock == nil {
		newInfo.TableCLock = &model.TableLock{
			Ts:           newInfo.TablePLock.Ts,
			CheckpointTs: checkpointTs,
		}
	}

	// add tables
	for _, pinfo := range addedTables {
		p.addTable(ctx, int64(pinfo.ID), pinfo.StartTs)
	}
}

// globalResolvedWorker read global resolve ts from changefeed level info and forward `tableInputChans` regularly.
func (p *processor) globalResolvedWorker(ctx context.Context) error {
	log.Info("Global resolved worker started")

	var (
		globalResolvedTs     uint64
		lastGlobalResolvedTs uint64
	)

	defer close(p.resolvedTxns)

	retryCfg := backoff.WithMaxRetries(
		backoff.WithContext(
			backoff.NewExponentialBackOff(), ctx),
		3,
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
			globalResolvedTs, err = p.tsRWriter.ReadGlobalResolvedTs(ctx)
			if err != nil {
				log.Error("Global resolved worker: read global resolved ts failed", zap.Error(err))
			}
			return err
		}, retryCfg)
		if err != nil {
			return errors.Trace(err)
		}

		if lastGlobalResolvedTs == globalResolvedTs {
			time.Sleep(waitGlobalResolvedTsDelay)
			continue
		}

		lastGlobalResolvedTs = globalResolvedTs

		wg, cctx := errgroup.WithContext(ctx)

		p.tablesMu.Lock()
		for _, table := range p.tables {
			input := table.inputChan
			wg.Go(func() error {
				// TODO: Forward should return an error if it's canceled
				input.Forward(cctx, globalResolvedTs, p.resolvedTxns)
				return nil
			})
		}
		p.tablesMu.Unlock()

		err = wg.Wait()
		if err != nil {
			return errors.Trace(err)
		}

		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case p.resolvedTxns <- model.RawTxn{
			Ts:         globalResolvedTs,
			IsResolved: true,
		}:
		}
	}
}

// pullDDLJob push ddl job into `p.ddlJobsCh`.
func (p *processor) pullDDLJob(ctx context.Context) error {
	defer close(p.ddlJobsCh)
	errg, ctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		return p.ddlPuller.Run(ctx)
	})

	errg.Go(func() error {
		err := p.ddlPuller.CollectRawTxns(ctx, func(ctxInner context.Context, rawTxn model.RawTxn) error {
			select {
			case <-ctxInner.Done():
				return ctxInner.Err()
			case p.ddlJobsCh <- rawTxn:
				return nil
			}
		})
		if err != nil {
			return errors.Annotate(err, "span: ddl")
		}
		return nil
	})
	return errg.Wait()
}

// syncResolved handle `p.ddlJobsCh` and `p.resolvedTxns`
func (p *processor) syncResolved(ctx context.Context) error {
	const bulkLimit = 128
	var lastResolved model.RawTxn
	pendingTxns := make([]model.Txn, 0, bulkLimit)

	flush := func(ctx2 context.Context) error {
		forwardResolved := func() {
			if lastResolved.Ts > 0 {
				select {
				case p.executedTxns <- lastResolved:
				case <-ctx2.Done():
				}
			}
		}
		if len(pendingTxns) == 0 {
			forwardResolved()
			return nil
		}
		if err := p.sink.EmitDMLs(ctx2, pendingTxns...); err != nil {
			return errors.Trace(err)
		}
		log.Info("DMLs Flushed", zap.Any("resolved", lastResolved), zap.Int("bulk", len(pendingTxns)))
		forwardResolved()
		txnCounter.WithLabelValues("executed", p.changefeedID, p.captureID).Add(float64(len(pendingTxns)))
		pendingTxns = pendingTxns[:0]
		return nil
	}

	defer close(p.executedTxns)

	flushTick := time.NewTicker(flushDMLsInterval)

	for {
		select {
		case rawTxn, ok := <-p.ddlJobsCh:
			if !ok {
				return nil
			}
			if rawTxn.IsFake() {
				atomic.StoreUint64(&p.ddlResolveTS, rawTxn.Ts)
				continue
			}
			t, err := p.mounter.Mount(rawTxn)
			if err != nil {
				return errors.Trace(err)
			}
			if !t.IsDDL() {
				if t.IsDML() {
					log.Warn("Receive DML txn from ddlJobsCh", zap.Any("txn", t))
				}
				continue
			}
			p.schemaStorage.AddJob(t.DDL.Job)
			if err := flush(ctx); err != nil {
				return errors.Trace(err)
			}
			atomic.StoreUint64(&p.ddlResolveTS, rawTxn.Ts)
		case rawTxn, ok := <-p.resolvedTxns:
			if !ok {
				return nil
			}
			if rawTxn.IsResolved {
				lastResolved = rawTxn
				continue
			}

			if err := p.schemaStorage.HandlePreviousDDLJobIfNeed(rawTxn.Ts); err != nil {
				return errors.Trace(err)
			}
			txn, err := p.mounter.Mount(rawTxn)
			if err != nil {
				return errors.Trace(err)
			}
			if p.filter.ShouldIgnoreTxn(&txn) {
				log.Info("DML txn ignored", zap.Uint64("ts", txn.Ts))
				continue
			}
			p.filter.FilterTxn(&txn)
			if len(txn.DMLs) == 0 {
				continue
			}
			pendingTxns = append(pendingTxns, txn)
			if len(pendingTxns) >= bulkLimit {
				if err := flush(ctx); err != nil {
					return errors.Trace(err)
				}
			}
		case <-ctx.Done():
			err := ctx.Err()
			if err == context.Canceled {
				timedCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				if err := flush(timedCtx); err != nil {
					log.Error("Failed to flush Txns before quiting", zap.Error(err))
				}
				cancel()
			}
			log.Info("syncResolved stopped")
			return ctx.Err()
		case <-flushTick.C:
			if err := flush(ctx); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func createSchemaStore(pdEndpoints []string) (*schema.Storage, error) {
	// here we create another pb client,we should reuse them
	kvStore, err := createTiStore(strings.Join(pdEndpoints, ","))
	if err != nil {
		return nil, err
	}
	originalJobs, err := kv.LoadHistoryDDLJobs(kvStore)
	jobs := make([]*timodel.Job, 0, len(originalJobs))
	if err != nil {
		return nil, err
	}
	for _, job := range originalJobs {
		if job.State != timodel.JobStateSynced && job.State != timodel.JobStateDone {
			continue
		}
		err := resetFinishedTs(kvStore.(tikv.Storage), job)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, job)
	}
	schemaStorage, err := schema.NewStorage(jobs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return schemaStorage, nil
}

func resetFinishedTs(kvStore tikv.Storage, job *timodel.Job) error {
	helper := helper.NewHelper(kvStore)
	diffKey := schemaDiffKey(job.BinlogInfo.SchemaVersion)
	resp, err := helper.GetMvccByEncodedKey(diffKey)
	if err != nil {
		return errors.Trace(err)
	}
	mvcc := resp.GetInfo()
	if mvcc == nil || len(mvcc.Writes) == 0 {
		return errors.NotFoundf("mvcc info, ddl job id: %d, schema version: %d", job.ID, job.BinlogInfo.SchemaVersion)
	}
	var finishedTS uint64
	for _, w := range mvcc.Writes {
		if finishedTS < w.CommitTs {
			finishedTS = w.CommitTs
		}
	}
	job.BinlogInfo.FinishedTS = finishedTS
	return nil
}

func schemaDiffKey(schemaVersion int64) []byte {
	metaPrefix := []byte("m")
	mSchemaDiffPrefix := "Diff"
	StringData := 's'
	key := []byte(fmt.Sprintf("%s:%d", mSchemaDiffPrefix, schemaVersion))

	ek := make([]byte, 0, len(metaPrefix)+len(key)+24)
	ek = append(ek, metaPrefix...)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(StringData))
}

func createTsRWriter(cli kv.CDCEtcdClient, changefeedID, captureID string) (storage.ProcessorTsRWriter, error) {
	return storage.NewProcessorTsEtcdRWriter(cli, changefeedID, captureID)
}

// getTsRwriter is used in unit test only
func (p *processor) getTsRwriter() storage.ProcessorTsRWriter {
	return p.tsRWriter
}

func (p *processor) addTable(ctx context.Context, tableID int64, startTs uint64) {
	p.tablesMu.Lock()
	defer p.tablesMu.Unlock()

	log.Debug("Add table", zap.Int64("tableID", tableID))
	if _, ok := p.tables[tableID]; ok {
		log.Warn("Ignore existing table", zap.Int64("ID", tableID))
	}

	table := &tableInfo{
		id:       tableID,
		inputTxn: make(chan model.RawTxn, defaultInputTxnChanSize),
	}

	tc := newTxnChannel(table.inputTxn, defaultOutputTxnChanSize, func(resolvedTs uint64) {
		table.storeResolvedTS(resolvedTs)
	})
	table.inputChan = tc

	span := util.GetTableSpan(tableID, true)

	ctx, cancel := context.WithCancel(ctx)
	plr := p.startPuller(ctx, span, startTs, table.inputTxn, p.errCh)
	table.puller = puller.CancellablePuller{Puller: plr, Cancel: cancel}
	p.collectMetrics(ctx, tableID, tc)

	p.tables[tableID] = table
	syncTableNumGauge.WithLabelValues(p.changefeedID, p.captureID).Inc()
}

func (p *processor) collectMetrics(ctx context.Context, tableID int64, tc *txnChannel) {
	go func() {
		for {
			tableIDStr := strconv.FormatInt(tableID, 10)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
				tableInputChanSizeGauge.WithLabelValues(p.changefeedID, p.captureID, tableIDStr).Set(float64(len(tc.inputTxn)))
				tableOutputChanSizeGauge.WithLabelValues(p.changefeedID, p.captureID, tableIDStr).Set(float64(len(tc.outputTxn)))
			}
		}
	}()
}

// startPuller start pull data with span and push resolved txn into txnChan in timestamp increasing order.
func (p *processor) startPuller(ctx context.Context, span util.Span, checkpointTs uint64, txnChan chan<- model.RawTxn, errCh chan<- error) puller.Puller {
	// Set it up so that one failed goroutine cancels all others sharing the same ctx
	errg, ctx := errgroup.WithContext(ctx)

	// The key in DML kv pair returned from TiKV is not memcompariable encoded,
	// so we set `needEncode` to true.
	puller := puller.NewPuller(p.pdCli, checkpointTs, []util.Span{span}, true, p.limitter)

	errg.Go(func() error {
		return puller.Run(ctx)
	})

	errg.Go(func() error {
		defer close(txnChan)
		err := puller.CollectRawTxns(ctx, func(ctxInner context.Context, rawTxn model.RawTxn) error {
			select {
			case <-ctxInner.Done():
				return ctxInner.Err()
			case txnChan <- rawTxn:
				txnCounter.WithLabelValues("received", p.changefeedID, p.captureID).Inc()
				return nil
			}
		})
		return errors.Annotatef(err, "span: %v", span)
	})

	go func() {
		err := errg.Wait()
		if errors.Cause(err) != context.Canceled {
			errCh <- err
		}
	}()

	return puller
}

func (p *processor) stop(ctx context.Context) error {
	p.tablesMu.Lock()
	for _, tbl := range p.tables {
		tbl.puller.Cancel()
	}
	p.tablesMu.Unlock()
	return errors.Trace(p.etcdCli.DeleteTaskStatus(ctx, p.changefeedID, p.captureID))
}

func newMounter(schema *schema.Storage) mounter {
	return entry.NewTxnMounter(schema)
}
