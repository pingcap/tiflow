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
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/cdc/roles/storage"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	pbackoff "google.golang.org/grpc/backoff"
)

const (
	updateInfoInterval          = time.Millisecond * 500
	resolveTsInterval           = time.Millisecond * 500
	waitGlobalResolvedTsDelay   = time.Millisecond * 500
	waitFallbackResolvedTsDelay = time.Millisecond * 500

	defaultOutputChanSize = 128

	// defaultMemBufferCapacity is the default memory buffer per change feed.
	defaultMemBufferCapacity int64 = 10 * 1024 * 1024 * 1024 // 10G

	defaultProcessorSessionTTL = 3 // 3 seconds
)

var (
	fNewPDCli     = pd.NewClient
	fNewTsRWriter = createTsRWriter
)

type processor struct {
	id           string
	captureID    string
	changefeedID string
	changefeed   model.ChangeFeedInfo
	limitter     *puller.BlurResourceLimitter

	pdCli   pd.Client
	etcdCli kv.CDCEtcdClient
	session *concurrency.Session

	sink sink.Sink

	ddlPuller     puller.Puller
	schemaBuilder *entry.StorageBuilder

	tsRWriter storage.ProcessorTsRWriter
	output    chan *model.RowChangedEvent

	status             *model.TaskStatus
	position           *model.TaskPosition
	resolvedTsFallback int32

	tablesMu sync.Mutex
	tables   map[int64]*tableInfo

	wg    *errgroup.Group
	errCh chan<- error
}

type tableInfo struct {
	id         int64
	mounter    entry.Mounter
	resolvedTS uint64
	cancel     context.CancelFunc
}

func (t *tableInfo) loadResolvedTS() uint64 {
	return atomic.LoadUint64(&t.resolvedTS)
}

func (t *tableInfo) storeResolvedTS(ts uint64) {
	atomic.StoreUint64(&t.resolvedTS, ts)
}

// NewProcessor creates and returns a processor for the specified change feed
func NewProcessor(
	ctx context.Context,
	pdEndpoints []string,
	changefeed model.ChangeFeedInfo,
	sink sink.Sink,
	changefeedID, captureID string,
	checkpointTs uint64) (*processor, error) {
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
	sess, err := concurrency.NewSession(etcdCli,
		concurrency.WithTTL(defaultProcessorSessionTTL))
	if err != nil {
		return nil, errors.Annotate(err, "new etcd session")
	}
	cdcEtcdCli := kv.NewCDCEtcdClient(etcdCli)

	tsRWriter, err := fNewTsRWriter(cdcEtcdCli, changefeedID, captureID)
	if err != nil {
		return nil, errors.Annotate(err, "failed to create ts RWriter")
	}

	limitter := puller.NewBlurResourceLimmter(defaultMemBufferCapacity)

	// The key in DDL kv pair returned from TiKV is already memcompariable encoded,
	// so we set `needEncode` to false.
	ddlPuller := puller.NewPuller(pdCli, checkpointTs, []util.Span{util.GetDDLSpan(), util.GetAddIndexDDLSpan()}, false, limitter)
	ddlEventCh := ddlPuller.SortedOutput(ctx)
	schemaBuilder, err := createSchemaBuilder(pdEndpoints, ddlEventCh)
	if err != nil {
		return nil, errors.Trace(err)
	}

	p := &processor{
		id:            uuid.New().String(),
		limitter:      limitter,
		captureID:     captureID,
		changefeedID:  changefeedID,
		changefeed:    changefeed,
		pdCli:         pdCli,
		etcdCli:       cdcEtcdCli,
		session:       sess,
		sink:          sink,
		ddlPuller:     ddlPuller,
		schemaBuilder: schemaBuilder,

		tsRWriter: tsRWriter,
		status:    tsRWriter.GetTaskStatus(),
		position:  &model.TaskPosition{CheckPointTs: checkpointTs},
		output:    make(chan *model.RowChangedEvent, defaultOutputChanSize),

		tables: make(map[int64]*tableInfo),
	}

	for _, table := range p.status.TableInfos {
		go p.addTable(ctx, int64(table.ID), table.StartTs)
	}

	return p, nil
}

func (p *processor) Run(ctx context.Context, errCh chan<- error) {
	wg, cctx := errgroup.WithContext(ctx)
	p.wg = wg
	p.errCh = errCh

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
		return p.ddlPuller.Run(cctx)
	})

	wg.Go(func() error {
		return p.schemaBuilder.Run(cctx)
	})

	wg.Go(func() error {
		return p.sink.PrintStatus(cctx)
	})

	if err := p.register(ctx); err != nil {
		errCh <- err
	}
	go func() {
		if err := wg.Wait(); err != nil {
			errCh <- err
		}
		_ = p.deregister(ctx)
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
func (p *processor) positionWorker(ctx context.Context) error {
	updateInfoTick := time.NewTicker(updateInfoInterval)
	resolveTsTick := time.NewTicker(resolveTsInterval)
	checkpointTsTick := time.NewTicker(resolveTsInterval)

	updateInfo := func() error {
		t0Update := time.Now()
		err := retry.Run(500*time.Millisecond, 3, func() error {
			inErr := p.updateInfo(ctx)
			if errors.Cause(inErr) == model.ErrAdminStopProcessor {
				return backoff.Permanent(inErr)
			}
			return inErr
		})
		updateInfoDuration.WithLabelValues(p.captureID).Observe(time.Since(t0Update).Seconds())
		if err != nil {
			return errors.Annotate(err, "failed to update info")
		}
		return nil
	}

	defer func() {
		updateInfoTick.Stop()
		resolveTsTick.Stop()
		checkpointTsTick.Stop()

		err := updateInfo()
		if err != nil && errors.Cause(err) != context.Canceled {
			log.Error("failed to update info", zap.Error(err))
		}

		log.Info("Local resolved worker exited")
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resolveTsTick.C:
			minResolvedTs := p.schemaBuilder.GetResolvedTs()
			p.tablesMu.Lock()
			for _, table := range p.tables {
				ts := table.loadResolvedTS()
				tableResolvedTsGauge.WithLabelValues(p.changefeedID, p.captureID, strconv.FormatInt(table.id, 10)).Set(float64(oracle.ExtractPhysical(ts)))

				if ts < minResolvedTs {
					minResolvedTs = ts
				}
			}
			p.tablesMu.Unlock()
			// some puller still haven't received the row changed data
			if minResolvedTs < p.position.ResolvedTs {
				atomic.StoreInt32(&p.resolvedTsFallback, 1)
				continue
			}
			atomic.StoreInt32(&p.resolvedTsFallback, 0)

			if minResolvedTs == p.position.ResolvedTs {
				continue
			}

			p.position.ResolvedTs = minResolvedTs
			select {
			case <-ctx.Done():
				return ctx.Err()
			case p.output <- &model.RowChangedEvent{Resolved: true, Ts: minResolvedTs}:
			}
			resolvedTsGauge.WithLabelValues(p.changefeedID, p.captureID).Set(float64(oracle.ExtractPhysical(minResolvedTs)))
		case <-checkpointTsTick.C:
			checkpointTs := p.sink.CheckpointTs()
			if p.position.CheckPointTs >= checkpointTs {
				continue
			}
			p.position.CheckPointTs = checkpointTs
			checkpointTsGauge.WithLabelValues(p.changefeedID, p.captureID).Set(float64(oracle.ExtractPhysical(checkpointTs)))
		case <-updateInfoTick.C:
			err := updateInfo()
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (p *processor) updateInfo(ctx context.Context) error {
	err := p.tsRWriter.WritePosition(ctx, p.position)
	if err != nil {
		return errors.Trace(err)
	}
	log.Debug("update task position", zap.Stringer("status", p.position))
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

	return retry.Run(500*time.Millisecond, 5, func() error {
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

	table.cancel()
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

// globalStatusWorker read global resolve ts from changefeed level info and forward `tableInputChans` regularly.
func (p *processor) globalStatusWorker(ctx context.Context) error {
	log.Info("Global status worker started")

	var (
		changefeedStatus *model.ChangeFeedStatus
		lastCheckPointTs uint64
		lastResolvedTs   uint64
	)

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
			changefeedStatus, err = p.tsRWriter.GetChangeFeedStatus(ctx)
			if err != nil {
				log.Error("Global resolved worker: read global resolved ts failed", zap.Error(err))
			}
			return err
		}, retryCfg)
		if err != nil {
			return errors.Trace(err)
		}

		if lastResolvedTs == changefeedStatus.ResolvedTs &&
			lastCheckPointTs == changefeedStatus.CheckpointTs {
			time.Sleep(waitGlobalResolvedTsDelay)
			continue
		}

		if lastCheckPointTs < changefeedStatus.CheckpointTs {
			err = p.schemaBuilder.DoGc(changefeedStatus.CheckpointTs)
			if err != nil {
				return errors.Trace(err)
			}
			lastCheckPointTs = changefeedStatus.CheckpointTs
		}

		if atomic.LoadInt32(&p.resolvedTsFallback) != 0 {
			time.Sleep(waitFallbackResolvedTsDelay)
			continue
		}

		if lastResolvedTs < changefeedStatus.ResolvedTs {
			err = p.sink.EmitResolvedEvent(ctx, changefeedStatus.ResolvedTs)
			if err != nil {
				return errors.Trace(err)
			}
			lastResolvedTs = changefeedStatus.ResolvedTs
		}

	}
}

// syncResolved handle `p.ddlJobsCh` and `p.resolvedTxns`
func (p *processor) syncResolved(ctx context.Context) error {
	defer log.Info("syncResolved stopped")
	for {
		select {
		case row := <-p.output:
			err := p.sink.EmitRowChangedEvent(ctx, row)
			if err != nil {
				return errors.Trace(err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func createSchemaBuilder(pdEndpoints []string, ddlEventCh <-chan *model.RawKVEntry) (*entry.StorageBuilder, error) {
	// TODO here we create another pb client,we should reuse them
	kvStore, err := kv.CreateTiStore(strings.Join(pdEndpoints, ","))
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs, err := kv.LoadHistoryDDLJobs(kvStore)
	if err != nil {
		return nil, errors.Trace(err)
	}
	builder := entry.NewStorageBuilder(jobs, ddlEventCh)
	return builder, nil
}

func createTsRWriter(cli kv.CDCEtcdClient, changefeedID, captureID string) (storage.ProcessorTsRWriter, error) {
	return storage.NewProcessorTsEtcdRWriter(cli, changefeedID, captureID)
}

// getTsRwriter is used in unit test only
//func (p *processor) getTsRwriter() storage.ProcessorTsRWriter {
//	return p.tsRWriter
//}

func (p *processor) addTable(ctx context.Context, tableID int64, startTs uint64) {
	p.tablesMu.Lock()
	defer p.tablesMu.Unlock()

	log.Debug("Add table", zap.Int64("tableID", tableID))
	if _, ok := p.tables[tableID]; ok {
		log.Warn("Ignore existing table", zap.Int64("ID", tableID))
	}

	ctx, cancel := context.WithCancel(ctx)
	table := &tableInfo{
		id:         tableID,
		resolvedTS: startTs,
		cancel:     cancel,
	}

	// start table puller
	// The key in DML kv pair returned from TiKV is not memcompariable encoded,
	// so we set `needEncode` to true.
	span := util.GetTableSpan(tableID, true)
	puller := puller.NewPuller(p.pdCli, startTs, []util.Span{span}, true, p.limitter)
	go func() {
		err := puller.Run(ctx)
		if errors.Cause(err) != context.Canceled {
			p.errCh <- err
		}
	}()
	storage, err := p.schemaBuilder.Build(startTs)
	if err != nil {
		p.errCh <- errors.Trace(err)
	}
	// start mounter
	mounter := entry.NewMounter(puller.SortedOutput(ctx), storage)
	go func() {
		err := mounter.Run(ctx)
		if errors.Cause(err) != context.Canceled {
			p.errCh <- err
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				if errors.Cause(ctx.Err()) != context.Canceled {
					p.errCh <- ctx.Err()
				}
				return
			case row := <-mounter.Output():
				if row.Resolved {
					table.storeResolvedTS(row.Ts)
					continue
				}
				select {
				case <-ctx.Done():
					if errors.Cause(ctx.Err()) != context.Canceled {
						p.errCh <- ctx.Err()
					}
					return
				case p.output <- row:
				}
			}
		}
	}()
	table.mounter = mounter
	p.tables[tableID] = table
	syncTableNumGauge.WithLabelValues(p.changefeedID, p.captureID).Inc()
}

func (p *processor) stop(ctx context.Context) error {
	p.tablesMu.Lock()
	for _, tbl := range p.tables {
		tbl.cancel()
	}
	p.tablesMu.Unlock()
	p.session.Close()

	if err := p.etcdCli.DeleteTaskPosition(ctx, p.changefeedID, p.captureID); err != nil {
		return err
	}
	if err := p.etcdCli.DeleteTaskStatus(ctx, p.changefeedID, p.captureID); err != nil {
		return err
	}

	return errors.Trace(p.deregister(ctx))
}

func (p *processor) register(ctx context.Context) error {
	info := &model.ProcessorInfo{
		ID:           p.id,
		CaptureID:    p.captureID,
		ChangeFeedID: p.changefeedID,
	}
	return p.etcdCli.PutProcessorInfo(ctx, p.captureID, info, p.session.Lease())
}
func (p *processor) deregister(ctx context.Context) error {
	return p.etcdCli.DeleteProcessorInfo(ctx, p.captureID, p.id)
}
