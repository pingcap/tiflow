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
	"math"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/cdc/roles/storage"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var (
	fCreateSchema = createSchemaStore
	fNewPDCli     = pd.NewClient
	fNewTsRWriter = createTsRWriter
	fNewMounter   = newMounter
	fNewMySQLSink = sink.NewMySQLSink
)

type mounter interface {
	Mount(rawTxn model.RawTxn) (*model.Txn, error)
}

// ProcessorTsRWriter reads or writes the resolvedTs and checkpointTs from the storage
type ProcessorTsRWriter interface {
	// WriteResolvedTs writes the loacl resolvedTs into the storage
	WriteResolvedTs(ctx context.Context, resolvedTs uint64) error
	// WriteCheckpointTs writes the checkpointTs into the storage
	WriteCheckpointTs(ctx context.Context, checkpointTs uint64) error
	// ReadGlobalResolvedTs reads the global resolvedTs from the storage
	ReadGlobalResolvedTs(ctx context.Context) (uint64, error)
	// GetSubChangeFeedInfo returns a copy of *model.SubChangeFeedInfo
	GetSubChangeFeedInfo() *model.SubChangeFeedInfo
	// WriteTableCLock writes C-lock to the storage
	WriteTableCLock(ctx context.Context, checkpointTs uint64) error
}

type txnChannel struct {
	inputTxn   <-chan model.RawTxn
	outputTxn  chan model.RawTxn
	putBackTxn *model.RawTxn
}

// Forward push all txn with commit ts not greater than ts into entryC.
func (p *txnChannel) Forward(ctx context.Context, tableID int64, ts uint64, entryC chan<- ProcessorEntry) {
	if p.putBackTxn != nil {
		t := *p.putBackTxn
		if t.Ts > ts {
			return
		}
		p.putBackTxn = nil
		pushProcessorEntry(ctx, entryC, newProcessorTxnEntry(t))
	}
	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-p.outputTxn:
			if !ok {
				log.Info("Input channel of table closed", zap.Int64("tableID", tableID))
				return
			}
			if t.Ts > ts {
				p.PutBack(t)
				return
			}
			pushProcessorEntry(ctx, entryC, newProcessorTxnEntry(t))
		}
	}
}

func pushProcessorEntry(ctx context.Context, entryC chan<- ProcessorEntry, e ProcessorEntry) {
	select {
	case <-ctx.Done():
		log.Info("lost processor entry during canceling", zap.Any("entry", e))
		return
	case entryC <- e:
	}
}

func (p *txnChannel) PutBack(t model.RawTxn) {
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

type processorEntryType int

const (
	processorEntryUnknown processorEntryType = iota
	processorEntryDMLS
	processorEntryResolved
)

// ProcessorEntry contains a transaction to be processed
// TODO: Do we really need this extra layer of wrapping?
type ProcessorEntry struct {
	Txn model.RawTxn
	Ts  uint64
	Typ processorEntryType
}

func newProcessorTxnEntry(txn model.RawTxn) ProcessorEntry {
	return ProcessorEntry{
		Txn: txn,
		Ts:  txn.Ts,
		Typ: processorEntryDMLS,
	}
}

func newProcessorResolvedEntry(ts uint64) ProcessorEntry {
	return ProcessorEntry{
		Ts:  ts,
		Typ: processorEntryResolved,
	}
}

const ddlPullerID int64 = -1

type processor struct {
	captureID    string
	changefeedID string
	changefeed   model.ChangeFeedDetail

	pdCli   pd.Client
	etcdCli *clientv3.Client

	mounter       mounter
	schemaStorage *schema.Storage
	sink          sink.Sink
	ddlPuller     puller.Puller

	tableResolvedTs sync.Map
	tsRWriter       ProcessorTsRWriter
	resolvedEntries chan ProcessorEntry
	executedEntries chan ProcessorEntry
	ddlJobsCh       chan model.RawTxn

	tblPullers map[int64]puller.CancellablePuller

	inputChansLock  sync.RWMutex
	tableInputChans map[int64]*txnChannel

	wg    *errgroup.Group
	errCh chan<- error
}

// NewProcessor creates and returns a processor for the specified change feed
func NewProcessor(pdEndpoints []string, changefeed model.ChangeFeedDetail, changefeedID, captureID string) (*processor, error) {
	pdCli, err := fNewPDCli(pdEndpoints, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Annotatef(err, "create pd client failed, addr: %v", pdEndpoints)
	}

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   pdEndpoints,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBackoffMaxDelay(time.Second * 3),
		},
	})
	if err != nil {
		return nil, errors.Annotate(err, "new etcd client")
	}

	schemaStorage, err := fCreateSchema(pdEndpoints)
	if err != nil {
		return nil, err
	}

	tsRWriter := fNewTsRWriter(etcdCli, changefeedID, captureID)

	// The key in DDL kv pair returned from TiKV is already memcompariable encoded,
	// so we set `needEncode` to false.
	ddlPuller := puller.NewPuller(pdCli, changefeed.StartTs, []util.Span{util.GetDDLSpan()}, false)

	// TODO: get time zone from config
	mounter := fNewMounter(schemaStorage, time.UTC)

	sink, err := fNewMySQLSink(changefeed.SinkURI, schemaStorage, changefeed.Opts)
	if err != nil {
		return nil, err
	}

	p := &processor{
		captureID:     captureID,
		changefeedID:  changefeedID,
		changefeed:    changefeed,
		pdCli:         pdCli,
		etcdCli:       etcdCli,
		mounter:       mounter,
		schemaStorage: schemaStorage,
		sink:          sink,
		ddlPuller:     ddlPuller,

		tsRWriter: tsRWriter,
		// TODO set the channel size
		resolvedEntries: make(chan ProcessorEntry),
		// TODO set the channel size
		executedEntries: make(chan ProcessorEntry),
		ddlJobsCh:       make(chan model.RawTxn, 16),

		tblPullers:      make(map[int64]puller.CancellablePuller),
		tableInputChans: make(map[int64]*txnChannel),
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
		return p.checkpointWorker(cctx)
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
		err := wg.Wait()
		if err != nil {
			errCh <- err
		}
	}()
}

func (p *processor) writeDebugInfo(w io.Writer) {
	fmt.Fprintf(w, "changefeedID: %s, detail: %+v", p.changefeedID, p.changefeed)
}

// localResolvedWorker scan all table's resolve ts and update resolved ts in subchangefeed info regularly.
func (p *processor) localResolvedWorker(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Info("Local resolved worker exited")
			if ctx.Err() != context.Canceled {
				return errors.Trace(ctx.Err())
			}
			return nil
		case <-time.After(1 * time.Second):
			minResolvedTs := uint64(math.MaxUint64)
			p.tableResolvedTs.Range(func(key, value interface{}) bool {
				resolvedTs := value.(uint64)
				if minResolvedTs > resolvedTs {
					minResolvedTs = resolvedTs
				}
				return true
			})
			if minResolvedTs == uint64(math.MaxUint64) {
				// no table in this processor
				continue
			}
			err := p.tsRWriter.WriteResolvedTs(ctx, minResolvedTs)
			// TODO: add retry when meeting error
			if err != nil {
				return errors.Annotate(err, "write resolved ts")
			}
		}
	}
}

func diffProcessTableInfos(oldInfo, newInfo []*model.ProcessTableInfo) (removed, added []*model.ProcessTableInfo) {
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
	return
}

func (p *processor) removeTable(tableID int64) {
	puller, ok := p.tblPullers[tableID]
	if !ok {
		log.Warn("table puller not found", zap.Int64("tableID", tableID))
	} else {
		puller.Cancel()
		delete(p.tblPullers, tableID)
	}
	p.inputChansLock.Lock()
	delete(p.tableInputChans, tableID)
	p.inputChansLock.Unlock()
}

// handleTables handles table scheduler on this processor, add or remove table puller
func (p *processor) handleTables(ctx context.Context, oldInfo, newInfo *model.SubChangeFeedInfo, checkpointTs uint64) error {
	removedTables, addedTables := diffProcessTableInfos(oldInfo.TableInfos, newInfo.TableInfos)
	// some tables are removed
	if newInfo.TablePLock != nil && newInfo.TableCLock == nil {
		for _, pinfo := range removedTables {
			p.removeTable(int64(pinfo.ID))
		}
		err := p.tsRWriter.WriteTableCLock(ctx, checkpointTs)
		if err != nil {
			return err
		}
	}
	for _, pinfo := range addedTables {
		err := p.addTable(ctx, int64(pinfo.ID), pinfo.StartTs)
		if err != nil {
			return err
		}
	}
	return nil
}

// checkpointWorker consume data from `executedEntries` and update checkpoint ts in subchangefeed info regularly.
func (p *processor) checkpointWorker(ctx context.Context) error {
	checkpointTs := uint64(0)
	for {
		select {
		case <-ctx.Done():
			timedCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := p.retryWriteCP(timedCtx, checkpointTs, 1)
			cancel()
			if err != nil {
				log.Error("Failed to flush checkpoint", zap.Uint64("checkpoint", checkpointTs), zap.Error(err))
			}
			log.Info("Checkpoint worker exited")
			if ctx.Err() != context.Canceled {
				return errors.Trace(ctx.Err())
			}
			return nil
		case e, ok := <-p.executedEntries:
			if !ok {
				log.Info("Checkpoint worker exited")
				return nil
			}
			if e.Typ == processorEntryResolved {
				checkpointTs = e.Ts
			}
		case <-time.After(1 * time.Second):
			oldInfo := p.tsRWriter.GetSubChangeFeedInfo()
			if err := p.retryWriteCP(ctx, checkpointTs, 3); err != nil {
				return err
			}
			newInfo := p.tsRWriter.GetSubChangeFeedInfo()
			if err := p.handleTables(ctx, oldInfo, newInfo, checkpointTs); err != nil {
				return errors.Annotate(err, "handle tables")
			}
		}
	}
}

func (p *processor) retryWriteCP(ctx context.Context, cpTs uint64, maxRetry uint64) error {
	retryCfg := backoff.WithMaxRetries(
		backoff.WithContext(
			backoff.NewExponentialBackOff(), ctx),
		maxRetry,
	)
	err := backoff.Retry(func() error {
		err := p.tsRWriter.WriteCheckpointTs(ctx, cpTs)
		if err != nil {
			switch errors.Cause(err) {
			case context.Canceled, context.DeadlineExceeded:
				return backoff.Permanent(err)
			}
		}
		return err
	}, retryCfg)
	if err != nil {
		return errors.Annotate(err, "write checkpoint ts")
	}
	return nil
}

// globalResolvedWorker read global resolve ts from changefeed level info and forward `tableInputChans` regularly.
func (p *processor) globalResolvedWorker(ctx context.Context) error {
	log.Info("Global resolved worker started")

	var (
		globalResolvedTs     uint64
		lastGlobalResolvedTs uint64
	)

	defer func() {
		close(p.resolvedEntries)
		close(p.executedEntries)
	}()

	retryCfg := backoff.WithMaxRetries(
		backoff.WithContext(
			backoff.NewExponentialBackOff(), ctx),
		3,
	)
	for {
		wg, cctx := errgroup.WithContext(ctx)
		select {
		case <-ctx.Done():
			log.Info("Global resolved worker exited")
			if ctx.Err() != context.Canceled {
				return errors.Trace(ctx.Err())
			}
			return nil
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
		if errors.Cause(err) == context.Canceled {
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}
		if lastGlobalResolvedTs == globalResolvedTs {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		lastGlobalResolvedTs = globalResolvedTs
		p.inputChansLock.RLock()
		for table, input := range p.tableInputChans {
			table := table
			input := input
			wg.Go(func() error {
				input.Forward(cctx, table, globalResolvedTs, p.resolvedEntries)
				return nil
			})
		}
		p.inputChansLock.RUnlock()
		wg.Wait()
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if errors.Cause(err) == context.Canceled {
				return nil
			}
			return errors.Trace(err)
		case p.resolvedEntries <- newProcessorResolvedEntry(globalResolvedTs):
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
			case <-ctx.Done():
				return ctx.Err()
			case <-ctxInner.Done():
				return ctxInner.Err()
			case p.ddlJobsCh <- rawTxn:
				return nil
			}
		})
		if err != nil && errors.Cause(err) != context.Canceled {
			return errors.Annotate(err, "span: ddl")
		}
		return nil
	})
	return errg.Wait()
}

// syncResolved handle `p.ddlJobsCh` and `p.resolvedEntries`
func (p *processor) syncResolved(ctx context.Context) error {
	for {
		select {
		case rawTxn, ok := <-p.ddlJobsCh:
			if !ok {
				return nil
			}
			if len(rawTxn.Entries) == 0 {
				// fake txn
				p.tableResolvedTs.Store(ddlPullerID, rawTxn.Ts)
				continue
			}
			t, err := p.mounter.Mount(rawTxn)
			if err != nil {
				return errors.Trace(err)
			}
			if !t.IsDDL() {
				continue
			}
			p.schemaStorage.AddJob(t.DDL.Job)
			p.tableResolvedTs.Store(ddlPullerID, rawTxn.Ts)
		case e, ok := <-p.resolvedEntries:
			if !ok {
				return nil
			}
			switch e.Typ {
			case processorEntryDMLS:
				err := p.schemaStorage.HandlePreviousDDLJobIfNeed(e.Ts)
				if err != nil {
					return errors.Trace(err)
				}
				txn, err := p.mounter.Mount(e.Txn)
				if err != nil {
					return errors.Trace(err)
				}
				if err := p.sink.Emit(ctx, *txn); err != nil {
					if err != context.Canceled {
						return errors.Trace(err)
					}
					return nil
				}
			case processorEntryResolved:
				select {
				case p.executedEntries <- e:
				case <-ctx.Done():
					if err := ctx.Err(); err != context.Canceled {
						return errors.Trace(err)
					}
					return nil
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *processor) setInputChan(tableID int64, inputTxn <-chan model.RawTxn) error {
	tc := newTxnChannel(inputTxn, 64, func(resolvedTs uint64) {
		p.tableResolvedTs.Store(tableID, resolvedTs)
	})
	p.inputChansLock.Lock()
	defer p.inputChansLock.Unlock()
	if _, exist := p.tableInputChans[tableID]; exist {
		return errors.Errorf("this chan is already exist, tableID: %d", tableID)
	}
	p.tableInputChans[tableID] = tc
	return nil
}

func createSchemaStore(pdEndpoints []string) (*schema.Storage, error) {
	// here we create another pb client,we should reuse them
	kvStore, err := createTiStore(strings.Join(pdEndpoints, ","))
	if err != nil {
		return nil, err
	}
	jobs, err := kv.LoadHistoryDDLJobs(kvStore)
	if err != nil {
		return nil, err
	}
	schemaStorage, err := schema.NewStorage(jobs, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return schemaStorage, nil
}

func createTsRWriter(cli *clientv3.Client, changefeedID, captureID string) ProcessorTsRWriter {
	// TODO: NewProcessorTsEtcdRWriter should return an interface, currently there
	// exists cycle import issue, will refine it later
	return storage.NewProcessorTsEtcdRWriter(cli, changefeedID, captureID)
}

// getTsRwriter is used in unit test only
func (p *processor) getTsRwriter() ProcessorTsRWriter {
	return p.tsRWriter
}

func (p *processor) addTable(ctx context.Context, tableID int64, startTs uint64) error {
	log.Debug("Add table", zap.Int64("tableID", tableID))
	// TODO: Make sure it's threadsafe or prove that it doesn't have to be
	if _, ok := p.tblPullers[tableID]; ok {
		log.Warn("Ignore existing table", zap.Int64("ID", tableID))
		return nil
	}
	span := util.GetTableSpan(tableID, true)
	// TODO: How large should the buffer be?
	txnChan := make(chan model.RawTxn, 16)
	if err := p.setInputChan(tableID, txnChan); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	plr := p.startPuller(ctx, span, startTs, txnChan, p.errCh)
	p.tblPullers[tableID] = puller.CancellablePuller{Puller: plr, Cancel: cancel}
	return nil
}

// startPuller start pull data with span and push resolved txn into txnChan.
func (p *processor) startPuller(ctx context.Context, span util.Span, checkpointTs uint64, txnChan chan<- model.RawTxn, errCh chan<- error) puller.Puller {
	// Set it up so that one failed goroutine cancels all others sharing the same ctx
	errg, ctx := errgroup.WithContext(ctx)

	// The key in DML kv pair returned from TiKV is not memcompariable encoded,
	// so we set `needEncode` to true.
	puller := puller.NewPuller(p.pdCli, checkpointTs, []util.Span{span}, true)

	errg.Go(func() error {
		return puller.Run(ctx)
	})

	errg.Go(func() error {
		defer close(txnChan)
		err := puller.CollectRawTxns(ctx, func(ctxInner context.Context, rawTxn model.RawTxn) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ctxInner.Done():
				return ctxInner.Err()
			case txnChan <- rawTxn:
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

func newMounter(schema *schema.Storage, loc *time.Location) mounter {
	return entry.NewTxnMounter(schema, loc)
}
