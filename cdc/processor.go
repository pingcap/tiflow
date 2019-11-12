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
	"math"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/model"
	"github.com/pingcap/tidb-cdc/cdc/puller"
	"github.com/pingcap/tidb-cdc/cdc/roles/storage"
	"github.com/pingcap/tidb-cdc/cdc/schema"
	"github.com/pingcap/tidb-cdc/cdc/sink"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"github.com/pingcap/tidb-cdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
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
	Mount(rawTxn txn.RawTxn) (*txn.Txn, error)
}

// Processor is used to push sync progress and calculate the checkpointTs
// How to use it:
// 1. Call SetInputChan to set a rawTxn input channel
//        (you can call SetInputChan many time to set multiple input channel)
// 2. Push rawTxn into rawTxn input channel
// 3. Pull ProcessorEntry from ResolvedChan, RawTxn is included in ProcessorEntry
// 4. execute the RawTxn in ProcessorEntry
// 5. Push ProcessorEntry to ExecutedChan
type Processor interface {
	// SetInputChan receives a table and listens a channel
	SetInputChan(tableID int64, inputTxn <-chan txn.RawTxn) error
	// ResolvedChan returns a channel, which output the resolved transaction or resolvedTs
	ResolvedChan() <-chan ProcessorEntry
	// ExecutedChan returns a channel, when a transaction is executed,
	// you should put the transaction into this channel,
	// processor will calculate checkpointTs according to this channel
	ExecutedChan() chan<- ProcessorEntry
	// Run starts the work routine of processor
	Run(ctx context.Context, errCh chan<- error)
	// Close closes the processor
	Close()
}

// ProcessorTsRWriter reads or writes the resolvedTs and checkpointTs from the storage
type ProcessorTsRWriter interface {
	// WriteResolvedTs writes the loacl resolvedTs into the storage
	WriteResolvedTs(ctx context.Context, resolvedTs uint64) error
	// WriteCheckpointTs writes the checkpointTs into the storage
	WriteCheckpointTs(ctx context.Context, checkpointTs uint64) error
	// ReadGlobalResolvedTs reads the global resolvedTs from the storage
	ReadGlobalResolvedTs(ctx context.Context) (uint64, error)
}

type txnChannel struct {
	inputTxn   <-chan txn.RawTxn
	outputTxn  chan txn.RawTxn
	putBackTxn *txn.RawTxn
}

func (p *txnChannel) Forward(ctx context.Context, tableID int64, ts uint64, entryC chan<- ProcessorEntry) {
	if p.putBackTxn != nil {
		t := *p.putBackTxn
		if t.Ts > ts {
			return
		}
		p.putBackTxn = nil
		entryC <- NewProcessorTxnEntry(t)
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
			entryC <- NewProcessorTxnEntry(t)
		}
	}
}

func (p *txnChannel) PutBack(t txn.RawTxn) {
	if p.putBackTxn != nil {
		log.Fatal("can not put back raw txn continuously")
	}
	p.putBackTxn = &t
}

func newTxnChannel(inputTxn <-chan txn.RawTxn, chanSize int, handleResolvedTs func(uint64)) *txnChannel {
	tc := &txnChannel{
		inputTxn:  inputTxn,
		outputTxn: make(chan txn.RawTxn, chanSize),
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

type ProcessorEntryType int

const (
	ProcessorEntryUnknown ProcessorEntryType = iota
	ProcessorEntryDMLS
	ProcessorEntryResolved
)

type ProcessorEntry struct {
	Txn txn.RawTxn
	Ts  uint64
	Typ ProcessorEntryType
}

func NewProcessorTxnEntry(txn txn.RawTxn) ProcessorEntry {
	return ProcessorEntry{
		Txn: txn,
		Ts:  txn.Ts,
		Typ: ProcessorEntryDMLS,
	}
}

func NewProcessorResolvedEntry(ts uint64) ProcessorEntry {
	return ProcessorEntry{
		Ts:  ts,
		Typ: ProcessorEntryResolved,
	}
}

const ddlPullerID int64 = -1

type processorImpl struct {
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
	ddlJobsCh       chan txn.RawTxn

	tblPullers      map[int64]puller.CancellablePuller
	tableInputChans map[int64]*txnChannel
	inputChansLock  sync.RWMutex
	wg              *errgroup.Group
}

func NewProcessor(pdEndpoints []string, changefeed model.ChangeFeedDetail, changefeedID, captureID string) (Processor, error) {
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

	ddlPuller := puller.NewPuller(pdCli, changefeed.StartTs, []util.Span{util.GetDDLSpan()})

	// TODO: get time zone from config
	mounter := fNewMounter(schemaStorage, time.UTC)

	sink, err := fNewMySQLSink(changefeed.SinkURI, schemaStorage, changefeed.Opts)
	if err != nil {
		return nil, err
	}

	p := &processorImpl{
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
		ddlJobsCh:       make(chan txn.RawTxn, 16),

		tblPullers:      make(map[int64]puller.CancellablePuller),
		tableInputChans: make(map[int64]*txnChannel),
	}

	return p, nil
}

func (p *processorImpl) Run(ctx context.Context, errCh chan<- error) {
	wg, cctx := errgroup.WithContext(ctx)
	p.wg = wg
	wg.Go(func() error {
		p.localResolvedWorker(cctx)
		return nil
	})
	wg.Go(func() error {
		p.checkpointWorker(cctx)
		return nil
	})
	wg.Go(func() error {
		p.globalResolvedWorker(cctx)
		return nil
	})
	wg.Go(func() error {
		p.pullerSchedule(cctx, errCh)
		return nil
	})
	wg.Go(func() error {
		return p.syncResolved(cctx)
	})
	wg.Go(func() error {
		return p.pullDDLJob(cctx)
	})
}

// pullerSchedule will be used to monitor table change and schedule pullers in the future
func (p *processorImpl) pullerSchedule(ctx context.Context, ch chan<- error) {
	// TODO: add DDL puller to maintain table schema

	err := p.initPullers(ctx, ch)
	if err != nil {
		if err != context.Canceled {
			log.Error("initial pullers", zap.Error(err))
			ch <- err
		}
		return
	}
}

func (p *processorImpl) localResolvedWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			if ctx.Err() != context.Canceled {
				log.Error("Local resolved worker exited", zap.Error(ctx.Err()))
			} else {
				log.Info("Local resolved worker exited")
			}
			return
		case <-time.After(3 * time.Second):
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
			if err != nil {
				log.Error("Local resolved worker: write resolved ts failed", zap.Error(err))
			}
		}
	}
}

func (p *processorImpl) checkpointWorker(ctx context.Context) {
	checkpointTs := uint64(0)
	for {
		select {
		case <-ctx.Done():
			if ctx.Err() != context.Canceled {
				log.Error("Checkpoint worker exited", zap.Error(ctx.Err()))
			} else {
				log.Info("Checkpoint worker exited")
			}
			return
		case e, ok := <-p.executedEntries:
			if !ok {
				log.Info("Checkpoint worker exited")
				return
			}
			if e.Typ == ProcessorEntryResolved {
				checkpointTs = e.Ts
			}
		case <-time.After(3 * time.Second):
			err := p.tsRWriter.WriteCheckpointTs(ctx, checkpointTs)
			if err != nil {
				log.Error("Checkpoint worker: write checkpoint ts failed", zap.Error(err))
			}
		}
	}
}

func (p *processorImpl) globalResolvedWorker(ctx context.Context) {
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
			if ctx.Err() != context.Canceled {
				log.Error("Global resolved worker exited", zap.Error(ctx.Err()))
			} else {
				log.Info("Global resolved worker exited")
			}
			return
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
			return
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
		p.resolvedEntries <- NewProcessorResolvedEntry(globalResolvedTs)
	}
}

func (p *processorImpl) pullDDLJob(ctx context.Context) error {
	defer close(p.ddlJobsCh)
	errg, ctx := errgroup.WithContext(ctx)

	errg.Go(func() error {
		return p.ddlPuller.Run(ctx)
	})

	errg.Go(func() error {
		err := p.ddlPuller.CollectRawTxns(ctx, func(ctxInner context.Context, rawTxn txn.RawTxn) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ctxInner.Done():
				return ctxInner.Err()
			case p.ddlJobsCh <- rawTxn:
				return nil
			}
		})
		if err != nil && err != context.Canceled {
			return errors.Annotate(err, "span: ddl")
		}
		return nil
	})
	return errg.Wait()
}

func (p *processorImpl) syncResolved(ctx context.Context) error {
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
			case ProcessorEntryDMLS:
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
			case ProcessorEntryResolved:
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

func (p *processorImpl) SetInputChan(tableID int64, inputTxn <-chan txn.RawTxn) error {
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

func (p *processorImpl) ResolvedChan() <-chan ProcessorEntry {
	return p.resolvedEntries
}

func (p *processorImpl) ExecutedChan() chan<- ProcessorEntry {
	return p.executedEntries
}

func (p *processorImpl) Close() {
	if p.wg != nil {
		if err := p.wg.Wait(); err != nil {
			log.Error("Waiting to close", zap.Error(err))
		}
	}
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
func (p *processorImpl) getTsRwriter() ProcessorTsRWriter {
	return p.tsRWriter
}

func (p *processorImpl) initPullers(ctx context.Context, errCh chan<- error) error {
	// the loop is only for test, not support add/remove table dynamically
	// TODO: the time sequence should be:
	// user create changefeed -> owner creates subchangefeed info in etcd -> scheduler create processors
	// but currently scheduler and owner are running concurrently
	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != context.Canceled {
				return err
			}
			return nil
		case <-time.After(time.Second * 5):
			_, info, err := kv.GetSubChangeFeedInfo(ctx, p.etcdCli, p.changefeedID, p.captureID)
			if err != nil {
				return err
			}
			for _, tblInfo := range info.TableInfos {
				if _, ok := p.tblPullers[int64(tblInfo.ID)]; ok {
					continue
				}
				if err := p.addTable(ctx, int64(tblInfo.ID), errCh); err != nil {
					return err
				}
			}
			if len(info.TableInfos) > 0 {
				return nil
			}
		}
	}
}

func (p *processorImpl) addTable(ctx context.Context, tableID int64, errCh chan<- error) error {
	log.Debug("Add table", zap.Int64("tableID", tableID))
	// TODO: Make sure it's threadsafe or prove that it doesn't have to be
	if _, ok := p.tblPullers[tableID]; ok {
		log.Warn("Ignore existing table", zap.Int64("ID", tableID))
		return nil
	}
	span := util.GetTableSpan(tableID)
	// TODO: How large should the buffer be?
	txnChan := make(chan txn.RawTxn, 16)
	if err := p.SetInputChan(tableID, txnChan); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	plr := p.startPuller(ctx, span, txnChan, errCh)
	p.tblPullers[tableID] = puller.CancellablePuller{Puller: plr, Cancel: cancel}
	return nil
}

func (p *processorImpl) startPuller(ctx context.Context, span util.Span, txnChan chan<- txn.RawTxn, errCh chan<- error) puller.Puller {
	// Set it up so that one failed goroutine cancels all others sharing the same ctx
	errg, ctx := errgroup.WithContext(ctx)

	checkpointTs := p.changefeed.StartTs
	if checkpointTs == 0 {
		checkpointTs = oracle.EncodeTSO(p.changefeed.CreateTime.Unix() * 1000)
	}

	puller := puller.NewPuller(p.pdCli, checkpointTs, []util.Span{span})

	errg.Go(func() error {
		return puller.Run(ctx)
	})

	errg.Go(func() error {
		defer close(txnChan)
		err := puller.CollectRawTxns(ctx, func(ctxInner context.Context, rawTxn txn.RawTxn) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ctxInner.Done():
				return ctxInner.Err()
			case txnChan <- rawTxn:
				return nil
			}
		})
		if err != nil {
			return errors.Annotatef(err, "span: %v", span)
		}
		return nil
	})

	go func() {
		err := errg.Wait()
		errCh <- err
	}()

	return puller
}

func newMounter(schema *schema.Storage, loc *time.Location) mounter {
	return txn.NewTxnMounter(schema, loc)
}
