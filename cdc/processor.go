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
)

// Processor is used to push sync progress and calculate the checkpointTS
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
	// ResolvedChan returns a channel, which output the resolved transaction or resolvedTS
	ResolvedChan() <-chan ProcessorEntry
	// ExecutedChan returns a channel, when a transaction is executed,
	// you should put the transaction into this channel,
	// processor will calculate checkpointTS according to this channel
	ExecutedChan() chan<- ProcessorEntry
	// Run starts the work routine of processor
	Run(ctx context.Context, errCh chan<- error)
	// Close closes the processor
	Close()
}

// ProcessorTSRWriter reads or writes the resolvedTS and checkpointTS from the storage
type ProcessorTSRWriter interface {
	// WriteResolvedTS writes the loacl resolvedTS into the storage
	WriteResolvedTS(ctx context.Context, resolvedTS uint64) error
	// WriteCheckpointTS writes the checkpointTS into the storage
	WriteCheckpointTS(ctx context.Context, checkpointTS uint64) error
	// ReadGlobalResolvedTS reads the global resolvedTS from the storage
	ReadGlobalResolvedTS(ctx context.Context) (uint64, error)
}

type txnChannel struct {
	inputTxn   <-chan txn.RawTxn
	outputTxn  chan txn.RawTxn
	putBackTxn *txn.RawTxn
}

func (p *txnChannel) Forward(tableID int64, ts uint64, entryC chan<- ProcessorEntry) {
	if p.putBackTxn != nil {
		t := *p.putBackTxn
		if t.TS > ts {
			return
		}
		p.putBackTxn = nil
		entryC <- NewProcessorDMLsEntry(t.Entries, t.TS)
	}
	for t := range p.outputTxn {
		if t.TS > ts {
			p.PutBack(t)
			return
		}
		entryC <- NewProcessorDMLsEntry(t.Entries, t.TS)
	}
	log.Info("Input channel of table closed", zap.Int64("tableID", tableID))
}

func (p *txnChannel) PutBack(t txn.RawTxn) {
	if p.putBackTxn != nil {
		log.Fatal("can not put back raw txn continuously")
	}
	p.putBackTxn = &t
}

func newTxnChannel(inputTxn <-chan txn.RawTxn, chanSize int, handleResolvedTS func(uint64)) *txnChannel {
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
			handleResolvedTS(t.TS)
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
	Entries []*kv.RawKVEntry
	TS      uint64
	Typ     ProcessorEntryType
}

func NewProcessorDMLsEntry(entries []*kv.RawKVEntry, ts uint64) ProcessorEntry {
	return ProcessorEntry{
		Entries: entries,
		TS:      ts,
		Typ:     ProcessorEntryDMLS,
	}
}

func NewProcessorResolvedEntry(ts uint64) ProcessorEntry {
	return ProcessorEntry{
		TS:  ts,
		Typ: ProcessorEntryResolved,
	}
}

type processorImpl struct {
	captureID    string
	changefeedID string
	changefeed   model.ChangeFeedDetail

	pdCli   pd.Client
	etcdCli *clientv3.Client

	mounter *txn.Mounter
	sink    sink.Sink

	tableResolvedTS sync.Map
	tsRWriter       ProcessorTSRWriter
	resolvedEntries chan ProcessorEntry
	executedEntries chan ProcessorEntry

	tblPullers      map[int64]CancellablePuller
	tableInputChans map[int64]*txnChannel
	inputChansLock  sync.RWMutex
	wg              *errgroup.Group
}

func NewProcessor(pdEndpoints []string, changefeed model.ChangeFeedDetail, captureID, changefeedID string) (Processor, error) {
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

	schema, err := fCreateSchema(pdEndpoints)
	if err != nil {
		return nil, err
	}

	tsRWriter := fNewTsRWriter(etcdCli, changefeedID, captureID)

	// TODO: get time zone from config
	mounter, err := txn.NewTxnMounter(schema, time.UTC)
	if err != nil {
		return nil, err
	}

	sink, err := sink.NewMySQLSink(changefeed.SinkURI, schema, changefeed.Opts)
	if err != nil {
		return nil, err
	}

	p := &processorImpl{
		captureID:    captureID,
		changefeedID: changefeedID,
		changefeed:   changefeed,
		pdCli:        pdCli,
		etcdCli:      etcdCli,
		mounter:      mounter,
		sink:         sink,

		tsRWriter: tsRWriter,
		// TODO set the channel size
		resolvedEntries: make(chan ProcessorEntry),
		// TODO set the channel size
		executedEntries: make(chan ProcessorEntry),

		tblPullers:      make(map[int64]CancellablePuller),
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
	// TODO: add sink
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
			p.tableResolvedTS.Range(func(key, value interface{}) bool {
				resolvedTS := value.(uint64)
				if minResolvedTs > resolvedTS {
					minResolvedTs = resolvedTS
				}
				return true
			})
			if minResolvedTs == uint64(math.MaxUint64) {
				// no table in this processor
				continue
			}
			err := p.tsRWriter.WriteResolvedTS(ctx, minResolvedTs)
			if err != nil {
				log.Error("Local resolved worker: write resolved ts failed", zap.Error(err))
			}
		}
	}
}

func (p *processorImpl) checkpointWorker(ctx context.Context) {
	checkpointTS := uint64(0)
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
				checkpointTS = e.TS
			}
		case <-time.After(3 * time.Second):
			err := p.tsRWriter.WriteCheckpointTS(ctx, checkpointTS)
			if err != nil {
				log.Error("Checkpoint worker: write checkpoint ts failed", zap.Error(err))
			}
		}
	}
}

func (p *processorImpl) globalResolvedWorker(ctx context.Context) {
	log.Info("Global resolved worker started")

	var (
		globalResolvedTS     uint64
		lastGlobalResolvedTS uint64
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
		wg, _ := errgroup.WithContext(ctx)
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
			globalResolvedTS, err = p.tsRWriter.ReadGlobalResolvedTS(ctx)
			if err != nil {
				log.Error("Global resolved worker: read global resolved ts failed", zap.Error(err))
			}
			return err
		}, retryCfg)
		if err != nil {
			return
		}
		if lastGlobalResolvedTS == globalResolvedTS {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		lastGlobalResolvedTS = globalResolvedTS
		p.inputChansLock.RLock()
		for table, input := range p.tableInputChans {
			table := table
			input := input
			globalResolvedTS := globalResolvedTS
			wg.Go(func() error {
				input.Forward(table, globalResolvedTS, p.resolvedEntries)
				return nil
			})
		}
		p.inputChansLock.RUnlock()
		wg.Wait()
		p.resolvedEntries <- NewProcessorResolvedEntry(globalResolvedTS)
	}
}

func (p *processorImpl) SetInputChan(tableID int64, inputTxn <-chan txn.RawTxn) error {
	tc := newTxnChannel(inputTxn, 64, func(resolvedTS uint64) {
		p.tableResolvedTS.Store(tableID, resolvedTS)
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
		p.wg.Wait()
	}
}

func createSchemaStore(pdEndpoints []string) (*schema.Schema, error) {
	// here we create another pb client,we should reuse them
	kvStore, err := createTiStore(strings.Join(pdEndpoints, ","))
	if err != nil {
		return nil, err
	}
	jobs, err := kv.LoadHistoryDDLJobs(kvStore)
	if err != nil {
		return nil, err
	}
	schema, err := schema.NewSchema(jobs, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return schema, nil
}

func createTsRWriter(cli *clientv3.Client, changefeedID, captureID string) ProcessorTSRWriter {
	// TODO: NewProcessorTSEtcdRWriter should return an interface, currently there
	// exists cycle import issue, will refine it later
	return storage.NewProcessorTSEtcdRWriter(cli, changefeedID, captureID)
}

// getTSRwriter is used in unit test only
func (p *processorImpl) getTSRwriter() ProcessorTSRWriter {
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
	puller := p.startPuller(ctx, span, txnChan, errCh)
	p.tblPullers[tableID] = CancellablePuller{Puller: puller, Cancel: cancel}
	return nil
}

func (p *processorImpl) startPuller(ctx context.Context, span util.Span, txnChan chan<- txn.RawTxn, errCh chan<- error) *Puller {
	// Set it up so that one failed goroutine cancels all others sharing the same ctx
	errg, ctx := errgroup.WithContext(ctx)

	checkpointTS := p.changefeed.StartTS
	if checkpointTS == 0 {
		checkpointTS = oracle.EncodeTSO(p.changefeed.CreateTime.Unix() * 1000)
	}

	puller := NewPuller(p.pdCli, checkpointTS, []util.Span{span})

	errg.Go(func() error {
		return puller.Run(ctx)
	})

	errg.Go(func() error {
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

// func (c *SubChangeFeed) writeToSink(context context.Context, rawTxn txn.RawTxn) error {
// 	log.Info("RawTxn", zap.Reflect("RawTxn", rawTxn.Entries))
// 	txn, err := c.mounter.Mount(rawTxn)
// 	if err != nil {
// 		return errors.Trace(err)
// 	}
// 	err = c.sink.Emit(context, *txn)
// 	if err != nil {
// 		return errors.Trace(err)
// 	}
// 	log.Info("Output Txn", zap.Reflect("Txn", txn))
// 	return nil
// }
