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

package roles

import (
	"math"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/schema"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"go.uber.org/zap"
)

// Processor is used to push sync progress and calculate the checkpointTS
type Processor interface {
	// SetInputChan receives a table and listens a channel
	SetInputChan(table schema.TableName, inputTxn <-chan txn.RawTxn)
	// ResolvedChan returns a channel, which output the resolved transaction or resolvedTS
	ResolvedChan() <-chan ProcessorEntry
	// ExecutedChan returns a channel, when a transaction is executed,
	// you should put the transaction into this channel,
	// processor will calculate checkpointTS according to this channel
	ExecutedChan() chan<- ProcessorEntry
	// Close closes the processor
	Close()
}

type ProcessorTSRWriter interface {
	WriteResolvedTS(resolvedTS uint64) error
	WriteCheckpointTS(checkpointTS uint64) error
	ReadGlobalResolvedTS() (uint64, error)
}

type txnChannel struct {
	inputTxn   <-chan txn.RawTxn
	outputTxn  chan txn.RawTxn
	putBackTxn *txn.RawTxn
}

func (p *txnChannel) Pull() (txn.RawTxn, bool) {
	if p.putBackTxn == nil {
		t, ok := <-p.outputTxn
		return t, ok
	}
	t := *p.putBackTxn
	p.putBackTxn = nil
	return t, true
}

func (p *txnChannel) PutBack(t txn.RawTxn) {
	if p.putBackTxn != nil {
		panic("can not put back raw txn continuously")
	}
	p.putBackTxn = &t
}

func newTxnChannel(inputTxn <-chan txn.RawTxn, handleResolvedTS func(uint64)) *txnChannel {
	tc := &txnChannel{
		inputTxn:  inputTxn,
		outputTxn: make(chan txn.RawTxn, 128),
	}
	go func() {
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
	tableResolvedTS sync.Map
	tsRWriter       ProcessorTSRWriter
	resolvedEntries chan ProcessorEntry
	executedEntries chan ProcessorEntry

	tableInputChans map[schema.TableName]*txnChannel
	inputChansLock  sync.RWMutex

	closed chan struct{}
}

func NewProcessor(tsRWriter ProcessorTSRWriter) Processor {
	p := &processorImpl{
		tsRWriter: tsRWriter,
		// TODO set the cannel size
		resolvedEntries: make(chan ProcessorEntry),
		// TODO set the cannel size
		executedEntries: make(chan ProcessorEntry),
		closed:          make(chan struct{}),
	}
	go p.localResolvedWorker()
	go p.checkpointWorker()
	go p.globalResolvedWorker()
	return p
}

func (p *processorImpl) localResolvedWorker() {
	for {
		select {
		case <-p.closed:
			log.Info("Local resolved worker exited")
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
			err := p.tsRWriter.WriteResolvedTS(minResolvedTs)
			if err != nil {
				log.Error("Local resolved worker: write resolved ts failed", zap.Error(err))
			}
		}
	}
}

func (p *processorImpl) checkpointWorker() {
	checkpointTS := uint64(0)
	for {
		select {
		case e, ok := <-p.executedEntries:
			if !ok {
				log.Info("Checkpoint worker exited")
				return
			}
			if e.Typ == ProcessorEntryResolved {
				checkpointTS = e.TS
			}
		case <-time.After(3 * time.Second):
			err := p.tsRWriter.WriteCheckpointTS(checkpointTS)
			if err != nil {
				log.Error("Checkpoint worker: write checkpoint ts failed", zap.Error(err))
			}
		}
	}
}

func (p *processorImpl) globalResolvedWorker() {
	log.Info("Global resolved worker started")
	wg := new(sync.WaitGroup)
	for {
		globalResolvedTS, err := p.tsRWriter.ReadGlobalResolvedTS()
		if err != nil {
			log.Error("Global resolved worker: read global resolved ts failed", zap.Error(err))
			continue
		}
		p.inputChansLock.RLock()
		for _, input := range p.tableInputChans {
			wg.Add(1)
			go p.pushTxn(input, globalResolvedTS, wg)
		}
		p.inputChansLock.RUnlock()
		wg.Wait()
		p.resolvedEntries <- NewProcessorResolvedEntry(globalResolvedTS)
	}
}

func (p *processorImpl) pushTxn(input *txnChannel, targetTS uint64, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		t, ok := input.Pull()
		if !ok {
			// TODO log
			return
		}
		if t.TS > targetTS {
			input.PutBack(t)
			return
		}
		p.resolvedEntries <- NewProcessorDMLsEntry(t.Entries, t.TS)
	}
}

func (p *processorImpl) SetInputChan(table schema.TableName, inputTxn <-chan txn.RawTxn) {
	tc := newTxnChannel(inputTxn, func(resolvedTS uint64) {
		p.tableResolvedTS.Store(table, resolvedTS)
	})
	p.inputChansLock.Lock()
	defer p.inputChansLock.Unlock()
	p.tableInputChans[table] = tc
}

func (p *processorImpl) ResolvedChan() <-chan ProcessorEntry {
	return p.resolvedEntries
}

func (p *processorImpl) ExecutedChan() chan<- ProcessorEntry {
	return p.executedEntries
}

func (p *processorImpl) Close() {
	// TODO close safety
	close(p.executedEntries)
	close(p.closed)
	close(p.resolvedEntries)
}
