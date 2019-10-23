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
	"github.com/pingcap/tidb-cdc/cdc/schema"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"go.uber.org/zap"
)

// Processor is used to push sync progress and calculate the checkpointTS
type Processor interface {
	// SetInputChan receives a table and listens a channel
	SetInputChan(table schema.TableName, input <-chan ProcessorEntry)
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

type ProcessorEntryType int

const (
	ProcessorEntryUnknown ProcessorEntryType = iota
	ProcessorEntryDMLS
	ProcessorEntryResolved
)

type ProcessorEntry struct {
	DMLs []*txn.DML
	TS   uint64
	Typ  ProcessorEntryType
}

func NewProcessorDMLsEntry(dmls []*txn.DML, ts uint64) ProcessorEntry {
	return ProcessorEntry{
		DMLs: dmls,
		TS:   ts,
		Typ:  ProcessorEntryDMLS,
	}
}

func NewProcessorResolvedEntry(ts uint64) ProcessorEntry {
	return ProcessorEntry{
		TS:  ts,
		Typ: ProcessorEntryResolved,
	}
}

type processorImpl struct {
	tableResolvedTS  sync.Map
	tsRWriter        ProcessorTSRWriter
	globalResolvedTS uint64
	resolvedEntries  chan ProcessorEntry
	finishedEntries  chan ProcessorEntry

	tableInputerGroup sync.WaitGroup
	resolvedCond      *sync.Cond
	closed            chan struct{}
}

func NewProcessor(tsRWriter ProcessorTSRWriter) Processor {
	p := &processorImpl{
		tsRWriter:    tsRWriter,
		resolvedCond: sync.NewCond(&sync.Mutex{}),
		// TODO set the cannel size
		resolvedEntries: make(chan ProcessorEntry),
		// TODO set the cannel size
		finishedEntries: make(chan ProcessorEntry),
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
			log.Info("Local resolved worker is exited")
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
		case e, ok := <-p.finishedEntries:
			if !ok {
				log.Info("Checkpoint worker is exited")
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
	log.Info("Global resolved worker is started")
	for {
		select {
		case <-p.closed:
			log.Info("Global resolved worker is exited")
			return
		case <-time.After(1 * time.Second):
			p.tableInputerGroup.Wait()
			p.resolvedEntries <- NewProcessorResolvedEntry(p.globalResolvedTS)
			globalResolvedTS, err := p.tsRWriter.ReadGlobalResolvedTS()
			if err != nil {
				log.Error("Global resolved worker: read global resolved ts failed", zap.Error(err))
				continue
			}
			log.Debug("Global resolved worker: update global resolved ts", zap.Uint64("globalResolvedTS", globalResolvedTS))
			p.globalResolvedTS = globalResolvedTS
			p.resolvedCond.Broadcast()
		}
	}
}

func (p *processorImpl) SetInputChan(table schema.TableName, input <-chan ProcessorEntry) {
	go func() {
		log.Info("Processor input channel listener is started", zap.Reflect("table", table))
		p.tableInputerGroup.Add(1)
		defer p.tableInputerGroup.Done()

		for {
			e, ok := <-input
			if !ok {
				log.Info("Processor input channel is closed, listener is existed", zap.Reflect("table", table))
				return
			}
			switch e.Typ {
			case ProcessorEntryDMLS:
				if e.TS > p.globalResolvedTS {
					log.Debug("Input channel listener: waiting for global resolved ts updating",
						zap.Reflect("table", table), zap.Uint64("global resolved ts", p.globalResolvedTS))
					p.tableInputerGroup.Done()
					p.resolvedCond.L.Lock()
					for e.TS > p.globalResolvedTS {
						p.resolvedCond.Wait()
					}
					p.resolvedCond.L.Unlock()
					p.tableInputerGroup.Add(1)
				}
				p.resolvedEntries <- e
			case ProcessorEntryResolved:
				p.tableResolvedTS.Store(table, e.TS)
			}
		}
	}()
}

func (p *processorImpl) ResolvedChan() <-chan ProcessorEntry {
	return p.resolvedEntries
}

func (p *processorImpl) ExecutedChan() chan<- ProcessorEntry {
	return p.finishedEntries
}

func (p *processorImpl) Close() {
	// TODO close safety
}
