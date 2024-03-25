// Copyright 2022 PingCAP, Inc.
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

package pebble

import (
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter/pebble/encoding"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	_ sorter.SortEngine    = (*EventSorter)(nil)
	_ sorter.EventIterator = (*EventIter)(nil)
)

var pebbleWriteOptions = pebble.WriteOptions{Sync: false}

// EventSorter is an event sort engine.
type EventSorter struct {
	// Read-only fields.
	changefeedID model.ChangeFeedID
	dbs          []*pebble.DB
	channs       []*chann.DrainableChann[eventWithTableID]
	serde        encoding.MsgPackGenSerde

	// To manage background goroutines.
	wg     sync.WaitGroup
	closed chan struct{}

	// Following fields are protected by mu.
	mu         sync.RWMutex
	isClosed   bool
	onResolves []func(tablepb.Span, model.Ts)
	tables     *spanz.HashMap[*tableState]
}

// EventIter implements sorter.EventIterator.
type EventIter struct {
	tableID      model.TableID
	iter         *pebble.Iterator
	currentEvent *model.PolymorphicEvent
	serde        encoding.MsgPackGenSerde

	nextDuration prometheus.Observer
}

// New creates an EventSorter instance.
func New(ID model.ChangeFeedID, dbs []*pebble.DB) *EventSorter {
	channs := make([]*chann.DrainableChann[eventWithTableID], 0, len(dbs))
	for i := 0; i < len(dbs); i++ {
		channs = append(channs, chann.NewAutoDrainChann[eventWithTableID](chann.Cap(128)))
	}

	eventSorter := &EventSorter{
		changefeedID: ID,
		dbs:          dbs,
		channs:       channs,
		closed:       make(chan struct{}),
		tables:       spanz.NewHashMap[*tableState](),
	}

	for i := range eventSorter.dbs {
		eventSorter.wg.Add(1)
		go func(x int) {
			defer eventSorter.wg.Done()
			eventSorter.handleEvents(x, dbs[x], channs[x].Out())
		}(i)
	}

	return eventSorter
}

// IsTableBased implements sorter.SortEngine.
func (s *EventSorter) IsTableBased() bool {
	return true
}

// AddTable implements sorter.SortEngine.
func (s *EventSorter) AddTable(span tablepb.Span, startTs model.Ts) {
	s.mu.Lock()
	if _, exists := s.tables.Get(span); exists {
		s.mu.Unlock()
		log.Warn("add an exist table",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Stringer("span", &span))
		return
	}
	state := &tableState{
		uniqueID: genUniqueID(),
		ch:       s.channs[getDB(span, len(s.dbs))],
	}
	state.maxReceivedResolvedTs.Store(startTs)
	s.tables.ReplaceOrInsert(span, state)
	s.mu.Unlock()
}

// RemoveTable implements sorter.SortEngine.
func (s *EventSorter) RemoveTable(span tablepb.Span) {
	s.mu.Lock()
	if _, exists := s.tables.Get(span); !exists {
		s.mu.Unlock()
		log.Warn("remove an unexist table",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Stringer("span", &span))
		return
	}
	s.tables.Delete(span)
	s.mu.Unlock()
}

// Add implements sorter.SortEngine.
//
// Panics if the table doesn't exist.
func (s *EventSorter) Add(span tablepb.Span, events ...*model.PolymorphicEvent) {
	s.mu.RLock()
	state, exists := s.tables.Get(span)
	s.mu.RUnlock()

	if !exists {
		log.Panic("add events into an non-existent table",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Stringer("span", &span))
	}

	maxCommitTs := state.maxReceivedCommitTs.Load()
	maxResolvedTs := state.maxReceivedResolvedTs.Load()
	for _, event := range events {
		if event.IsResolved() {
			if event.CRTs > maxResolvedTs {
				maxResolvedTs = event.CRTs
				state.maxReceivedResolvedTs.Store(maxResolvedTs)
			}
		} else {
			if event.CRTs > maxCommitTs {
				maxCommitTs = event.CRTs
				state.maxReceivedCommitTs.Store(maxCommitTs)
			}
		}
		state.ch.In() <- eventWithTableID{uniqueID: state.uniqueID, span: span, event: event}
	}
}

// OnResolve implements sorter.SortEngine.
func (s *EventSorter) OnResolve(action func(tablepb.Span, model.Ts)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onResolves = append(s.onResolves, action)
}

// FetchByTable implements sorter.SortEngine.
func (s *EventSorter) FetchByTable(span tablepb.Span, lowerBound, upperBound sorter.Position) sorter.EventIterator {
	iterReadDur := sorter.IterReadDuration()
	eventIter := &EventIter{
		tableID:      span.TableID,
		serde:        s.serde,
		nextDuration: iterReadDur.WithLabelValues(s.changefeedID.Namespace, s.changefeedID.ID, "next"),
	}

	s.mu.RLock()
	state, exists := s.tables.Get(span)
	s.mu.RUnlock()
	if !exists {
		return eventIter
	}

	sortedResolved := state.sortedResolved.Load()
	if upperBound.CommitTs > sortedResolved {
		log.Panic("fetch unresolved events",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Uint64("upperBound", upperBound.CommitTs),
			zap.Uint64("lowerBound", lowerBound.CommitTs),
			zap.Uint64("resolved", sortedResolved))
	}

	db := s.dbs[getDB(span, len(s.dbs))]

	seekStart := time.Now()
	iter := iterTable(db, state.uniqueID, span.TableID, lowerBound, upperBound)
	iterReadDur.WithLabelValues(s.changefeedID.Namespace, s.changefeedID.ID, "first").
		Observe(time.Since(seekStart).Seconds())

	eventIter.iter = iter
	return eventIter
}

// FetchAllTables implements sorter.SortEngine.
func (s *EventSorter) FetchAllTables(lowerBound sorter.Position) sorter.EventIterator {
	log.Panic("FetchAllTables should never be called",
		zap.String("namespace", s.changefeedID.Namespace),
		zap.String("changefeed", s.changefeedID.ID))
	return nil
}

// CleanByTable implements sorter.SortEngine.
func (s *EventSorter) CleanByTable(span tablepb.Span, upperBound sorter.Position) error {
	s.mu.RLock()
	state, exists := s.tables.Get(span)
	s.mu.RUnlock()

	if !exists {
		return nil
	}

	return s.cleanTable(state, span, upperBound)
}

// CleanAllTables implements sorter.EventSortEngine.
func (s *EventSorter) CleanAllTables(upperBound sorter.Position) error {
	log.Panic("CleanAllTables should never be called",
		zap.String("namespace", s.changefeedID.Namespace),
		zap.String("changefeed", s.changefeedID.ID))
	return nil
}

// GetStatsByTable implements sorter.SortEngine.
//
// Panics if the table doesn't exist.
func (s *EventSorter) GetStatsByTable(span tablepb.Span) sorter.TableStats {
	s.mu.RLock()
	state, exists := s.tables.Get(span)
	s.mu.RUnlock()

	if !exists {
		log.Panic("Get stats from an non-existent table",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Stringer("span", &span))
	}

	maxCommitTs := state.maxReceivedCommitTs.Load()
	maxResolvedTs := state.maxReceivedResolvedTs.Load()
	if maxCommitTs < maxResolvedTs {
		// In case, there is no write for the table,
		// we use maxResolvedTs as maxCommitTs to make the stats meaningful.
		maxCommitTs = maxResolvedTs
	}
	return sorter.TableStats{
		ReceivedMaxCommitTs:   maxCommitTs,
		ReceivedMaxResolvedTs: maxResolvedTs,
	}
}

// Close implements sorter.SortEngine.
func (s *EventSorter) Close() error {
	s.mu.Lock()
	if s.isClosed {
		s.mu.Unlock()
		return nil
	}
	s.isClosed = true
	s.mu.Unlock()

	close(s.closed)
	s.wg.Wait()
	for _, ch := range s.channs {
		ch.CloseAndDrain()
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var err error
	s.tables.Range(func(span tablepb.Span, state *tableState) bool {
		// TODO: maybe we can use a unified prefix for a changefeed,
		//       so that we can speed up it when closing a changefeed.
		if err1 := s.cleanTable(state, span); err1 != nil {
			err = err1
			return false
		}
		return true
	})
	return err
}

// SlotsAndHasher implements sorter.SortEngine.
func (s *EventSorter) SlotsAndHasher() (slotCount int, hasher func(tablepb.Span, int) int) {
	return len(s.dbs), spanz.HashTableSpan
}

// Next implements sorter.EventIterator.
// txnFinished indicates whether all events in the current transaction are
// fetched or not.
func (s *EventIter) Next() (event *model.PolymorphicEvent, txnFinished sorter.Position, err error) {
	valid := s.iter != nil && s.iter.Valid()
	var value []byte
	var nextEvent *model.PolymorphicEvent

	// We need to decide whether the current event is the last event in this transactions
	// If the current event is the last one, we need to set txnFinished
	// Thus, we need to fetch the next event and compare the commitTs and startTs with it
	for valid {
		nextStart := time.Now()
		value, valid = s.iter.Value(), s.iter.Next()
		s.nextDuration.Observe(time.Since(nextStart).Seconds())

		nextEvent = &model.PolymorphicEvent{}
		if _, err = s.serde.Unmarshal(nextEvent, value); err != nil {
			return
		}
		if s.currentEvent != nil {
			break
		}
		s.currentEvent, nextEvent = nextEvent, nil
	}
	if s.currentEvent != nil {
		if nextEvent == nil || s.currentEvent.CRTs != nextEvent.CRTs || s.currentEvent.StartTs != nextEvent.StartTs {
			txnFinished.CommitTs = s.currentEvent.CRTs
			txnFinished.StartTs = s.currentEvent.StartTs
		}
		event = s.currentEvent
		s.currentEvent = nextEvent
	}
	return
}

// Close implements sorter.EventIterator.
func (s *EventIter) Close() error {
	if s.iter != nil {
		return s.iter.Close()
	}
	return nil
}

type eventWithTableID struct {
	uniqueID uint32
	span     tablepb.Span
	event    *model.PolymorphicEvent
}

type tableState struct {
	uniqueID       uint32
	ch             *chann.DrainableChann[eventWithTableID]
	sortedResolved atomic.Uint64 // indicates events are ready for fetching.
	// For statistics.
	maxReceivedCommitTs   atomic.Uint64
	maxReceivedResolvedTs atomic.Uint64

	// Following fields are protected by mu.
	mu      sync.RWMutex
	cleaned sorter.Position
}

// DBBatchEvent is used to contains a batch of events and the corresponding resolvedTs info.
type DBBatchEvent struct {
	batch         *pebble.Batch
	batchResolved *spanz.HashMap[model.Ts]
}

// batchCommitAndUpdateResolvedTs commits the batch and updates the resolved ts of the table.
func (s *EventSorter) batchCommitAndUpdateResolvedTs(
	batchCh chan *DBBatchEvent,
	id int,
) {
	idstr := strconv.Itoa(id + 1)
	writeDuration := sorter.WriteDuration().WithLabelValues(idstr)
	writeBytes := sorter.WriteBytes().WithLabelValues(idstr)

	for {
		select {
		case <-s.closed:
			s.wg.Done()
			return
		case batchEvent := <-batchCh:
			// do batch commit
			batch := batchEvent.batch
			if !batch.Empty() {
				writeBytes.Observe(float64(len(batch.Repr())))
				start := time.Now()
				if err := batch.Commit(&pebbleWriteOptions); err != nil {
					log.Panic("failed to commit pebble batch", zap.Error(err),
						zap.String("namespace", s.changefeedID.Namespace),
						zap.String("changefeed", s.changefeedID.ID))
				}
				writeDuration.Observe(time.Since(start).Seconds())
			}

			// update resolved ts after commit successfully
			batchResolved := batchEvent.batchResolved
			batchResolved.Range(func(span tablepb.Span, resolved uint64) bool {
				s.mu.RLock()
				ts, ok := s.tables.Get(span)
				s.mu.RUnlock()
				if !ok {
					log.Debug("Table is removed, skip updating resolved",
						zap.String("namespace", s.changefeedID.Namespace),
						zap.String("changefeed", s.changefeedID.ID),
						zap.Stringer("span", &span),
						zap.Uint64("resolved", resolved))
					return true
				}
				ts.sortedResolved.Store(resolved)
				for _, onResolve := range s.onResolves {
					onResolve(span, resolved)
				}
				return true
			})
		}
	}
}

// handleEvents encode events from channel and try to write them into pebble.
// It will commit the batch when the size of the batch is larger than batchCommitSize or
// the time since the last commit is larger than batchCommitInterval.
// It will also update the resolved ts of the table when the batch is committed.
// Considering commit is a heavy operation, we make [fetch and decode event] and
// [do commit and update resolved ts] as two separate goroutines to make pipeline.
func (s *EventSorter) handleEvents(
	id int, db *pebble.DB, inputCh <-chan eventWithTableID,
) {
	// We set a relatively small channel value to avoid possible OOM caused by too much information.
	// The number of channels will not affect the performance of commit consumption currently.
	batchCh := make(chan *DBBatchEvent, 8)
	s.wg.Add(1)
	go s.batchCommitAndUpdateResolvedTs(batchCh, id)

	ticker := time.NewTicker(batchCommitInterval / 2)
	defer ticker.Stop()

	encodeItemAndBatch := func(batch *pebble.Batch, newResolved *spanz.HashMap[model.Ts], item eventWithTableID) {
		if item.event.IsResolved() {
			newResolved.ReplaceOrInsert(item.span, item.event.CRTs)
			return
		}
		key := encoding.EncodeKey(item.uniqueID, uint64(item.span.TableID), item.event)
		value, err := s.serde.Marshal(item.event, []byte{})
		if err != nil {
			log.Panic("failed to marshal event", zap.Error(err),
				zap.String("namespace", s.changefeedID.Namespace),
				zap.String("changefeed", s.changefeedID.ID))
		}
		if err = batch.Set(key, value, &pebbleWriteOptions); err != nil {
			log.Panic("failed to update pebble batch", zap.Error(err),
				zap.String("namespace", s.changefeedID.Namespace),
				zap.String("changefeed", s.changefeedID.ID))
		}
	}

	// Batch item and commit until batch size is larger than batchCommitSize,
	// or the time since the last commit is larger than batchCommitInterval.
	// Only return false when the sorter is closed.
	doBatching := func() (*DBBatchEvent, bool) {
		batch := db.NewBatch()
		newResolved := spanz.NewHashMap[model.Ts]()
		startToBatch := time.Now()
		for {
			select {
			case item := <-inputCh:
				encodeItemAndBatch(batch, newResolved, item)
				if len(batch.Repr()) >= batchCommitSize {
					return &DBBatchEvent{batch, newResolved}, true
				}
			case <-s.closed:
				return nil, false
			case <-ticker.C:
				if time.Since(startToBatch) >= batchCommitInterval {
					return &DBBatchEvent{batch, newResolved}, true
				}
			}
		}
	}

	for {
		batchEvent, ok := doBatching()
		if !ok {
			return
		}
		batchCh <- batchEvent
	}
}

// cleanTable uses DeleteRange to clean data of the given table.
func (s *EventSorter) cleanTable(
	state *tableState, span tablepb.Span, upperBound ...sorter.Position,
) error {
	var toClean sorter.Position
	var start, end []byte

	if len(upperBound) == 1 {
		toClean = upperBound[0]
	} else {
		toClean = sorter.Position{CommitTs: math.MaxUint64, StartTs: math.MaxUint64 - 1}
	}

	state.mu.RLock()
	defer state.mu.RUnlock()

	if state.cleaned.Compare(toClean) >= 0 {
		return nil
	}

	start = encoding.EncodeTsKey(state.uniqueID, uint64(span.TableID), 0)
	toCleanNext := toClean.Next()
	end = encoding.EncodeTsKey(state.uniqueID, uint64(span.TableID), toCleanNext.CommitTs, toCleanNext.StartTs)

	db := s.dbs[getDB(span, len(s.dbs))]
	err := db.DeleteRange(start, end, &pebbleWriteOptions)
	if err != nil {
		log.Info("clean stale table range fails",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Stringer("span", &span),
			zap.Error(err))
		return err
	}

	sorter.RangeCleanCount().Inc()
	state.cleaned = toClean
	return nil
}

// ----- Some internal variable and functions -----
const (
	batchCommitSize     int = 16 * 1024 * 1024
	batchCommitInterval     = 20 * time.Millisecond
)

var uniqueIDGen uint32 = 0

func genUniqueID() uint32 {
	return atomic.AddUint32(&uniqueIDGen, 1)
}

func getDB(span tablepb.Span, dbCount int) int {
	return spanz.HashTableSpan(span, dbCount)
}
