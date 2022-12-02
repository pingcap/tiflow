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
	"encoding/binary"
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/pebble/encoding"
	"github.com/pingcap/tiflow/pkg/chann"
	"go.uber.org/zap"
)

var (
	_ engine.SortEngine    = (*EventSorter)(nil)
	_ engine.EventIterator = (*EventIter)(nil)
)

// EventSorter is an event sort engine.
type EventSorter struct {
	// Read-only fields.
	changefeedID model.ChangeFeedID
	uniqueID     uint32
	dbs          []*pebble.DB
	channs       []*chann.Chann[eventWithTableID]
	serde        encoding.MsgPackGenSerde

	// To manage background goroutines.
	wg     sync.WaitGroup
	closed chan struct{}

	// Following fields are protected by mu.
	mu         sync.RWMutex
	isClosed   bool
	onResolves []func(model.TableID, model.Ts)
	tables     map[model.TableID]*tableState
}

// EventIter implements sorter.EventIterator.
type EventIter struct {
	tableID  model.TableID
	state    *tableState
	iter     *pebble.Iterator
	headItem *model.PolymorphicEvent
	serde    encoding.MsgPackGenSerde
}

// New creates an EventSorter instance.
func New(ID model.ChangeFeedID, dbs []*pebble.DB) *EventSorter {
	channs := make([]*chann.Chann[eventWithTableID], 0, len(dbs))
	for i := 0; i < len(dbs); i++ {
		channs = append(channs, chann.New[eventWithTableID](chann.Cap(-1)))
	}

	eventSorter := &EventSorter{
		changefeedID: ID,
		uniqueID:     genUniqueID(),
		dbs:          dbs,
		channs:       channs,
		closed:       make(chan struct{}),
		tables:       make(map[model.TableID]*tableState),
	}

	for i := range eventSorter.dbs {
		eventSorter.wg.Add(1)
		go func(x int) {
			defer eventSorter.wg.Done()
			eventSorter.handleEvents(dbs[x], channs[x].Out())
		}(i)
	}

	return eventSorter
}

// IsTableBased implements engine.SortEngine.
func (s *EventSorter) IsTableBased() bool {
	return true
}

// AddTable implements engine.SortEngine.
func (s *EventSorter) AddTable(tableID model.TableID) {
	s.mu.Lock()
	if _, exists := s.tables[tableID]; exists {
		s.mu.Unlock()
		log.Warn("add an exist table",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Int64("tableID", tableID))
		return
	}
	s.tables[tableID] = &tableState{ch: s.channs[getDB(tableID, len(s.dbs))]}
	s.mu.Unlock()
}

// RemoveTable implements engine.SortEngine.
func (s *EventSorter) RemoveTable(tableID model.TableID) {
	s.mu.Lock()
	if _, exists := s.tables[tableID]; !exists {
		s.mu.Unlock()
		log.Warn("remove an unexist table",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Int64("tableID", tableID))
		return
	}
	delete(s.tables, tableID)
	s.mu.Unlock()
}

// Add implements engine.SortEngine.
func (s *EventSorter) Add(tableID model.TableID, events ...*model.PolymorphicEvent) error {
	s.mu.RLock()
	state, exists := s.tables[tableID]
	s.mu.RUnlock()

	if !exists {
		log.Panic("add events into an non-existent table",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}

	maxCommitTs := model.Ts(0)
	maxResolvedTs := model.Ts(0)
	for _, event := range events {
		state.ch.In() <- eventWithTableID{tableID, event}
		if event.IsResolved() {
			if event.CRTs > maxResolvedTs {
				maxResolvedTs = event.CRTs
			}
		} else {
			state.receivedEvents.Add(1)
			if event.CRTs > maxCommitTs {
				maxCommitTs = event.CRTs
			}
		}
	}

	if maxCommitTs > state.maxReceivedCommitTs.Load() {
		state.maxReceivedCommitTs.Store(maxCommitTs)
	}
	if maxResolvedTs > state.maxReceivedResolvedTs.Load() {
		state.maxReceivedResolvedTs.Store(maxResolvedTs)
	}

	return nil
}

// GetResolvedTs implements engine.SortEngine.
func (s *EventSorter) GetResolvedTs(tableID model.TableID) model.Ts {
	s.mu.RLock()
	state, exists := s.tables[tableID]
	s.mu.RUnlock()

	if !exists {
		log.Panic("get resolved ts from an non-existent table",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}

	return state.maxReceivedResolvedTs.Load()
}

// OnResolve implements engine.SortEngine.
func (s *EventSorter) OnResolve(action func(model.TableID, model.Ts)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onResolves = append(s.onResolves, action)
}

// FetchByTable implements engine.SortEngine.
func (s *EventSorter) FetchByTable(
	tableID model.TableID,
	lowerBound, upperBound engine.Position,
) engine.EventIterator {
	s.mu.RLock()
	state, exists := s.tables[tableID]
	s.mu.RUnlock()

	if !exists {
		log.Panic("fetch events from an non-existent table",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}

	sortedResolved := state.sortedResolved.Load()
	if upperBound.CommitTs > sortedResolved {
		log.Panic("fetch unresolved events",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Int64("tableID", tableID),
			zap.Uint64("upperBound", upperBound.CommitTs),
			zap.Uint64("resolved", sortedResolved))
	}

	db := s.dbs[getDB(tableID, len(s.dbs))]
	iter := iterTable(db, s.uniqueID, tableID, lowerBound, upperBound)
	return &EventIter{tableID: tableID, state: state, iter: iter, serde: s.serde}
}

// FetchAllTables implements engine.SortEngine.
func (s *EventSorter) FetchAllTables(lowerBound engine.Position) engine.EventIterator {
	log.Panic("FetchAllTables should never be called",
		zap.String("namespace", s.changefeedID.Namespace),
		zap.String("changefeed", s.changefeedID.ID))
	return nil
}

// CleanByTable implements engine.SortEngine.
func (s *EventSorter) CleanByTable(tableID model.TableID, upperBound engine.Position) error {
	s.mu.RLock()
	state, exists := s.tables[tableID]
	s.mu.RUnlock()

	if !exists {
		log.Panic("clean an non-existent table",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}

	return s.cleanTable(state, tableID, upperBound)
}

// CleanAllTables implements engine.EventSortEngine.
func (s *EventSorter) CleanAllTables(upperBound engine.Position) error {
	log.Panic("CleanAllTables should never be called",
		zap.String("namespace", s.changefeedID.Namespace),
		zap.String("changefeed", s.changefeedID.ID))
	return nil
}

// GetStatsByTable implements engine.SortEngine.
func (s *EventSorter) GetStatsByTable(tableID model.TableID) engine.TableStats {
	s.mu.RLock()
	state, exists := s.tables[tableID]
	s.mu.RUnlock()

	if !exists {
		log.Panic("Get stats from an non-existent table",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Int64("tableID", tableID))
	}

	maxCommitTs := state.maxReceivedCommitTs.Load()
	maxResolvedTs := state.maxReceivedResolvedTs.Load()
	if maxCommitTs < maxResolvedTs {
		// In case, there is no write for the table,
		// we use maxResolvedTs as maxCommitTs to make the stats meaningful.
		maxCommitTs = maxResolvedTs
	}
	return engine.TableStats{
		ReceivedMaxCommitTs:   maxCommitTs,
		ReceivedMaxResolvedTs: maxResolvedTs,
	}
}

// ReceivedEvents implements engine.SortEngine.
func (s *EventSorter) ReceivedEvents() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	totalReceivedEvents := int64(0)
	for _, state := range s.tables {
		totalReceivedEvents += state.receivedEvents.Load()
	}
	return totalReceivedEvents
}

// Close implements engine.SortEngine.
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
		ch.Close()
		for range ch.Out() {
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for tableID, state := range s.tables {
		if err := s.cleanTable(state, tableID); err != nil {
			return err
		}
	}
	return nil
}

// Next implements sorter.EventIterator.
func (s *EventIter) Next() (event *model.PolymorphicEvent, pos engine.Position, err error) {
	valid := s.iter != nil && s.iter.Valid()
	var value []byte
	for valid {
		value, valid = s.iter.Value(), s.iter.Next()
		event = &model.PolymorphicEvent{}
		if _, err = s.serde.Unmarshal(event, value); err != nil {
			return
		}
		if s.headItem != nil {
			break
		}
		s.headItem, event = event, nil
	}
	if s.headItem != nil {
		if event == nil || s.headItem.CRTs != event.CRTs || s.headItem.StartTs != event.StartTs {
			pos.CommitTs = s.headItem.CRTs
			pos.StartTs = s.headItem.StartTs
		}
		event, s.headItem = s.headItem, event
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
	tableID model.TableID
	event   *model.PolymorphicEvent
}

type tableState struct {
	ch             *chann.Chann[eventWithTableID]
	sortedResolved atomic.Uint64 // indicates events are ready for fetching.
	// For statistics.
	maxReceivedCommitTs   atomic.Uint64
	maxReceivedResolvedTs atomic.Uint64
	receivedEvents        atomic.Int64

	// Following fields are protected by mu.
	mu      sync.RWMutex
	cleaned engine.Position
}

func (s *EventSorter) handleEvents(db *pebble.DB, inputCh <-chan eventWithTableID) {
	batch := db.NewBatch()
	writeOpts := &pebble.WriteOptions{Sync: false}
	newResolved := make(map[model.TableID]model.Ts)

	handleItem := func(item eventWithTableID) bool {
		if item.event.IsResolved() {
			newResolved[item.tableID] = item.event.CRTs
			return false
		}
		key := encoding.EncodeKey(s.uniqueID, uint64(item.tableID), item.event)
		value, err := s.serde.Marshal(item.event, []byte{})
		if err != nil {
			log.Panic("failed to marshal event", zap.Error(err),
				zap.String("namespace", s.changefeedID.Namespace),
				zap.String("changefeed", s.changefeedID.ID))
		}
		if err = batch.Set(key, value, writeOpts); err != nil {
			log.Panic("failed to update pebble batch", zap.Error(err),
				zap.String("namespace", s.changefeedID.Namespace),
				zap.String("changefeed", s.changefeedID.ID))
		}
		return batch.Count() >= batchCommitCount
	}

	for {
		select {
		case item := <-inputCh:
			if handleItem(item) {
				goto CommitBatch
			}
		case <-s.closed:
			return
		}
		for {
			select {
			case item := <-inputCh:
				if handleItem(item) {
					goto CommitBatch
				}
			case <-s.closed:
				return
			default:
				goto CommitBatch
			}
		}
	CommitBatch:
		if err := batch.Commit(writeOpts); err != nil {
			log.Panic("failed to commit pebble batch", zap.Error(err),
				zap.String("namespace", s.changefeedID.Namespace),
				zap.String("changefeed", s.changefeedID.ID))
		}
		batch = db.NewBatch()

		for table, resolved := range newResolved {
			s.mu.RLock()
			ts, ok := s.tables[table]
			if !ok {
				log.Debug("Table is removed, skip updating resolved",
					zap.String("namespace", s.changefeedID.Namespace),
					zap.String("changefeed", s.changefeedID.ID),
					zap.Int64("table", table),
					zap.Uint64("resolved", resolved))
				s.mu.RUnlock()
				continue
			}
			ts.sortedResolved.Store(resolved)
			for _, onResolve := range s.onResolves {
				onResolve(table, resolved)
			}
			s.mu.RUnlock()
		}
		newResolved = make(map[model.TableID]model.Ts)
	}
}

// cleanTable uses DeleteRange to clean data of the given table.
func (s *EventSorter) cleanTable(state *tableState, tableID model.TableID, upperBound ...engine.Position) error {
	var toClean engine.Position
	var start, end []byte

	if len(upperBound) == 1 {
		toClean = upperBound[0]
	} else {
		toClean = engine.Position{CommitTs: math.MaxUint64, StartTs: math.MaxUint64 - 1}
	}

	state.mu.RLock()
	defer state.mu.RUnlock()

	if state.cleaned.Compare(toClean) >= 0 {
		return nil
	}

	start = encoding.EncodeTsKey(s.uniqueID, uint64(tableID), 0)
	toCleanNext := toClean.Next()
	end = encoding.EncodeTsKey(s.uniqueID, uint64(tableID), toCleanNext.CommitTs, toCleanNext.StartTs)

	db := s.dbs[getDB(tableID, len(s.dbs))]
	err := db.DeleteRange(start, end, &pebble.WriteOptions{Sync: false})
	if err != nil {
		return err
	}

	state.cleaned = toClean
	return nil
}

// ----- Some internal variable and functions -----
const batchCommitCount uint32 = 1024

var uniqueIDGen uint32 = 0

func genUniqueID() uint32 {
	return atomic.AddUint32(&uniqueIDGen, 1)
}

// TODO: add test for this function.
func getDB(tableID model.TableID, dbCount int) int {
	h := fnv.New64()
	b := [8]byte{}
	binary.LittleEndian.PutUint64(b[:], uint64(tableID))
	h.Write(b[:])
	return int(h.Sum64() % uint64(dbCount))
}
