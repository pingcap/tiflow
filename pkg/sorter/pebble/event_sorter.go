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
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/sorter"
	"github.com/pingcap/tiflow/pkg/sorter/pebble/encoding"
	"go.uber.org/zap"
)

var (
	_ sorter.EventSortEngine = (*EventSorter)(nil)
	_ sorter.EventIterator   = (*EventIter)(nil)
)

// EventSorter is an event sort engine.
type EventSorter struct {
	// Read-only fields.
    changefeedID model.ChangeFeedID
	uniqueID uint32
	dbs      []*pebble.DB
	channs   []*chann.Chann[eventWithTableID]
	serde    encoding.MsgPackGenSerde

	// To manage background goroutines.
	wg     sync.WaitGroup
	closed chan struct{}

	// Following fields are protected by mu.
	mu         sync.RWMutex
	onResolves []func(model.TableID, model.Ts)
	tables     map[model.TableID]*tableState
}

type EventIter struct {
	tableID model.TableID
	iter    *pebble.Iterator
	serde   encoding.MsgPackGenSerde
}

func New(ID model.ChangeFeedID, dbs []*pebble.DB) *EventSorter {
	channs := make([]*chann.Chann[eventWithTableID], 0, len(dbs))
	for i := 0; i < len(dbs); i++ {
		channs = append(channs, chann.New[eventWithTableID](chann.Cap(-1)))
	}

	eventSorter := &EventSorter{
        changefeedID: ID,
		uniqueID: genUniqueID(),
		dbs:      dbs,
		channs:   channs,
		tables:   make(map[model.TableID]*tableState),
	}

	for i := range eventSorter.dbs {
		eventSorter.wg.Add(1)
		go func() {
			defer eventSorter.wg.Done()
			eventSorter.handleEvents(i)
		}()
	}

	return eventSorter
}

// IsTableBased implements sorter.EventSortEngine.
func (s *EventSorter) IsTableBased() bool {
	return true
}

// AddTable implements sorter.EventSortEngine.
func (s *EventSorter) AddTable(tableID model.TableID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[tableID]; exists {
		log.Panic("add an exist table")
	}
	s.tables[tableID] = &tableState{
		ch:       s.channs[getDB(tableID, len(s.dbs))],
		resolved: uint64(0),
	}
}

// RemoveTable implements sorter.EventSortEngine.
func (s *EventSorter) RemoveTable(tableID model.TableID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tables[tableID]; !exists {
		log.Panic("remove an unexist table")
	}
	delete(s.tables, tableID)
}

// Add implements sorter.EventSortEngine.
func (s *EventSorter) Add(tableID model.TableID, events ...*model.PolymorphicEvent) (err error) {
	s.mu.RLock()
	state, exists := s.tables[tableID]
	if !exists {
		log.Panic("add events into an unexist table")
	}
	s.mu.Unlock()

	for _, event := range events {
		state.ch.In() <- eventWithTableID{tableID, event}
	}
	return
}

// OnResolve implements sorter.EventSortEngine.
func (s *EventSorter) OnResolve(action func(model.TableID, model.Ts)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onResolves = append(s.onResolves, action)
}

// FetchByTable implements sorter.EventSortEngine.
func (s *EventSorter) FetchByTable(
	tableID model.TableID,
	lowerBound, upperBound model.Ts,
) sorter.EventIterator {
	s.mu.RLock()
	state, exists := s.tables[tableID]
	if !exists {
		log.Panic("fetch events from an unexist table")
	}
	s.mu.Unlock()

	if upperBound > atomic.LoadUint64(&state.resolved) {
		log.Panic("fetch unresolved events")
	}

	db := s.dbs[getDB(tableID, len(s.dbs))]
	iter := iterTable(db, s.uniqueID, tableID, lowerBound, upperBound+1)
	return &EventIter{tableID, iter, s.serde}
}

// FetchAllTables implements sorter.EventSortEngine.
func (s *EventSorter) FetchAllTables(lowerBound model.Ts) sorter.EventIterator {
	log.Panic("FetchAllTables should never be called")
	return nil
}

// CleanByTable implements sorter.EventSortEngine.
func (s *EventSorter) CleanByTable(tableID model.TableID, upperBound model.Ts) error {
	s.mu.RLock()
	state, exists := s.tables[tableID]
	if !exists {
		log.Panic("clean an unexist table")
	}
	s.mu.Unlock()

	return s.cleanTable(state, tableID, upperBound)
}

// CleanAllTables implements sorter.EventSortEngine.
func (s *EventSorter) CleanAllTables(upperBound model.Ts) error {
	log.Panic("CleanAllTables should never be called")
	return nil
}

// Close implements sorter.EventSortEngine.
func (s *EventSorter) Close() error {
	close(s.closed)
	s.wg.Done()
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
func (s *EventIter) Next() (event *model.PolymorphicEvent, err error) {
	if s.iter != nil && s.iter.Valid() {
		event = &model.PolymorphicEvent{}
		_, err = s.serde.Unmarshal(event, s.iter.Value())
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
	ch       *chann.Chann[eventWithTableID]
	resolved uint64
	cleaned  uint64
}

func (s *EventSorter) handleEvents(offset int) {
	db := s.dbs[offset]
	inputCh := s.channs[offset].Out()

	batch := db.NewBatch()
	writeOpts := &pebble.WriteOptions{Sync: false}
	newResolved := make(map[model.TableID]model.Ts)

	handleItem := func(item eventWithTableID) bool {
		key := encoding.EncodeKey(s.uniqueID, uint64(item.tableID), item.event)
		value, err := s.serde.Marshal(item.event, []byte{})
		if err != nil {
			log.Panic("failed to marshal event", zap.Error(err))
		}
		if err = batch.Set(key, value, writeOpts); err != nil {
			log.Panic("failed to update pebble batch", zap.Error(err))
		}
		if item.event.IsResolved() {
			newResolved[item.tableID] = item.event.CRTs
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
			break
		}
		for {
			select {
			case item := <-inputCh:
				if handleItem(item) {
					goto CommitBatch
				}
			case <-s.closed:
				break
			default:
				break
			}
		}
	CommitBatch:
		if err := batch.Commit(writeOpts); err != nil {
			log.Panic("failed to commit pebble batch", zap.Error(err))
		}
		batch = db.NewBatch()
		s.mu.RLock()
		for table, resolved := range newResolved {
			for _, onResolve := range s.onResolves {
				onResolve(table, resolved)
			}
		}
		s.mu.RUnlock()
	}
}

// cleanTable uses DeleteRange to clean data of the given table.
func (s *EventSorter) cleanTable(state *tableState, tableID model.TableID, upperBound ...model.Ts) error {
	var cleaned model.Ts
	var start, end []byte

	if len(upperBound) == 1 {
		cleaned = upperBound[0]
	} else {
		cleaned = math.MaxUint64
	}

	if atomic.LoadUint64(&state.cleaned) >= cleaned {
		return nil
	}

	start = encoding.EncodeTsKey(s.uniqueID, uint64(tableID), 0)
	if cleaned == math.MaxUint64 {
		end = encoding.EncodeTsKey(s.uniqueID, uint64(tableID)+1, 0)
	} else {
		end = encoding.EncodeTsKey(s.uniqueID, uint64(tableID), cleaned+1)
	}

	db := s.dbs[getDB(tableID, len(s.dbs))]
	err := db.DeleteRange(start, end, &pebble.WriteOptions{Sync: false})
	if err != nil {
		return err
	}
	for {
		prev := atomic.LoadUint64(&state.cleaned)
		if prev >= cleaned || atomic.CompareAndSwapUint64(&state.cleaned, prev, cleaned) {
			break
		}
	}
	return nil
}

// / ----- Some internal variable and functions ----- ///
const batchCommitCount uint32 = 1024

var uniqueIDGen uint32 = 0

func genUniqueID() uint32 {
	return atomic.AddUint32(&uniqueIDGen, 1)
}

func getDB(tableID model.TableID, dbCount int) int {
	h := fnv.New64()
	b := [8]byte{}
	binary.LittleEndian.PutUint64(b[:], uint64(tableID))
	h.Write(b[:])
	return int(h.Sum64()) % dbCount
}
