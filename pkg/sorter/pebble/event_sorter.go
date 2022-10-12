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
	"context"
	"encoding/binary"
	"hash/fnv"
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
	uniqueID uint32
	dbs      []*pebble.DB
	channs   []*chann.Chann[eventWithTableID]
	serde    encoding.MsgPackGenSerde

	// Just like map[model.TableID]*chann.Chann[eventWithTableID]
	tables sync.Map

	// To manage background goroutines.
	wg     sync.WaitGroup
	closed chan struct{}

	// Following fields are protected by mu.
	mu         sync.RWMutex
	onResolves []func(model.TableID, model.Ts)
}

type EventIter struct{}

func New(ctx context.Context, dbs []*pebble.DB) *EventSorter {
	channs := make([]*chann.Chann[eventWithTableID], 0, len(dbs))
	for i := 0; i < len(dbs); i++ {
		channs = append(channs, chann.New[eventWithTableID](chann.Cap(-1)))
	}

	eventSorter := &EventSorter{
		uniqueID: genUniqueID(),
		dbs:      dbs,
		channs:   channs,
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
	dbID := getDB(tableID, len(s.dbs))
	if _, loaded := s.tables.LoadOrStore(tableID, s.channs[dbID]); loaded {
		log.Panic("add an exist table")
	}
}

// RemoveTable implements sorter.EventSortEngine.
func (s *EventSorter) RemoveTable(tableID model.TableID) {
	if _, loaded := s.tables.LoadAndDelete(tableID); !loaded {
		log.Panic("remove an unexist table")
	}
}

// Add implements sorter.EventSortEngine.
func (s *EventSorter) Add(tableID model.TableID, events ...*model.PolymorphicEvent) (err error) {
	value, ok := s.tables.Load(tableID)
	if !ok {
		log.Panic("add event into an unexist table")
	}
	ch := value.(*chann.Chann[eventWithTableID])

	for _, event := range events {
		ch.In() <- eventWithTableID{tableID, event}
	}
	return
}

// SetOnResolve implements sorter.EventSortEngine.
func (s *EventSorter) SetOnResolve(action func(model.TableID, model.Ts)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onResolves = append(s.onResolves, action)
}

// Fetch implements sorter.EventSortEngine.
func (s *EventSorter) Fetch(tableID model.TableID, lowerBound model.Ts) sorter.EventIterator {
	return &EventIter{}
}

// Clean implements sorter.EventSortEngine.
func (s *EventSorter) Clean(tableID model.TableID, upperBound model.Ts) {
}

// Next implements sorter.EventIterator.
func (s *EventIter) Next() (event *model.PolymorphicEvent, err error) {
	return
}

// Close implements sorter.EventIterator.
func (s *EventIter) Close() error {
	return nil
}

type eventWithTableID struct {
	tableID model.TableID
	event   *model.PolymorphicEvent
}

func (s *EventSorter) handleEvents(offset int) {
	db := s.dbs[offset]
	inputCh := s.channs[offset].Out()

	batch := db.NewBatch()
	needCommit := false
	writeOpts := &pebble.WriteOptions{Sync: false}

	handleItem := func(item eventWithTableID) (mustCommit bool) {
		key := encoding.EncodeKey(s.uniqueID, uint64(item.tableID), item.event)
		value, err := s.serde.Marshal(item.event, []byte{})
		if err != nil {
			log.Panic("failed to marshal event", zap.Error(err))
		}
		if err = batch.Set(key, value, writeOpts); err != nil {
			log.Panic("failed to update pebble batch", zap.Error(err))
		}
		needCommit = needCommit || item.event.IsResolved()
		mustCommit = batch.Count() >= batchCommitCount
		needCommit = needCommit || mustCommit
		return
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
		if needCommit {
			if err := batch.Commit(writeOpts); err != nil {
				log.Panic("failed to commit pebble batch", zap.Error(err))
			}
			batch = db.NewBatch()
			needCommit = false
		}
	}
}

const (
	batchCommitCount uint32 = 1024
)

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
