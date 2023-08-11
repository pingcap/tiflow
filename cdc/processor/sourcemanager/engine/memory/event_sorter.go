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

package memory

import (
	"container/heap"
	"context"
	"sort"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"go.uber.org/zap"
)

var (
	_ engine.SortEngine    = (*EventSorter)(nil)
	_ engine.EventIterator = (*EventIter)(nil)
)

// EventSorter accepts out-of-order raw kv entries and output sorted entries.
type EventSorter struct {
	// Just like map[model.TableID]*tableSorter.
	tables sync.Map

	mu         sync.RWMutex
	onResolves []func(model.TableID, model.Ts)
}

// EventIter implements sorter.EventIterator.
type EventIter struct {
	resolved []*model.PolymorphicEvent
	position int
}

// New creates a new tableSorter.
func New(_ context.Context) *EventSorter {
	return &EventSorter{}
}

// IsTableBased implements engine.SortEngine.
func (s *EventSorter) IsTableBased() bool {
	return true
}

// AddTable implements engine.SortEngine.
func (s *EventSorter) AddTable(tableID model.TableID) {
	if _, exists := s.tables.LoadOrStore(tableID, &tableSorter{}); exists {
		log.Panic("add an exist table", zap.Int64("tableID", tableID))
	}
}

// RemoveTable implements engine.SortEngine.
func (s *EventSorter) RemoveTable(tableID model.TableID) {
	if _, exists := s.tables.LoadAndDelete(tableID); !exists {
		log.Panic("remove an unexist table", zap.Int64("tableID", tableID))
	}
}

// Add implements engine.SortEngine.
func (s *EventSorter) Add(tableID model.TableID, events ...*model.PolymorphicEvent) {
	value, exists := s.tables.Load(tableID)
	if !exists {
		log.Panic("add events into an unexist table", zap.Int64("tableID", tableID))
	}

	resolvedTs, hasNewResolved := value.(*tableSorter).add(events...)
	if hasNewResolved {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for _, onResolve := range s.onResolves {
			onResolve(tableID, resolvedTs)
		}
	}
}

// OnResolve implements engine.SortEngine.
func (s *EventSorter) OnResolve(action func(model.TableID, model.Ts)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onResolves = append(s.onResolves, action)
}

// FetchByTable implements engine.SortEngine.
func (s *EventSorter) FetchByTable(tableID model.TableID, lowerBound, upperBound engine.Position) engine.EventIterator {
	value, exists := s.tables.Load(tableID)
	if !exists {
		log.Panic("fetch events from an unexist table", zap.Int64("tableID", tableID))
	}

	return value.(*tableSorter).fetch(tableID, lowerBound, upperBound)
}

// FetchAllTables implements engine.SortEngine.
func (s *EventSorter) FetchAllTables(lowerBound engine.Position) engine.EventIterator {
	log.Panic("FetchAllTables should never be called")
	return nil
}

// CleanByTable implements engine.SortEngine.
func (s *EventSorter) CleanByTable(tableID model.TableID, upperBound engine.Position) error {
	value, exists := s.tables.Load(tableID)
	if !exists {
		log.Panic("clean an unexist table", zap.Int64("tableID", tableID))
	}

	value.(*tableSorter).clean(tableID, upperBound)
	return nil
}

// CleanAllTables implements engine.SortEngine.
func (s *EventSorter) CleanAllTables(upperBound engine.Position) error {
	log.Panic("CleanAllTables should never be called")
	return nil
}

// GetStatsByTable implements engine.SortEngine.
func (s *EventSorter) GetStatsByTable(tableID model.TableID) engine.TableStats {
	log.Panic("GetStatsByTable should never be called")
	return engine.TableStats{}
}

// Close implements engine.SortEngine.
func (s *EventSorter) Close() error {
	s.tables = sync.Map{}
	return nil
}

// Next implements sorter.EventIterator.
func (s *EventIter) Next() (event *model.PolymorphicEvent, txnFinished engine.Position, err error) {
	if len(s.resolved) == 0 {
		return
	}
	event = s.resolved[s.position]
	s.position += 1

	var next *model.PolymorphicEvent
	if s.position < len(s.resolved) {
		next = s.resolved[s.position]
	} else {
		s.resolved = nil
	}

	if next == nil || next.CRTs != event.CRTs || next.StartTs != event.StartTs {
		txnFinished.CommitTs = event.CRTs
		txnFinished.StartTs = event.StartTs
	}

	return
}

// Close implements sorter.EventIterator.
func (s *EventIter) Close() error {
	s.resolved = nil
	return nil
}

type tableSorter struct {
	// All following fields are protected by mu.
	mu         sync.RWMutex
	resolvedTs *model.Ts
	unresolved eventHeap
	resolved   []*model.PolymorphicEvent
}

func (s *tableSorter) add(events ...*model.PolymorphicEvent) (resolvedTs model.Ts, hasNewResolved bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range events {
		heap.Push(&s.unresolved, event)
		if event.IsResolved() {
			if s.resolvedTs == nil {
				s.resolvedTs = new(model.Ts)
				hasNewResolved = true
			} else if *s.resolvedTs < event.CRTs {
				hasNewResolved = true
			}
			if hasNewResolved {
				*s.resolvedTs = event.CRTs
				resolvedTs = event.CRTs
			}

			for s.unresolved.Len() > 0 {
				item := heap.Pop(&s.unresolved).(*model.PolymorphicEvent)
				if item == event {
					break
				}
				s.resolved = append(s.resolved, item)
			}
		}
	}
	return
}

func (s *tableSorter) fetch(tableID model.TableID, lowerBound, upperBound engine.Position) engine.EventIterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	iter := &EventIter{}
	if s.resolvedTs == nil || upperBound.CommitTs > *s.resolvedTs {
		log.Panic("fetch unresolved events", zap.Int64("tableID", tableID))
	}

	startIdx := sort.Search(len(s.resolved), func(idx int) bool {
		x := s.resolved[idx]
		return x.CRTs > lowerBound.CommitTs ||
			x.CRTs == lowerBound.CommitTs && x.StartTs >= lowerBound.StartTs
	})
	endIdx := sort.Search(len(s.resolved), func(idx int) bool {
		x := s.resolved[idx]
		return x.CRTs > upperBound.CommitTs ||
			x.CRTs == upperBound.CommitTs && x.StartTs > upperBound.StartTs
	})
	iter.resolved = s.resolved[startIdx:endIdx]
	return iter
}

func (s *tableSorter) clean(tableID model.TableID, upperBound engine.Position) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.resolvedTs == nil || upperBound.CommitTs > *s.resolvedTs {
		log.Panic("clean unresolved events", zap.Int64("tableID", tableID))
	}

	startIdx := sort.Search(len(s.resolved), func(idx int) bool {
		x := s.resolved[idx]
		return x.CRTs > upperBound.CommitTs ||
			x.CRTs == upperBound.CommitTs && x.StartTs > upperBound.StartTs
	})
	s.resolved = s.resolved[startIdx:]
}

func eventLess(i *model.PolymorphicEvent, j *model.PolymorphicEvent) bool {
	return model.ComparePolymorphicEvents(i, j)
}

type eventHeap []*model.PolymorphicEvent

func (h eventHeap) Len() int           { return len(h) }
func (h eventHeap) Less(i, j int) bool { return eventLess(h[i], h[j]) }
func (h eventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) {
	*h = append(*h, x.(*model.PolymorphicEvent))
}

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
