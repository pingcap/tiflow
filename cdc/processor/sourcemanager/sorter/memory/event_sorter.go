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
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

var (
	_ sorter.SortEngine    = (*EventSorter)(nil)
	_ sorter.EventIterator = (*EventIter)(nil)
)

// EventSorter accepts out-of-order raw kv entries and output sorted entries.
type EventSorter struct {
	// Just like map[tablepb.Span]*tableSorter.
	tables spanz.SyncMap

	mu         sync.RWMutex
	onResolves []func(tablepb.Span, model.Ts)
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

// IsTableBased implements sorter.SortEngine.
func (s *EventSorter) IsTableBased() bool {
	return true
}

// AddTable implements sorter.SortEngine.
func (s *EventSorter) AddTable(span tablepb.Span, startTs model.Ts) {
	resolvedTs := startTs
	if _, exists := s.tables.LoadOrStore(span, &tableSorter{resolvedTs: &resolvedTs}); exists {
		log.Panic("add an exist table", zap.Stringer("span", &span))
	}
}

// RemoveTable implements sorter.SortEngine.
func (s *EventSorter) RemoveTable(span tablepb.Span) {
	if _, exists := s.tables.LoadAndDelete(span); !exists {
		log.Panic("remove an unexist table", zap.Stringer("span", &span))
	}
}

// Add implements sorter.SortEngine.
func (s *EventSorter) Add(span tablepb.Span, events ...*model.PolymorphicEvent) {
	value, exists := s.tables.Load(span)
	if !exists {
		log.Panic("add events into an unexist table", zap.Stringer("span", &span))
	}

	resolvedTs, hasNewResolved := value.(*tableSorter).add(events...)
	if hasNewResolved {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for _, onResolve := range s.onResolves {
			onResolve(span, resolvedTs)
		}
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
	value, exists := s.tables.Load(span)
	if !exists {
		log.Panic("fetch events from an unexist table", zap.Stringer("span", &span))
	}

	return value.(*tableSorter).fetch(span, lowerBound, upperBound)
}

// FetchAllTables implements sorter.SortEngine.
func (s *EventSorter) FetchAllTables(lowerBound sorter.Position) sorter.EventIterator {
	log.Panic("FetchAllTables should never be called")
	return nil
}

// CleanByTable implements sorter.SortEngine.
func (s *EventSorter) CleanByTable(span tablepb.Span, upperBound sorter.Position) error {
	value, exists := s.tables.Load(span)
	if !exists {
		log.Panic("clean an unexist table", zap.Stringer("span", &span))
	}

	value.(*tableSorter).clean(span, upperBound)
	return nil
}

// CleanAllTables implements sorter.SortEngine.
func (s *EventSorter) CleanAllTables(upperBound sorter.Position) error {
	log.Panic("CleanAllTables should never be called")
	return nil
}

// GetStatsByTable implements sorter.SortEngine.
func (s *EventSorter) GetStatsByTable(span tablepb.Span) sorter.TableStats {
	log.Panic("GetStatsByTable should never be called")
	return sorter.TableStats{}
}

// Close implements sorter.SortEngine.
func (s *EventSorter) Close() error {
	s.tables = spanz.SyncMap{}
	return nil
}

// SlotsAndHasher implements sorter.SortEngine.
func (s *EventSorter) SlotsAndHasher() (slotCount int, hasher func(tablepb.Span, int) int) {
	return 1, func(_ tablepb.Span, _ int) int { return 0 }
}

// Next implements sorter.EventIterator.
func (s *EventIter) Next() (event *model.PolymorphicEvent, txnFinished sorter.Position, err error) {
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

func (s *tableSorter) fetch(
	span tablepb.Span, lowerBound, upperBound sorter.Position,
) sorter.EventIterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	iter := &EventIter{}
	if s.resolvedTs == nil || upperBound.CommitTs > *s.resolvedTs {
		log.Panic("fetch unresolved events", zap.Stringer("span", &span))
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

func (s *tableSorter) clean(span tablepb.Span, upperBound sorter.Position) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.resolvedTs == nil || upperBound.CommitTs > *s.resolvedTs {
		log.Panic("clean unresolved events", zap.Stringer("span", &span))
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
