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

	"github.com/pingcap/tiflow/cdc/model"
)

// EventSorter accepts out-of-order raw kv entries and output sorted entries.
// For now, it only uses for DDL puller and test.
type EventSorter struct {
	// All following fields are protected by mu.
	mu              sync.RWMutex
	unresolved      eventHeap
	resolved        []*model.PolymorphicEvent
	resolvedTsGroup []uint64
	onResolves      []func(model.TableID, model.Ts)
}

type EventIter struct {
	resolved []*model.PolymorphicEvent
	position int
}

// New creates a new EventSorter.
func New(ctx context.Context) *EventSorter {
	// TODO: add metrics.
	// changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	// _, tableName := contextutil.TableIDFromCtx(ctx)
	return &EventSorter{}
}

// IsTableBased implements sorter.EventSortEngine.
func (s *EventSorter) IsTableBased() bool {
	return false
}

// AddTable implements sorter.EventSortEngine.
func (s *EventSorter) AddTable(_ model.TableID) {}

// RemoveTable implements sorter.EventSortEngine.
func (s *EventSorter) RemoveTable(_ model.TableID) {}

// Add implements sorter.EventSortEngine.
func (s *EventSorter) Add(event *model.PolymorphicEvent) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	heap.Push(&s.unresolved, event)
	if event.IsResolved() {
		s.resolvedTsGroup = append(s.resolvedTsGroup, event.CRTs)
		for s.unresolved.Len() > 0 {
			item := heap.Pop(&s.unresolved).(*model.PolymorphicEvent)
			s.resolved = append(s.resolved, item)
			if item == event {
				break
			}
		}
	}
	return
}

// SetOnResolve implements sorter.EventSortEngine.
func (s *EventSorter) SetOnResolve(action func(model.TableID, model.Ts)) {
	s.mu.Lock()
	defer s.mu.RUnlock()
	s.onResolves = append(s.onResolves, action)
}

// Fetch implements sorter.EventSortEngine.
func (s *EventSorter) Fetch(tableID model.TableID, lowerBound model.Ts) (iter EventIter) {
	if tableID != -1 {
		panic("only for DDL puller")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.resolvedTsGroup) == 0 {
		return
	}

	startIdx := sort.Search(len(s.resolved), func(idx int) bool {
		return s.resolved[idx].CRTs >= lowerBound
	})

	endIdx := sort.Search(len(s.resolved), func(idx int) bool {
		return s.resolved[idx].CRTs > s.resolvedTsGroup[len(s.resolvedTsGroup)-1]
	})

	iter.resolved = s.resolved[startIdx:endIdx]
	return
}

// Clean implements sorter.EventSortEngine.
func (s *EventSorter) Clean(tableID model.TableID, upperBound model.Ts) {
	if tableID != -1 {
		panic("only for DDL puller")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := sort.Search(len(s.resolvedTsGroup), func(idx int) bool {
		return s.resolvedTsGroup[idx] > upperBound
	})
	s.resolvedTsGroup = s.resolvedTsGroup[idx:]
	s.resolved = s.resolved[idx:]
}

// Next implements sorter.EventIterator.
func (s *EventIter) Next() (event *model.PolymorphicEvent, err error) {
	if len(s.resolved) == 0 {
		return
	}
	event = s.resolved[s.position]
	s.position += 1
	if s.position >= len(s.resolved) {
		s.resolved = nil
	}
	return
}

// Close implements sorter.EventIterator.
func (s *EventIter) Close() {
	s.resolved = nil
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
