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

package sinkmanager

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/prometheus/client_golang/prometheus"
)

// redoEventCache caches events fetched from EventSortEngine.
type redoEventCache struct {
	capacity  uint64 // it's a constant.
	allocated uint64 // atomically shared in several goroutines.

	mu     sync.Mutex
	tables map[model.TableID]*eventAppender

	metricRedoEventCache prometheus.Gauge
}

type eventAppender struct {
	capacity uint64
	cache    *redoEventCache

	mu sync.Mutex
	// If an eventAppender is broken, it means it missed some events.
	broken bool
	events []*model.RowChangedEvent
	sizes  []uint64
	// Count of ready events.
	readyCount int
	// Multiple RowChangedEvents can come from one PolymorphicEvent.
	pushCounts []byte

	// Both of them are included.
	lowerBound engine.Position
	upperBound engine.Position
}

type popResult struct {
	events      []*model.RowChangedEvent
	size        uint64 // size of events.
	releaseSize uint64 // size of all released events.

	// many RowChangedEvent can come from one same PolymorphicEvent.
	// pushCount indicates the count of raw PolymorphicEvents.
	pushCount int

	// success indicates whether there is a gap between cached events and required events.
	success bool

	// If success, upperBoundIfSuccess is the upperBound of poped events.
	// The caller should fetch events (upperBoundIfSuccess, upperBound] from engine.
	upperBoundIfSuccess engine.Position
	// If fail, lowerBoundIfFail is the lowerBound of cached events.
	// The caller should fetch events [lowerBound, lowerBoundIfFail) from engine.
	lowerBoundIfFail engine.Position
}

// newRedoEventCache creates a redoEventCache instance.
func newRedoEventCache(changefeedID model.ChangeFeedID, capacity uint64) *redoEventCache {
	return &redoEventCache{
		capacity:  capacity,
		allocated: 0,
		tables:    make(map[model.TableID]*eventAppender),

		metricRedoEventCache: RedoEventCache.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}
}

func (r *redoEventCache) removeTable(tableID model.TableID) {
	r.mu.Lock()
	item, exists := r.tables[tableID]
	defer r.mu.Unlock()
	if exists {
		item.mu.Lock()
		totalSize := uint64(0)
		for _, size := range item.sizes {
			totalSize += size
		}
		r.metricRedoEventCache.Sub(float64(totalSize))
		item.events = nil
		item.sizes = nil
		item.pushCounts = nil
		item.mu.Unlock()
		delete(r.tables, tableID)
	}
}

func (r *redoEventCache) maybeCreateAppender(tableID model.TableID, lowerBound engine.Position) *eventAppender {
	r.mu.Lock()
	defer r.mu.Unlock()

	item, exists := r.tables[tableID]
	if !exists {
		item = &eventAppender{
			capacity:   r.capacity,
			cache:      r,
			lowerBound: lowerBound,
		}
		r.tables[tableID] = item
		return item
	}

	item.mu.Lock()
	defer item.mu.Unlock()
	if item.broken {
		if item.readyCount == 0 {
			item.broken = false
			item.events = nil
			item.lowerBound = lowerBound
			item.upperBound = engine.Position{}
		} else {
			// The appender is still broken.
			item = nil
		}
	}
	return item
}

func (r *redoEventCache) getAppender(tableID model.TableID) *eventAppender {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tables[tableID]
}

func (e *eventAppender) pop(lowerBound, upperBound engine.Position) (res popResult) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if lowerBound.Compare(e.lowerBound) < 0 {
		// if lowerBound is less than e.lowerBound, it means there is a gap between
		// the required range and the cached range. For example, [100, 110) is required,
		// but there is [120, ...) in the cache.
		// NOTE: the caller will fetch events [lowerBound, res.boundary) from engine.
		res.success = false
		if e.lowerBound.Compare(upperBound.Next()) <= 0 {
			res.lowerBoundIfFail = e.lowerBound
		} else {
			res.lowerBoundIfFail = upperBound.Next()
		}
		return
	}

	if !e.upperBound.Valid() {
		// It means there are no resolved cached transactions in the required range.
		// NOTE: the caller will fetch events [lowerBound, res.boundary) from engine.
		res.success = false
		res.lowerBoundIfFail = upperBound.Next()
		return
	}

	res.success = true
	if lowerBound.Compare(e.upperBound) > 0 {
		res.upperBoundIfSuccess = lowerBound.Prev()
	} else if upperBound.Compare(e.upperBound) > 0 {
		res.upperBoundIfSuccess = e.upperBound
	} else {
		res.upperBoundIfSuccess = upperBound
	}

	startIdx := sort.Search(e.readyCount, func(i int) bool {
		pos := engine.Position{CommitTs: e.events[i].CommitTs, StartTs: e.events[i].StartTs}
		return pos.Compare(lowerBound) >= 0
	})

	for i := 0; i < startIdx; i++ {
		res.releaseSize += e.sizes[i]
	}

	endIdx := sort.Search(e.readyCount, func(i int) bool {
		pos := engine.Position{CommitTs: e.events[i].CommitTs, StartTs: e.events[i].StartTs}
		return pos.Compare(res.upperBoundIfSuccess) > 0
	})
	res.events = e.events[startIdx:endIdx]
	for i := startIdx; i < endIdx; i++ {
		res.size += e.sizes[i]
		res.pushCount += int(e.pushCounts[i])
	}
	res.releaseSize += res.size

	e.events = e.events[endIdx:]
	e.sizes = e.sizes[endIdx:]
	e.pushCounts = e.pushCounts[endIdx:]
	e.readyCount -= endIdx
	// Update boundaries. Set upperBound to invalid if the range has been drained.
	e.lowerBound = res.upperBoundIfSuccess.Next()
	if e.lowerBound.Compare(e.upperBound) > 0 {
		e.upperBound = engine.Position{}
	}

	atomic.AddUint64(&e.cache.allocated, ^(res.releaseSize - 1))
	e.cache.metricRedoEventCache.Sub(float64(res.releaseSize))
	return
}

// All events should come from one PolymorphicEvent.
func (e *eventAppender) pushBatch(events []*model.RowChangedEvent, size uint64, txnFinished engine.Position) (bool, uint64) {
	if len(events) == 0 {
		return e.push(nil, size, txnFinished)
	}
	return e.push(events[0], size, txnFinished, events[1:]...)
}

func (e *eventAppender) push(
	event *model.RowChangedEvent,
	size uint64,
	txnFinished engine.Position,
	eventsInSameBatch ...*model.RowChangedEvent,
) (success bool, brokenSize uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.broken {
		return false, 0
	}

	// The caller just wants to update the upperbound.
	if event == nil {
		if txnFinished.Valid() {
			e.upperBound = txnFinished
		}
		return true, 0
	}

	for {
		allocated := atomic.LoadUint64(&e.cache.allocated)
		if allocated >= e.capacity {
			e.broken = true
			brokenSize := e.onBroken()
			return false, brokenSize
		}
		if atomic.CompareAndSwapUint64(&e.cache.allocated, allocated, allocated+size) {
			e.cache.metricRedoEventCache.Add(float64(size))
			break
		}
	}

	e.events = append(e.events, event)
	e.sizes = append(e.sizes, size)
	e.pushCounts = append(e.pushCounts, 1)
	for _, event := range eventsInSameBatch {
		e.events = append(e.events, event)
		e.sizes = append(e.sizes, 0)
		e.pushCounts = append(e.pushCounts, 0)
	}
	if txnFinished.Valid() {
		e.readyCount = len(e.events)
		e.upperBound = txnFinished
	}
	return true, 0
}

func (e *eventAppender) onBroken() (pendingSize uint64) {
	if e.readyCount < len(e.events) {
		for i := e.readyCount; i < len(e.events); i++ {
			pendingSize += e.sizes[i]
			e.events[i] = nil
		}

		e.events = e.events[0:e.readyCount]
		e.sizes = e.sizes[0:e.readyCount]
		e.pushCounts = e.pushCounts[0:e.readyCount]

		atomic.AddUint64(&e.cache.allocated, ^(pendingSize - 1))
		e.cache.metricRedoEventCache.Sub(float64(pendingSize))
	}
	return
}
