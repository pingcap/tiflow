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
	"sync"
	"sync/atomic"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sorter"
)

// redoEventCache caches events fetched from EventSortEngine.
type redoEventCache struct {
	capacity  uint64 // it's a constant.
	allocated uint64 // atomically shared in several goroutines.

	mu     sync.Mutex
	tables map[model.TableID]*eventsAndSize
}

// newRedoEventCache creates a redoEventCache instance.
func newRedoEventCache(capacity uint64) *redoEventCache {
	return &redoEventCache{
		capacity:  capacity,
		allocated: 0,
		tables:    make(map[model.TableID]*eventsAndSize),
	}
}

// getAppender returns an eventsAndSize instance which can be used to
// append events into the cache.
func (r *redoEventCache) getAppender(tableID model.TableID) *eventsAndSize {
	r.mu.Lock()
	defer r.mu.Unlock()
	item, exists := r.tables[tableID]
	if !exists {
		item = &eventsAndSize{capacity: r.capacity, allocated: &r.allocated}
		r.tables[tableID] = item
	}
	return item
}

// pop pops some events from the cache.
func (r *redoEventCache) pop(tableID model.TableID) ([]*model.RowChangedEvent, uint64, sorter.Position) {
	r.mu.Lock()
	item, exists := r.tables[tableID]
	if !exists {
		r.mu.Unlock()
		return nil, 0, sorter.Position{}
	}
	r.mu.Unlock()

	item.mu.RLock()
	defer item.mu.RUnlock()
	if len(item.events) == 0 || item.readyCount == 0 {
		return nil, 0, sorter.Position{}
	}

	events := item.events[0:item.readyCount]
	size := item.readySize
	pos := sorter.Position{
		CommitTs: item.events[item.readyCount-1].CommitTs,
		StartTs:  item.events[item.readyCount-1].StartTs,
	}

	item.events = item.events[item.readyCount:]
	if len(item.events) == 0 {
		r.mu.Lock()
		delete(r.tables, tableID)
		r.mu.Unlock()
	} else {
		item.readyCount = 0
		item.readySize = 0
	}

	atomic.AddUint64(&r.allocated, ^(size - 1))
	return events, size, pos
}

func (r *redoEventCache) removeTable(tableID model.TableID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item, exists := r.tables[tableID]
	if exists {
		item.mu.Lock()
		defer item.mu.Unlock()
		delete(r.tables, tableID)
		item.events = nil
	}
}

type eventsAndSize struct {
	capacity  uint64
	allocated *uint64

	broken bool

	mu          sync.RWMutex
	events      []*model.RowChangedEvent
	readySize   uint64
	pendingSize uint64
	readyCount  int
}

func (e *eventsAndSize) push(event *model.RowChangedEvent, size uint64, txnFinished bool) bool {
	// At most only one client can call push on a given eventsAndSize instance,
	// so lock is unnecessary.
	if e.broken {
		return false
	}

	for {
		allocated := atomic.LoadUint64(e.allocated)
		if allocated >= e.capacity {
			e.broken = true
			return false
		}
		if atomic.CompareAndSwapUint64(e.allocated, allocated, allocated+size) {
			break
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, event)
	e.pendingSize += size
	if txnFinished {
		e.readySize += e.pendingSize
		e.readyCount = len(e.events)
		e.pendingSize = 0
	}
	return true
}

func (e *eventsAndSize) cleanBrokenEvents() (size uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i := e.readyCount; i < len(e.events); i++ {
		e.events[i] = nil
	}
	size = e.pendingSize
	e.pendingSize = 0
	e.broken = false
	atomic.AddUint64(e.allocated, ^(size - 1))
	return
}
