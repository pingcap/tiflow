// Copyright 2021 PingCAP, Inc.
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

package kv

import (
	"container/heap"
	"time"
)

type tsItem struct {
	sortByEvTime bool
	resolvedTs   uint64
	eventTime    time.Time
	penalty      int
}

func newResolvedTsItem(ts uint64) tsItem {
	return tsItem{resolvedTs: ts, eventTime: time.Now()}
}

func newEventTimeItem() tsItem {
	return tsItem{sortByEvTime: true, eventTime: time.Now()}
}

// regionTsInfo contains region resolvedTs information
type regionTsInfo struct {
	regionID uint64
	index    int
	ts       tsItem
}

type regionTsHeap []*regionTsInfo

func (rh regionTsHeap) Len() int { return len(rh) }

func (rh regionTsHeap) Less(i, j int) bool {
	if rh[i].ts.sortByEvTime {
		return rh[i].ts.eventTime.Before(rh[j].ts.eventTime)
	}
	return rh[i].ts.resolvedTs < rh[j].ts.resolvedTs
}

func (rh regionTsHeap) Swap(i, j int) {
	rh[i], rh[j] = rh[j], rh[i]
	rh[i].index = i
	rh[j].index = j
}

func (rh *regionTsHeap) Push(x interface{}) {
	n := len(*rh)
	item := x.(*regionTsInfo)
	item.index = n
	*rh = append(*rh, item)
}

func (rh *regionTsHeap) Pop() interface{} {
	old := *rh
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*rh = old[0 : n-1]
	return item
}

// regionTsManager is a used to maintain resolved ts information for N regions.
// This struct is not thread safe
type regionTsManager struct {
	// mapping from regionID to regionTsInfo object
	m map[uint64]*regionTsInfo
	h regionTsHeap
}

func newRegionTsManager() *regionTsManager {
	return &regionTsManager{
		m: make(map[uint64]*regionTsInfo),
		h: make(regionTsHeap, 0),
	}
}

// Upsert implements insert	and update on duplicated key
func (rm *regionTsManager) Upsert(item *regionTsInfo) {
	if old, ok := rm.m[item.regionID]; ok {
		// in a single resolved ts manager, we should not expect a fallback resolved event
		// but it's ok that we use fallback resolved event to increase penalty
		if !item.ts.sortByEvTime {
			if item.ts.resolvedTs <= old.ts.resolvedTs && item.ts.eventTime.After(old.ts.eventTime) {
				old.ts.penalty++
				old.ts.eventTime = item.ts.eventTime
				heap.Fix(&rm.h, old.index)
			} else if item.ts.resolvedTs > old.ts.resolvedTs {
				old.ts.resolvedTs = item.ts.resolvedTs
				old.ts.eventTime = item.ts.eventTime
				old.ts.penalty = 0
				heap.Fix(&rm.h, old.index)
			}
		} else {
			if item.ts.eventTime.After(old.ts.eventTime) {
				old.ts.eventTime = item.ts.eventTime
				heap.Fix(&rm.h, old.index)
			}
		}
	} else {
		heap.Push(&rm.h, item)
		rm.m[item.regionID] = item
	}
}

// Pop pops a regionTsInfo from rts heap, delete it from region rts map
func (rm *regionTsManager) Pop() *regionTsInfo {
	if rm.Len() == 0 {
		return nil
	}
	item := heap.Pop(&rm.h).(*regionTsInfo)
	delete(rm.m, item.regionID)
	return item
}

// Remove removes item from regionTsManager
func (rm *regionTsManager) Remove(regionID uint64) *regionTsInfo {
	if item, ok := rm.m[regionID]; ok {
		delete(rm.m, item.regionID)
		return heap.Remove(&rm.h, item.index).(*regionTsInfo)
	}
	return nil
}

// Len returns the item count in regionTsManager
func (rm *regionTsManager) Len() int {
	return len(rm.m)
}
