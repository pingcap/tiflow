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

import "container/heap"

// regionResolvedTs contains region resolvedTs information
type regionResolvedTs struct {
	regionID   uint64
	resolvedTs uint64
	index      int
}

type resolvedTsHeap []*regionResolvedTs

func (rh resolvedTsHeap) Len() int { return len(rh) }

func (rh resolvedTsHeap) Less(i, j int) bool {
	return rh[i].resolvedTs < rh[j].resolvedTs
}

func (rh resolvedTsHeap) Swap(i, j int) {
	rh[i], rh[j] = rh[j], rh[i]
	rh[i].index = i
	rh[j].index = j
}

func (rh *resolvedTsHeap) Push(x interface{}) {
	n := len(*rh)
	item := x.(*regionResolvedTs)
	item.index = n
	*rh = append(*rh, item)
}

func (rh *resolvedTsHeap) Pop() interface{} {
	old := *rh
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*rh = old[0 : n-1]
	return item
}

// resolvedTsManager is a used to maintain resolved ts information for N regions.
// This struct is not thread safe
type resolvedTsManager struct {
	// mapping from regionID to regionResolvedTs object
	m map[uint64]*regionResolvedTs
	h resolvedTsHeap
}

func newResolvedTsManager() *resolvedTsManager {
	return &resolvedTsManager{
		m: make(map[uint64]*regionResolvedTs),
		h: make(resolvedTsHeap, 0),
	}
}

// Upsert implements insert	and update on duplicated key
func (rm *resolvedTsManager) Upsert(item *regionResolvedTs) {
	if old, ok := rm.m[item.regionID]; ok {
		// in a single resolved ts manager, the resolved ts of a region should not be fallen back
		if item.resolvedTs > old.resolvedTs {
			old.resolvedTs = item.resolvedTs
			heap.Fix(&rm.h, old.index)
		}
	} else {
		heap.Push(&rm.h, item)
		rm.m[item.regionID] = item
	}
}

// Pop pops a regionResolvedTs from rts heap, delete it from region rts map
func (rm *resolvedTsManager) Pop() *regionResolvedTs {
	if rm.Len() == 0 {
		return nil
	}
	item := heap.Pop(&rm.h).(*regionResolvedTs)
	delete(rm.m, item.regionID)
	return item
}

func (rm *resolvedTsManager) Len() int {
	return len(rm.m)
}
