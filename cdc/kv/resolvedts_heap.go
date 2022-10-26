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
	"time"
)

// regionTsInfo contains region resolvedTs information
type regionTsInfo struct {
	regionID   uint64
	resolvedTs uint64
	eventTime  time.Time
	penalty    int
}

type regionTsHeap []*regionTsInfo

func (rh regionTsHeap) Len() int { return len(rh) }

func (rh regionTsHeap) Less(i, j int) bool {
	return rh[i].resolvedTs < rh[j].resolvedTs
}

func (rh regionTsHeap) Swap(i, j int) {
	rh[i], rh[j] = rh[j], rh[i]
}

// regionTsManager is a used to maintain resolved ts information for N regions.
// This struct is not thread safe
type regionTsManager struct {
	// mapping from regionID to regionTsInfo object
	m map[uint64]*regionTsInfo
}

func newRegionTsManager() *regionTsManager {
	return &regionTsManager{
		m: make(map[uint64]*regionTsInfo),
	}
}

// Upsert implements insert	and update on duplicated key
// if the region is exists update the resolvedTs, eventTime, penalty, and fixed heap order
// otherwise, insert a new regionTsInfo with penalty 0
func (rm *regionTsManager) Upsert(regionID, resolvedTs uint64, eventTime time.Time) {
	if old, ok := rm.m[regionID]; ok {
		// in a single resolved ts manager, we should not expect a fallback resolved event
		// but, it's ok that we use fallback resolved event to increase penalty
		if resolvedTs <= old.resolvedTs && eventTime.After(old.eventTime) {
			old.penalty++
			old.eventTime = eventTime
		} else if resolvedTs > old.resolvedTs {
			old.resolvedTs = resolvedTs
			old.eventTime = eventTime
			old.penalty = 0
		}
	} else {
		item := &regionTsInfo{
			regionID:   regionID,
			resolvedTs: resolvedTs,
			eventTime:  eventTime,
			penalty:    0,
		}
		rm.Insert(item)
	}
}

// Insert inserts a regionTsInfo to rts heap
func (rm *regionTsManager) Insert(item *regionTsInfo) {
	rm.m[item.regionID] = item
}

// Remove removes item from regionTsManager
func (rm *regionTsManager) Remove(regionID uint64) *regionTsInfo {
	if item, ok := rm.m[regionID]; ok {
		delete(rm.m, item.regionID)
	}
	return nil
}
