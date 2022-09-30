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

package kv

import (
	"runtime"
	"sync"
)

// regionStateMap map regionID to regionFeedState
type regionStateMap struct {
	sync.RWMutex
	states map[uint64]*regionFeedState
}

func newRegionStateMap() *regionStateMap {
	return &regionStateMap{
		RWMutex: sync.RWMutex{},
		states:  make(map[uint64]*regionFeedState),
	}
}

func (m *regionStateMap) setState(regionID uint64, state *regionFeedState) {
	m.Lock()
	defer m.Unlock()
	m.states[regionID] = state
}

func (m *regionStateMap) getState(regionID uint64) (*regionFeedState, bool) {
	m.RLock()
	defer m.RUnlock()
	result, ok := m.states[regionID]
	return result, ok
}

func (m *regionStateMap) delState(regionID uint64) {
	m.Lock()
	defer m.Unlock()
	delete(m.states, regionID)
}

func (m *regionStateMap) readOnlyRange(f func(key, value interface{}) bool) {
	m.RLock()
	defer m.RUnlock()
	for key, value := range m.states {
		if !f(key, value) {
			return
		}
	}
}

// regionStateManager provides the get/put way like a sync.Map, and it is divided
// into several buckets to reduce lock contention
type regionStateManager struct {
	bucket int
	states []*regionStateMap
}

func newRegionStateManager(bucket int) *regionStateManager {
	if bucket <= 0 {
		bucket = runtime.NumCPU()
		if bucket > maxRegionStateBucket {
			bucket = maxRegionStateBucket
		}
		if bucket < minRegionStateBucket {
			bucket = minRegionStateBucket
		}
	}
	rsm := &regionStateManager{
		bucket: bucket,
		states: make([]*regionStateMap, bucket),
	}
	for i := range rsm.states {
		rsm.states[i] = newRegionStateMap()
	}
	return rsm
}

func (m *regionStateManager) getBucket(regionID uint64) int {
	return int(regionID) % m.bucket
}

func (m *regionStateManager) getState(regionID uint64) (*regionFeedState, bool) {
	bucket := m.getBucket(regionID)
	return m.states[bucket].getState(regionID)
}

func (m *regionStateManager) setState(regionID uint64, state *regionFeedState) {
	bucket := m.getBucket(regionID)
	m.states[bucket].setState(regionID, state)
}

func (m *regionStateManager) delState(regionID uint64) {
	bucket := m.getBucket(regionID)
	m.states[bucket].delState(regionID)
}
