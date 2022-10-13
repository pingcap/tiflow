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
	"sync/atomic"
	"time"

	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/tikv/client-go/v2/tikv"
)

const (
	minRegionStateBucket = 4
	maxRegionStateBucket = 16
)

type singleRegionInfo struct {
	verID      tikv.RegionVerID
	span       regionspan.ComparableSpan
	resolvedTs uint64
	rpcCtx     *tikv.RPCContext
}

func newSingleRegionInfo(
	verID tikv.RegionVerID,
	span regionspan.ComparableSpan,
	ts uint64,
	rpcCtx *tikv.RPCContext,
) singleRegionInfo {
	return singleRegionInfo{
		verID:      verID,
		span:       span,
		resolvedTs: ts,
		rpcCtx:     rpcCtx,
	}
}

type regionFeedState struct {
	sri       singleRegionInfo
	requestID uint64
	stopped   int32

	lock           sync.RWMutex
	initialized    bool
	matcher        *matcher
	startFeedTime  time.Time
	lastResolvedTs uint64
}

func newRegionFeedState(sri singleRegionInfo, requestID uint64) *regionFeedState {
	return &regionFeedState{
		sri:       sri,
		requestID: requestID,
		stopped:   0,
	}
}

func (s *regionFeedState) start() {
	s.startFeedTime = time.Now()
	s.lastResolvedTs = s.sri.resolvedTs
	s.matcher = newMatcher()
}

func (s *regionFeedState) getStartTime() time.Time {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.startFeedTime
}

func (s *regionFeedState) markStopped() {
	atomic.StoreInt32(&s.stopped, 1)
}

func (s *regionFeedState) isStopped() bool {
	return atomic.LoadInt32(&s.stopped) > 0
}

func (s *regionFeedState) isInitialized() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.initialized
}

func (s *regionFeedState) setInitialized() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.initialized = true
}

func (s *regionFeedState) getRegionSpan() regionspan.ComparableSpan {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.sri.span
}

func (s *regionFeedState) getRegionID() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.sri.verID.GetID()
}

func (s *regionFeedState) getRequestID() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.requestID
}

func (s *regionFeedState) getLastResolvedTs() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.lastResolvedTs
}

// updateResolvedTs update the resolved ts of the current region feed
func (s *regionFeedState) updateResolvedTs(resolvedTs uint64) {
	if resolvedTs > s.getLastResolvedTs() {
		s.lock.Lock()
		defer s.lock.Unlock()
		s.lastResolvedTs = resolvedTs
	}
}

func (s *regionFeedState) getStoreAddr() string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.sri.rpcCtx.Addr
}

// setRegionInfoResolvedTs is only called when the region disconnect,
// to update the `singleRegionInfo` which is reused by reconnect.
func (s *regionFeedState) setRegionInfoResolvedTs() {
	s.lock.RLock()
	if s.lastResolvedTs <= s.sri.resolvedTs {
		s.lock.RUnlock()
		return
	}
	s.lock.RUnlock()

	s.lock.Lock()
	defer s.lock.Unlock()
	s.sri.resolvedTs = s.lastResolvedTs
}

func (s *regionFeedState) getRegionInfoResolvedTs() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.sri.resolvedTs
}

func (s *regionFeedState) getRegionInfo() singleRegionInfo {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.sri
}

func (s *regionFeedState) getRegionMeta() (uint64, regionspan.ComparableSpan, time.Time, string) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.sri.verID.GetID(), s.sri.span, s.startFeedTime, s.sri.rpcCtx.Addr
}

type syncRegionFeedStateMap struct {
	mu     sync.RWMutex
	states map[uint64]*regionFeedState
}

func newSyncRegionFeedStateMap() *syncRegionFeedStateMap {
	return &syncRegionFeedStateMap{
		mu:     sync.RWMutex{},
		states: make(map[uint64]*regionFeedState),
	}
}

func (m *syncRegionFeedStateMap) setByRequestID(requestID uint64, state *regionFeedState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states[requestID] = state
}

func (m *syncRegionFeedStateMap) takeByRequestID(requestID uint64) (*regionFeedState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.states[requestID]
	if ok {
		delete(m.states, requestID)
	}
	return state, ok
}

func (m *syncRegionFeedStateMap) takeAll() map[uint64]*regionFeedState {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.states
	m.states = make(map[uint64]*regionFeedState)
	return state
}

func (m *syncRegionFeedStateMap) setByRegionID(regionID uint64, state *regionFeedState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states[regionID] = state
}

func (m *syncRegionFeedStateMap) getByRegionID(regionID uint64) (*regionFeedState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result, ok := m.states[regionID]
	return result, ok
}

func (m *syncRegionFeedStateMap) delByRegionID(regionID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.states, regionID)
}

func (m *syncRegionFeedStateMap) len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.states)
}

type regionStateManagerInterface interface {
	getState(regionID uint64) (*regionFeedState, bool)
	setState(regionID uint64, state *regionFeedState)
	delState(regionID uint64)
}

// regionStateManager provides the get/put way like a sync.Map, and it is divided
// into several buckets to reduce lock contention
type regionStateManager struct {
	bucket int
	states []*syncRegionFeedStateMap
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
		states: make([]*syncRegionFeedStateMap, bucket),
	}
	for i := range rsm.states {
		rsm.states[i] = newSyncRegionFeedStateMap()
	}
	return rsm
}

func (rsm *regionStateManager) getBucket(regionID uint64) int {
	return int(regionID) % rsm.bucket
}

func (rsm *regionStateManager) getState(regionID uint64) (*regionFeedState, bool) {
	bucket := rsm.getBucket(regionID)
	state, ok := rsm.states[bucket].getByRegionID(regionID)
	return state, ok
}

func (rsm *regionStateManager) setState(regionID uint64, state *regionFeedState) {
	bucket := rsm.getBucket(regionID)
	rsm.states[bucket].setByRegionID(regionID, state)
}

func (rsm *regionStateManager) delState(regionID uint64) {
	bucket := rsm.getBucket(regionID)
	rsm.states[bucket].delByRegionID(regionID)
}

func (rsm *regionStateManager) regionCount() (count int64) {
	for _, bucket := range rsm.states {
		count += int64(bucket.len())
	}
	return
}
