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

	"github.com/pingcap/tiflow/cdc/kv/regionlock"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/tikv/client-go/v2/tikv"
)

const (
	minRegionStateBucket = 4
	maxRegionStateBucket = 16

	stateNormal  uint32 = 0
	stateStopped uint32 = 1
	stateRemoved uint32 = 2
)

type singleRegionInfo struct {
	verID  tikv.RegionVerID
	span   tablepb.Span
	rpcCtx *tikv.RPCContext

	lockedRange *regionlock.LockedRange
}

func newSingleRegionInfo(
	verID tikv.RegionVerID,
	span tablepb.Span,
	rpcCtx *tikv.RPCContext,
) singleRegionInfo {
	return singleRegionInfo{
		verID:  verID,
		span:   span,
		rpcCtx: rpcCtx,
	}
}

func (s singleRegionInfo) resolvedTs() uint64 {
	return s.lockedRange.CheckpointTs.Load()
}

type regionFeedState struct {
	sri       singleRegionInfo
	requestID uint64
	matcher   *matcher

	// Transform: normal -> stopped -> removed.
	// normal: the region is in replicating.
	// stopped: some error happens.
	// removed: the region is returned into the pending list,
	//   will be re-resolved and re-scheduled later.
	state struct {
		sync.RWMutex
		v uint32
	}
}

func newRegionFeedState(sri singleRegionInfo, requestID uint64) *regionFeedState {
	return &regionFeedState{
		sri:       sri,
		requestID: requestID,
	}
}

func (s *regionFeedState) start() {
	s.matcher = newMatcher()
}

// mark regionFeedState as stopped with the given error if possible.
func (s *regionFeedState) markStopped() {
	s.state.Lock()
	defer s.state.Unlock()
	if s.state.v == stateNormal {
		s.state.v = stateStopped
	}
}

func (s *regionFeedState) isStale() bool {
	s.state.RLock()
	defer s.state.RUnlock()
	return s.state.v == stateStopped || s.state.v == stateRemoved
}

func (s *regionFeedState) isInitialized() bool {
	return s.sri.lockedRange.Initialzied.Load()
}

func (s *regionFeedState) setInitialized() {
	s.sri.lockedRange.Initialzied.Store(true)
}

func (s *regionFeedState) getRegionID() uint64 {
	return s.sri.verID.GetID()
}

func (s *regionFeedState) getLastResolvedTs() uint64 {
	return s.sri.lockedRange.CheckpointTs.Load()
}

// updateResolvedTs update the resolved ts of the current region feed
func (s *regionFeedState) updateResolvedTs(resolvedTs uint64) {
	state := s.sri.lockedRange
	for {
		last := state.CheckpointTs.Load()
		if last > resolvedTs {
			return
		}
		if state.CheckpointTs.CompareAndSwap(last, resolvedTs) {
			break
		}
	}
<<<<<<< HEAD
=======
	if s.sri.requestedTable != nil {
		s.sri.requestedTable.postUpdateRegionResolvedTs(
			s.sri.verID.GetID(),
			s.sri.verID.GetVer(),
			state,
			s.sri.span,
		)
	}
>>>>>>> 8ca4b58dd3 (puller(cdc): more tests and logs for frontier and kv-client (#10333))
}

func (s *regionFeedState) getRegionInfo() singleRegionInfo {
	return s.sri
}

func (s *regionFeedState) getRegionMeta() (uint64, tablepb.Span, string) {
	return s.sri.verID.GetID(), s.sri.span, s.sri.rpcCtx.Addr
}

type syncRegionFeedStateMap struct {
	mu sync.RWMutex
	// statesInternal is an internal field and must not be accessed from outside.
	statesInternal map[uint64]*regionFeedState
}

func newSyncRegionFeedStateMap() *syncRegionFeedStateMap {
	return &syncRegionFeedStateMap{
		mu:             sync.RWMutex{},
		statesInternal: make(map[uint64]*regionFeedState),
	}
}

func (m *syncRegionFeedStateMap) iter(fn func(requestID uint64, state *regionFeedState) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for requestID, state := range m.statesInternal {
		if !fn(requestID, state) {
			break
		}
	}
}

func (m *syncRegionFeedStateMap) setByRequestID(requestID uint64, state *regionFeedState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statesInternal[requestID] = state
}

func (m *syncRegionFeedStateMap) takeByRequestID(requestID uint64) (*regionFeedState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.statesInternal[requestID]
	if ok {
		delete(m.statesInternal, requestID)
	}
	return state, ok
}

func (m *syncRegionFeedStateMap) takeAll() map[uint64]*regionFeedState {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.statesInternal
	m.statesInternal = make(map[uint64]*regionFeedState)
	return state
}

func (m *syncRegionFeedStateMap) setByRegionID(regionID uint64, state *regionFeedState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statesInternal[regionID] = state
}

func (m *syncRegionFeedStateMap) getByRegionID(regionID uint64) (*regionFeedState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result, ok := m.statesInternal[regionID]
	return result, ok
}

func (m *syncRegionFeedStateMap) delByRegionID(regionID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.statesInternal, regionID)
}

func (m *syncRegionFeedStateMap) len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.statesInternal)
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
