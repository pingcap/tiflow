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
	"sync"

	"github.com/pingcap/tiflow/cdc/kv/regionlock"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/tikv/client-go/v2/tikv"
)

const (
	stateNormal  uint32 = 0
	stateStopped uint32 = 1
	stateRemoved uint32 = 2
)

type regionInfo struct {
	verID tikv.RegionVerID
	// The span of the region.
	// Note(dongmen): The span doesn't always represent the whole span of a region.
	// Instead, it is the portion of the region that belongs the subcribed table.
	// Multiple tables can belong to the same region.
	// For instance, consider region-1 with a span of [a, d).
	// It contains 3 tables: t1[a, b), t2[b,c), and t3[c,d).
	// If only table t1 is subscribed to, then the span of interest is [a,b).
	span   tablepb.Span
	rpcCtx *tikv.RPCContext

	// The table that the region belongs to.
	subscribedTable *subscribedTable
	// The state of the locked range of the region.
	lockedRangeState *regionlock.LockedRangeState
}

func (s regionInfo) isStoped() bool {
	// lockedRange only nil when the region's subscribedTable is stopped.
	return s.lockedRangeState == nil
}

func newRegionInfo(
	verID tikv.RegionVerID,
	span tablepb.Span,
	rpcCtx *tikv.RPCContext,
	subscribedTable *subscribedTable,
) regionInfo {
	return regionInfo{
		verID:           verID,
		span:            span,
		rpcCtx:          rpcCtx,
		subscribedTable: subscribedTable,
	}
}

func (s regionInfo) resolvedTs() uint64 {
	return s.lockedRangeState.ResolvedTs.Load()
}

type regionErrorInfo struct {
	regionInfo
	err error
}

func newRegionErrorInfo(info regionInfo, err error) regionErrorInfo {
	return regionErrorInfo{
		regionInfo: info,
		err:        err,
	}
}

type regionFeedState struct {
	region    regionInfo
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
		// All region errors should be handled in region workers.
		// `err` is used to retrieve errors generated outside.
		err error
	}
}

func newRegionFeedState(region regionInfo, requestID uint64) *regionFeedState {
	return &regionFeedState{
		region:    region,
		requestID: requestID,
	}
}

func (s *regionFeedState) start() {
	s.matcher = newMatcher()
}

// mark regionFeedState as stopped with the given error if possible.
func (s *regionFeedState) markStopped(err error) {
	s.state.Lock()
	defer s.state.Unlock()
	if s.state.v == stateNormal {
		s.state.v = stateStopped
		s.state.err = err
	}
}

// mark regionFeedState as removed if possible.
func (s *regionFeedState) markRemoved() (changed bool) {
	s.state.Lock()
	defer s.state.Unlock()
	if s.state.v == stateStopped {
		s.state.v = stateRemoved
		changed = true
	}
	return
}

func (s *regionFeedState) isStale() bool {
	s.state.RLock()
	defer s.state.RUnlock()
	return s.state.v == stateStopped || s.state.v == stateRemoved
}

func (s *regionFeedState) takeError() (err error) {
	s.state.Lock()
	defer s.state.Unlock()
	err = s.state.err
	s.state.err = nil
	return
}

func (s *regionFeedState) isInitialized() bool {
	return s.region.lockedRangeState.Initialzied.Load()
}

func (s *regionFeedState) setInitialized() {
	s.region.lockedRangeState.Initialzied.Store(true)
}

func (s *regionFeedState) getRegionID() uint64 {
	return s.region.verID.GetID()
}

func (s *regionFeedState) getLastResolvedTs() uint64 {
	return s.region.lockedRangeState.ResolvedTs.Load()
}

// updateResolvedTs update the resolved ts of the current region feed
func (s *regionFeedState) updateResolvedTs(resolvedTs uint64) {
	state := s.region.lockedRangeState
	for {
		last := state.ResolvedTs.Load()
		if last > resolvedTs {
			return
		}
		if state.ResolvedTs.CompareAndSwap(last, resolvedTs) {
			break
		}
	}

	if s.region.subscribedTable != nil {
		// When resolvedTs is received, we need to try to resolve the lock of the region.
		// Because the updated resolvedTs may less than the target resolvedTs we want advance to.
		s.region.subscribedTable.tryResolveLock(
			s.region.verID.GetID(),
			state,
		)
	}
}

func (s *regionFeedState) getRegionInfo() regionInfo {
	return s.region
}

func (s *regionFeedState) getRegionMeta() (uint64, tablepb.Span, string) {
	return s.region.verID.GetID(), s.region.span, s.region.rpcCtx.Addr
}
