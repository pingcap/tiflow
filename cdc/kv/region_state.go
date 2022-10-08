package kv

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/tikv/client-go/v2/tikv"
)

type singleRegionInfo struct {
	verID        tikv.RegionVerID
	span         regionspan.ComparableSpan
	checkpointTs uint64
	rpcCtx       *tikv.RPCContext
}

func newSingleRegionInfo(
	verID tikv.RegionVerID,
	span regionspan.ComparableSpan,
	ts uint64,
	rpcCtx *tikv.RPCContext,
) singleRegionInfo {
	return singleRegionInfo{
		verID:        verID,
		span:         span,
		checkpointTs: ts,
		rpcCtx:       rpcCtx,
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
	s.lastResolvedTs = s.sri.checkpointTs
	s.matcher = newMatcher()
}

func (s *regionFeedState) markStopped() {
	atomic.StoreInt32(&s.stopped, 1)
}

func (s *regionFeedState) isStopped() bool {
	return atomic.LoadInt32(&s.stopped) > 0
}

func (s *regionFeedState) getLastResolvedTs() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.lastResolvedTs
}

func (s *regionFeedState) getRegionSpan() regionspan.ComparableSpan {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.sri.span
}

type syncRegionFeedStateMap struct {
	mu            sync.Mutex
	regionInfoMap map[uint64]*regionFeedState
}

func newSyncRegionFeedStateMap() *syncRegionFeedStateMap {
	return &syncRegionFeedStateMap{
		mu:            sync.Mutex{},
		regionInfoMap: make(map[uint64]*regionFeedState),
	}
}

func (m *syncRegionFeedStateMap) insert(requestID uint64, state *regionFeedState) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.regionInfoMap[requestID]
	m.regionInfoMap[requestID] = state
	return ok
}

func (m *syncRegionFeedStateMap) take(requestID uint64) (*regionFeedState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.regionInfoMap[requestID]
	if ok {
		delete(m.regionInfoMap, requestID)
	}
	return state, ok
}

func (m *syncRegionFeedStateMap) takeAll() map[uint64]*regionFeedState {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.regionInfoMap
	m.regionInfoMap = make(map[uint64]*regionFeedState)
	return state
}
