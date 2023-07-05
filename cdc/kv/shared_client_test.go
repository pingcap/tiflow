// Copyright 2020 PingCAP, Inc.
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
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/kv/regionlock"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestRequestedStreamRequestedRegions(t *testing.T) {
	stream := &requestedStream{streamID: 100, requests: chann.NewAutoDrainChann[singleRegionInfo]()}
	defer stream.requests.CloseAndDrain()
	stream.requestedRegions.m = make(map[uint64]map[uint64]*regionFeedState)

	require.Nil(t, stream.getState(1, 2))
	require.Nil(t, stream.takeState(1, 2))

	stream.setState(1, 2, &regionFeedState{sri: singleRegionInfo{requestedTable: &requestedTable{}}})
	require.NotNil(t, stream.getState(1, 2))
	require.NotNil(t, stream.takeState(1, 2))
	require.Nil(t, stream.getState(1, 2))

	requestedTable := &requestedTable{requestID: 1}
	requestedTable.removed.Store(true)
	for i := uint64(2); i < uint64(5); i++ {
		stream.setState(1, i, &regionFeedState{sri: singleRegionInfo{requestedTable: requestedTable}})
	}

	require.Nil(t, stream.getState(1, 2))
	select {
	case sri := <-stream.requests.Out():
		require.Equal(t, sri.requestedTable.requestID, uint64(1))
	case <-time.NewTimer(100 * time.Millisecond).C:
		require.True(t, false, "must get a singleRegionInfo")
	}

	require.Nil(t, stream.getState(1, 3))
	select {
	case <-stream.requests.Out():
		require.False(t, true, "shouldn't get a singleRegionInfo")
	case <-time.NewTimer(100 * time.Millisecond).C:
	}

	requestedTable.deregister.Store(uint64(100), time.Now().Add(-1*time.Hour))
	require.Nil(t, stream.getState(1, 4))
	select {
	case sri := <-stream.requests.Out():
		require.Equal(t, sri.requestedTable.requestID, uint64(1))
	case <-time.NewTimer(100 * time.Millisecond).C:
		require.True(t, false, "must get a singleRegionInfo")
	}
}

func TestRequestedTable(t *testing.T) {
	s := &SharedClient{resolveLockCh: chann.NewAutoDrainChann[resolveLockTask]()}
	span := tablepb.Span{TableID: 1, StartKey: []byte{'a'}, EndKey: []byte{'z'}}
	table := s.newRequestedTable(SubscriptionID(1), span, 100, nil)
	s.totalSpans.m = spanz.NewHashMap[*requestedTable]()
	s.totalSpans.m.ReplaceOrInsert(span, table)
	s.pdClock = pdutil.NewClock4Test()

	// Lock a range, and then ResolveLock will trigger a task for it.
	res := table.rangeLock.LockRange(context.Background(), []byte{'b'}, []byte{'c'}, 1, 100)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	res.LockedRange.Initialzied.Store(true)

	require.True(t, s.ResolveLock(span, 200))
	select {
	case <-s.resolveLockCh.Out():
	case <-time.NewTimer(100 * time.Millisecond).C:
		require.True(t, false, "must get a resolve lock task")
	}

	// Lock another range, no task will be triggered before initialized.
	res = table.rangeLock.LockRange(context.Background(), []byte{'c'}, []byte{'d'}, 2, 100)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	state := newRegionFeedState(singleRegionInfo{lockedRange: res.LockedRange, requestedTable: table}, 1)
	select {
	case <-s.resolveLockCh.Out():
		require.True(t, false, "shouldn't get a resolve lock task")
	case <-time.NewTimer(100 * time.Millisecond).C:
	}

	// Task will be triggered after initialized.
	state.setInitialized()
	state.updateResolvedTs(101)
	select {
	case <-s.resolveLockCh.Out():
	case <-time.NewTimer(100 * time.Millisecond).C:
		require.True(t, false, "must get a resolve lock task")
	}

	s.resolveLockCh.CloseAndDrain()
}
