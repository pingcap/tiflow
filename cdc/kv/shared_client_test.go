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

	stream.requestedRegions.m[1] = make(map[uint64]*regionFeedState)
	stream.requestedRegions.m[1][2] = &regionFeedState{sri: singleRegionInfo{requestedTable: &requestedTable{}}}
	require.NotNil(t, stream.getState(1, 2))
	require.NotNil(t, stream.takeState(1, 2))
	require.Nil(t, stream.getState(1, 2))

	requestedTable := &requestedTable{requestID: 1}
	requestedTable.removed.Store(true)
	for i := uint64(2); i < uint64(5); i++ {
		stream.requestedRegions.m[1][i] = &regionFeedState{sri: singleRegionInfo{requestedTable: requestedTable}}
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

	require.True(t, s.ResolveLock(span))
	time.Sleep(100 * time.Millisecond)
	select {
	case <-s.resolveLockCh.Out():
	default:
		require.True(t, false, "must get a resolve lock task")
	}

	// Lock another range, and it will auto trigger a task for it.
	res = table.rangeLock.LockRange(context.Background(), []byte{'c'}, []byte{'d'}, 2, 200)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	time.Sleep(100 * time.Millisecond)
	select {
	case <-s.resolveLockCh.Out():
	default:
		require.True(t, false, "must get a resolve lock task")
	}

	s.resolveLockCh.CloseAndDrain()
}
