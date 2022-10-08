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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func checkRegionTsInfoWithoutEvTime(t *testing.T, obtained, expected *regionTsInfo) {
	require.Equal(t, expected.regionID, obtained.regionID)
	require.Equal(t, expected.index, obtained.index)
	require.Equal(t, expected.ts.resolvedTs, obtained.ts.resolvedTs)
}

func TestRegionTsManagerResolvedTs(t *testing.T) {
	t.Parallel()
	mgr := newRegionTsManager()
	initRegions := []*regionTsInfo{
		{regionID: 102, ts: newResolvedTsItem(1040)},
		{regionID: 100, ts: newResolvedTsItem(1000)},
		{regionID: 101, ts: newResolvedTsItem(1020)},
	}
	for _, rts := range initRegions {
		mgr.Upsert(rts.regionID, rts.ts.resolvedTs, rts.ts.eventTime)
	}
	require.Equal(t, 3, mgr.Len())
	rts := mgr.Pop()
	checkRegionTsInfoWithoutEvTime(t, rts, &regionTsInfo{regionID: 100, ts: newResolvedTsItem(1000), index: -1})

	// resolved ts is not updated
	mgr.Upsert(rts.regionID, rts.ts.resolvedTs, rts.ts.eventTime)
	rts = mgr.Pop()
	checkRegionTsInfoWithoutEvTime(t, rts, &regionTsInfo{regionID: 100, ts: newResolvedTsItem(1000), index: -1})

	// resolved ts updated
	rts.ts.resolvedTs = 1001
	mgr.Upsert(rts.regionID, rts.ts.resolvedTs, rts.ts.eventTime)
	mgr.Upsert(100, 1100, time.Now())

	rts = mgr.Pop()
	checkRegionTsInfoWithoutEvTime(t, rts, &regionTsInfo{regionID: 101, ts: newResolvedTsItem(1020), index: -1})
	rts = mgr.Pop()
	checkRegionTsInfoWithoutEvTime(t, rts, &regionTsInfo{regionID: 102, ts: newResolvedTsItem(1040), index: -1})
	rts = mgr.Pop()
	checkRegionTsInfoWithoutEvTime(t, rts, &regionTsInfo{regionID: 100, ts: newResolvedTsItem(1100), index: -1})
	rts = mgr.Pop()
	require.Nil(t, rts)
}

func TestRegionTsManagerPenalty(t *testing.T) {
	t.Parallel()
	mgr := newRegionTsManager()
	initRegions := []*regionTsInfo{
		{regionID: 100, ts: newResolvedTsItem(1000)},
	}
	for _, rts := range initRegions {
		mgr.Upsert(rts.regionID, rts.ts.resolvedTs, rts.ts.eventTime)
	}
	require.Equal(t, 1, mgr.Len())

	// test penalty increases if resolved ts keeps unchanged
	for i := 0; i < 6; i++ {
		rts := &regionTsInfo{regionID: 100, ts: newResolvedTsItem(1000)}
		mgr.Upsert(rts.regionID, rts.ts.resolvedTs, rts.ts.eventTime)
	}
	rts := mgr.Pop()
	require.Equal(t, uint64(1000), rts.ts.resolvedTs)
	require.Equal(t, 6, rts.ts.penalty)

	// test penalty is cleared to zero if resolved ts is advanced
	mgr.Upsert(rts.regionID, rts.ts.resolvedTs, rts.ts.eventTime)
	rtsNew := &regionTsInfo{regionID: 100, ts: newResolvedTsItem(2000)}
	mgr.Upsert(rtsNew.regionID, rtsNew.ts.resolvedTs, rtsNew.ts.eventTime)
	rts = mgr.Pop()
	require.Equal(t, 0, rts.ts.penalty)
	require.Equal(t, uint64(2000), rts.ts.resolvedTs)
}

func TestRegionTsManagerPenaltyForFallBackEvent(t *testing.T) {
	t.Parallel()
	mgr := newRegionTsManager()
	initRegions := []*regionTsInfo{
		{regionID: 100, ts: newResolvedTsItem(1000)},
	}
	for _, rts := range initRegions {
		mgr.Upsert(rts.regionID, rts.ts.resolvedTs, rts.ts.eventTime)
	}
	require.Equal(t, 1, mgr.Len())

	// test penalty increases if we meet a fallback event
	for i := 0; i < 6; i++ {
		rts := &regionTsInfo{regionID: 100, ts: newResolvedTsItem(uint64(1000 - i))}
		mgr.Upsert(rts.regionID, rts.ts.resolvedTs, rts.ts.eventTime)
	}
	rts := mgr.Pop()
	// original resolvedTs will remain unchanged
	require.Equal(t, uint64(1000), rts.ts.resolvedTs)
	require.Equal(t, 6, rts.ts.penalty)

	// test penalty is cleared to zero if resolved ts is advanced
	mgr.Upsert(rts.regionID, rts.ts.resolvedTs, rts.ts.eventTime)
	rtsNew := &regionTsInfo{regionID: 100, ts: newResolvedTsItem(2000)}
	mgr.Upsert(rtsNew.regionID, rtsNew.ts.resolvedTs, rtsNew.ts.eventTime)
	rts = mgr.Pop()
	require.Equal(t, 0, rts.ts.penalty)
	require.Equal(t, uint64(2000), rts.ts.resolvedTs)
}
