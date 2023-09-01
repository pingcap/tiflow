// Copyright 2023 PingCAP, Inc.
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

package keyspan

import (
	"context"
	"encoding/hex"
	"math"
	"strconv"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/stretchr/testify/require"
)

func prepareRegionsInfo(writtenKeys [7]int) ([]pdutil.RegionInfo, map[int][]byte, map[int][]byte) {
	regions := []pdutil.RegionInfo{
		pdutil.NewTestRegionInfo(2, []byte("a"), []byte("b"), uint64(writtenKeys[0])),
		pdutil.NewTestRegionInfo(3, []byte("b"), []byte("c"), uint64(writtenKeys[1])),
		pdutil.NewTestRegionInfo(4, []byte("c"), []byte("d"), uint64(writtenKeys[2])),
		pdutil.NewTestRegionInfo(5, []byte("e"), []byte("f"), uint64(writtenKeys[3])),
		pdutil.NewTestRegionInfo(6, []byte("f"), []byte("fa"), uint64(writtenKeys[4])),
		pdutil.NewTestRegionInfo(7, []byte("fa"), []byte("fc"), uint64(writtenKeys[5])),
		pdutil.NewTestRegionInfo(8, []byte("fc"), []byte("ff"), uint64(writtenKeys[6])),
	}
	startKeys := map[int][]byte{}
	endKeys := map[int][]byte{}
	for _, r := range regions {
		b, _ := hex.DecodeString(r.StartKey)
		startKeys[int(r.ID)] = b
	}
	for _, r := range regions {
		b, _ := hex.DecodeString(r.EndKey)
		endKeys[int(r.ID)] = b
	}
	return regions, startKeys, endKeys
}

func cloneRegions(info []pdutil.RegionInfo) []pdutil.RegionInfo {
	return append([]pdutil.RegionInfo{}, info...)
}

func TestSplitRegionsByWrittenKeysUniform(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	regions, startKeys, endKeys := prepareRegionsInfo(
		[7]int{100, 100, 100, 100, 100, 100, 100})
	splitter := newWriteSplitter(model.ChangeFeedID4Test("test", "test"), nil, 0)
	info := splitter.splitRegionsByWrittenKeys(0, cloneRegions(regions), 1)
	re.Len(info.RegionCounts, 1)
	re.EqualValues(7, info.RegionCounts[0])
	re.Len(info.Spans, 1)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[8], info.Spans[0].EndKey)

	info = splitter.splitRegionsByWrittenKeys(0, cloneRegions(regions), 2) // [2,3,4], [5,6,7,8]
	re.Len(info.RegionCounts, 2)
	re.EqualValues(3, info.RegionCounts[0])
	re.EqualValues(4, info.RegionCounts[1])
	re.Len(info.Weights, 2)
	re.EqualValues(303, info.Weights[0])
	re.EqualValues(404, info.Weights[1])
	re.Len(info.Spans, 2)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[4], info.Spans[0].EndKey)
	re.EqualValues(startKeys[5], info.Spans[1].StartKey)
	re.EqualValues(endKeys[8], info.Spans[1].EndKey)

	info = splitter.splitRegionsByWrittenKeys(0, cloneRegions(regions), 3) // [2,3], [4,5,6], [7,8]
	re.Len(info.RegionCounts, 3)
	re.EqualValues(2, info.RegionCounts[0])
	re.EqualValues(2, info.RegionCounts[1])
	re.EqualValues(3, info.RegionCounts[2])
	re.Len(info.Weights, 3)
	re.EqualValues(202, info.Weights[0])
	re.EqualValues(202, info.Weights[1])
	re.EqualValues(303, info.Weights[2])
	re.Len(info.Spans, 3)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[3], info.Spans[0].EndKey)
	re.EqualValues(startKeys[4], info.Spans[1].StartKey)
	re.EqualValues(endKeys[5], info.Spans[1].EndKey)
	re.EqualValues(startKeys[6], info.Spans[2].StartKey)
	re.EqualValues(endKeys[8], info.Spans[2].EndKey)

	// spans > regions
	for p := 7; p <= 10; p++ {
		info = splitter.splitRegionsByWrittenKeys(0, cloneRegions(regions), p)
		re.Len(info.RegionCounts, 7)
		for _, c := range info.RegionCounts {
			re.EqualValues(1, c)
		}
		re.Len(info.Weights, 7)
		for _, w := range info.Weights {
			re.EqualValues(101, w, info)
		}
		re.Len(info.Spans, 7)
		for i, r := range info.Spans {
			re.EqualValues(startKeys[2+i], r.StartKey)
			re.EqualValues(endKeys[2+i], r.EndKey)
		}
	}
}

func TestSplitRegionsByWrittenKeysHotspot1(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	// Hotspots
	regions, startKeys, endKeys := prepareRegionsInfo(
		[7]int{100, 1, 100, 1, 1, 1, 100})
	splitter := newWriteSplitter(model.ChangeFeedID4Test("test", "test"), nil, 4)
	info := splitter.splitRegionsByWrittenKeys(0, regions, 4) // [2], [3,4], [5,6,7], [8]
	re.Len(info.RegionCounts, 4)
	re.EqualValues(1, info.RegionCounts[0])
	re.EqualValues(1, info.RegionCounts[1])
	re.EqualValues(3, info.RegionCounts[2])
	re.EqualValues(1, info.RegionCounts[3])
	re.Len(info.Weights, 4)
	re.EqualValues(101, info.Weights[0])
	re.EqualValues(2, info.Weights[1])
	re.EqualValues(6, info.Weights[2])
	re.EqualValues(101, info.Weights[3])
	re.Len(info.Spans, 4)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[2], info.Spans[0].EndKey)
	re.EqualValues(startKeys[3], info.Spans[1].StartKey)
	re.EqualValues(endKeys[3], info.Spans[1].EndKey)
	re.EqualValues(startKeys[4], info.Spans[2].StartKey)
	re.EqualValues(endKeys[4], info.Spans[2].EndKey)
	re.EqualValues(startKeys[5], info.Spans[3].StartKey)
	re.EqualValues(endKeys[8], info.Spans[3].EndKey)
}

func TestSplitRegionsByWrittenKeysHotspot2(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	// Hotspots
	regions, startKeys, endKeys := prepareRegionsInfo(
		[7]int{1000, 1, 1, 1, 100, 1, 99})
	splitter := newWriteSplitter(model.ChangeFeedID4Test("test", "test"), nil, 4)
	info := splitter.splitRegionsByWrittenKeys(0, regions, 4)
	re.Len(info.Spans, 4)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[2], info.Spans[0].EndKey)
	re.EqualValues(startKeys[3], info.Spans[1].StartKey)
	re.EqualValues(endKeys[6], info.Spans[1].EndKey)
	re.EqualValues(startKeys[7], info.Spans[2].StartKey)
	re.EqualValues(endKeys[7], info.Spans[2].EndKey)
	re.EqualValues(startKeys[8], info.Spans[3].StartKey)
	re.EqualValues(endKeys[8], info.Spans[3].EndKey)
}

func TestSplitRegionsByWrittenKeysCold(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	splitter := newWriteSplitter(model.ChangeFeedID4Test("test", "test"), nil, 4)
	regions, startKeys, endKeys := prepareRegionsInfo([7]int{})
	info := splitter.splitRegionsByWrittenKeys(0, regions, 3) // [2,3], [4,5], [6,7,8]
	re.Len(info.RegionCounts, 3)
	re.EqualValues(2, info.RegionCounts[0], info)
	re.EqualValues(2, info.RegionCounts[1])
	re.EqualValues(3, info.RegionCounts[2])
	re.Len(info.Weights, 3)
	re.EqualValues(2, info.Weights[0])
	re.EqualValues(2, info.Weights[1])
	re.EqualValues(3, info.Weights[2])
	re.Len(info.Spans, 3)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[3], info.Spans[0].EndKey)
	re.EqualValues(startKeys[4], info.Spans[1].StartKey)
	re.EqualValues(endKeys[5], info.Spans[1].EndKey)
	re.EqualValues(startKeys[6], info.Spans[2].StartKey)
	re.EqualValues(endKeys[8], info.Spans[2].EndKey)
}

func TestSplitRegionsByWrittenKeysConfig(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	splitter := newWriteSplitter(model.ChangeFeedID4Test("test", "test"), nil, math.MaxInt)
	regions, startKeys, endKeys := prepareRegionsInfo([7]int{1, 1, 1, 1, 1, 1, 1})
	info := splitter.splitRegionsByWrittenKeys(1, regions, 3) // [2,3,4,5,6,7,8]
	re.Len(info.RegionCounts, 1)
	re.EqualValues(7, info.RegionCounts[0], info)
	re.Len(info.Weights, 1)
	re.EqualValues(14, info.Weights[0])
	re.Len(info.Spans, 1)
	re.EqualValues(startKeys[2], info.Spans[0].StartKey)
	re.EqualValues(endKeys[8], info.Spans[0].EndKey)
	re.EqualValues(1, info.Spans[0].TableID)

	splitter.writeKeyThreshold = 0
	spans := splitter.split(context.Background(), tablepb.Span{}, 3)
	require.Empty(t, spans)
}

func TestSplitRegionEven(t *testing.T) {
	tblID := model.TableID(1)
	regionCount := 4653 + 1051 + 745 + 9530 + 1
	regions := make([]pdutil.RegionInfo, regionCount)
	for i := 0; i < regionCount; i++ {
		regions[i] = pdutil.RegionInfo{
			ID:          uint64(i),
			StartKey:    "" + strconv.Itoa(i),
			EndKey:      "" + strconv.Itoa(i),
			WrittenKeys: 2,
		}
	}
	splitter := newWriteSplitter(model.ChangeFeedID4Test("test", "test"), nil, 4)
	info := splitter.splitRegionsByWrittenKeys(tblID, regions, 5)
	require.Len(t, info.RegionCounts, 5)
	require.Len(t, info.Weights, 5)
	for i, w := range info.Weights {
		if i == 4 {
			require.Equal(t, 9579, w, i)
		} else {
			require.Equal(t, 9588, w, i)
		}
	}
}
