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

package frontier

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/pingcap/tiflow/cdc/kv/regionlock"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func TestSpanFrontier(t *testing.T) {
	t.Parallel()
	keyA := []byte("a")
	keyB := []byte("b")
	keyC := []byte("c")
	keyD := []byte("d")

	spAB := tablepb.Span{StartKey: keyA, EndKey: keyB}
	spAC := tablepb.Span{StartKey: keyA, EndKey: keyC}
	spAD := tablepb.Span{StartKey: keyA, EndKey: keyD}
	spBC := tablepb.Span{StartKey: keyB, EndKey: keyC}
	spBD := tablepb.Span{StartKey: keyB, EndKey: keyD}
	spCD := tablepb.Span{StartKey: keyC, EndKey: keyD}

	f := NewFrontier(5, spAD).(*spanFrontier)

	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[a @ 5] [d @ Max] `, f.String())
	checkFrontier(t, f)

	f.Forward(
		0, tablepb.Span{StartKey: []byte("d"), EndKey: []byte("e")},
		100,
	)
	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[a @ 5] [d @ 100] [e @ Max] `, f.String())
	checkFrontier(t, f)

	f.Forward(
		0, tablepb.Span{StartKey: []byte("g"), EndKey: []byte("h")},
		200,
	)
	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[a @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward the tracked span space.
	f.Forward(
		0, tablepb.Span{StartKey: []byte("a"), EndKey: []byte("d")},
		1,
	)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, `[a @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// // Forward it again
	f.Forward(
		0, tablepb.Span{StartKey: []byte("a"), EndKey: []byte("d")},
		2,
	)
	require.Equal(t, uint64(2), f.Frontier())
	require.Equal(t, `[a @ 2] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// // Forward to smaller ts
	f.Forward(
		0, tablepb.Span{StartKey: []byte("a"), EndKey: []byte("d")},
		1,
	)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, `[a @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// // Forward b-c
	f.Forward(0, spBC, 3)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, `[a @ 1] [b @ 3] [c @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward b-c more to be 4
	f.Forward(0, spBC, 4)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, `[a @ 1] [b @ 4] [c @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward all to at least 3
	f.Forward(0, spAD, 3)
	require.Equal(t, uint64(3), f.Frontier())
	require.Equal(t, `[a @ 3] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward AB and CD to be 5, keep BC at 4
	f.Forward(0, spAB, 5)
	require.Equal(t, uint64(3), f.Frontier())
	require.Equal(t, `[a @ 5] [b @ 3] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	f.Forward(0, spCD, 5)
	require.Equal(t, uint64(3), f.Frontier())
	require.Equal(t, `[a @ 5] [b @ 3] [c @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Catch BC to be 5 too
	f.Forward(0, spBC, 5)
	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[a @ 5] [b @ 5] [c @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward all to be 6
	f.Forward(0, spAD, 6)
	require.Equal(t, uint64(6), f.Frontier())
	require.Equal(t, `[a @ 6] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward ac to 7
	f.Forward(0, spAC, 7)
	require.Equal(t, uint64(6), f.Frontier())
	require.Equal(t, `[a @ 7] [c @ 6] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward bd to 8
	f.Forward(0, spBD, 8)
	require.Equal(t, uint64(7), f.Frontier())
	require.Equal(t, `[a @ 7] [b @ 8] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward ab to 8
	f.Forward(0, spAB, 8)
	require.Equal(t, uint64(8), f.Frontier())
	require.Equal(t, `[a @ 8] [b @ 8] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	f.Forward(0, tablepb.Span{StartKey: []byte("1"), EndKey: []byte("g")}, 9)
	require.Equal(t, uint64(9), f.Frontier())
	require.Equal(t, `[1 @ 9] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	f.Forward(0, tablepb.Span{StartKey: []byte("g"), EndKey: []byte("i")}, 10)
	require.Equal(t, uint64(9), f.Frontier())
	require.Equal(t, `[1 @ 9] [g @ 10] [i @ Max] `, f.String())
	checkFrontier(t, f)
}

func TestSpanFrontierFallback(t *testing.T) {
	t.Parallel()
	keyA := []byte("a")
	keyB := []byte("b")
	keyC := []byte("c")
	keyD := []byte("d")
	keyE := []byte("e")

	spAB := tablepb.Span{StartKey: keyA, EndKey: keyB}
	spBC := tablepb.Span{StartKey: keyB, EndKey: keyC}
	spCD := tablepb.Span{StartKey: keyC, EndKey: keyD}
	spDE := tablepb.Span{StartKey: keyD, EndKey: keyE}

	f := NewFrontier(20, spAB).(*spanFrontier)
	f.Forward(0, spBC, 20)
	f.Forward(0, spCD, 10)
	f.Forward(0, spDE, 20)

	// [A, B) [B, C) [C, D) [D, E)
	// 20     20     10     20
	require.Equal(t, uint64(10), f.Frontier())
	require.Equal(t, `[a @ 20] [b @ 20] [c @ 10] [d @ 20] [e @ Max] `, f.String())
	checkFrontier(t, f)

	// [A, B) [B, D) [D, E)
	// 20     10     10
	// [B, D) does not forward, because of split to [B, C) and [C, D) immediately

	// [A, B) [B, C) [C, D) [D, E)
	// 20     10     10     20
	// [B, C) does not forward, because of merge into [A, C) immediately
	f.Forward(0, spCD, 20)
	require.Equal(t, uint64(20), f.Frontier())
	// the frontier stoes [A, B) and [B, C) but they are not correct exactly
	require.Equal(t, `[a @ 20] [b @ 20] [c @ 20] [d @ 20] [e @ Max] `, f.String())
	checkFrontier(t, f)

	// Bump, here we meet resolved ts fall back, where 10 is less than f.Frontier()
	// But there is no data loss actually.
	// f.Forward(spAC, 10)
}

func TestSpanString(t *testing.T) {
	t.Parallel()

	spAB := tablepb.Span{StartKey: []byte("a"), EndKey: []byte("b")}
	spBC := tablepb.Span{StartKey: []byte("b"), EndKey: []byte("c")}
	spCD := tablepb.Span{StartKey: []byte("c"), EndKey: []byte("d")}
	spDE := tablepb.Span{StartKey: []byte("d"), EndKey: []byte("e")}
	spEF := tablepb.Span{StartKey: []byte("e"), EndKey: []byte("f")}
	spFG := tablepb.Span{StartKey: []byte("f"), EndKey: []byte("g")}
	spGH := tablepb.Span{StartKey: []byte("g"), EndKey: []byte("h")}

	spAH := tablepb.Span{StartKey: []byte("a"), EndKey: []byte("h")}
	f := NewFrontier(1, spAH).(*spanFrontier)
	require.Equal(t, `[0:a @ 1] [0:h @ Max] `, f.SpanString(spAH))

	f.Forward(1, spAB, 2)
	f.Forward(2, spBC, 5)
	f.Forward(3, spCD, 10)
	f.Forward(4, spDE, 20)
	f.Forward(5, spEF, 30)
	f.Forward(6, spFG, 25)
	f.Forward(7, spGH, 35)
	require.Equal(t, uint64(2), f.Frontier())
	require.Equal(t, `[1:a @ 2] [2:b @ 5] [3:c @ 10] [4:d @ 20] [5:e @ 30] [6:f @ 25] [7:g @ 35] [0:h @ Max] `, f.stringWtihRegionID())
	// Print 5 span: start, before, target span, next, end
	require.Equal(t, `[1:a @ 2] [3:c @ 10] [4:d @ 20] [5:e @ 30] [0:h @ Max] `, f.SpanString(spDE))

	spBH := tablepb.Span{StartKey: []byte("b"), EndKey: []byte("h")}
	f.Forward(8, spBH, 18)
	require.Equal(t, uint64(2), f.Frontier())
	require.Equal(t, `[1:a @ 2] [8:b @ 18] [0:h @ Max] `, f.stringWtihRegionID())
}

func TestMinMax(t *testing.T) {
	t.Parallel()
	var keyMin []byte
	var keyMax []byte
	keyMid := []byte("m")

	spMinMid := tablepb.Span{StartKey: keyMin, EndKey: keyMid}
	spMinMid = spanz.HackSpan(spMinMid)
	spMidMax := tablepb.Span{StartKey: keyMid, EndKey: keyMax}
	spMidMax = spanz.HackSpan(spMidMax)
	spMinMax := tablepb.Span{StartKey: keyMin, EndKey: keyMax}
	spMinMax = spanz.HackSpan(spMinMax)

	f := NewFrontier(0, spMinMax)
	require.Equal(t, uint64(0), f.Frontier())
	require.Equal(t, "[ @ 0] [\xff\xff\xff\xff\xff @ Max] ", f.String())
	checkFrontier(t, f)

	f.Forward(0, spMinMax, 1)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, "[ @ 1] [\xff\xff\xff\xff\xff @ Max] ", f.String())
	checkFrontier(t, f)

	f.Forward(0, spMinMid, 2)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, "[ @ 2] [m @ 1] [\xff\xff\xff\xff\xff @ Max] ", f.String())
	checkFrontier(t, f)

	f.Forward(0, spMidMax, 2)
	require.Equal(t, uint64(2), f.Frontier())
	require.Equal(t, "[ @ 2] [m @ 2] [\xff\xff\xff\xff\xff @ Max] ", f.String())
	checkFrontier(t, f)

	f.Forward(0, spMinMax, 3)
	require.Equal(t, uint64(3), f.Frontier())
	require.Equal(t, "[ @ 3] [\xff\xff\xff\xff\xff @ Max] ", f.String())
	checkFrontier(t, f)
}

func TestSpanFrontierDisjoinSpans(t *testing.T) {
	t.Parallel()
	key1 := []byte("1")
	key2 := []byte("2")
	keyA := []byte("a")
	keyB := []byte("b")
	keyC := []byte("c")
	keyD := []byte("d")
	keyE := []byte("e")
	keyF := []byte("f")

	spAB := tablepb.Span{StartKey: keyA, EndKey: keyB}
	spAD := tablepb.Span{StartKey: keyA, EndKey: keyD}
	spAE := tablepb.Span{StartKey: keyA, EndKey: keyE}
	spDE := tablepb.Span{StartKey: keyD, EndKey: keyE}
	spCE := tablepb.Span{StartKey: keyC, EndKey: keyE}
	sp12 := tablepb.Span{StartKey: key1, EndKey: key2}
	sp1F := tablepb.Span{StartKey: key1, EndKey: keyF}

	f := NewFrontier(0, spAB, spCE)
	require.Equal(t, uint64(0), f.Frontier())
	require.Equal(t, `[a @ 0] [b @ Max] [c @ 0] [e @ Max] `, f.String())
	checkFrontier(t, f)

	// Advance the tracked spans
	f.Forward(0, spAB, 1)
	require.Equal(t, uint64(0), f.Frontier())
	require.Equal(t, `[a @ 1] [b @ Max] [c @ 0] [e @ Max] `, f.String())
	checkFrontier(t, f)
	f.Forward(0, spCE, 1)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, `[a @ 1] [b @ Max] [c @ 1] [e @ Max] `, f.String())
	checkFrontier(t, f)

	// Advance d-e split c-e to c-d and d-e
	f.Forward(0, spDE, 2)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, `[a @ 1] [b @ Max] [c @ 1] [d @ 2] [e @ Max] `, f.String())
	checkFrontier(t, f)

	// Advance a-d cover a-b and c-d
	f.Forward(0, spAD, 3)
	require.Equal(t, uint64(2), f.Frontier())
	require.Equal(t, `[a @ 3] [d @ 2] [e @ Max] `, f.String())
	checkFrontier(t, f)

	// Advance one cover all 3 span
	f.Forward(0, spAE, 4)
	require.Equal(t, uint64(4), f.Frontier())
	require.Equal(t, `[a @ 4] [e @ Max] `, f.String())
	checkFrontier(t, f)

	// Advance all with a larger span
	f.Forward(0, sp1F, 5)
	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[1 @ 5] [f @ Max] `, f.String())
	checkFrontier(t, f)

	// Advance span smaller than all tracked spans
	f.Forward(0, sp12, 6)
	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[1 @ 6] [2 @ 5] [f @ Max] `, f.String())
	checkFrontier(t, f)
}

func TestSpanFrontierRandomly(t *testing.T) {
	t.Parallel()
	var keyMin []byte
	var keyMax []byte
	spMinMax := tablepb.Span{StartKey: keyMin, EndKey: keyMax}
	f := NewFrontier(0, spMinMax)

	var spans []tablepb.Span
	for len(spans) < 500000 {
		span := tablepb.Span{
			StartKey: make([]byte, rand.Intn(32)+1),
			EndKey:   make([]byte, rand.Intn(32)+1),
		}
		rand.Read(span.StartKey)
		rand.Read(span.EndKey)
		cmp := bytes.Compare(span.StartKey, span.EndKey)
		if cmp == 0 {
			continue
		} else if cmp > 0 {
			span.StartKey, span.EndKey = span.EndKey, span.StartKey
		}

		spans = append(spans, span)

		ts := rand.Uint64()

		f.Forward(0, span, ts)
		checkFrontier(t, f)
	}
}

func checkFrontier(t *testing.T, f Frontier) {
	sf := f.(*spanFrontier)
	var tsInList, tsInHeap []uint64
	sf.spanList.Entries(func(n *skipListNode) bool {
		tsInList = append(tsInList, n.Value().key)
		return true
	})
	sf.minTsHeap.Entries(func(n *fibonacciHeapNode) bool {
		tsInHeap = append(tsInHeap, n.key)
		return true
	})
	require.Equal(t, len(tsInHeap), len(tsInList))
	sort.Slice(tsInList, func(i, j int) bool { return tsInList[i] < tsInList[j] })
	sort.Slice(tsInHeap, func(i, j int) bool { return tsInHeap[i] < tsInHeap[j] })
	require.Equal(t, tsInHeap, tsInList)
	require.Equal(t, tsInList[0], f.Frontier())
}

func TestMinMaxWithRegionSplitMerge(t *testing.T) {
	t.Parallel()

	ab := tablepb.Span{StartKey: []byte("a"), EndKey: []byte("b")}
	bc := tablepb.Span{StartKey: []byte("b"), EndKey: []byte("c")}
	cd := tablepb.Span{StartKey: []byte("c"), EndKey: []byte("d")}
	de := tablepb.Span{StartKey: []byte("d"), EndKey: []byte("e")}
	ef := tablepb.Span{StartKey: []byte("e"), EndKey: []byte("f")}
	af := tablepb.Span{StartKey: []byte("a"), EndKey: []byte("f")}

	f := NewFrontier(0, af)
	require.Equal(t, uint64(0), f.Frontier())
	f.Forward(1, ab, 1)
	require.Equal(t, uint64(0), f.Frontier())
	f.Forward(2, bc, 1)
	require.Equal(t, uint64(0), f.Frontier())
	f.Forward(3, cd, 1)
	require.Equal(t, uint64(0), f.Frontier())
	f.Forward(4, de, 1)
	require.Equal(t, uint64(0), f.Frontier())
	f.Forward(5, ef, 1)
	require.Equal(t, uint64(1), f.Frontier())
	f.Forward(6, tablepb.Span{StartKey: []byte("a"), EndKey: []byte("d")}, 6)
	require.Equal(t, uint64(1), f.Frontier())
	f.Forward(7, tablepb.Span{StartKey: []byte("d"), EndKey: []byte("f")}, 2)
	require.Equal(t, uint64(2), f.Frontier())
	f.Forward(7, tablepb.Span{StartKey: []byte("d"), EndKey: []byte("f")}, 3)
	require.Equal(t, uint64(3), f.Frontier())
	f.Forward(7, tablepb.Span{StartKey: []byte("d"), EndKey: []byte("f")}, 4)
	require.Equal(t, uint64(4), f.Frontier())
	f.Forward(8, tablepb.Span{StartKey: []byte("d"), EndKey: []byte("e")}, 4)
	require.Equal(t, uint64(4), f.Frontier())
	f.Forward(9, tablepb.Span{StartKey: []byte("e"), EndKey: []byte("f")}, 4)
	require.Equal(t, uint64(4), f.Frontier())
	f.Forward(9, tablepb.Span{StartKey: []byte("e"), EndKey: []byte("f")}, 7)
	require.Equal(t, uint64(4), f.Frontier())
	f.Forward(8, tablepb.Span{StartKey: []byte("d"), EndKey: []byte("e")}, 5)
	require.Equal(t, uint64(5), f.Frontier())
}

func TestFrontierEntries(t *testing.T) {
	t.Parallel()

	ab := tablepb.Span{StartKey: []byte("a"), EndKey: []byte("b")}
	bc := tablepb.Span{StartKey: []byte("b"), EndKey: []byte("c")}
	cd := tablepb.Span{StartKey: []byte("c"), EndKey: []byte("d")}
	de := tablepb.Span{StartKey: []byte("d"), EndKey: []byte("e")}
	ef := tablepb.Span{StartKey: []byte("e"), EndKey: []byte("f")}
	af := tablepb.Span{StartKey: []byte("a"), EndKey: []byte("f")}
	f := NewFrontier(0, af)

	var slowestTs uint64 = math.MaxUint64
	var slowestRange tablepb.Span
	getSlowestRange := func() {
		slowestTs = math.MaxUint64
		slowestRange = tablepb.Span{}
		f.Entries(func(key []byte, ts uint64) {
			if ts < slowestTs {
				slowestTs = ts
				slowestRange.StartKey = key
				slowestRange.EndKey = nil
			} else if slowestTs != math.MaxUint64 && len(slowestRange.EndKey) == 0 {
				slowestRange.EndKey = key
			}
		})
	}

	getSlowestRange()
	require.Equal(t, uint64(0), slowestTs)
	require.Equal(t, []byte("a"), []byte(slowestRange.StartKey))
	require.Equal(t, []byte("f"), []byte(slowestRange.EndKey))

	f.Forward(1, ab, 100)
	f.Forward(2, bc, 200)
	f.Forward(3, cd, 300)
	f.Forward(4, de, 400)
	f.Forward(5, ef, 500)
	getSlowestRange()
	require.Equal(t, uint64(100), slowestTs)
	require.Equal(t, []byte("a"), []byte(slowestRange.StartKey))
	require.Equal(t, []byte("b"), []byte(slowestRange.EndKey))
}

func TestRandomMergeAndSplit(t *testing.T) {
	t.Parallel()

	start, end := spanz.GetTableRange(8616)
	rangelock := regionlock.NewRegionRangeLock(1, start, end, 100, "")
	frontier := NewFrontier(100, tablepb.Span{StartKey: start, EndKey: end})
	ctx := context.Background()

	var nextRegionID uint64 = 1
	var nextVersion uint64 = 1
	var nextTs uint64 = 100
	rangelock.LockRange(ctx, start, end, nextRegionID, nextVersion)

	nextTs += 1
	frontier.Forward(1, tablepb.Span{StartKey: start, EndKey: end}, nextTs)
	require.Equal(t, nextTs, frontier.Frontier())

	for i := 0; i < 100000; i++ {
		totalLockedRanges := rangelock.LockedRanges()
		unchangedRegions := make([]lockedRegion, 0, totalLockedRanges)

		mergeOrSplit := "split"
		if totalLockedRanges > 1 && rand.Intn(2) > 0 {
			mergeOrSplit = "merge"
		}

		nextTs += 1
		if mergeOrSplit == "split" {
			var r1, r2 lockedRegion
			selected := rand.Intn(totalLockedRanges)
			count := 0
			rangelock.CollectLockedRangeAttrs(func(regionID, version uint64, state *regionlock.LockedRange, span tablepb.Span) {
				ts := state.CheckpointTs.Load()
				startKey := span.StartKey
				endKey := span.EndKey
				if count == selected {
					r1 = lockedRegion{regionID, version, startKey, endKey, ts}
				} else {
					r := lockedRegion{regionID, version, startKey, endKey, ts}
					unchangedRegions = append(unchangedRegions, r)
				}
				count += 1
			})

			rangelock.UnlockRange(r1.startKey, r1.endKey, r1.regionID, r1.version)

			r2 = r1.split(&nextRegionID, &nextVersion)
			rangelock.LockRange(ctx, r1.startKey, r1.endKey, r1.regionID, nextVersion)
			rangelock.LockRange(ctx, r2.startKey, r2.endKey, r2.regionID, nextVersion)

			frontier.Forward(r1.regionID, tablepb.Span{StartKey: r1.startKey, EndKey: r1.endKey}, nextTs)
			frontier.Forward(r2.regionID, tablepb.Span{StartKey: r2.startKey, EndKey: r2.endKey}, nextTs)
		} else {
			var r1, r2 lockedRegion
			selected := rand.Intn(totalLockedRanges - 1)
			count := 0
			rangelock.CollectLockedRangeAttrs(func(regionID, version uint64, state *regionlock.LockedRange, span tablepb.Span) {
				ts := state.CheckpointTs.Load()
				startKey := span.StartKey
				endKey := span.EndKey
				if count == selected {
					r1 = lockedRegion{regionID, version, startKey, endKey, ts}
				} else if count == selected+1 {
					r2 = lockedRegion{regionID, version, startKey, endKey, ts}
				} else {
					r := lockedRegion{regionID, version, startKey, endKey, ts}
					unchangedRegions = append(unchangedRegions, r)
				}
				count += 1
			})

			rangelock.UnlockRange(r1.startKey, r1.endKey, r1.regionID, r1.version)
			rangelock.UnlockRange(r2.startKey, r2.endKey, r2.regionID, r2.version)

			r2.merge(r1, &nextVersion)
			rangelock.LockRange(ctx, r2.startKey, r2.endKey, r2.regionID, nextVersion)

			frontier.Forward(r2.regionID, tablepb.Span{StartKey: r2.startKey, EndKey: r2.endKey}, nextTs)
		}
		for _, r := range unchangedRegions {
			frontier.Forward(r.regionID, tablepb.Span{StartKey: r.startKey, EndKey: r.endKey}, nextTs)
		}
		require.Equal(t, nextTs, frontier.Frontier())
	}
}

type lockedRegion struct {
	regionID uint64
	version  uint64
	startKey []byte
	endKey   []byte
	ts       uint64
}

func (r *lockedRegion) split(regionIDGen *uint64, versionGen *uint64) (s lockedRegion) {
	*regionIDGen += 1
	*versionGen += 1

	s.regionID = *regionIDGen
	s.version = *versionGen
	s.ts = r.ts
	s.startKey = r.startKey

	s.endKey = make([]byte, len(r.startKey)+1)
	copy(s.endKey, r.startKey)
	for {
		s.endKey[len(s.endKey)-1] = '1'
		if bytes.Compare(s.endKey, r.endKey) < 0 {
			break
		}
		s.endKey[len(s.endKey)-1] = '0'
		s.endKey = append(s.endKey, '0')
	}

	r.version = *versionGen
	r.startKey = make([]byte, len(s.endKey))
	copy(r.startKey, s.endKey)
	return
}

func (r *lockedRegion) merge(s lockedRegion, versionGen *uint64) {
	if !bytes.Equal(r.startKey, s.endKey) {
		panic("bad merge")
	}

	*versionGen += 1
	r.startKey = s.startKey
	r.version = *versionGen
}
