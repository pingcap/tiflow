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
	"math/rand"
	"sort"
	"testing"

	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/stretchr/testify/require"
)

func TestSpanFrontier(t *testing.T) {
	t.Parallel()
	keyA := []byte("a")
	keyB := []byte("b")
	keyC := []byte("c")
	keyD := []byte("d")

	spAB := regionspan.ComparableSpan{Start: keyA, End: keyB}
	spAC := regionspan.ComparableSpan{Start: keyA, End: keyC}
	spAD := regionspan.ComparableSpan{Start: keyA, End: keyD}
	spBC := regionspan.ComparableSpan{Start: keyB, End: keyC}
	spBD := regionspan.ComparableSpan{Start: keyB, End: keyD}
	spCD := regionspan.ComparableSpan{Start: keyC, End: keyD}

	f := NewFrontier(5, spAD).(*spanFrontier)

	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[a @ 5] [d @ Max] `, f.String())
	checkFrontier(t, f)

	f.Forward(
		regionspan.ComparableSpan{Start: []byte("d"), End: []byte("e")},
		100,
	)
	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[a @ 5] [d @ 100] [e @ Max] `, f.String())
	checkFrontier(t, f)

	f.Forward(
		regionspan.ComparableSpan{Start: []byte("g"), End: []byte("h")},
		200,
	)
	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[a @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward the tracked span space.
	f.Forward(
		regionspan.ComparableSpan{Start: []byte("a"), End: []byte("d")},
		1,
	)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, `[a @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// // Forward it again
	f.Forward(
		regionspan.ComparableSpan{Start: []byte("a"), End: []byte("d")},
		2,
	)
	require.Equal(t, uint64(2), f.Frontier())
	require.Equal(t, `[a @ 2] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// // Forward to smaller ts
	f.Forward(
		regionspan.ComparableSpan{Start: []byte("a"), End: []byte("d")},
		1,
	)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, `[a @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// // Forward b-c
	f.Forward(spBC, 3)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, `[a @ 1] [b @ 3] [c @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward b-c more to be 4
	f.Forward(spBC, 4)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, `[a @ 1] [b @ 4] [c @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward all to at least 3
	f.Forward(spAD, 3)
	require.Equal(t, uint64(3), f.Frontier())
	require.Equal(t, `[a @ 3] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward AB and CD to be 5, keep BC at 4
	f.Forward(spAB, 5)
	require.Equal(t, uint64(3), f.Frontier())
	require.Equal(t, `[a @ 5] [b @ 3] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	f.Forward(spCD, 5)
	require.Equal(t, uint64(3), f.Frontier())
	require.Equal(t, `[a @ 5] [b @ 3] [c @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Catch BC to be 5 too
	f.Forward(spBC, 5)
	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[a @ 5] [b @ 5] [c @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward all to be 6
	f.Forward(spAD, 6)
	require.Equal(t, uint64(6), f.Frontier())
	require.Equal(t, `[a @ 6] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward ac to 7
	f.Forward(spAC, 7)
	require.Equal(t, uint64(6), f.Frontier())
	require.Equal(t, `[a @ 7] [c @ 6] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward bd to 8
	f.Forward(spBD, 8)
	require.Equal(t, uint64(7), f.Frontier())
	require.Equal(t, `[a @ 7] [b @ 8] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward ab to 8
	f.Forward(spAB, 8)
	require.Equal(t, uint64(8), f.Frontier())
	require.Equal(t, `[a @ 8] [b @ 8] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	f.Forward(regionspan.ComparableSpan{Start: []byte("1"), End: []byte("g")}, 9)
	require.Equal(t, uint64(9), f.Frontier())
	require.Equal(t, `[1 @ 9] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	f.Forward(regionspan.ComparableSpan{Start: []byte("g"), End: []byte("i")}, 10)
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

	spAB := regionspan.ComparableSpan{Start: keyA, End: keyB}
	spBC := regionspan.ComparableSpan{Start: keyB, End: keyC}
	spCD := regionspan.ComparableSpan{Start: keyC, End: keyD}
	spDE := regionspan.ComparableSpan{Start: keyD, End: keyE}

	f := NewFrontier(20, spAB).(*spanFrontier)
	f.Forward(spBC, 20)
	f.Forward(spCD, 10)
	f.Forward(spDE, 20)

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
	f.Forward(spCD, 20)
	require.Equal(t, uint64(20), f.Frontier())
	// the frontier stoes [A, B) and [B, C) but they are not correct exactly
	require.Equal(t, `[a @ 20] [b @ 20] [c @ 20] [d @ 20] [e @ Max] `, f.String())
	checkFrontier(t, f)

	// Bump, here we meet resolved ts fall back, where 10 is less than f.Frontier()
	// But there is no data loss actually.
	// f.Forward(spAC, 10)
}

func TestMinMax(t *testing.T) {
	t.Parallel()
	var keyMin []byte
	var keyMax []byte
	keyMid := []byte("m")

	spMinMid := regionspan.ComparableSpan{Start: keyMin, End: keyMid}
	spMidMax := regionspan.ComparableSpan{Start: keyMid, End: keyMax}
	spMinMax := regionspan.ComparableSpan{Start: keyMin, End: keyMax}

	f := NewFrontier(0, spMinMax)
	require.Equal(t, uint64(0), f.Frontier())
	require.Equal(t, "[ @ 0] [\xff\xff\xff\xff\xff @ Max] ", f.String())
	checkFrontier(t, f)

	f.Forward(spMinMax, 1)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, "[ @ 1] [\xff\xff\xff\xff\xff @ Max] ", f.String())
	checkFrontier(t, f)

	f.Forward(spMinMid, 2)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, "[ @ 2] [m @ 1] [\xff\xff\xff\xff\xff @ Max] ", f.String())
	checkFrontier(t, f)

	f.Forward(spMidMax, 2)
	require.Equal(t, uint64(2), f.Frontier())
	require.Equal(t, "[ @ 2] [m @ 2] [\xff\xff\xff\xff\xff @ Max] ", f.String())
	checkFrontier(t, f)

	f.Forward(spMinMax, 3)
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

	spAB := regionspan.ComparableSpan{Start: keyA, End: keyB}
	spAD := regionspan.ComparableSpan{Start: keyA, End: keyD}
	spAE := regionspan.ComparableSpan{Start: keyA, End: keyE}
	spDE := regionspan.ComparableSpan{Start: keyD, End: keyE}
	spCE := regionspan.ComparableSpan{Start: keyC, End: keyE}
	sp12 := regionspan.ComparableSpan{Start: key1, End: key2}
	sp1F := regionspan.ComparableSpan{Start: key1, End: keyF}

	f := NewFrontier(0, spAB, spCE)
	require.Equal(t, uint64(0), f.Frontier())
	require.Equal(t, `[a @ 0] [b @ Max] [c @ 0] [e @ Max] `, f.String())
	checkFrontier(t, f)

	// Advance the tracked spans
	f.Forward(spAB, 1)
	require.Equal(t, uint64(0), f.Frontier())
	require.Equal(t, `[a @ 1] [b @ Max] [c @ 0] [e @ Max] `, f.String())
	checkFrontier(t, f)
	f.Forward(spCE, 1)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, `[a @ 1] [b @ Max] [c @ 1] [e @ Max] `, f.String())
	checkFrontier(t, f)

	// Advance d-e split c-e to c-d and d-e
	f.Forward(spDE, 2)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, `[a @ 1] [b @ Max] [c @ 1] [d @ 2] [e @ Max] `, f.String())
	checkFrontier(t, f)

	// Advance a-d cover a-b and c-d
	f.Forward(spAD, 3)
	require.Equal(t, uint64(2), f.Frontier())
	require.Equal(t, `[a @ 3] [d @ 2] [e @ Max] `, f.String())
	checkFrontier(t, f)

	// Advance one cover all 3 span
	f.Forward(spAE, 4)
	require.Equal(t, uint64(4), f.Frontier())
	require.Equal(t, `[a @ 4] [e @ Max] `, f.String())
	checkFrontier(t, f)

	// Advance all with a larger span
	f.Forward(sp1F, 5)
	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[1 @ 5] [f @ Max] `, f.String())
	checkFrontier(t, f)

	// Advance span smaller than all tracked spans
	f.Forward(sp12, 6)
	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[1 @ 6] [2 @ 5] [f @ Max] `, f.String())
	checkFrontier(t, f)
}

func TestSpanFrontierRandomly(t *testing.T) {
	t.Parallel()
	var keyMin []byte
	var keyMax []byte
	spMinMax := regionspan.ComparableSpan{Start: keyMin, End: keyMax}
	f := NewFrontier(0, spMinMax)

	var spans []regionspan.ComparableSpan
	for len(spans) < 500000 {
		span := regionspan.ComparableSpan{
			Start: make([]byte, rand.Intn(32)+1),
			End:   make([]byte, rand.Intn(32)+1),
		}
		rand.Read(span.Start)
		rand.Read(span.End)
		cmp := bytes.Compare(span.Start, span.End)
		if cmp == 0 {
			continue
		} else if cmp > 0 {
			span.Start, span.End = span.End, span.Start
		}

		spans = append(spans, span)

		ts := rand.Uint64()

		f.Forward(span, ts)
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
