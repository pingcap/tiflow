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
	"github.com/prometheus/client_golang/prometheus"
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

	f := NewFrontier(5, c, spAD).(*spanFrontier)

	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[a @ 5] [d @ Max] `, f.String())
	checkFrontier(t, f)

	f.Forward(
		0, regionspan.ComparableSpan{Start: []byte("d"), End: []byte("e")},
		100,
	)
	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[a @ 5] [d @ 100] [e @ Max] `, f.String())
	checkFrontier(t, f)

	f.Forward(
		0, regionspan.ComparableSpan{Start: []byte("g"), End: []byte("h")},
		200,
	)
	require.Equal(t, uint64(5), f.Frontier())
	require.Equal(t, `[a @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// Forward the tracked span space.
	f.Forward(
		0, regionspan.ComparableSpan{Start: []byte("a"), End: []byte("d")},
		1,
	)
	require.Equal(t, uint64(1), f.Frontier())
	require.Equal(t, `[a @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// // Forward it again
	f.Forward(
		0, regionspan.ComparableSpan{Start: []byte("a"), End: []byte("d")},
		2,
	)
	require.Equal(t, uint64(2), f.Frontier())
	require.Equal(t, `[a @ 2] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	// // Forward to smaller ts
	f.Forward(
		0, regionspan.ComparableSpan{Start: []byte("a"), End: []byte("d")},
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

	f.Forward(0, regionspan.ComparableSpan{Start: []byte("1"), End: []byte("g")}, 9)
	require.Equal(t, uint64(9), f.Frontier())
	require.Equal(t, `[1 @ 9] [g @ 200] [h @ Max] `, f.String())
	checkFrontier(t, f)

	f.Forward(0, regionspan.ComparableSpan{Start: []byte("g"), End: []byte("i")}, 10)
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

	f := NewFrontier(20, c, spAB).(*spanFrontier)
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

func TestMinMax(t *testing.T) {
	t.Parallel()
	var keyMin []byte
	var keyMax []byte
	keyMid := []byte("m")

	spMinMid := regionspan.ComparableSpan{Start: keyMin, End: keyMid}
	spMinMid = spMinMid.Hack()
	spMidMax := regionspan.ComparableSpan{Start: keyMid, End: keyMax}
	spMidMax = spMidMax.Hack()
	spMinMax := regionspan.ComparableSpan{Start: keyMin, End: keyMax}
	spMinMax = spMinMax.Hack()

	f := NewFrontier(0, c, spMinMax)
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

	spAB := regionspan.ComparableSpan{Start: keyA, End: keyB}
	spAD := regionspan.ComparableSpan{Start: keyA, End: keyD}
	spAE := regionspan.ComparableSpan{Start: keyA, End: keyE}
	spDE := regionspan.ComparableSpan{Start: keyD, End: keyE}
	spCE := regionspan.ComparableSpan{Start: keyC, End: keyE}
	sp12 := regionspan.ComparableSpan{Start: key1, End: key2}
	sp1F := regionspan.ComparableSpan{Start: key1, End: keyF}

	f := NewFrontier(0, c, spAB, spCE)
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

var c = prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"type"}).WithLabelValues("a")

func TestSpanFrontierRandomly(t *testing.T) {
	t.Parallel()
	var keyMin []byte
	var keyMax []byte
	spMinMax := regionspan.ComparableSpan{Start: keyMin, End: keyMax}
	f := NewFrontier(0, c, spMinMax)

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

	ab := regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")}
	bc := regionspan.ComparableSpan{Start: []byte("b"), End: []byte("c")}
	cd := regionspan.ComparableSpan{Start: []byte("c"), End: []byte("d")}
	de := regionspan.ComparableSpan{Start: []byte("d"), End: []byte("e")}
	ef := regionspan.ComparableSpan{Start: []byte("e"), End: []byte("f")}
	af := regionspan.ComparableSpan{Start: []byte("a"), End: []byte("f")}

	f := NewFrontier(0, c, af)
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
	f.Forward(6, regionspan.ComparableSpan{Start: []byte("a"), End: []byte("d")}, 6)
	require.Equal(t, uint64(1), f.Frontier())
	f.Forward(7, regionspan.ComparableSpan{Start: []byte("d"), End: []byte("f")}, 2)
	require.Equal(t, uint64(2), f.Frontier())
	f.Forward(7, regionspan.ComparableSpan{Start: []byte("d"), End: []byte("f")}, 3)
	require.Equal(t, uint64(3), f.Frontier())
	f.Forward(7, regionspan.ComparableSpan{Start: []byte("d"), End: []byte("f")}, 4)
	require.Equal(t, uint64(4), f.Frontier())
	f.Forward(8, regionspan.ComparableSpan{Start: []byte("d"), End: []byte("e")}, 4)
	require.Equal(t, uint64(4), f.Frontier())
	f.Forward(9, regionspan.ComparableSpan{Start: []byte("e"), End: []byte("f")}, 4)
	require.Equal(t, uint64(4), f.Frontier())
	f.Forward(9, regionspan.ComparableSpan{Start: []byte("e"), End: []byte("f")}, 7)
	require.Equal(t, uint64(4), f.Frontier())
	f.Forward(8, regionspan.ComparableSpan{Start: []byte("d"), End: []byte("e")}, 5)
	require.Equal(t, uint64(5), f.Frontier())
}
