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

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/regionspan"
)

type spanFrontierSuite struct{}

func Test(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&spanFrontierSuite{})

func (s *spanFrontierSuite) TestSpanFrontier(c *check.C) {
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

	c.Assert(f.Frontier(), check.Equals, uint64(5))
	c.Assert(f.String(), check.Equals, `[a @ 5] [d @ Max] `)
	checkFrontier(c, f)

	f.Forward(
		regionspan.ComparableSpan{Start: []byte("d"), End: []byte("e")},
		100,
	)
	c.Assert(f.Frontier(), check.Equals, uint64(5))
	c.Assert(f.String(), check.Equals, `[a @ 5] [d @ 100] [e @ Max] `)
	checkFrontier(c, f)

	f.Forward(
		regionspan.ComparableSpan{Start: []byte("g"), End: []byte("h")},
		200,
	)
	c.Assert(f.Frontier(), check.Equals, uint64(5))
	c.Assert(f.String(), check.Equals, `[a @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	// Forward the tracked span space.
	f.Forward(
		regionspan.ComparableSpan{Start: []byte("a"), End: []byte("d")},
		1,
	)
	c.Assert(f.Frontier(), check.Equals, uint64(1))
	c.Assert(f.String(), check.Equals, `[a @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	// // Forward it again
	f.Forward(
		regionspan.ComparableSpan{Start: []byte("a"), End: []byte("d")},
		2,
	)
	c.Assert(f.Frontier(), check.Equals, uint64(2))
	c.Assert(f.String(), check.Equals, `[a @ 2] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	// // Forward to smaller ts
	f.Forward(
		regionspan.ComparableSpan{Start: []byte("a"), End: []byte("d")},
		1,
	)
	c.Assert(f.Frontier(), check.Equals, uint64(1))
	c.Assert(f.String(), check.Equals, `[a @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	// // Forward b-c
	f.Forward(spBC, 3)
	c.Assert(f.Frontier(), check.Equals, uint64(1))
	c.Assert(f.String(), check.Equals, `[a @ 1] [b @ 3] [c @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	// Forward b-c more to be 4
	f.Forward(spBC, 4)
	c.Assert(f.Frontier(), check.Equals, uint64(1))
	c.Assert(f.String(), check.Equals, `[a @ 1] [b @ 4] [c @ 1] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	// Forward all to at least 3
	f.Forward(spAD, 3)
	c.Assert(f.Frontier(), check.Equals, uint64(3))
	c.Assert(f.String(), check.Equals, `[a @ 3] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	// Forward AB and CD to be 5, keep BC at 4
	f.Forward(spAB, 5)
	c.Assert(f.Frontier(), check.Equals, uint64(3))
	c.Assert(f.String(), check.Equals, `[a @ 5] [b @ 3] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	f.Forward(spCD, 5)
	c.Assert(f.Frontier(), check.Equals, uint64(3))
	c.Assert(f.String(), check.Equals, `[a @ 5] [b @ 3] [c @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	// Catch BC to be 5 too
	f.Forward(spBC, 5)
	c.Assert(f.Frontier(), check.Equals, uint64(5))
	c.Assert(f.String(), check.Equals, `[a @ 5] [b @ 5] [c @ 5] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	// Forward all to be 6
	f.Forward(spAD, 6)
	c.Assert(f.Frontier(), check.Equals, uint64(6))
	c.Assert(f.String(), check.Equals, `[a @ 6] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	// Forward ac to 7
	f.Forward(spAC, 7)
	c.Assert(f.Frontier(), check.Equals, uint64(6))
	c.Assert(f.String(), check.Equals, `[a @ 7] [c @ 6] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	// Forward bd to 8
	f.Forward(spBD, 8)
	c.Assert(f.Frontier(), check.Equals, uint64(7))
	c.Assert(f.String(), check.Equals, `[a @ 7] [b @ 8] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	// Forward ab to 8
	f.Forward(spAB, 8)
	c.Assert(f.Frontier(), check.Equals, uint64(8))
	c.Assert(f.String(), check.Equals, `[a @ 8] [b @ 8] [d @ 100] [e @ Max] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	f.Forward(regionspan.ComparableSpan{Start: []byte("1"), End: []byte("g")}, 9)
	c.Assert(f.Frontier(), check.Equals, uint64(9))
	c.Assert(f.String(), check.Equals, `[1 @ 9] [g @ 200] [h @ Max] `)
	checkFrontier(c, f)

	f.Forward(regionspan.ComparableSpan{Start: []byte("g"), End: []byte("i")}, 10)
	c.Assert(f.Frontier(), check.Equals, uint64(9))
	c.Assert(f.String(), check.Equals, `[1 @ 9] [g @ 10] [i @ Max] `)
	checkFrontier(c, f)

}

func (s *spanFrontierSuite) TestMinMax(c *check.C) {
	var keyMin []byte
	var keyMax []byte
	var keyMid = []byte("m")

	spMinMid := regionspan.ComparableSpan{Start: keyMin, End: keyMid}
	spMidMax := regionspan.ComparableSpan{Start: keyMid, End: keyMax}
	spMinMax := regionspan.ComparableSpan{Start: keyMin, End: keyMax}

	f := NewFrontier(0, spMinMax)
	c.Assert(f.Frontier(), check.Equals, uint64(0))
	c.Assert(f.String(), check.Equals, "[ @ 0] [\xff\xff\xff\xff\xff @ Max] ")
	checkFrontier(c, f)

	f.Forward(spMinMax, 1)
	c.Assert(f.Frontier(), check.Equals, uint64(1))
	c.Assert(f.String(), check.Equals, "[ @ 1] [\xff\xff\xff\xff\xff @ Max] ")
	checkFrontier(c, f)

	f.Forward(spMinMid, 2)
	c.Assert(f.Frontier(), check.Equals, uint64(1))
	c.Assert(f.String(), check.Equals, "[ @ 2] [m @ 1] [\xff\xff\xff\xff\xff @ Max] ")
	checkFrontier(c, f)

	f.Forward(spMidMax, 2)
	c.Assert(f.Frontier(), check.Equals, uint64(2))
	c.Assert(f.String(), check.Equals, "[ @ 2] [m @ 2] [\xff\xff\xff\xff\xff @ Max] ")
	checkFrontier(c, f)

	f.Forward(spMinMax, 3)
	c.Assert(f.Frontier(), check.Equals, uint64(3))
	c.Assert(f.String(), check.Equals, "[ @ 3] [\xff\xff\xff\xff\xff @ Max] ")
	checkFrontier(c, f)
}

func (s *spanFrontierSuite) TestSpanFrontierDisjoinSpans(c *check.C) {
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
	c.Assert(f.Frontier(), check.Equals, uint64(0))
	c.Assert(f.String(), check.Equals, `[a @ 0] [b @ Max] [c @ 0] [e @ Max] `)
	checkFrontier(c, f)

	// Advance the tracked spans
	f.Forward(spAB, 1)
	c.Assert(f.Frontier(), check.Equals, uint64(0))
	c.Assert(f.String(), check.Equals, `[a @ 1] [b @ Max] [c @ 0] [e @ Max] `)
	checkFrontier(c, f)
	f.Forward(spCE, 1)
	c.Assert(f.Frontier(), check.Equals, uint64(1))
	c.Assert(f.String(), check.Equals, `[a @ 1] [b @ Max] [c @ 1] [e @ Max] `)
	checkFrontier(c, f)

	// Advance d-e split c-e to c-d and d-e
	f.Forward(spDE, 2)
	c.Assert(f.Frontier(), check.Equals, uint64(1))
	c.Assert(f.String(), check.Equals, `[a @ 1] [b @ Max] [c @ 1] [d @ 2] [e @ Max] `)
	checkFrontier(c, f)

	// Advance a-d cover a-b and c-d
	f.Forward(spAD, 3)
	c.Assert(f.Frontier(), check.Equals, uint64(2))
	c.Assert(f.String(), check.Equals, `[a @ 3] [d @ 2] [e @ Max] `)
	checkFrontier(c, f)

	// Advance one cover all 3 span
	f.Forward(spAE, 4)
	c.Assert(f.Frontier(), check.Equals, uint64(4))
	c.Assert(f.String(), check.Equals, `[a @ 4] [e @ Max] `)
	checkFrontier(c, f)

	// Advance all with a larger span
	f.Forward(sp1F, 5)
	c.Assert(f.Frontier(), check.Equals, uint64(5))
	c.Assert(f.String(), check.Equals, `[1 @ 5] [f @ Max] `)
	checkFrontier(c, f)

	// Advance span smaller than all tracked spans
	f.Forward(sp12, 6)
	c.Assert(f.Frontier(), check.Equals, uint64(5))
	c.Assert(f.String(), check.Equals, `[1 @ 6] [2 @ 5] [f @ Max] `)
	checkFrontier(c, f)
}

func (s *spanFrontierSuite) TestSpanFrontierRandomly(c *check.C) {
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
		checkFrontier(c, f)
	}
}

func checkFrontier(c *check.C, f Frontier) {
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
	c.Assert(len(tsInList), check.Equals, len(tsInHeap))
	sort.Slice(tsInList, func(i, j int) bool { return tsInList[i] < tsInList[j] })
	sort.Slice(tsInHeap, func(i, j int) bool { return tsInHeap[i] < tsInHeap[j] })
	c.Assert(tsInList, check.DeepEquals, tsInHeap)
	c.Assert(f.Frontier(), check.Equals, tsInList[0])
}
