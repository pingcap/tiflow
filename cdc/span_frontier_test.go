package cdc

import (
	"fmt"
	"strings"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb-cdc/cdc/util"
)

type spanFrontierSuite struct{}

var _ = check.Suite(&spanFrontierSuite{})

func (s *spanFrontier) testStr() string {
	var buf strings.Builder
	s.Entries(func(sp util.Span, ts uint64) {
		if buf.Len() != 0 {
			buf.WriteString(` `)
		}
		fmt.Fprintf(&buf, `%s@%d`, sp, ts)
	})

	return buf.String()
}

func (s *spanFrontierSuite) TestSpanFrontier(c *check.C) {
	keyA := []byte("a")
	keyB := []byte("b")
	keyC := []byte("c")
	keyD := []byte("d")

	spAB := util.Span{Start: keyA, End: keyB}
	spAC := util.Span{Start: keyA, End: keyC}
	spAD := util.Span{Start: keyA, End: keyD}
	spBC := util.Span{Start: keyB, End: keyC}
	spBD := util.Span{Start: keyB, End: keyD}
	spCD := util.Span{Start: keyC, End: keyD}

	f := makeSpanFrontier(spAD)

	c.Assert(f.Frontier(), check.Equals, uint64(0))
	c.Assert(f.testStr(), check.Equals, `{a d}@0`)

	// Untracked spans are ignored
	adv := f.Forward(
		util.Span{Start: []byte("d"), End: []byte("e")},
		100,
	)
	c.Assert(adv, check.IsFalse)
	c.Assert(f.Frontier(), check.Equals, uint64(0))
	c.Assert(f.testStr(), check.Equals, `{a d}@0`)

	// Forward the tracked span space.
	adv = f.Forward(
		util.Span{Start: []byte("a"), End: []byte("d")},
		1,
	)
	c.Assert(adv, check.IsTrue)
	c.Assert(f.Frontier(), check.Equals, uint64(1))
	c.Assert(f.testStr(), check.Equals, `{a d}@1`)

	// Forward it again
	adv = f.Forward(
		util.Span{Start: []byte("a"), End: []byte("d")},
		2,
	)
	c.Assert(adv, check.IsTrue)
	c.Assert(f.Frontier(), check.Equals, uint64(2))
	c.Assert(f.testStr(), check.Equals, `{a d}@2`)

	// Forward to old ts is ignored.
	adv = f.Forward(
		util.Span{Start: []byte("a"), End: []byte("d")},
		1,
	)
	c.Assert(adv, check.IsFalse)
	c.Assert(f.Frontier(), check.Equals, uint64(2))
	c.Assert(f.testStr(), check.Equals, `{a d}@2`)

	// Forward b-c
	adv = f.Forward(spBC, 3)
	c.Assert(adv, check.IsFalse)
	c.Assert(f.Frontier(), check.Equals, uint64(2))
	c.Assert(f.testStr(), check.Equals, `{a b}@2 {b c}@3 {c d}@2`)

	// Forward b-c more to be 4
	adv = f.Forward(spBC, 4)
	c.Assert(adv, check.IsFalse)
	c.Assert(f.Frontier(), check.Equals, uint64(2))
	c.Assert(f.testStr(), check.Equals, `{a b}@2 {b c}@4 {c d}@2`)

	// Forward all to at least 3
	adv = f.Forward(spAD, 3)
	c.Assert(adv, check.IsTrue)
	c.Assert(f.Frontier(), check.Equals, uint64(3))
	c.Assert(f.testStr(), check.Equals, `{a b}@3 {b c}@4 {c d}@3`)

	// Forward AB and CD to be 5, keep BC at 4
	adv = f.Forward(spAB, 5)
	c.Assert(adv, check.IsFalse)
	c.Assert(f.Frontier(), check.Equals, uint64(3))
	adv = f.Forward(spCD, 5)
	c.Assert(adv, check.IsTrue)
	c.Assert(f.Frontier(), check.Equals, uint64(4))
	c.Assert(f.testStr(), check.Equals, `{a b}@5 {b c}@4 {c d}@5`)

	// Catch BC to be 5 too
	adv = f.Forward(spBC, 5)
	c.Assert(adv, check.IsTrue)
	c.Assert(f.Frontier(), check.Equals, uint64(5))
	c.Assert(f.testStr(), check.Equals, `{a b}@5 {b c}@5 {c d}@5`)

	// Forward all to be 6
	adv = f.Forward(spAD, 6)
	c.Assert(adv, check.IsTrue)
	c.Assert(f.Frontier(), check.Equals, uint64(6))
	c.Assert(f.testStr(), check.Equals, `{a b}@6 {b c}@6 {c d}@6`)

	// Forward ac to 7
	adv = f.Forward(spAC, 7)
	c.Assert(adv, check.IsFalse)
	c.Assert(f.Frontier(), check.Equals, uint64(6))
	c.Assert(f.testStr(), check.Equals, `{a b}@7 {b c}@7 {c d}@6`)
	// Forward bd to 8
	adv = f.Forward(spBD, 8)
	c.Assert(adv, check.IsTrue)
	c.Assert(f.Frontier(), check.Equals, uint64(7))
	c.Assert(f.testStr(), check.Equals, `{a b}@7 {b c}@8 {c d}@8`)
	// Forward ab to 8
	adv = f.Forward(spAB, 8)
	c.Assert(adv, check.IsTrue)
	c.Assert(f.Frontier(), check.Equals, uint64(8))
	c.Assert(f.testStr(), check.Equals, `{a b}@8 {b c}@8 {c d}@8`)
}

func (s *spanFrontierSuite) TestMinMax(c *check.C) {
	var keyMin []byte = nil
	var keyMax []byte = nil
	var keyMid []byte = []byte("m")

	spMinMid := util.Span{Start: keyMin, End: keyMid}
	spMidMax := util.Span{Start: keyMid, End: keyMax}
	spMinMax := util.Span{Start: keyMin, End: keyMax}

	f := makeSpanFrontier(spMinMax)
	c.Assert(f.Frontier(), check.Equals, uint64(0))
	c.Assert(f.testStr(), check.Equals, "{ \xff\xff\xff\xff\xff}@0")

	adv := f.Forward(spMinMax, 1)
	c.Assert(adv, check.IsTrue)
	c.Assert(f.Frontier(), check.Equals, uint64(1))
	c.Assert(f.testStr(), check.Equals, "{ \xff\xff\xff\xff\xff}@1")

	adv = f.Forward(spMinMid, 2)
	c.Assert(adv, check.IsFalse)
	c.Assert(f.Frontier(), check.Equals, uint64(1))
	c.Assert(f.testStr(), check.Equals, "{ m}@2 {m \xff\xff\xff\xff\xff}@1")

	adv = f.Forward(spMidMax, 2)
	c.Assert(adv, check.IsTrue)
	c.Assert(f.Frontier(), check.Equals, uint64(2))
	c.Assert(f.testStr(), check.Equals, "{ m}@2 {m \xff\xff\xff\xff\xff}@2")
}

func (s *spanFrontierSuite) TestSpanFrontierDisjoinSpans(c *check.C) {
	keyA := []byte("a")
	keyB := []byte("b")
	keyC := []byte("c")
	keyD := []byte("d")
	keyE := []byte("e")

	spAB := util.Span{Start: keyA, End: keyB}
	spAD := util.Span{Start: keyA, End: keyD}
	spAE := util.Span{Start: keyA, End: keyE}
	spDE := util.Span{Start: keyD, End: keyE}
	spCE := util.Span{Start: keyC, End: keyE}

	f := makeSpanFrontier(spAB, spCE)
	c.Assert(f.Frontier(), check.Equals, uint64(0))
	c.Assert(f.testStr(), check.Equals, `{a b}@0 {c e}@0`)

	// Advance the tracked spans
	adv := f.Forward(spAB, 1)
	c.Assert(adv, check.IsFalse)
	c.Assert(f.Frontier(), check.Equals, uint64(0))
	c.Assert(f.testStr(), check.Equals, `{a b}@1 {c e}@0`)
	adv = f.Forward(spCE, 1)
	c.Assert(adv, check.IsTrue)
	c.Assert(f.Frontier(), check.Equals, uint64(1))
	c.Assert(f.testStr(), check.Equals, `{a b}@1 {c e}@1`)

	// Advance d-e split c-e to c-d and d-e
	adv = f.Forward(spDE, 2)
	c.Assert(adv, check.IsFalse)
	c.Assert(f.Frontier(), check.Equals, uint64(1))
	c.Assert(f.testStr(), check.Equals, `{a b}@1 {c d}@1 {d e}@2`)

	// Advance a-d cover a-b and c-d
	adv = f.Forward(spAD, 3)
	c.Assert(adv, check.IsTrue)
	c.Assert(f.Frontier(), check.Equals, uint64(2))
	c.Assert(f.testStr(), check.Equals, `{a b}@3 {c d}@3 {d e}@2`)

	// Advance one cover all 3 span
	adv = f.Forward(spAE, 4)
	c.Assert(adv, check.IsTrue)
	c.Assert(f.Frontier(), check.Equals, uint64(4))
	c.Assert(f.testStr(), check.Equals, `{a b}@4 {c d}@4 {d e}@4`)
}
