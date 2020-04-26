package frontier

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/pingcap/ticdc/pkg/util"
)

// Frontier checks resolved event of spans and moves the global resolved ts ahead
type Frontier interface {
	Forward(span util.Span, ts uint64) bool
	Frontier() uint64
}

type node struct {
	start []byte
	end   []byte
	ts    uint64

	// used by list
	nexts []*node

	// used by heap
	left     *node
	right    *node
	children *node
	parent   *node
	rank     int
	marked   bool
}

func (s *node) String() string {
	return fmt.Sprintf("[{%s %s} @ %d]", s.start, s.end, s.ts)
}

// spanFrontier tracks the minimum timestamp of a set of spans.
type spanFrontier struct {
	list    spanList
	minHeap minTsHeap
}

// NewFrontier creates Froniter from the given spans.
func NewFrontier(spans ...util.Span) Frontier {
	// spanFrontier don't support use Nil as the maximum key of End range
	// So we use set it as util.UpperBoundKey, the means the real use case *should not* have a
	// End key bigger than util.UpperBoundKey
	for i, span := range spans {
		spans[i] = span.Hack()
	}

	s := &spanFrontier{}
	s.list.init()

	for _, span := range spans {
		e := &node{
			start: span.Start,
			end:   span.End,
			ts:    0,
		}

		s.list.insert(e)
		s.minHeap.insert(e)
	}

	return s
}

// Frontier return the minimum tiemstamp.
func (s *spanFrontier) Frontier() uint64 {
	min := s.minHeap.getMin()
	if min == nil {
		return 0
	}
	return min.ts
}

// Forward advances the timestamp for a span.
// True is returned if the frontier advanced.
func (s *spanFrontier) Forward(span util.Span, ts uint64) bool {
	span = span.Hack()

	pre := s.Frontier()
	s.insert(span, ts)
	return pre < s.Frontier()
}

func (s *spanFrontier) insert(span util.Span, ts uint64) {
	n := s.list.seek(span.Start)

	if n == nil || bytes.Compare(span.End, n.start) <= 0 {
		// No overlapped spans.
		return
	}

	if bytes.Compare(n.end, span.Start) <= 0 {
		// Pointed span has no overlap, step to next.
		n = n.next()
	}

	if n != nil && bytes.Compare(span.Start, n.start) > 0 {
		e := &node{
			start: n.start,
			end:   span.Start,
			ts:    n.ts,
		}
		n.start = span.Start
		s.list.insert(e)
		s.minHeap.insert(e)
	}

	for n != nil {
		oldTs := n.ts
		if ts > n.ts {
			s.minHeap.increaseTs(n, ts)
		}

		next := n.next()
		if next != nil && bytes.Compare(next.start, span.End) < 0 {
			n = next
			continue
		}

		// no more overlapped spans.
		if cmp := bytes.Compare(span.End, n.end); cmp < 0 {
			e := &node{
				start: span.End,
				end:   n.end,
				ts:    oldTs,
			}
			s.list.insert(e)
			n.end = span.End
			s.minHeap.insert(e)
		}
		return
	}
}

// Entries visit all traced spans.
func (s *spanFrontier) Entries(fn func(start, end []byte, ts uint64)) {
	for n := s.list.head.next(); n != nil; n = n.next() {
		fn(n.start, n.end, n.ts)
	}
}

func (s *spanFrontier) String() string {
	var buf strings.Builder
	for n := s.list.head.next(); n != nil; n = n.next() {
		if buf.Len() != 0 {
			buf.WriteString(` `)
		}
		buf.WriteString(n.String())
	}
	return buf.String()
}
