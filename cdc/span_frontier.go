package cdc

import (
	"bytes"
	"container/heap"
	"fmt"
	"reflect"
	"strings"

	"github.com/biogo/store/interval"
	"github.com/pingcap/tidb-cdc/pkg/util"
)

// Generic intervals
type Bytes []byte

// Compare implements interval.Comparable.
func (b Bytes) Compare(a interval.Comparable) int {
	return bytes.Compare(b, a.(Bytes))
}

// Range represents a kv range interval
// [Start, End), close Start and open End.
type Range struct {
	Start Bytes
	End   Bytes
}

func asRange(span util.Span) Range {
	return Range{
		Start: span.Start,
		End:   span.End,
	}
}

type spanFrontierEntry struct {
	id     int64
	irange Range
	span   util.Span
	ts     uint64

	// index in the head array
	index int
}

type Mutable struct{ start, end Bytes }

func (m *Mutable) Start() interval.Comparable     { return m.start }
func (m *Mutable) End() interval.Comparable       { return m.end }
func (m *Mutable) SetStart(c interval.Comparable) { m.start = c.(Bytes) }
func (m *Mutable) SetEnd(c interval.Comparable)   { m.end = c.(Bytes) }

// Overlap implements interval.Overlapper
func (s spanFrontierEntry) Overlap(b interval.Range) bool {
	return s.irange.Overlap(b)
}

// Overlap implements interval.Overlapper
func (s Range) Overlap(b interval.Range) bool {
	var start, end Bytes
	switch bc := b.(type) {
	case *spanFrontierEntry:
		start, end = bc.irange.Start, bc.irange.End
	case *Mutable:
		start, end = bc.start, bc.end
	default:
		s := fmt.Sprintf("unknown type: %v", reflect.TypeOf(b))
		panic(s)
	}

	return util.EndCompare(s.End, start) > 0 && util.StartCompare(s.Start, end) < 0
}

// Start implements interval.Interface.
func (s spanFrontierEntry) Start() interval.Comparable {
	return s.irange.Start
}

// End implements interval.Interface.
func (s spanFrontierEntry) End() interval.Comparable {
	return s.irange.End
}

// ID implements interval.Interface.
func (s *spanFrontierEntry) ID() uintptr {
	return uintptr(s.id)
}

// NewMutable implements interval.Interface.
func (s *spanFrontierEntry) NewMutable() interval.Mutable {
	return &Mutable{s.irange.Start, s.irange.End}
}

func (s *spanFrontierEntry) String() string {
	return fmt.Sprintf("[%s @ %d]", s.span, s.ts)
}

// spanFrontierHeap implements heap.Interface and holds `spanFrontierEntry`s.
// Entries are sorted based on their timestamp such that the oldest will rise to
// the top of the heap.
type spanFrontierHeap []*spanFrontierEntry

// Len inplements heap.Interface
func (h spanFrontierHeap) Len() int { return len(h) }

// Less inplements heap.Interface
func (h spanFrontierHeap) Less(i, j int) bool {
	if h[i].ts == h[j].ts {
		return util.StartCompare(h[i].span.Start, h[j].span.Start) < 0
	}

	return h[i].ts < h[j].ts
}

// Swap implements heap.Interface.
func (h spanFrontierHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

// Push implements heap.Interface.
func (h *spanFrontierHeap) Push(a interface{}) {
	n := len(*h)
	entry := a.(*spanFrontierEntry)
	entry.index = n
	*h = append(*h, entry)
}

// Pop implements heap.Interface.
func (h *spanFrontierHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	entry.index = -1
	old[n-1] = nil
	*h = old[0 : n-1]
	return entry
}

// spanFrontier tracks the minimum timestamp of a set of spans.
type spanFrontier struct {
	tree *interval.Tree

	minHeap spanFrontierHeap

	idAlloc int64
}

func makeSpanFrontier(spans ...util.Span) *spanFrontier {
	// spanFrontier don't support use Nil as the maximum key of End range
	// So we use set it as util.UpperBoundKey, the means the real use case *should not* have a
	// End key bigger than util.UpperBoundKey
	for i, span := range spans {
		spans[i] = span.Hack()
	}

	s := &spanFrontier{
		tree: &interval.Tree{},
	}

	for _, span := range spans {
		e := &spanFrontierEntry{
			id:     s.idAlloc,
			irange: asRange(span),
			span:   span,
			ts:     0,
		}

		s.idAlloc++

		err := s.tree.Insert(e, true)
		if err != nil {
			panic(err)
		}

		heap.Push(&s.minHeap, e)
	}

	s.tree.AdjustRanges()
	return s
}

// Frontier return the minimum tiemstamp.
func (s *spanFrontier) Frontier() uint64 {
	if s.minHeap.Len() == 0 {
		return 0
	}

	return s.minHeap[0].ts
}

func (s *spanFrontier) peekFrontierSpan() util.Span {
	if s.minHeap.Len() == 0 {
		return util.Span{}
	}

	return s.minHeap[0].span
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
	irange := asRange(span)

	// Get all interval overlap with irange.
	overlap := s.tree.Get(irange)

	if len(overlap) == 0 {
		return
	}

	spanCov := util.Covering{{Start: span.Start, End: span.End, Payload: ts}}

	overlapCov := make(util.Covering, len(overlap))
	for i, o := range overlap {
		e := o.(*spanFrontierEntry)
		overlapCov[i] = util.Range{
			Start:   e.span.Start,
			End:     e.span.End,
			Payload: e,
		}
	}

	merged := util.OverlapCoveringMerge([]util.Covering{spanCov, overlapCov})

	toInsert := make([]spanFrontierEntry, 0, len(merged))

	for _, m := range merged {
		// Compute the newest timestamp seen for this span and note whether it's tracked.
		// There will be either 1 or 2 payloads.
		var mergeTS uint64
		var tracked bool

		for _, payload := range m.Payload.([]interface{}) {
			switch p := payload.(type) {
			case uint64:
				if mergeTS < p {
					mergeTS = p
				}
			case *spanFrontierEntry:
				if mergeTS < p.ts {
					mergeTS = p.ts
				}
				tracked = true
			}
		}

		if tracked {
			toInsert = append(toInsert, spanFrontierEntry{
				id:     s.idAlloc,
				irange: Range{Start: m.Start, End: m.End},
				span:   util.Span{Start: m.Start, End: m.End},
				ts:     mergeTS,
			})
			s.idAlloc++
		}
	}

	// Delete old ones
	for i := range overlap {
		e := overlap[i].(*spanFrontierEntry)
		err := s.tree.Delete(e, true)
		if err != nil {
			panic(err)
		}
		heap.Remove(&s.minHeap, e.index)
	}

	// Insert new ones
	for i := range toInsert {
		err := s.tree.Insert(&toInsert[i], true)
		if err != nil {
			panic(err)
		}
		heap.Push(&s.minHeap, &toInsert[i])
	}

	s.tree.AdjustRanges()
}

// Entries visit all traced spans.
func (s *spanFrontier) Entries(fn func(span util.Span, ts uint64)) {
	s.tree.Do(func(i interval.Interface) bool {
		e := i.(*spanFrontierEntry)
		fn(e.span, e.ts)
		return false
	})
}

func (s *spanFrontier) String() string {
	var buf strings.Builder
	s.tree.Do(func(i interval.Interface) bool {
		if buf.Len() != 0 {
			buf.WriteString(` `)
		}

		buf.WriteString(i.(*spanFrontierEntry).String())
		return false
	})

	return buf.String()
}
