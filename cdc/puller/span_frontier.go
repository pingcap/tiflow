package puller

import (
	"container/heap"
	"fmt"
	"strings"

	"github.com/pingcap/ticdc/pkg/interval"
	"github.com/pingcap/ticdc/pkg/util"
)

// Bytes represents generic intervals
type Bytes []byte

// Range represents a kv range interval
// [Start, End), close Start and open End.
type Range struct {
	Start Bytes
	End   Bytes
}

func asRange(span util.Span) interval.Range {
	return interval.Range{
		Start: span.Start,
		End:   span.End,
	}
}

type spanFrontierEntry struct {
	id     int64
	irange interval.Range
	ts     uint64

	// index in the head array
	index int
}

// Range implements interval.Interface.
func (s *spanFrontierEntry) Range() interval.Range {
	return s.irange
}

// ID implements interval.Interface.
func (s *spanFrontierEntry) ID() int64 {
	return s.id
}

func (s *spanFrontierEntry) String() string {
	return fmt.Sprintf("[%s @ %d]", s.irange, s.ts)
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
		return util.StartCompare(h[i].irange.Start, h[j].irange.Start) < 0
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
		tree: interval.NewTree(interval.ExclusiveOverlapper),
	}

	for _, span := range spans {
		e := &spanFrontierEntry{
			id:     s.idAlloc,
			irange: asRange(span),
			ts:     0,
		}

		s.idAlloc++

		err := s.tree.Insert(e, false)
		if err != nil {
			panic(err)
		}

		heap.Push(&s.minHeap, e)
	}

	return s
}

// Frontier return the minimum tiemstamp.
func (s *spanFrontier) Frontier() uint64 {
	if s.minHeap.Len() == 0 {
		return 0
	}

	return s.minHeap[0].ts
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

	// fast path
	// in most times, the span we forward will match exactly one interval in the tree.
	if len(overlap) == 1 {
		e := overlap[0].(*spanFrontierEntry)
		if irange.Start.Compare(e.Range().Start) == 0 &&
			irange.End.Compare(e.Range().End) == 0 {

			if ts > e.ts {
				e.ts = ts
			}
			heap.Remove(&s.minHeap, e.index)
			heap.Push(&s.minHeap, e)
			return
		}
	}

	spanCov := util.Covering{{Start: span.Start, End: span.End, Payload: ts}}

	overlapCov := make(util.Covering, len(overlap))
	for i, o := range overlap {
		e := o.(*spanFrontierEntry)
		overlapCov[i] = util.Range{
			Start:   e.irange.Start,
			End:     e.irange.End,
			Payload: e,
		}
	}

	merged := util.OverlapCoveringMerge([]util.Covering{spanCov, overlapCov})

	toInsert := make([]spanFrontierEntry, 0, len(merged))

	for _, m := range merged {
		// Compute the newest timestamp seen for this span and note whether it's tracked.
		// There will be either 1 or 2 payloads.
		var mergeTs uint64
		var tracked bool

		for _, payload := range m.Payload.([]interface{}) {
			switch p := payload.(type) {
			case uint64:
				if mergeTs < p {
					mergeTs = p
				}
			case *spanFrontierEntry:
				if mergeTs < p.ts {
					mergeTs = p.ts
				}
				tracked = true
			}
		}

		if tracked {
			toInsert = append(toInsert, spanFrontierEntry{
				id:     s.idAlloc,
				irange: interval.Range{Start: m.Start, End: m.End},
				ts:     mergeTs,
			})
			s.idAlloc++
		}
	}

	// Delete old ones
	for i := range overlap {
		e := overlap[i].(*spanFrontierEntry)
		err := s.tree.Delete(e, false)
		if err != nil {
			panic(err)
		}
		heap.Remove(&s.minHeap, e.index)
	}

	// Insert new ones
	for i := range toInsert {
		err := s.tree.Insert(&toInsert[i], false)
		if err != nil {
			panic(err)
		}
		heap.Push(&s.minHeap, &toInsert[i])
	}
}

// Entries visit all traced spans.
func (s *spanFrontier) Entries(fn func(irange interval.Range, ts uint64)) {
	s.tree.Do(func(i interval.Interface) bool {
		e := i.(*spanFrontierEntry)
		fn(e.irange, e.ts)
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
