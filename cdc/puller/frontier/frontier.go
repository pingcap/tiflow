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
	"fmt"
	"strings"

	"github.com/pingcap/ticdc/pkg/regionspan"
)

// Frontier checks resolved event of spans and moves the global resolved ts ahead
type Frontier interface {
	Forward(span regionspan.Span, ts uint64)
	Frontier() uint64
	String() string
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
func NewFrontier(spans ...regionspan.Span) Frontier {
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
func (s *spanFrontier) Forward(span regionspan.Span, ts uint64) {
	span = span.Hack()
	s.insert(span, ts)
}

func (s *spanFrontier) insert(span regionspan.Span, ts uint64) {
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
		if ts != n.ts {
			s.minHeap.updateTs(n, ts)
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
