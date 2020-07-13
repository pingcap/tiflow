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
	"math"
	"strings"

	"github.com/pingcap/ticdc/pkg/regionspan"
)

// Frontier checks resolved event of spans and moves the global resolved ts ahead
type Frontier interface {
	Forward(span regionspan.ComparableSpan, ts uint64)
	Frontier() uint64
	String() string
}

// spanFrontier tracks the minimum timestamp of a set of spans.
type spanFrontier struct {
	spanList  skipList
	minTsHeap fibonacciHeap
}

// NewFrontier creates Froniter from the given spans.
func NewFrontier(checkpointTs uint64, spans ...regionspan.ComparableSpan) Frontier {
	// spanFrontier don't support use Nil as the maximum key of End range
	// So we use set it as util.UpperBoundKey, the means the real use case *should not* have a
	// End key bigger than util.UpperBoundKey
	for i, span := range spans {
		spans[i] = span.Hack()
	}

	s := &spanFrontier{
		spanList: *newSpanList(),
	}
	firstSpan := true
	for _, span := range spans {
		if firstSpan {
			s.spanList.Insert(span.Start, s.minTsHeap.Insert(checkpointTs))
			s.spanList.Insert(span.End, s.minTsHeap.Insert(math.MaxUint64))
			firstSpan = false
			continue
		}
		s.insert(span, checkpointTs)
	}

	return s
}

// Frontier return the minimum tiemstamp.
func (s *spanFrontier) Frontier() uint64 {
	return s.minTsHeap.GetMinKey()
}

// Forward advances the timestamp for a span.
func (s *spanFrontier) Forward(span regionspan.ComparableSpan, ts uint64) {
	span = span.Hack()
	s.insert(span, ts)
}

func (s *spanFrontier) insert(span regionspan.ComparableSpan, ts uint64) {
	seekRes := s.spanList.Seek(span.Start)

	// if there is no change in the region span
	// We just need to update the ts corresponding to the span in list
	next := seekRes.Node().Next()
	if next != nil {
		cmpStart := bytes.Compare(seekRes.Node().Key(), span.Start)
		cmpEnd := bytes.Compare(next.Key(), span.End)
		if cmpStart == 0 && cmpEnd == 0 {
			s.minTsHeap.UpdateKey(seekRes.Node().Value(), ts)
			return
		}
	}

	// regions are merged or split, overwrite span into list
	node := seekRes.Node()
	lastNodeTs := uint64(math.MaxUint64)
	shouldInsertStartNode := true
	if node.Value() != nil {
		lastNodeTs = node.Value().key
	}
	for ; node != nil; node = node.Next() {
		cmpStart := bytes.Compare(node.Key(), span.Start)
		if cmpStart < 0 {
			continue
		}
		if bytes.Compare(node.Key(), span.End) > 0 {
			break
		}
		lastNodeTs = node.Value().key
		if cmpStart == 0 {
			s.minTsHeap.UpdateKey(node.Value(), ts)
			shouldInsertStartNode = false
		} else {
			s.spanList.Remove(seekRes, node)
			s.minTsHeap.Remove(node.Value())
		}
	}
	if shouldInsertStartNode {
		s.spanList.InsertNextToNode(seekRes, span.Start, s.minTsHeap.Insert(ts))
		seekRes.Next()
	}
	s.spanList.InsertNextToNode(seekRes, span.End, s.minTsHeap.Insert(lastNodeTs))
}

// Entries visit all traced spans.
func (s *spanFrontier) Entries(fn func(key []byte, ts uint64)) {
	s.spanList.Entries(func(n *skipListNode) bool {
		fn(n.Key(), n.Value().key)
		return true
	})
}

func (s *spanFrontier) String() string {
	var buf strings.Builder
	s.Entries(func(key []byte, ts uint64) {
		if ts == math.MaxUint64 {
			buf.WriteString(fmt.Sprintf("[%s @ Max] ", key))
		} else {
			buf.WriteString(fmt.Sprintf("[%s @ %d] ", key, ts))
		}
	})
	return buf.String()
}
