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

	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/prometheus/client_golang/prometheus"
)

// Frontier checks resolved event of spans and moves the global resolved ts ahead
type Frontier interface {
	Forward(regionID uint64, span regionspan.ComparableSpan, ts uint64)
	Frontier() uint64
	String() string
}

// spanFrontier tracks the minimum timestamp of a set of spans.
type spanFrontier struct {
	spanList                                  skipList
	minTsHeap                                 fibonacciHeap
	result                                    []*skipListNode
	nodes                                     map[uint64]*skipListNode
	metricResolvedRegionCachedCounterResolved prometheus.Counter
}

// NewFrontier creates Frontier from the given spans.
// spanFrontier don't support use Nil as the maximum key of End range
// So we use set it as util.UpperBoundKey, the means the real use case *should not* have an
// End key bigger than util.UpperBoundKey
func NewFrontier(checkpointTs uint64, metricResolvedRegionCachedCounterResolved prometheus.Counter, spans ...regionspan.ComparableSpan) Frontier {
	s := &spanFrontier{
		spanList: *newSpanList(),
		result:   make(seekResult, maxHeight),
		nodes:    map[uint64]*skipListNode{},

		metricResolvedRegionCachedCounterResolved: metricResolvedRegionCachedCounterResolved,
	}
	firstSpan := true
	for _, span := range spans {
		if firstSpan {
			s.spanList.Insert(span.Start, s.minTsHeap.Insert(checkpointTs))
			s.spanList.Insert(span.End, s.minTsHeap.Insert(math.MaxUint64))
			firstSpan = false
			continue
		}
		s.insert(0, span, checkpointTs)
	}

	return s
}

// Frontier return the minimum timestamp.
func (s *spanFrontier) Frontier() uint64 {
	return s.minTsHeap.GetMinKey()
}

// Forward advances the timestamp for a span.
func (s *spanFrontier) Forward(regionID uint64, span regionspan.ComparableSpan, ts uint64) {
	if n, ok := s.nodes[regionID]; ok && n.regionID > 0 && n.end != nil {
		if bytes.Equal(n.Key(), span.Start) && bytes.Equal(n.End(), span.End) {
			s.minTsHeap.UpdateKey(n.Value(), ts)
			s.metricResolvedRegionCachedCounterResolved.Inc()
			return
		}
	}
	s.insert(regionID, span, ts)
}

func (s *spanFrontier) insert(regionID uint64, span regionspan.ComparableSpan, ts uint64) {
	for i := 0; i < len(s.result); i++ {
		s.result[i] = nil
	}
	seekRes := s.spanList.Seek(span.Start, s.result)
	// if there is no change in the region span
	// We just need to update the ts corresponding to the span in list
	next := seekRes.Node().Next()
	if next != nil {
		if bytes.Equal(seekRes.Node().Key(), span.Start) && bytes.Equal(next.Key(), span.End) {
			s.minTsHeap.UpdateKey(seekRes.Node().Value(), ts)
			s.nodes[regionID] = seekRes.Node()
			s.nodes[regionID].regionID = regionID
			s.nodes[regionID].end = next.key
			return
		}
	}
	for _, n := range seekRes {
		if n == nil {
			break
		}
		if n.regionID > 0 {
			delete(s.nodes, n.regionID)
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
		//n.regionID = regionID
		//n.end = span.End
		//s.nodes[regionID] = n
		//seekRes.Next()
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
