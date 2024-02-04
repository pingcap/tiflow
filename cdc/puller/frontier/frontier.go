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
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/tiflow/cdc/processor/tablepb"
)

// fakeRegionID when the frontier is initializing, there is no region ID
// use fakeRegionID ,so this span will be cached
const fakeRegionID = 0

// Frontier checks resolved event of spans and moves the global resolved ts ahead
type Frontier interface {
	Forward(regionID uint64, span tablepb.Span, ts uint64)
	Frontier() uint64
	String() string
	SpanString(span tablepb.Span) string
	Entries(fn func(key []byte, ts uint64))
}

// spanFrontier tracks the minimum timestamp of a set of spans.
type spanFrontier struct {
	spanList  skipList
	minTsHeap fibonacciHeap

	seekTempResult []*skipListNode

	cachedRegions map[uint64]*skipListNode
}

// NewFrontier creates Frontier from the given spans.
// spanFrontier don't support use Nil as the maximum key of End range
// So we use set it as util.UpperBoundKey, the means the real use case *should not* have an
// End key bigger than util.UpperBoundKey
func NewFrontier(checkpointTs uint64, spans ...tablepb.Span) Frontier {
	s := &spanFrontier{
		spanList:       *newSpanList(),
		seekTempResult: make(seekResult, maxHeight),
		cachedRegions:  map[uint64]*skipListNode{},
	}

	firstSpan := true
	for _, span := range spans {
		if firstSpan {
			s.spanList.Insert(span.StartKey, s.minTsHeap.Insert(checkpointTs))
			s.spanList.Insert(span.EndKey, s.minTsHeap.Insert(math.MaxUint64))
			firstSpan = false
			continue
		}
		s.insert(fakeRegionID, span, checkpointTs)
	}

	return s
}

// Frontier return the minimum timestamp.
func (s *spanFrontier) Frontier() uint64 {
	return s.minTsHeap.GetMinKey()
}

// Forward advances the timestamp for a span.
func (s *spanFrontier) Forward(regionID uint64, span tablepb.Span, ts uint64) {
	// it's the fast part to detect if the region is split or merged,
	// if not we can update the minTsHeap with use new ts directly
	if n, ok := s.cachedRegions[regionID]; ok && n.regionID == regionID && n.end != nil {
		if bytes.Equal(n.Key(), span.StartKey) && bytes.Equal(n.End(), span.EndKey) {
			s.minTsHeap.UpdateKey(n.Value(), ts)
			return
		}
	}
	s.insert(regionID, span, ts)
}

func (s *spanFrontier) insert(regionID uint64, span tablepb.Span, ts uint64) {
	// clear the seek result
	for i := 0; i < len(s.seekTempResult); i++ {
		s.seekTempResult[i] = nil
	}
	seekRes := s.spanList.Seek(span.StartKey, s.seekTempResult)
	// if there is no change in the region span
	// We just need to update the ts corresponding to the span in list
	next := seekRes.Node().Next()
	// next is nil means the span.StartKey is larger than all the spans in list
	if next != nil {
		if bytes.Equal(seekRes.Node().Key(), span.StartKey) &&
			bytes.Equal(next.Key(), span.EndKey) {
			s.minTsHeap.UpdateKey(seekRes.Node().Value(), ts)
			delete(s.cachedRegions, seekRes.Node().regionID)
			if regionID != fakeRegionID {
				s.cachedRegions[regionID] = seekRes.Node()
				s.cachedRegions[regionID].regionID = regionID
				s.cachedRegions[regionID].end = next.key
			}
			return
		}
	}

	// regions are merged or split, overwrite span into list
	node := seekRes.Node()
	delete(s.cachedRegions, node.regionID)
	lastNodeTs := uint64(math.MaxUint64)
	lastRegionID := uint64(fakeRegionID)
	shouldInsertStartNode := true
	if node.Value() != nil {
		lastNodeTs = node.Value().key
		lastRegionID = node.regionID
	}
	for ; node != nil; node = node.Next() {
		delete(s.cachedRegions, node.regionID)
		cmpStart := bytes.Compare(node.Key(), span.StartKey)
		if cmpStart < 0 {
			continue
		}
		if bytes.Compare(node.Key(), span.EndKey) > 0 {
			break
		}
		lastNodeTs = node.Value().key
		lastRegionID = node.regionID
		if cmpStart == 0 {
			s.minTsHeap.UpdateKey(node.Value(), ts)
			node.regionID = regionID
			shouldInsertStartNode = false
		} else {
			s.spanList.Remove(seekRes, node)
			s.minTsHeap.Remove(node.Value())
		}
	}
	if shouldInsertStartNode {
		s.spanList.InsertNextToNode(seekRes, span.StartKey, s.minTsHeap.Insert(ts), regionID)
		seekRes.Next()
	}
	s.spanList.InsertNextToNode(seekRes, span.EndKey, s.minTsHeap.Insert(lastNodeTs), lastRegionID)
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

func (s *spanFrontier) stringWtihRegionID() string {
	var buf strings.Builder
	s.spanList.Entries(func(n *skipListNode) bool {
		if n.Value().key == math.MaxUint64 {
			buf.WriteString(fmt.Sprintf("[%d:%s @ Max] ", n.regionID, hex.EncodeToString(n.Key())))
		} else { // the next span
			buf.WriteString(fmt.Sprintf("[%d:%s @ %d] ", n.regionID, hex.EncodeToString(n.Key()), n.Value().key))
		}
		return true
	})
	return buf.String()
}

// SpanString returns the string of the span's frontier.
func (s *spanFrontier) SpanString(span tablepb.Span) string {
	var buf strings.Builder
	idx := 0
	s.spanList.Entries(func(n *skipListNode) bool {
		key := n.Key()
		nextKey := []byte{}
		if n.Next() != nil {
			nextKey = n.Next().Key()
		}
		if n.Value().key == math.MaxUint64 {
			buf.WriteString(fmt.Sprintf("[%d:%s @ Max] ", n.regionID, hex.EncodeToString(n.Key())))
		} else if idx == 0 || // head
			bytes.Equal(key, span.StartKey) || // start key sapn
			bytes.Equal(nextKey, span.StartKey) || // the previous sapn of start key
			bytes.Equal(key, span.EndKey) { // the end key span
			buf.WriteString(fmt.Sprintf("[%d:%s @ %d] ", n.regionID,
				hex.EncodeToString(n.Key()), n.Value().key))
		}
		idx++
		return true
	})
	return buf.String()
}
