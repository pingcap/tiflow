// Copyright 2022 PingCAP, Inc.
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

package spanz

import (
	"bytes"
	"time"

	"github.com/google/btree"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"go.uber.org/zap"
)

// spanItem is an btree item that wraps a span (key) and an item (value).
type spanItem[T any] struct {
	tablepb.Span
	Value T
}

// lessSpanItem compares two Spans, defines the order between spans.
func lessSpanItem[T any](a, b spanItem[T]) bool {
	return a.Less(&b.Span)
}

// BtreeMap is a specialized btree map that map a Span to a value.
type BtreeMap[T any] struct {
	tree *btree.BTreeG[spanItem[T]]

	cache *struct {
		coveredSpans, holes []tablepb.Span
		lastGC              time.Time
	}
}

// NewBtreeMap returns a new BtreeMap.
func NewBtreeMap[T any]() *BtreeMap[T] {
	const defaultDegree = 16
	return NewBtreeMapWithDegree[T](defaultDegree)
}

// NewBtreeMapWithDegree returns a new BtreeMap with the given degree.
func NewBtreeMapWithDegree[T any](degree int) *BtreeMap[T] {
	return &BtreeMap[T]{
		tree: btree.NewG(degree, lessSpanItem[T]),
	}
}

// Len returns the number of items currently in the tree.
func (m *BtreeMap[T]) Len() int {
	return m.tree.Len()
}

// Has returns true if the given key is in the tree.
func (m *BtreeMap[T]) Has(span tablepb.Span) bool {
	return m.tree.Has(spanItem[T]{Span: span})
}

// Get looks for the key item in the tree, returning it.
// It returns (zeroValue, false) if unable to find that item.
func (m *BtreeMap[T]) Get(span tablepb.Span) (T, bool) {
	item, ok := m.tree.Get(spanItem[T]{Span: span})
	return item.Value, ok
}

// GetV looks for the key item in the tree, returning it.
// It returns zeroValue if unable to find that item.
func (m *BtreeMap[T]) GetV(span tablepb.Span) T {
	item, _ := m.tree.Get(spanItem[T]{Span: span})
	return item.Value
}

// Delete removes an item equal to the passed in item from the tree, returning
// it.  If no such item exists, returns (zeroValue, false).
func (m *BtreeMap[T]) Delete(span tablepb.Span) (T, bool) {
	item, ok := m.tree.Delete(spanItem[T]{Span: span})
	return item.Value, ok
}

// ReplaceOrInsert adds the given item to the tree.  If an item in the tree
// already equals the given one, it is removed from the tree and returned,
// and the second return value is true.  Otherwise, (zeroValue, false)
//
// nil cannot be added to the tree (will panic).
func (m *BtreeMap[T]) ReplaceOrInsert(span tablepb.Span, value T) (T, bool) {
	old, ok := m.tree.ReplaceOrInsert(spanItem[T]{Span: span, Value: value})
	return old.Value, ok
}

// ItemIterator allows callers of Ascend to iterate in-order over portions of
// the tree. Similar to btree.ItemIterator.
// Note: The span must not be mutated.
type ItemIterator[T any] func(span tablepb.Span, value T) bool

// Ascend calls the iterator for every value in the tree within the range
// [first, last], until iterator returns false.
func (m *BtreeMap[T]) Ascend(iterator ItemIterator[T]) {
	m.tree.Ascend(func(item spanItem[T]) bool {
		return iterator(item.Span, item.Value)
	})
}

// AscendRange calls the iterator for every value in the tree within the range
// [start, end), until iterator returns false.
func (m *BtreeMap[T]) AscendRange(start, end tablepb.Span, iterator ItemIterator[T]) {
	m.tree.AscendRange(spanItem[T]{Span: start}, spanItem[T]{Span: end},
		func(item spanItem[T]) bool {
			return iterator(item.Span, item.Value)
		})
}

// FindHoles returns an array of Span that are not covered in the range
// [start, end).
// Note:
// * Table ID is not set in returned holes.
// * Returned slice is read only and will be changed on next FindHoles.
func (m *BtreeMap[T]) FindHoles(start, end tablepb.Span) ([]tablepb.Span, []tablepb.Span) {
	if bytes.Compare(start.StartKey, end.StartKey) >= 0 {
		log.Panic("start must be larger than end",
			zap.String("start", start.String()),
			zap.String("end", end.String()))
	}
	if m.cache == nil || time.Since(m.cache.lastGC) > time.Minute {
		m.cache = &struct {
			coveredSpans []tablepb.Span
			holes        []tablepb.Span
			lastGC       time.Time
		}{lastGC: time.Now()}
	}
	m.cache.coveredSpans = m.cache.coveredSpans[:0]
	m.cache.holes = m.cache.holes[:0]

	lastSpan := tablepb.Span{
		StartKey: start.StartKey,
		EndKey:   start.StartKey,
	}
	m.AscendRange(start, end, func(current tablepb.Span, _ T) bool {
		ord := bytes.Compare(lastSpan.EndKey, current.StartKey)
		if ord < 0 {
			// Find a hole.
			m.cache.holes = append(m.cache.holes, tablepb.Span{
				StartKey: lastSpan.EndKey,
				EndKey:   current.StartKey,
			})
		} else if ord > 0 {
			log.Panic("map is out of order",
				zap.String("lastSpan", lastSpan.String()),
				zap.String("current", current.String()))
		}

		lastSpan = current
		m.cache.coveredSpans = append(m.cache.coveredSpans, current)
		return true
	})
	if len(m.cache.coveredSpans) == 0 {
		// No such span in the map.
		m.cache.holes = append(m.cache.holes, tablepb.Span{
			StartKey: start.StartKey, EndKey: end.StartKey,
		})
		return m.cache.coveredSpans, m.cache.holes
	}
	// Check if there is a hole in the end.
	if !bytes.Equal(lastSpan.EndKey, end.StartKey) {
		m.cache.holes = append(m.cache.holes, tablepb.Span{
			StartKey: lastSpan.EndKey,
			EndKey:   end.StartKey,
		})
	}
	return m.cache.coveredSpans, m.cache.holes
}
