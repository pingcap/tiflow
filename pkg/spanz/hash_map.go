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
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
)

// HashMap is a specialized hash map that map a Span to a value.
type HashMap[T any] struct {
	hashMap map[hashableSpan]T
}

// NewHashMap returns a new HashMap.
func NewHashMap[T any]() *HashMap[T] {
	return &HashMap[T]{
		hashMap: make(map[hashableSpan]T),
	}
}

// Len returns the number of items currently in the tree.
func (m *HashMap[T]) Len() int {
	return len(m.hashMap)
}

// Has returns true if the given key is in the tree.
func (m *HashMap[T]) Has(span tablepb.Span) bool {
	_, ok := m.hashMap[toHashableSpan(span)]
	return ok
}

// Get looks for the key item in the tree, returning it.
// It returns (zeroValue, false) if unable to find that item.
func (m *HashMap[T]) Get(span tablepb.Span) (T, bool) {
	item, ok := m.hashMap[toHashableSpan(span)]
	return item, ok
}

// GetV looks for the key item in the tree, returning it.
// It returns zeroValue if unable to find that item.
func (m *HashMap[T]) GetV(span tablepb.Span) T {
	item := m.hashMap[toHashableSpan(span)]
	return item
}

// Delete removes an item whose key equals to the span.
func (m *HashMap[T]) Delete(span tablepb.Span) {
	delete(m.hashMap, toHashableSpan(span))
}

// ReplaceOrInsert adds the given item to the tree.  If an item in the tree
// already equals the given one, it is removed from the tree and returned,
// and the second return value is true.  Otherwise, (zeroValue, false)
//
// nil cannot be added to the tree (will panic).
func (m *HashMap[T]) ReplaceOrInsert(span tablepb.Span, value T) {
	m.hashMap[toHashableSpan(span)] = value
}

// Iter calls the iterator for every value in the tree until iterator returns
// false.
func (m *HashMap[T]) Iter(iterator ItemIterator[T]) {
	for k, v := range m.hashMap {
		ok := iterator(k.toSpan(), v)
		if !ok {
			break
		}
	}
}
