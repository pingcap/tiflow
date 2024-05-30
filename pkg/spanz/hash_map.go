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
	"encoding/binary"

	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/util/seahash"
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

// Len returns the number of items currently in the map.
func (m *HashMap[T]) Len() int {
	return len(m.hashMap)
}

// Has returns true if the given key is in the map.
func (m *HashMap[T]) Has(span tablepb.Span) bool {
	_, ok := m.hashMap[toHashableSpan(span)]
	return ok
}

// Get looks for the key item in the map, returning it.
// It returns (zeroValue, false) if unable to find that item.
func (m *HashMap[T]) Get(span tablepb.Span) (T, bool) {
	item, ok := m.hashMap[toHashableSpan(span)]
	return item, ok
}

// GetV looks for the key item in the map, returning it.
// It returns zeroValue if unable to find that item.
func (m *HashMap[T]) GetV(span tablepb.Span) T {
	item := m.hashMap[toHashableSpan(span)]
	return item
}

// Delete removes an item whose key equals to the span.
func (m *HashMap[T]) Delete(span tablepb.Span) {
	delete(m.hashMap, toHashableSpan(span))
}

// ReplaceOrInsert adds the given item to the map.
func (m *HashMap[T]) ReplaceOrInsert(span tablepb.Span, value T) {
	m.hashMap[toHashableSpan(span)] = value
}

// Range calls the iterator for every value in the map until iterator returns
// false.
func (m *HashMap[T]) Range(iterator ItemIterator[T]) {
	for k, v := range m.hashMap {
		ok := iterator(k.toSpan(), v)
		if !ok {
			break
		}
	}
}

// HashTableSpan hashes the given span to a slot offset.
func HashTableSpan(span tablepb.Span, slots int) int {
	b := make([]byte, 8+len(span.StartKey))
	binary.LittleEndian.PutUint64(b[0:8], uint64(span.TableID))
	copy(b[8:], span.StartKey)
	return int(seahash.Sum64(b) % uint64(slots))
}
