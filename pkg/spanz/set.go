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

import "github.com/pingcap/tiflow/cdc/processor/tablepb"

// Set maintains a set of Span.
type Set struct {
	memo *BtreeMap[struct{}]
}

// NewSet creates a Set.
func NewSet() *Set {
	return &Set{
		memo: NewBtreeMap[struct{}](),
	}
}

// Add adds a span to Set.
func (s *Set) Add(span tablepb.Span) {
	s.memo.ReplaceOrInsert(span, struct{}{})
}

// Remove removes a span from a Set.
func (s *Set) Remove(span tablepb.Span) {
	s.memo.Delete(span)
}

// Keys returns a collection of Span.
func (s *Set) Keys() []tablepb.Span {
	result := make([]tablepb.Span, 0, s.memo.Len())
	s.memo.Ascend(func(span tablepb.Span, value struct{}) bool {
		result = append(result, span)
		return true
	})
	return result
}

// Contain checks whether a Span is in Set.
func (s *Set) Contain(span tablepb.Span) bool {
	return s.memo.Has(span)
}

// Size returns the size of Set.
func (s *Set) Size() int {
	return s.memo.Len()
}
