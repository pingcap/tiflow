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
	"sync"

	"github.com/pingcap/tiflow/cdc/processor/tablepb"
)

// SyncMap is thread-safe map, its key is tablepb.Span.
type SyncMap struct {
	m sync.Map
}

// Load returns the value stored in the map for a key, or nil if no
// value is present.
// The ok result indicates whether value was found in the map.
func (m *SyncMap) Load(key tablepb.Span) (value any, ok bool) {
	return m.m.Load(toHashableSpan(key))
}

// Store sets the value for a key.
func (m *SyncMap) Store(key tablepb.Span, value any) {
	m.m.Store(toHashableSpan(key), value)
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *SyncMap) LoadOrStore(key tablepb.Span, value any) (actual any, loaded bool) {
	return m.m.LoadOrStore(toHashableSpan(key), value)
}

// Delete deletes the value for a key.
func (m *SyncMap) Delete(key tablepb.Span) {
	m.m.Delete(toHashableSpan(key))
}

// LoadAndDelete deletes the value for a key, returning the previous value if any.
// The loaded result reports whether the key was present.
func (m *SyncMap) LoadAndDelete(key tablepb.Span) (value any, loaded bool) {
	return m.m.LoadAndDelete(toHashableSpan(key))
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
func (m *SyncMap) Range(f func(span tablepb.Span, value any) bool) {
	m.m.Range(func(key, value any) bool {
		span := key.(hashableSpan).toSpan()
		return f(span, value)
	})
}
