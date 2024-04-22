// Copyright 2024 PingCAP, Inc.
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

package regionlock

import (
	"bytes"
	"math"

	"github.com/google/btree"
	"github.com/pingcap/log"
)

// rangeTsMap represents a map from key range to a timestamp. It supports
// range set and calculating min value among a specified range.
type rangeTsMap struct {
	m     *btree.BTreeG[rangeTsEntry]
	start []byte
	end   []byte
}

// newRangeTsMap creates a RangeTsMap.
func newRangeTsMap(startKey, endKey []byte, startTs uint64) *rangeTsMap {
	m := &rangeTsMap{
		m:     btree.NewG(16, rangeTsEntryLess),
		start: startKey,
		end:   endKey,
	}
	m.set(startKey, endKey, startTs)
	return m
}

func (m *rangeTsMap) clone() (res *rangeTsMap) {
	res = &rangeTsMap{
		m:     btree.NewG(16, rangeTsEntryLess),
		start: m.start,
		end:   m.end,
	}
	m.m.Ascend(func(i rangeTsEntry) bool {
		res.m.ReplaceOrInsert(i)
		return true
	})
	return
}

// set the timestamp of range [startKey, endKey) to ts.
// RangeLock uses rangeTsMap to store unsubscribed regions.
// So `set` should be called after a region gets unlocked from RangeLock.
// Note: It leaves the timestamp of range [endKey, +∞) intact.
func (m *rangeTsMap) set(startKey, endKey []byte, ts uint64) {
	startEntry := rangeTsEntryWithKey(startKey)
	endEntry := rangeTsEntryWithKey(endKey)

	// Check and update the range overlapped with endKey is it exists.
	if !bytes.Equal(m.end, endKey) && !m.m.Has(endEntry) {
		var found bool
		var endKeyOverlapped rangeTsEntry
		m.m.DescendLessOrEqual(endEntry, func(i rangeTsEntry) bool {
			found = true
			endKeyOverlapped.ts = i.ts
			endKeyOverlapped.isSet = i.isSet
			return false
		})
		if found {
			if endKeyOverlapped.isSet {
				log.Panic("rangeTsMap double set")
			}
			endKeyOverlapped.startKey = endKey
			m.m.ReplaceOrInsert(endKeyOverlapped)
		}
	}

	// Check and update the range overlapped with startKey if it exists.
	if !m.m.Has(startEntry) {
		var found bool
		var startKeyOverlapped rangeTsEntry
		m.m.DescendLessOrEqual(startEntry, func(i rangeTsEntry) bool {
			found = true
			startKeyOverlapped.isSet = i.isSet
			return false
		})
		if found && startKeyOverlapped.isSet {
			log.Panic("rangeTsMap double set")
		}
	}

	// Check and delete all covered ranges.
	entriesToDelete := make([]rangeTsEntry, 0)
	m.m.AscendRange(startEntry, endEntry, func(i rangeTsEntry) bool {
		if i.isSet {
			log.Panic("rangeTsMap double set")
		}
		entriesToDelete = append(entriesToDelete, i)
		return true
	})
	for _, e := range entriesToDelete {
		m.m.Delete(e)
	}

	m.m.ReplaceOrInsert(rangeTsEntry{startKey: startKey, ts: ts, isSet: true})
}

// RangeLock uses rangeTsMap to store unsubscribed regions.
// So `unset` should be called after a region gets locked in RangeLock.
func (m *rangeTsMap) unset(startKey, endKey []byte) {
	var neighbor rangeTsEntry
	var exist bool
	startEntry := rangeTsEntryWithKey(startKey)
	endEntry := rangeTsEntryWithKey(endKey)

	// Check and update the range overlapped with endKey is it exists.
	if !bytes.Equal(m.end, endKey) {
		if neighbor, exist = m.m.Get(endEntry); !exist {
			var found bool
			var endKeyOverlapped rangeTsEntry
			m.m.DescendLessOrEqual(endEntry, func(i rangeTsEntry) bool {
				found = true
				endKeyOverlapped.ts = i.ts
				endKeyOverlapped.isSet = i.isSet
				return false
			})
			if found {
				if !endKeyOverlapped.isSet {
					log.Panic("rangeTsMap double unset")
				}
				endKeyOverlapped.startKey = endKey
				m.m.ReplaceOrInsert(endKeyOverlapped)
			}
		} else if !neighbor.isSet {
			m.m.Delete(neighbor)
		}
	}

	// Check and update the range overlapped with startKey if it exists.
	exist = false
	m.m.DescendLessOrEqual(startEntry, func(i rangeTsEntry) bool {
		if bytes.Compare(i.startKey, startKey) < 0 {
			neighbor = i
			exist = true
			return false
		}
		return true
	})
	shouldInsert := !exist || neighbor.isSet

	// Check and delete all covered ranges.
	entriesToDelete := make([]rangeTsEntry, 0)
	m.m.AscendRange(startEntry, endEntry, func(i rangeTsEntry) bool {
		if !i.isSet {
			log.Panic("rangeTsMap double unset")
		}
		entriesToDelete = append(entriesToDelete, i)
		return true
	})
	for _, e := range entriesToDelete {
		m.m.Delete(e)
	}

	if shouldInsert {
		m.m.ReplaceOrInsert(rangeTsEntry{startKey: startKey, isSet: false})
	}
}

func (m *rangeTsMap) getMinTsInRange(startKey, endKey []byte) uint64 {
	var ts uint64 = math.MaxUint64

	startEntry := rangeTsEntryWithKey(startKey)
	if _, ok := m.m.Get(startEntry); !ok {
		m.m.DescendLessOrEqual(startEntry, func(i rangeTsEntry) bool {
			if !i.isSet {
				log.Panic("rangeTsMap get after unset")
			}
			ts = i.ts
			return false
		})
	}

	endEntry := rangeTsEntryWithKey(endKey)
	m.m.AscendRange(startEntry, endEntry, func(i rangeTsEntry) bool {
		if !i.isSet {
			log.Panic("rangeTsMap get after unset")
		}
		if ts > i.ts {
			ts = i.ts
		}
		return true
	})

	return ts
}

func (m *rangeTsMap) getMinTs() uint64 {
	var ts uint64 = math.MaxUint64

	m.m.Ascend(func(i rangeTsEntry) bool {
		if i.isSet && ts > i.ts {
			ts = i.ts
		}
		return true
	})

	return ts
}

// rangeTsEntry is the entry of rangeTsMap.
type rangeTsEntry struct {
	// Only startKey is necessary. End key can be inferred by the next item,
	// since the map always keeps a continuous range.
	startKey []byte
	ts       uint64

	// rangeTsEntry is used in rangeTsMap. rangeTsMap.set will associate a timestamp to a given key range,
	// and rangeTsMap.unset will revoke the relationship. `isSet` indicates whether a key range is
	// generated from a set or unset operation.
	isSet bool
}

func rangeTsEntryWithKey(key []byte) rangeTsEntry {
	return rangeTsEntry{startKey: key}
}

func rangeTsEntryLess(a, b rangeTsEntry) bool {
	return bytes.Compare(a.startKey, b.startKey) < 0
}
