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

package util

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"math"
	"sync"
	"time"

	"github.com/google/btree"
)

type rangeTsEntry struct {
	// Only startKey is necessary. End key can be inferred by the next item since the map always keeps a continuous
	// range.
	startKey []byte
	ts       uint64
}

func rangeTsEntryWithKey(key []byte) *rangeTsEntry {
	return &rangeTsEntry{
		startKey: key,
	}
}

func (e *rangeTsEntry) Less(than btree.Item) bool {
	return bytes.Compare(e.startKey, than.(*rangeTsEntry).startKey) < 0
}

type rangeTsMap struct {
	m *btree.BTree
}

func newRangeTsMap() *rangeTsMap {
	return &rangeTsMap{
		m: btree.New(16),
	}
}

func (m *rangeTsMap) set(startKey, endKey []byte, ts uint64) {
	if m.m.Get(rangeTsEntryWithKey(endKey)) == nil {
		tailTs := uint64(0)
		m.m.DescendLessOrEqual(rangeTsEntryWithKey(endKey), func(i btree.Item) bool {
			tailTs = i.(*rangeTsEntry).ts
			return false
		})
		m.m.ReplaceOrInsert(&rangeTsEntry{
			startKey: endKey,
			ts:       tailTs,
		})
	}

	entriesToDelete := make([]*rangeTsEntry, 0)
	m.m.AscendRange(rangeTsEntryWithKey(startKey), rangeTsEntryWithKey(endKey), func(i btree.Item) bool {
		entriesToDelete = append(entriesToDelete, i.(*rangeTsEntry))
		return true
	})

	for _, e := range entriesToDelete {
		m.m.Delete(e)
	}

	m.m.ReplaceOrInsert(&rangeTsEntry{
		startKey: startKey,
		ts:       ts,
	})
}

func (m *rangeTsMap) getMin(startKey, endKey []byte) uint64 {
	var ts uint64 = math.MaxUint64
	m.m.DescendLessOrEqual(rangeTsEntryWithKey(startKey), func(i btree.Item) bool {
		ts = i.(*rangeTsEntry).ts
		return false
	})
	m.m.AscendRange(rangeTsEntryWithKey(startKey), rangeTsEntryWithKey(endKey), func(i btree.Item) bool {
		thisTs := i.(*rangeTsEntry).ts
		if ts > thisTs {
			ts = thisTs
		}
		return true
	})
	return ts
}

type rangeLockEntry struct {
	startKey []byte
	endKey   []byte
	version  uint64
}

func rangeLockEntryWithKey(key []byte) *rangeLockEntry {
	return &rangeLockEntry{
		startKey: key,
	}
}

func (e *rangeLockEntry) Less(than btree.Item) bool {
	return bytes.Compare(e.startKey, than.(*rangeLockEntry).startKey) < 0
}

// RegionRangeLock is specifically used for kv client to manage exclusive region ranges. Acquiring lock will be blocked
// if part of its range is already locked. It also manages checkpoint ts of all ranges. The ranges are marked by a
// version number, which should comes from the Region's Epoch version. The version is used to compare which range is
// new and which is old if two ranges are overlapping.
type RegionRangeLock struct {
	cond              *sync.Cond
	rangeCheckpointTs *rangeTsMap
	rangeLock         *btree.BTree
}

// NewRegionRangeLock creates a new RegionRangeLock.
func NewRegionRangeLock() *RegionRangeLock {
	cond := sync.NewCond(&sync.Mutex{})

	return &RegionRangeLock{
		cond:              cond,
		rangeCheckpointTs: newRangeTsMap(),
		rangeLock:         btree.New(16),
	}
}

func (l *RegionRangeLock) getOverlappedEntries(startKey, endKey []byte) []*rangeLockEntry {
	overlappingRanges := make([]*rangeLockEntry, 0)
	l.rangeLock.DescendLessOrEqual(rangeLockEntryWithKey(startKey), func(i btree.Item) bool {
		entry := i.(*rangeLockEntry)
		if bytes.Compare(entry.startKey, startKey) < 0 &&
			bytes.Compare(startKey, entry.endKey) < 0 {
			overlappingRanges = append(overlappingRanges, entry)
		}
		return false
	})
	l.rangeLock.AscendRange(rangeLockEntryWithKey(startKey), rangeLockEntryWithKey(endKey), func(i btree.Item) bool {
		overlappingRanges = append(overlappingRanges, i.(*rangeLockEntry))
		return true
	})

	return overlappingRanges
}

func (l *RegionRangeLock) tryLockRange(startKey, endKey []byte, version uint64) LockRangeResult {
	overlappingRanges := l.getOverlappedEntries(startKey, endKey)

	if len(overlappingRanges) == 0 {
		checkpointTs := l.rangeCheckpointTs.getMin(startKey, endKey)
		l.rangeLock.ReplaceOrInsert(&rangeLockEntry{
			startKey: startKey,
			endKey:   endKey,
			version:  version,
		})

		return LockRangeResult{
			Status:       LockRangeResultSuccess,
			CheckpointTs: checkpointTs,
		}
	}

	isStale := false
	for _, r := range overlappingRanges {
		if r.version >= version {
			isStale = true
			break
		}
	}
	if isStale {
		retryRanges := make([]Span, 0)
		currentRangeStartKey := startKey

		for _, r := range overlappingRanges {
			if bytes.Compare(currentRangeStartKey, r.startKey) < 0 {
				retryRanges = append(retryRanges, Span{Start: currentRangeStartKey, End: r.startKey})
			}
			currentRangeStartKey = r.endKey
		}
		if bytes.Compare(currentRangeStartKey, endKey) < 0 {
			retryRanges = append(retryRanges, Span{Start: currentRangeStartKey, End: endKey})
		}

		return LockRangeResult{
			Status:      LockRangeResultStale,
			RetryRanges: retryRanges,
		}
	}

	return LockRangeResult{
		Status: LockRangeResultWait,
	}
}

// LockRange locks a range with specified version.
func (l *RegionRangeLock) LockRange(startKey, endKey []byte, version uint64) LockRangeResult {
	l.cond.L.Lock()

	res := l.tryLockRange(startKey, endKey, version)

	if res.Status != LockRangeResultWait {
		l.cond.L.Unlock()
		return res
	}

	res.WaitFn = func() LockRangeResult {
		c := make(chan int, 1)
		go func() {
			select {
			case <-c:
			case <-time.After(time.Second * 30):
				log.Error("cannot acquire range lock in 30 sec",
					zap.Binary("startKey", startKey),
					zap.Binary("endKey", endKey))
			}
		}()
		defer func() { c <- 1 }()

		for {
			l.cond.Wait()
			res1 := l.tryLockRange(startKey, endKey, version)
			if res1.Status != LockRangeResultWait {
				l.cond.L.Unlock()
				return res1
			}
		}
	}

	return res
}

// UnlockRange unlocks a range and update checkpointTs of the range to specivied value.
func (l *RegionRangeLock) UnlockRange(startKey, endKey []byte, version uint64, checkpointTs uint64) {
	l.cond.L.Lock()
	defer l.cond.L.Unlock()

	item := l.rangeLock.Get(rangeLockEntryWithKey(startKey))

	if item == nil {
		panic(fmt.Sprintf("unlocking a not locked range: [%v, %v), version %v",
			hex.EncodeToString(startKey), hex.EncodeToString(endKey), version))
	}

	entry := item.(*rangeLockEntry)
	if entry.version != version || bytes.Compare(entry.endKey, endKey) != 0 {
		panic(fmt.Sprintf("unlocking region not match the locked region. "+
			"Locked: [%v, %v), version %v; Unlocking: [%v, %v), %v",
			entry.startKey, entry.endKey, entry.version, startKey, endKey, version))
	}

	l.rangeLock.Delete(entry)
	l.rangeCheckpointTs.set(startKey, endKey, checkpointTs)

	l.cond.Broadcast()
}

const (
	// LockRangeResultSuccess means a LockRange operation succeeded.
	LockRangeResultSuccess = 0
	// LockRangeResultWait means a LockRange operation is blocked and should wait for it being finished.
	LockRangeResultWait = 1
	// LockRangeResultStale means a LockRange operation is rejected because of the range's version is stale.
	LockRangeResultStale = 2
)

// LockRangeResult represents the result of LockRange method of RegionRangeLock.
type LockRangeResult struct {
	Status       int
	CheckpointTs uint64
	WaitFn       func() LockRangeResult
	RetryRanges  []Span
}
