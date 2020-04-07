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
	"math"
	"sync"
	"sync/atomic"

	"github.com/google/btree"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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

// RangeTsMap represents a map from key range to a timestamp. It supports range set and calculating min value among a
// a specified range.
type RangeTsMap struct {
	m *btree.BTree
}

// NewRangeTsMap creates a RangeTsMap.
func NewRangeTsMap() *RangeTsMap {
	return &RangeTsMap{
		m: btree.New(16),
	}
}

// Set sets the corresponding ts of the given range to the specified value.
func (m *RangeTsMap) Set(startKey, endKey []byte, ts uint64) {
	if m.m.Get(rangeTsEntryWithKey(endKey)) == nil {
		// To calculate the minimal ts, the default value is math.MaxUint64
		tailTs := uint64(math.MaxUint64)
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

// GetMin gets the min ts value among the given range. endKey must be greater than startKey.
func (m *RangeTsMap) GetMin(startKey, endKey []byte) uint64 {
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
	regionID uint64
	version  uint64
	waiters  []chan<- interface{}
}

func rangeLockEntryWithKey(key []byte) *rangeLockEntry {
	return &rangeLockEntry{
		startKey: key,
	}
}

func (e *rangeLockEntry) Less(than btree.Item) bool {
	return bytes.Compare(e.startKey, than.(*rangeLockEntry).startKey) < 0
}

var currentID uint64 = 0

func allocID() uint64 {
	return atomic.AddUint64(&currentID, 1)
}

// RegionRangeLock is specifically used for kv client to manage exclusive region ranges. Acquiring lock will be blocked
// if part of its range is already locked. It also manages checkpoint ts of all ranges. The ranges are marked by a
// version number, which should comes from the Region's Epoch version. The version is used to compare which range is
// new and which is old if two ranges are overlapping.
type RegionRangeLock struct {
	mu                sync.Mutex
	rangeCheckpointTs *RangeTsMap
	rangeLock         *btree.BTree
	// ID to identify different RegionRangeLock instances, so logs of different instances can be distinguished.
	id uint64
}

// NewRegionRangeLock creates a new RegionRangeLock.
func NewRegionRangeLock() *RegionRangeLock {
	return &RegionRangeLock{
		rangeCheckpointTs: NewRangeTsMap(),
		rangeLock:         btree.New(16),
		id:                allocID(),
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

func (l *RegionRangeLock) tryLockRange(startKey, endKey []byte, regionID, version uint64) (LockRangeResult, []<-chan interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	overlappingRanges := l.getOverlappedEntries(startKey, endKey)

	if len(overlappingRanges) == 0 {
		checkpointTs := l.rangeCheckpointTs.GetMin(startKey, endKey)
		l.rangeLock.ReplaceOrInsert(&rangeLockEntry{
			startKey: startKey,
			endKey:   endKey,
			regionID: regionID,
			version:  version,
		})

		log.Info("range locked", zap.Uint64("lockID", l.id), zap.Uint64("regionID", regionID),
			zap.String("startKey", hex.EncodeToString(startKey)), zap.String("endKey", hex.EncodeToString(endKey)))

		return LockRangeResult{
			Status:       LockRangeStatusSuccess,
			CheckpointTs: checkpointTs,
		}, nil
	}

	// Format overlapping ranges for printing log
	var overlapStr []string
	for _, r := range overlappingRanges {
		overlapStr = append(overlapStr, fmt.Sprintf("regionID: %v, ver: %v, start: %v, end: %v",
			r.regionID, r.version, hex.EncodeToString(r.startKey), hex.EncodeToString(r.endKey))) //DEBUG
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

		log.Info("tryLockRange stale", zap.Uint64("lockID", l.id), zap.Uint64("regionID", regionID),
			zap.String("startKey", hex.EncodeToString(startKey)), zap.String("endKey", hex.EncodeToString(endKey)), zap.Strings("allOverlapping", overlapStr)) //DEBUG

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
			Status:      LockRangeStatusStale,
			RetryRanges: retryRanges,
		}, nil
	}

	var signalChs []<-chan interface{}

	for _, r := range overlappingRanges {
		ch := make(chan interface{}, 1)
		signalChs = append(signalChs, ch)
		r.waiters = append(r.waiters, ch)

	}

	log.Info("lock range blocked", zap.Uint64("lockID", l.id), zap.Uint64("regionID", regionID),
		zap.String("startKey", hex.EncodeToString(startKey)), zap.String("endKey", hex.EncodeToString(endKey)), zap.Strings("blockedBy", overlapStr)) //DEBUG

	return LockRangeResult{
		Status: LockRangeStatusWait,
	}, signalChs
}

// LockRange locks a range with specified version.
func (l *RegionRangeLock) LockRange(startKey, endKey []byte, regionID, version uint64) LockRangeResult {
	res, signalChs := l.tryLockRange(startKey, endKey, regionID, version)

	if res.Status != LockRangeStatusWait {
		return res
	}

	res.WaitFn = func() LockRangeResult {
		signalChs1 := signalChs
		var res1 LockRangeResult
		for {
			for _, ch := range signalChs1 {
				<-ch
			}
			res1, signalChs1 = l.tryLockRange(startKey, endKey, regionID, version)
			if res1.Status != LockRangeStatusWait {
				return res1
			}
		}
	}

	return res
}

// UnlockRange unlocks a range and update checkpointTs of the range to specivied value.
func (l *RegionRangeLock) UnlockRange(startKey, endKey []byte, version uint64, checkpointTs uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	item := l.rangeLock.Get(rangeLockEntryWithKey(startKey))

	if item == nil {
		panic(fmt.Sprintf("unlocking a not locked range: [%v, %v), version %v",
			hex.EncodeToString(startKey), hex.EncodeToString(endKey), version))
	}

	entry := item.(*rangeLockEntry)
	if entry.version != version || !bytes.Equal(entry.endKey, endKey) {
		panic(fmt.Sprintf("unlocking region not match the locked region. "+
			"Locked: [%v, %v), version %v; Unlocking: [%v, %v), %v",
			entry.startKey, entry.endKey, entry.version, startKey, endKey, version))
	}

	for _, ch := range entry.waiters {
		ch <- nil
	}

	i := l.rangeLock.Delete(entry)
	if i == nil {
		panic("impossible (entry just get from BTree disappeared)")
	}
	l.rangeCheckpointTs.Set(startKey, endKey, checkpointTs)
	log.Info("unlocked range", zap.Uint64("lockID", l.id), zap.Uint64("regionID", entry.regionID),
		zap.String("startKey", hex.EncodeToString(startKey)), zap.String("endKey", hex.EncodeToString(endKey)))
}

const (
	// LockRangeStatusSuccess means a LockRange operation succeeded.
	LockRangeStatusSuccess = 0
	// LockRangeStatusWait means a LockRange operation is blocked and should wait for it being finished.
	LockRangeStatusWait = 1
	// LockRangeStatusStale means a LockRange operation is rejected because of the range's version is stale.
	LockRangeStatusStale = 2
)

// LockRangeResult represents the result of LockRange method of RegionRangeLock.
// If Status is LockRangeStatusSuccess, the CheckpointTs field will be the minimal checkpoint ts among the locked
// range.
// If Status is LockRangeStatusWait, it means the lock cannot be acquired immediately. WaitFn must be invoked to
// continue waiting and acquiring the lock.
// If Status is LockRangeStatusStale, it means the LockRange request is stale because there's already a overlapping
// locked range, whose version is greater or equals to the requested one.
type LockRangeResult struct {
	Status       int
	CheckpointTs uint64
	WaitFn       func() LockRangeResult
	RetryRanges  []Span
}
