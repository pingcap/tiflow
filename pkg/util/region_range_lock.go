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
	"github.com/pingcap/tidb/store/tikv/oracle"
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

// GetMin gets the min ts value among the given range.
func (m *RangeTsMap) GetMin(startKey, endKey []byte) uint64 {
	//fmt.Printf("GetMin %+q %+q\n", startKey, endKey)
	//m.m.Ascend(func(i btree.Item) bool {
	//	e := i.(*rangeTsEntry)
	//	fmt.Printf("RangeTsMap: %+q: %v\n", e.startKey, e.ts)
	//	return true
	//})

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
	//fmt.Printf("GetMin returns %v\n", ts)
	return ts
}

func (m *RangeTsMap) String() string {
	result := ""
	m.m.Ascend(func(i btree.Item) bool {
		e := i.(*rangeTsEntry)
		result += fmt.Sprintf("%v %v %v\n", e.ts, oracle.GetTimeFromTS(e.ts), hex.EncodeToString(e.startKey))
		return true
	})
	return result
}

type rangeLockEntry struct {
	startKey []byte
	endKey   []byte
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

// RegionRangeLock is specifically used for kv client to manage exclusive region ranges. Acquiring lock will be blocked
// if part of its range is already locked. It also manages checkpoint ts of all ranges. The ranges are marked by a
// version number, which should comes from the Region's Epoch version. The version is used to compare which range is
// new and which is old if two ranges are overlapping.
type RegionRangeLock struct {
	mu                sync.Mutex
	rangeCheckpointTs *RangeTsMap
	rangeLock         *btree.BTree
}

// NewRegionRangeLock creates a new RegionRangeLock.
func NewRegionRangeLock() *RegionRangeLock {
	return &RegionRangeLock{
		rangeCheckpointTs: NewRangeTsMap(),
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

func (l *RegionRangeLock) tryLockRange(startKey, endKey []byte, version uint64) (LockRangeResult, []<-chan interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	overlappingRanges := l.getOverlappedEntries(startKey, endKey)

	if len(overlappingRanges) == 0 {
		checkpointTs := l.rangeCheckpointTs.GetMin(startKey, endKey)
		l.rangeLock.ReplaceOrInsert(&rangeLockEntry{
			startKey: startKey,
			endKey:   endKey,
			version:  version,
		})

		return LockRangeResult{
			Status:       LockRangeResultSuccess,
			CheckpointTs: checkpointTs,
		}, nil
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
		}, nil
	}

	var blockedBy []string //DEBUG
	var signalChs []<-chan interface{}

	for _, r := range overlappingRanges {
		ch := make(chan interface{}, 1)
		signalChs = append(signalChs, ch)
		r.waiters = append(r.waiters, ch)

		blockedBy = append(blockedBy, fmt.Sprintf("start: %v, end: %v", hex.EncodeToString(r.startKey), hex.EncodeToString(r.endKey))) //DEBUG
	}

	log.Debug("tryLockRange blocked", zap.String("startKey", hex.EncodeToString(startKey)), zap.String("endKey", hex.EncodeToString(endKey)), zap.Strings("blockedBy", blockedBy)) //DEBUG

	return LockRangeResult{
		Status: LockRangeResultWait,
	}, signalChs
}

// LockRange locks a range with specified version.
func (l *RegionRangeLock) LockRange(startKey, endKey []byte, version uint64) LockRangeResult {
	res, signalChs := l.tryLockRange(startKey, endKey, version)

	if res.Status != LockRangeResultWait {
		return res
	}

	res.WaitFn = func() LockRangeResult {
		// <====DEBUG====
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
		// ====DEBUG====>

		signalChs1 := signalChs
		var res1 LockRangeResult
		for {
			for _, ch := range signalChs1 {
				<-ch
			}
			res1, signalChs1 = l.tryLockRange(startKey, endKey, version)
			if res1.Status != LockRangeResultWait {
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
	if entry.version != version || bytes.Compare(entry.endKey, endKey) != 0 {
		panic(fmt.Sprintf("unlocking region not match the locked region. "+
			"Locked: [%v, %v), version %v; Unlocking: [%v, %v), %v",
			entry.startKey, entry.endKey, entry.version, startKey, endKey, version))
	}

	for _, ch := range entry.waiters {
		ch <- nil
	}

	l.rangeLock.Delete(entry)
	l.rangeCheckpointTs.Set(startKey, endKey, checkpointTs)
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
