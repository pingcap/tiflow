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

package regionlock

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
)

const (
	// LockRangeStatusSuccess means a LockRange operation succeeded.
	LockRangeStatusSuccess = 0
	// LockRangeStatusWait means a LockRange operation is blocked and should wait for it being finished.
	LockRangeStatusWait = 1
	// LockRangeStatusStale means a LockRange operation is rejected because of the range's version is stale.
	LockRangeStatusStale = 2
	// LockRangeStatusCancel means a LockRange operation is cancelled.
	LockRangeStatusCancel = 3
)

// LockRangeResult represents the result of LockRange method of RangeLock.
// If Status is LockRangeStatusSuccess:
//   - LockedRangeState is for recording real-time state changes of the locked range.
//     Its ResolvedTs is the minimum resolvedTs of the range.
//
// If Status is LockRangeStatusWait, it means the lock cannot be acquired immediately. WaitFn must be invoked to
// continue waiting and acquiring the lock.
//
// If Status is LockRangeStatusStale, it means the LockRange request is stale because there's already a overlapping
// locked range, whose version is greater or equals to the requested one.
type LockRangeResult struct {
	Status           int
	LockedRangeState *LockedRangeState
	WaitFn           func() LockRangeResult

	// RetryRanges is only used when Status is LockRangeStatusStale.
	// It contains the ranges that should be retried to lock.
	RetryRanges []tablepb.Span
}

// LockedRangeState is used to access the real-time state changes of a locked range.
type LockedRangeState struct {
	ResolvedTs  atomic.Uint64
	Initialzied atomic.Bool
	Created     time.Time
}

// rangeLockEntry represents a locked range that defined by [startKey, endKey).
type rangeLockEntry struct {
	startKey      []byte
	endKey        []byte
	regionID      uint64
	regionVersion uint64
	// lockedRangeState is used to record the real-time state changes of this locked range.
	lockedRangeState LockedRangeState
	// waiterSignalChs is a list of channels that are used to
	// notify the waiter of this lock entry that the lock is released.
	waiterSignalChs []chan<- interface{}
}

func rangeLockEntryWithKey(key []byte) *rangeLockEntry {
	return &rangeLockEntry{
		startKey: key,
	}
}

func rangeLockEntryLess(a, b *rangeLockEntry) bool {
	return bytes.Compare(a.startKey, b.startKey) < 0
}

func (e *rangeLockEntry) String() string {
	return fmt.Sprintf("region %v [%v, %v), version %v, %d waiters",
		e.regionID,
		hex.EncodeToString(e.startKey),
		hex.EncodeToString(e.endKey),
		e.regionVersion,
		len(e.waiterSignalChs))
}

// RangeLock is used to ensure that a table's same range is only requested once at a time.
// Before sending a region request to TiKV, the client should lock the region's range to avoid sending another
// request to the same region. After stopping the table or removing the region, the client should unlock the range.
// It also helps calculate the resolvedTs ts of the table it belongs to.
// Note(dongmen): A table has one RangeLock, and within that RangeLock, there are multiple regionLocks for each region.
type RangeLock struct {
	// ID to identify different RangeLock instances, so logs of different instances can be distinguished.
	id uint64
	// totalSpan is the total range of the table, totalSpan = unlockedRanges + lockedRanges
	totalSpan tablepb.Span
	// changefeed is used to identify the changefeed which the RangeLock belongs to.
	changefeed string

	mu sync.RWMutex
	// unlockedRanges is used to store the resolvedTs of unlocked ranges.
	unlockedRanges *rangeTsMap
	// lockedRanges is a btree that stores all locked ranges.
	lockedRanges *btree.BTreeG[*rangeLockEntry]
	// regionIDToLockedRanges is used to quickly locate the lock entry by regionID.
	regionIDToLockedRanges map[uint64]*rangeLockEntry
	stopped                bool
}

// NewRangeLock creates a new RangeLock.
func NewRangeLock(
	id uint64,
	startKey, endKey []byte, startTs uint64, changefeedLogInfo string,
) *RangeLock {
	return &RangeLock{
		id:                     id,
		totalSpan:              tablepb.Span{StartKey: startKey, EndKey: endKey},
		changefeed:             changefeedLogInfo,
		unlockedRanges:         newRangeTsMap(startKey, endKey, startTs),
		lockedRanges:           btree.NewG(16, rangeLockEntryLess),
		regionIDToLockedRanges: make(map[uint64]*rangeLockEntry),
	}
}

// LockRange locks a range with specified version.
func (l *RangeLock) LockRange(
	ctx context.Context, startKey, endKey []byte, regionID, version uint64,
) LockRangeResult {
	res, signalChs := l.tryLockRange(startKey, endKey, regionID, version)

	if res.Status != LockRangeStatusWait {
		return res
	}

	res.WaitFn = func() LockRangeResult {
		signalChs1 := signalChs
		var res1 LockRangeResult
		for {
			for _, ch := range signalChs1 {
				select {
				case <-ctx.Done():
					return LockRangeResult{Status: LockRangeStatusCancel}
				case <-ch:
				}
			}
			res1, signalChs1 = l.tryLockRange(startKey, endKey, regionID, version)
			if res1.Status != LockRangeStatusWait {
				return res1
			}
		}
	}

	return res
}

// UnlockRange unlocks a range and update resolvedTs of the range to specified value.
// If it returns true it means it is stopped and all ranges are unlocked correctly.
func (l *RangeLock) UnlockRange(
	startKey, endKey []byte, regionID, version uint64,
	resolvedTs ...uint64,
) (drained bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, ok := l.lockedRanges.Get(rangeLockEntryWithKey(startKey))
	if !ok {
		log.Panic("unlocking a not locked range",
			zap.String("changefeed", l.changefeed),
			zap.Uint64("regionID", regionID),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)),
			zap.Uint64("version", version))
	}
	if entry.regionID != regionID {
		log.Panic("unlocked a range but regionID mismatch",
			zap.String("changefeed", l.changefeed),
			zap.Uint64("expectedRegionID", regionID),
			zap.Uint64("foundRegionID", entry.regionID),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)))
	}
	if entry != l.regionIDToLockedRanges[regionID] {
		log.Panic("range lock and region id lock mismatch when trying to unlock",
			zap.String("changefeed", l.changefeed),
			zap.Uint64("unlockingRegionID", regionID),
			zap.String("rangeLockEntry", entry.String()),
			zap.String("regionIDLockEntry", l.regionIDToLockedRanges[regionID].String()))
	}
	delete(l.regionIDToLockedRanges, regionID)
	drained = l.stopped && len(l.regionIDToLockedRanges) == 0

	if entry.regionVersion != version || !bytes.Equal(entry.endKey, endKey) {
		log.Panic("unlocking region doesn't match the locked region",
			zap.String("changefeed", l.changefeed),
			zap.Uint64("regionID", regionID),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)),
			zap.Uint64("version", version),
			zap.String("foundLockEntry", entry.String()))
	}

	for _, ch := range entry.waiterSignalChs {
		ch <- nil
	}

	if entry, ok = l.lockedRanges.Delete(entry); !ok {
		panic("unreachable")
	}

	var newResolvedTs uint64
	if len(resolvedTs) > 0 {
		newResolvedTs = resolvedTs[0]
	} else {
		newResolvedTs = entry.lockedRangeState.ResolvedTs.Load()
	}

	l.unlockedRanges.set(startKey, endKey, newResolvedTs)
	log.Debug("unlocked range",
		zap.String("changefeed", l.changefeed),
		zap.Uint64("lockID", l.id), zap.Uint64("regionID", entry.regionID),
		zap.Uint64("resolvedTs", newResolvedTs),
		zap.String("startKey", hex.EncodeToString(startKey)),
		zap.String("endKey", hex.EncodeToString(endKey)))
	return
}

// Len returns len of locked ranges.
func (l *RangeLock) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lockedRanges.Len()
}

// ResolvedTs calculates and returns the minimum resolvedTs
// of all ranges in the RangeLock.
func (l *RangeLock) ResolvedTs() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var minTs uint64 = math.MaxUint64
	l.lockedRanges.Ascend(func(item *rangeLockEntry) bool {
		ts := item.lockedRangeState.ResolvedTs.Load()
		if ts < minTs {
			minTs = ts
		}
		return true
	})

	unlockedMinTs := l.unlockedRanges.getMinTs()
	if unlockedMinTs < minTs {
		minTs = unlockedMinTs
	}

	return minTs
}

// RangeLockStatistics represents some statistics of a RangeLock.
type RangeLockStatistics struct {
	LockedRegionCount int
	// UnLockedRanges represents the unlocked ranges in the table.
	// If UnLockedRanges isn't empty, it implies that some regions were not captured at the time.
	// These regions could have been split, merged, transferred, or temporarily unavailable.
	UnLockedRanges []UnLockRangeStatistic

	FastestRegion LockedRangeStatistic
	SlowestRegion LockedRangeStatistic
}

// LockedRangeStatistic represents a locked range.
type LockedRangeStatistic struct {
	RegionID    uint64
	ResolvedTs  uint64
	Initialized bool
	Created     time.Time
}

// UnLockRangeStatistic represents a range that is unlocked.
type UnLockRangeStatistic struct {
	Span       tablepb.Span
	ResolvedTs uint64
}

// IterAll iterates all locked ranges in the RangeLock and performs the action on each locked range.
// It also returns some statistics of the RangeLock.
func (l *RangeLock) IterAll(
	action func(regionID uint64, state *LockedRangeState),
) (r RangeLockStatistics) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	r.LockedRegionCount = l.lockedRanges.Len()
	r.FastestRegion.ResolvedTs = 0
	r.SlowestRegion.ResolvedTs = math.MaxUint64

	lastEnd := l.totalSpan.StartKey
	l.lockedRanges.Ascend(func(item *rangeLockEntry) bool {
		if action != nil {
			action(item.regionID, &item.lockedRangeState)
		}

		if spanz.EndCompare(lastEnd, item.startKey) < 0 {
			span := tablepb.Span{StartKey: lastEnd, EndKey: item.startKey}
			ts := l.unlockedRanges.getMinTsInRange(lastEnd, item.startKey)
			r.UnLockedRanges = append(r.UnLockedRanges, UnLockRangeStatistic{Span: span, ResolvedTs: ts})
		}
		resolvedTs := item.lockedRangeState.ResolvedTs.Load()
		if resolvedTs > r.FastestRegion.ResolvedTs {
			r.FastestRegion.RegionID = item.regionID
			r.FastestRegion.ResolvedTs = resolvedTs
			r.FastestRegion.Initialized = item.lockedRangeState.Initialzied.Load()
			r.FastestRegion.Created = item.lockedRangeState.Created
		}
		if resolvedTs < r.SlowestRegion.ResolvedTs {
			r.SlowestRegion.RegionID = item.regionID
			r.SlowestRegion.ResolvedTs = resolvedTs
			r.SlowestRegion.Initialized = item.lockedRangeState.Initialzied.Load()
			r.SlowestRegion.Created = item.lockedRangeState.Created
		}
		lastEnd = item.endKey
		return true
	})
	if spanz.EndCompare(lastEnd, l.totalSpan.EndKey) < 0 {
		span := tablepb.Span{StartKey: lastEnd, EndKey: l.totalSpan.EndKey}
		ts := l.unlockedRanges.getMinTsInRange(lastEnd, l.totalSpan.EndKey)
		r.UnLockedRanges = append(r.UnLockedRanges, UnLockRangeStatistic{Span: span, ResolvedTs: ts})
	}
	return
}

// IterForTest iterates all locked ranges in the RangeLock and performs the action on each locked range.
// It is used for testing only.
func (l *RangeLock) IterForTest(
	action func(regionID, version uint64, state *LockedRangeState, span tablepb.Span),
) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	l.lockedRanges.Ascend(func(item *rangeLockEntry) bool {
		if action != nil {
			span := tablepb.Span{StartKey: item.startKey, EndKey: item.endKey}
			action(item.regionID, item.regionVersion, &item.lockedRangeState, span)
		}
		return true
	})
}

// Stop stops the instance.
func (l *RangeLock) Stop() (drained bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stopped = true
	return l.stopped && len(l.regionIDToLockedRanges) == 0
}

func (l *RangeLock) getOverlappedLockEntries(startKey, endKey []byte, regionID uint64) []*rangeLockEntry {
	regionIDFound := false

	overlappedLocks := make([]*rangeLockEntry, 0)
	l.lockedRanges.DescendLessOrEqual(rangeLockEntryWithKey(startKey),
		func(entry *rangeLockEntry) bool {
			if bytes.Compare(entry.startKey, startKey) < 0 &&
				bytes.Compare(startKey, entry.endKey) < 0 {
				overlappedLocks = append(overlappedLocks, entry)
				if entry.regionID == regionID {
					regionIDFound = true
				}
			}
			return false
		})
	l.lockedRanges.AscendRange(rangeLockEntryWithKey(startKey), rangeLockEntryWithKey(endKey),
		func(entry *rangeLockEntry) bool {
			overlappedLocks = append(overlappedLocks, entry)
			if entry.regionID == regionID {
				regionIDFound = true
			}
			return true
		})

	// The entry with the same regionID should also be checked.
	if !regionIDFound {
		entry, ok := l.regionIDToLockedRanges[regionID]
		if ok {
			overlappedLocks = append(overlappedLocks, entry)
		}
	}

	return overlappedLocks
}

// tryLockRange works in this way:
// 1. If the range is totally disjointed with all locked ranges, it will be directly locked.
// 2. If the range is overlapping with some locked ranges:
//   - If the current region's version is stale, it will return LockRangeStatusStale and the overlapping ranges to the caller.
//   - If the current region's version is not stale, it will return LockRangeStatusWait and the overlapping ranges to the caller,
//     and the caller should wait for the overlapping ranges to be released and retry to lock the rest of the range.
func (l *RangeLock) tryLockRange(startKey, endKey []byte, regionID, regionVersion uint64) (LockRangeResult, []<-chan interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return LockRangeResult{Status: LockRangeStatusCancel}, nil
	}

	overlappedRangeLocks := l.getOverlappedLockEntries(startKey, endKey, regionID)

	// 1. If the range is totally disjointed with all locked ranges, it will be directly locked.
	if len(overlappedRangeLocks) == 0 {
		resolvedTs := l.unlockedRanges.getMinTsInRange(startKey, endKey)
		newEntry := &rangeLockEntry{
			startKey:      startKey,
			endKey:        endKey,
			regionID:      regionID,
			regionVersion: regionVersion,
		}
		newEntry.lockedRangeState.ResolvedTs.Store(resolvedTs)
		newEntry.lockedRangeState.Created = time.Now()
		l.lockedRanges.ReplaceOrInsert(newEntry)
		l.regionIDToLockedRanges[regionID] = newEntry

		l.unlockedRanges.unset(startKey, endKey)
		log.Debug("range locked",
			zap.String("changefeed", l.changefeed),
			zap.Uint64("lockID", l.id),
			zap.Uint64("regionID", regionID),
			zap.Uint64("version", regionVersion),
			zap.Uint64("resolvedTs", resolvedTs),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)))

		return LockRangeResult{
			Status:           LockRangeStatusSuccess,
			LockedRangeState: &newEntry.lockedRangeState,
		}, nil
	}

	// Format overlapping ranges for printing log
	var overlapStr []string
	for _, r := range overlappedRangeLocks {
		overlapStr = append(overlapStr, fmt.Sprintf("regionID: %v, ver: %v, start: %v, end: %v",
			r.regionID, r.regionVersion, hex.EncodeToString(r.startKey), hex.EncodeToString(r.endKey))) // DEBUG
	}

	// Check if the current acuqiring range is stale,
	// which means there's already a locked range with a equal or greater version.
	isStale := false
	for _, rangeLock := range overlappedRangeLocks {
		if rangeLock.regionVersion >= regionVersion {
			isStale = true
			break
		}
	}
	// If the range is stale, we should return the overlapping ranges to the caller,
	// so that the caller can retry to lock the rest of the range.
	if isStale {
		retryRanges := make([]tablepb.Span, 0)
		currentRangeStartKey := startKey

		log.Info("try lock range staled",
			zap.String("changefeed", l.changefeed),
			zap.Uint64("lockID", l.id), zap.Uint64("regionID", regionID),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)),
			zap.Strings("allOverlapping", overlapStr)) // DEBUG

		for _, r := range overlappedRangeLocks {
			// Ignore the totally-disjointed range which may be added to the list because of
			// searching by regionID.
			if bytes.Compare(r.endKey, startKey) <= 0 || bytes.Compare(endKey, r.startKey) <= 0 {
				continue
			}
			// The rest should come from range searching and is sorted in increasing order, and they
			// must intersect with the current given range.
			if bytes.Compare(currentRangeStartKey, r.startKey) < 0 {
				retryRanges = append(retryRanges,
					tablepb.Span{StartKey: currentRangeStartKey, EndKey: r.startKey})
			}
			currentRangeStartKey = r.endKey
		}

		if bytes.Compare(currentRangeStartKey, endKey) < 0 {
			retryRanges = append(retryRanges,
				tablepb.Span{StartKey: currentRangeStartKey, EndKey: endKey})
		}

		return LockRangeResult{
			Status:      LockRangeStatusStale,
			RetryRanges: retryRanges,
		}, nil
	}

	var lockReleaseSignalChs []<-chan interface{}

	for _, r := range overlappedRangeLocks {
		ch := make(chan interface{}, 1)
		lockReleaseSignalChs = append(lockReleaseSignalChs, ch)
		r.waiterSignalChs = append(r.waiterSignalChs, ch)
	}

	log.Info("lock range blocked",
		zap.String("changefeed", l.changefeed),
		zap.Uint64("lockID", l.id), zap.Uint64("regionID", regionID),
		zap.String("startKey", hex.EncodeToString(startKey)),
		zap.String("endKey", hex.EncodeToString(endKey)),
		zap.Strings("blockedBy", overlapStr)) // DEBUG

	return LockRangeResult{
		Status: LockRangeStatusWait,
	}, lockReleaseSignalChs
}
