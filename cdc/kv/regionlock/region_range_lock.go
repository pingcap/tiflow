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
// RegionRangeLock uses rangeTsMap to store unsubscribed regions.
// So `set` should be called after a region gets unlocked from RegionRangeLock.
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

// RegionRangeLock uses rangeTsMap to store unsubscribed regions.
// So `unset` should be called after a region gets locked in RegionRangeLock.
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

type rangeLockEntry struct {
	startKey []byte
	endKey   []byte
	regionID uint64
	version  uint64
	waiters  []chan<- interface{}
	state    LockedRange
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
		e.version,
		len(e.waiters))
}

// RegionRangeLock is specifically used for kv client to manage exclusive region ranges. Acquiring lock will be blocked
// if part of its range is already locked. It also manages checkpoint ts of all ranges. The ranges are marked by a
// version number, which should comes from the Region's Epoch version. The version is used to compare which range is
// new and which is old if two ranges are overlapping.
type RegionRangeLock struct {
	// ID to identify different RegionRangeLock instances, so logs of different instances can be distinguished.
	id                uint64
	totalSpan         tablepb.Span
	changefeedLogInfo string

	mu              sync.RWMutex
	rangeResolvedTs *rangeTsMap
	rangeLock       *btree.BTreeG[*rangeLockEntry]
	regionIDLock    map[uint64]*rangeLockEntry
	stopped         bool
}

// NewRegionRangeLock creates a new RegionRangeLock.
func NewRegionRangeLock(
	id uint64,
	startKey, endKey []byte, startTs uint64, changefeedLogInfo string,
) *RegionRangeLock {
	return &RegionRangeLock{
		id:                id,
		totalSpan:         tablepb.Span{StartKey: startKey, EndKey: endKey},
		changefeedLogInfo: changefeedLogInfo,
		rangeResolvedTs:   newRangeTsMap(startKey, endKey, startTs),
		rangeLock:         btree.NewG(16, rangeLockEntryLess),
		regionIDLock:      make(map[uint64]*rangeLockEntry),
	}
}

func (l *RegionRangeLock) getOverlappedEntries(startKey, endKey []byte, regionID uint64) []*rangeLockEntry {
	regionIDFound := false

	overlappingRanges := make([]*rangeLockEntry, 0)
	l.rangeLock.DescendLessOrEqual(rangeLockEntryWithKey(startKey),
		func(entry *rangeLockEntry) bool {
			if bytes.Compare(entry.startKey, startKey) < 0 &&
				bytes.Compare(startKey, entry.endKey) < 0 {
				overlappingRanges = append(overlappingRanges, entry)
				if entry.regionID == regionID {
					regionIDFound = true
				}
			}
			return false
		})
	l.rangeLock.AscendRange(rangeLockEntryWithKey(startKey), rangeLockEntryWithKey(endKey),
		func(entry *rangeLockEntry) bool {
			overlappingRanges = append(overlappingRanges, entry)
			if entry.regionID == regionID {
				regionIDFound = true
			}
			return true
		})

	// The entry with the same regionID should also be checked.
	if !regionIDFound {
		entry, ok := l.regionIDLock[regionID]
		if ok {
			overlappingRanges = append(overlappingRanges, entry)
		}
	}

	return overlappingRanges
}

func (l *RegionRangeLock) tryLockRange(startKey, endKey []byte, regionID, version uint64) (LockRangeResult, []<-chan interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return LockRangeResult{Status: LockRangeStatusCancel}, nil
	}

	overlappingEntries := l.getOverlappedEntries(startKey, endKey, regionID)

	if len(overlappingEntries) == 0 {
		resolvedTs := l.rangeResolvedTs.getMinTsInRange(startKey, endKey)
		newEntry := &rangeLockEntry{
			startKey: startKey,
			endKey:   endKey,
			regionID: regionID,
			version:  version,
		}
		newEntry.state.ResolvedTs.Store(resolvedTs)
		newEntry.state.Created = time.Now()
		l.rangeLock.ReplaceOrInsert(newEntry)
		l.regionIDLock[regionID] = newEntry

		l.rangeResolvedTs.unset(startKey, endKey)
		log.Debug("range locked",
			zap.String("changefeed", l.changefeedLogInfo),
			zap.Uint64("lockID", l.id),
			zap.Uint64("regionID", regionID),
			zap.Uint64("version", version),
			zap.Uint64("resolvedTs", resolvedTs),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)))

		return LockRangeResult{
			Status:      LockRangeStatusSuccess,
			ResolvedTs:  resolvedTs,
			LockedRange: &newEntry.state,
		}, nil
	}

	// Format overlapping ranges for printing log
	var overlapStr []string
	for _, r := range overlappingEntries {
		overlapStr = append(overlapStr, fmt.Sprintf("regionID: %v, ver: %v, start: %v, end: %v",
			r.regionID, r.version, hex.EncodeToString(r.startKey), hex.EncodeToString(r.endKey))) // DEBUG
	}

	isStale := false
	for _, r := range overlappingEntries {
		if r.version >= version {
			isStale = true
			break
		}
	}
	if isStale {
		retryRanges := make([]tablepb.Span, 0)
		currentRangeStartKey := startKey

		log.Info("try lock range staled",
			zap.String("changefeed", l.changefeedLogInfo),
			zap.Uint64("lockID", l.id), zap.Uint64("regionID", regionID),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)),
			zap.Strings("allOverlapping", overlapStr)) // DEBUG

		for _, r := range overlappingEntries {
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

	var signalChs []<-chan interface{}

	for _, r := range overlappingEntries {
		ch := make(chan interface{}, 1)
		signalChs = append(signalChs, ch)
		r.waiters = append(r.waiters, ch)

	}

	log.Info("lock range blocked",
		zap.String("changefeed", l.changefeedLogInfo),
		zap.Uint64("lockID", l.id), zap.Uint64("regionID", regionID),
		zap.String("startKey", hex.EncodeToString(startKey)),
		zap.String("endKey", hex.EncodeToString(endKey)),
		zap.Strings("blockedBy", overlapStr)) // DEBUG

	return LockRangeResult{
		Status: LockRangeStatusWait,
	}, signalChs
}

// LockRange locks a range with specified version.
func (l *RegionRangeLock) LockRange(
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
func (l *RegionRangeLock) UnlockRange(
	startKey, endKey []byte, regionID, version uint64,
	resolvedTs ...uint64,
) (drained bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, ok := l.rangeLock.Get(rangeLockEntryWithKey(startKey))
	if !ok {
		log.Panic("unlocking a not locked range",
			zap.String("changefeed", l.changefeedLogInfo),
			zap.Uint64("regionID", regionID),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)),
			zap.Uint64("version", version))
	}
	if entry.regionID != regionID {
		log.Panic("unlocked a range but regionID mismatch",
			zap.String("changefeed", l.changefeedLogInfo),
			zap.Uint64("expectedRegionID", regionID),
			zap.Uint64("foundRegionID", entry.regionID),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)))
	}
	if entry != l.regionIDLock[regionID] {
		log.Panic("range lock and region id lock mismatch when trying to unlock",
			zap.String("changefeed", l.changefeedLogInfo),
			zap.Uint64("unlockingRegionID", regionID),
			zap.String("rangeLockEntry", entry.String()),
			zap.String("regionIDLockEntry", l.regionIDLock[regionID].String()))
	}
	delete(l.regionIDLock, regionID)
	drained = l.stopped && len(l.regionIDLock) == 0

	if entry.version != version || !bytes.Equal(entry.endKey, endKey) {
		log.Panic("unlocking region doesn't match the locked region",
			zap.String("changefeed", l.changefeedLogInfo),
			zap.Uint64("regionID", regionID),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)),
			zap.Uint64("version", version),
			zap.String("foundLockEntry", entry.String()))
	}

	for _, ch := range entry.waiters {
		ch <- nil
	}

	if entry, ok = l.rangeLock.Delete(entry); !ok {
		panic("unreachable")
	}

	var newResolvedTs uint64
	if len(resolvedTs) > 0 {
		newResolvedTs = resolvedTs[0]
	} else {
		newResolvedTs = entry.state.ResolvedTs.Load()
	}

	l.rangeResolvedTs.set(startKey, endKey, newResolvedTs)
	log.Debug("unlocked range",
		zap.String("changefeed", l.changefeedLogInfo),
		zap.Uint64("lockID", l.id), zap.Uint64("regionID", entry.regionID),
		zap.Uint64("resolvedTs", newResolvedTs),
		zap.String("startKey", hex.EncodeToString(startKey)),
		zap.String("endKey", hex.EncodeToString(endKey)))
	return
}

// LockedRanges returns count of locked ranges.
func (l *RegionRangeLock) LockedRanges() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.rangeLock.Len()
}

// RefCount returns how many ranges are locked.
func (l *RegionRangeLock) RefCount() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return uint64(len(l.regionIDLock))
}

// Stop stops the instance.
func (l *RegionRangeLock) Stop() (drained bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stopped = true
	return l.stopped && len(l.regionIDLock) == 0
}

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

// LockRangeResult represents the result of LockRange method of RegionRangeLock.
// If Status is LockRangeStatusSuccess:
//   - ResolvedTs will be the minimal checkpoint ts among the locked range;
//   - LockedRange is for recording real-time state changes;
//
// If Status is LockRangeStatusWait, it means the lock cannot be acquired immediately. WaitFn must be invoked to
// continue waiting and acquiring the lock.
//
// If Status is LockRangeStatusStale, it means the LockRange request is stale because there's already a overlapping
// locked range, whose version is greater or equals to the requested one.
type LockRangeResult struct {
	Status      int
	ResolvedTs  uint64
	LockedRange *LockedRange
	WaitFn      func() LockRangeResult
	RetryRanges []tablepb.Span
}

// LockedRange is returned by `RegionRangeLock.LockRange`, which can be used to
// collect informations for the range. And collected informations can be accessed
// by iterating `RegionRangeLock`.
type LockedRange struct {
	ResolvedTs  atomic.Uint64
	Initialzied atomic.Bool
	Created     time.Time
}

// CollectLockedRangeAttrs collects locked range attributes.
func (l *RegionRangeLock) CollectLockedRangeAttrs(
	action func(regionID, version uint64, state *LockedRange, span tablepb.Span),
) (r CollectedLockedRangeAttrs) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	r.LockedRegionCount = l.rangeLock.Len()
	r.FastestRegion.ResolvedTs = 0
	r.SlowestRegion.ResolvedTs = math.MaxUint64

	lastEnd := l.totalSpan.StartKey
	l.rangeLock.Ascend(func(item *rangeLockEntry) bool {
		if action != nil {
			span := tablepb.Span{StartKey: item.startKey, EndKey: item.endKey}
			action(item.regionID, item.version, &item.state, span)
		}
		if spanz.EndCompare(lastEnd, item.startKey) < 0 {
			span := tablepb.Span{StartKey: lastEnd, EndKey: item.startKey}
			ts := l.rangeResolvedTs.getMinTsInRange(lastEnd, item.startKey)
			r.Holes = append(r.Holes, HoleAttrs{Span: span, ResolvedTs: ts})
		}
		ckpt := item.state.ResolvedTs.Load()
		if ckpt > r.FastestRegion.ResolvedTs {
			r.FastestRegion.RegionID = item.regionID
			r.FastestRegion.ResolvedTs = ckpt
			r.FastestRegion.Initialized = item.state.Initialzied.Load()
			r.FastestRegion.Created = item.state.Created
		}
		if ckpt < r.SlowestRegion.ResolvedTs {
			r.SlowestRegion.RegionID = item.regionID
			r.SlowestRegion.ResolvedTs = ckpt
			r.SlowestRegion.Initialized = item.state.Initialzied.Load()
			r.SlowestRegion.Created = item.state.Created
		}
		lastEnd = item.endKey
		return true
	})
	if spanz.EndCompare(lastEnd, l.totalSpan.EndKey) < 0 {
		span := tablepb.Span{StartKey: lastEnd, EndKey: l.totalSpan.EndKey}
		ts := l.rangeResolvedTs.getMinTsInRange(lastEnd, l.totalSpan.EndKey)
		r.Holes = append(r.Holes, HoleAttrs{Span: span, ResolvedTs: ts})
	}
	return
}

// CollectedLockedRangeAttrs returns by `RegionRangeLock.CollectedLockedRangeAttrs`.
type CollectedLockedRangeAttrs struct {
	LockedRegionCount int

	// If Holes isn't empty it means some regions are not captured at the time.
	// Those regions can just split or merge, or be transferred, or temporarily be unavailable.
	Holes []HoleAttrs

	FastestRegion LockedRangeAttrs
	SlowestRegion LockedRangeAttrs
}

// LockedRangeAttrs is like `LockedRange`, but only contains some read-only attributes.
type LockedRangeAttrs struct {
	RegionID    uint64
	ResolvedTs  uint64
	Initialized bool
	Created     time.Time
}

// HoleAttrs is used for `CollectedLockedRangeAttrs`.
type HoleAttrs struct {
	Span       tablepb.Span
	ResolvedTs uint64
}

// CalculateMinResolvedTs gets the minimum checkpoint timestamp from range lock.
func (l *RegionRangeLock) CalculateMinResolvedTs() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var minTs uint64 = math.MaxUint64
	l.rangeLock.Ascend(func(item *rangeLockEntry) bool {
		ts := item.state.ResolvedTs.Load()
		if ts < minTs {
			minTs = ts
		}
		return true
	})

	unlockedMinTs := l.rangeResolvedTs.getMinTs()
	if unlockedMinTs < minTs {
		minTs = unlockedMinTs
	}

	return minTs
}
