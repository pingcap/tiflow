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

package regionspan

import (
	"math"
	"time"

	"github.com/pingcap/check"
)

type regionRangeLockSuit struct{}

var _ = check.Suite(&regionRangeLockSuit{})

func mustSuccess(c *check.C, res LockRangeResult, expectedCheckpointTs uint64) {
	c.Assert(res.Status, check.Equals, LockRangeStatusSuccess)
	c.Assert(res.CheckpointTs, check.Equals, expectedCheckpointTs)
}

func mustStale(c *check.C, res LockRangeResult, expectedRetryRanges ...ComparableSpan) {
	c.Assert(res.Status, check.Equals, LockRangeStatusStale)
	c.Assert(res.RetryRanges, check.DeepEquals, expectedRetryRanges)
}

func mustWaitFn(c *check.C, res LockRangeResult) func() LockRangeResult {
	c.Assert(res.Status, check.Equals, LockRangeStatusWait)
	return res.WaitFn
}

func mustLockRangeSuccess(c *check.C, l *RegionRangeLock, startKey, endKey string, regionID, version uint64, expectedCheckpointTs uint64) {
	res := l.LockRange([]byte(startKey), []byte(endKey), regionID, version)
	mustSuccess(c, res, expectedCheckpointTs)
}

func mustLockRangeStale(c *check.C, l *RegionRangeLock, startKey, endKey string, regionID, version uint64, expectRetrySpans ...string) {
	res := l.LockRange([]byte(startKey), []byte(endKey), regionID, version)
	spans := make([]ComparableSpan, 0)
	for i := 0; i < len(expectRetrySpans); i += 2 {
		spans = append(spans, ComparableSpan{Start: []byte(expectRetrySpans[i]), End: []byte(expectRetrySpans[i+1])})
	}
	mustStale(c, res, spans...)
}

func mustLockRangeWait(c *check.C, l *RegionRangeLock, startKey, endKey string, regionID, version uint64) func() LockRangeResult {
	res := l.LockRange([]byte(startKey), []byte(endKey), regionID, version)
	return mustWaitFn(c, res)
}

func unlockRange(l *RegionRangeLock, startKey, endKey string, regionID, version uint64, checkpointTs uint64) {
	l.UnlockRange([]byte(startKey), []byte(endKey), regionID, version, checkpointTs)
}

func (s *regionRangeLockSuit) TestRegionRangeLock(c *check.C) {
	l := NewRegionRangeLock([]byte("a"), []byte("h"), math.MaxUint64)
	mustLockRangeSuccess(c, l, "a", "e", 1, 1, math.MaxUint64)
	unlockRange(l, "a", "e", 1, 1, 100)

	mustLockRangeSuccess(c, l, "a", "e", 1, 2, 100)
	mustLockRangeStale(c, l, "a", "e", 1, 2)
	wait := mustLockRangeWait(c, l, "a", "h", 1, 3)

	unlockRange(l, "a", "e", 1, 2, 110)
	res := wait()
	mustSuccess(c, res, 110)
	unlockRange(l, "a", "h", 1, 3, 120)
}

func (s *regionRangeLockSuit) TestRegionRangeLockStale(c *check.C) {
	l := NewRegionRangeLock([]byte("a"), []byte("z"), math.MaxUint64)
	mustLockRangeSuccess(c, l, "c", "g", 1, 10, math.MaxUint64)
	mustLockRangeSuccess(c, l, "j", "n", 2, 8, math.MaxUint64)

	mustLockRangeStale(c, l, "c", "g", 1, 10)
	mustLockRangeStale(c, l, "c", "i", 1, 9, "g", "i")
	mustLockRangeStale(c, l, "a", "z", 1, 9, "a", "c", "g", "j", "n", "z")
	mustLockRangeStale(c, l, "a", "e", 1, 9, "a", "c")
	mustLockRangeStale(c, l, "e", "h", 1, 9, "g", "h")
	mustLockRangeStale(c, l, "e", "k", 1, 9, "g", "j")
	mustLockRangeSuccess(c, l, "g", "j", 3, 1, math.MaxUint64)
	unlockRange(l, "g", "j", 3, 1, 2)
	unlockRange(l, "c", "g", 1, 10, 5)
	unlockRange(l, "j", "n", 2, 8, 8)
	mustLockRangeSuccess(c, l, "a", "z", 1, 11, 2)
	unlockRange(l, "a", "z", 1, 11, 2)
}

func (s *regionRangeLockSuit) TestRegionRangeLockLockingRegionID(c *check.C) {
	l := NewRegionRangeLock([]byte("a"), []byte("z"), math.MaxUint64)
	mustLockRangeSuccess(c, l, "c", "d", 1, 10, math.MaxUint64)

	mustLockRangeStale(c, l, "e", "f", 1, 5, "e", "f")
	mustLockRangeStale(c, l, "e", "f", 1, 10, "e", "f")
	wait := mustLockRangeWait(c, l, "e", "f", 1, 11)
	unlockRange(l, "c", "d", 1, 10, 10)
	mustSuccess(c, wait(), math.MaxUint64)
	// Now ["e", "f") is locked by region 1 at version 11 and ts 11.

	mustLockRangeSuccess(c, l, "g", "h", 2, 10, math.MaxUint64)
	wait = mustLockRangeWait(c, l, "g", "h", 1, 12)
	ch := make(chan LockRangeResult, 1)
	go func() {
		ch <- wait()
	}()
	unlockRange(l, "g", "h", 2, 10, 20)
	// Locking should still be blocked because the regionID 1 is still locked.
	select {
	case <-ch:
		c.Fatalf("locking finished unexpectedly")
	case <-time.After(time.Millisecond * 50):
	}

	unlockRange(l, "e", "f", 1, 11, 11)
	res := <-ch
	// CheckpointTS calculation should still be based on range and do not consider the regionID. So
	// the result's checkpointTs should be 20 from of range ["g", "h"), instead of 11 from min(11, 20).
	mustSuccess(c, res, 20)
	unlockRange(l, "g", "h", 1, 12, 30)
}

func (s *regionRangeLockSuit) TestRangeTsMap(c *check.C) {
	m := NewRangeTsMap([]byte("a"), []byte("z"), math.MaxUint64)

	mustGetMin := func(startKey, endKey string, expectedTs uint64) {
		ts := m.GetMin([]byte(startKey), []byte(endKey))
		c.Assert(ts, check.Equals, expectedTs)
	}
	set := func(startKey, endKey string, ts uint64) {
		m.Set([]byte(startKey), []byte(endKey), ts)
	}

	mustGetMin("a", "z", math.MaxUint64)
	set("b", "e", 100)
	mustGetMin("a", "z", 100)
	mustGetMin("b", "e", 100)
	mustGetMin("a", "c", 100)
	mustGetMin("d", "f", 100)
	mustGetMin("a", "b", math.MaxUint64)
	mustGetMin("e", "f", math.MaxUint64)
	mustGetMin("a", "b\x00", 100)

	set("d", "g", 80)
	mustGetMin("d", "g", 80)
	mustGetMin("a", "z", 80)
	mustGetMin("d", "e", 80)
	mustGetMin("a", "d", 100)

	set("c", "f", 120)
	mustGetMin("c", "f", 120)
	mustGetMin("c", "d", 120)
	mustGetMin("d", "e", 120)
	mustGetMin("e", "f", 120)
	mustGetMin("b", "e", 100)
	mustGetMin("a", "z", 80)

	set("c", "f", 130)
	mustGetMin("c", "f", 130)
	mustGetMin("c", "d", 130)
	mustGetMin("d", "e", 130)
	mustGetMin("e", "f", 130)
	mustGetMin("b", "e", 100)
	mustGetMin("a", "z", 80)
}
