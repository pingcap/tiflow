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

func mustLockRangeSuccess(c *check.C, l *RegionRangeLock, startKey, endKey string, version uint64, expectedCheckpointTs uint64) {
	res := l.LockRange([]byte(startKey), []byte(endKey), 1, version)
	mustSuccess(c, res, expectedCheckpointTs)
}

func mustLockRangeStale(c *check.C, l *RegionRangeLock, startKey, endKey string, version uint64, expectRetrySpans ...string) {
	res := l.LockRange([]byte(startKey), []byte(endKey), 1, version)
	spans := make([]ComparableSpan, 0)
	for i := 0; i < len(expectRetrySpans); i += 2 {
		spans = append(spans, ComparableSpan{Start: []byte(expectRetrySpans[i]), End: []byte(expectRetrySpans[i+1])})
	}
	mustStale(c, res, spans...)
}

func mustLockRangeWait(c *check.C, l *RegionRangeLock, startKey, endKey string, version uint64) func() LockRangeResult {
	res := l.LockRange([]byte(startKey), []byte(endKey), 1, version)
	return mustWaitFn(c, res)
}

func unlockRange(l *RegionRangeLock, startKey, endKey string, version uint64, checkpointTs uint64) {
	l.UnlockRange([]byte(startKey), []byte(endKey), version, checkpointTs)
}

func (s *regionRangeLockSuit) TestRegionRangeLock(c *check.C) {
	l := NewRegionRangeLock()
	mustLockRangeSuccess(c, l, "a", "e", 1, math.MaxUint64)
	unlockRange(l, "a", "e", 1, 100)

	mustLockRangeSuccess(c, l, "a", "e", 2, 100)
	mustLockRangeStale(c, l, "a", "e", 2)
	wait := mustLockRangeWait(c, l, "a", "h", 3)

	unlockRange(l, "a", "e", 2, 110)
	res := wait()
	mustSuccess(c, res, 110)
	unlockRange(l, "a", "h", 3, 120)
}

func (s *regionRangeLockSuit) TestRegionRangeLockStale(c *check.C) {
	l := NewRegionRangeLock()
	mustLockRangeSuccess(c, l, "c", "g", 10, math.MaxUint64)
	mustLockRangeSuccess(c, l, "j", "n", 8, math.MaxUint64)

	mustLockRangeStale(c, l, "c", "g", 10)
	mustLockRangeStale(c, l, "c", "i", 9, "g", "i")
	mustLockRangeStale(c, l, "a", "z", 9, "a", "c", "g", "j", "n", "z")
	mustLockRangeStale(c, l, "a", "e", 9, "a", "c")
	mustLockRangeStale(c, l, "e", "h", 9, "g", "h")
	mustLockRangeStale(c, l, "e", "k", 9, "g", "j")
	mustLockRangeSuccess(c, l, "g", "j", 1, math.MaxUint64)
	unlockRange(l, "g", "j", 1, 2)
	unlockRange(l, "c", "g", 10, 5)
	unlockRange(l, "j", "n", 8, 8)
	mustLockRangeSuccess(c, l, "a", "z", 11, 2)
	unlockRange(l, "a", "z", 11, 2)
}

func (s *regionRangeLockSuit) TestRangeTsMap(c *check.C) {
	m := NewRangeTsMap()

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
