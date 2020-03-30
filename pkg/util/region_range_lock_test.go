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
	"github.com/pingcap/check"
	"math"
)

type regionRangeLockSuit struct{}

var _ = check.Suite(&regionRangeLockSuit{})

func mustLockRangeSuccess(c *check.C, l *RegionRangeLock, startKey, endKey string, version uint64) uint64 {
	res := l.LockRange([]byte(startKey), []byte(endKey), version)
	c.Assert(res.Status, check.Equals, LockRangeResultSuccess)
	return res.CheckpointTs
}

func mustLockRangeStale(c *check.C, l *RegionRangeLock, startKey, endKey string, version uint64) []Span {
	res := l.LockRange([]byte(startKey), []byte(endKey), version)
	c.Assert(res.Status, check.Equals, LockRangeResultStale)
	return res.RetryRanges
}

func mustLockRangeWait(c *check.C, l *RegionRangeLock, startKey, endKey string, version uint64) func() LockRangeResult {
	res := l.LockRange([]byte(startKey), []byte(endKey), version)
	c.Assert(res.Status, check.Equals, LockRangeResultWait)
	return res.WaitFn
}

func unlockRange(l *RegionRangeLock, startKey, endKey string, version uint64, checkpointTs uint64) {
	l.UnlockRange([]byte(startKey), []byte(endKey), version, checkpointTs)
}

func (s *regionRangeLockSuit) TestRegionRangeLock(c *check.C) {
	l := NewRegionRangeLock()
	ts := mustLockRangeSuccess(c, l, "a", "e", 1)
	c.Assert(ts, check.Equals, uint64(math.MaxUint64))

	unlockRange(l, "a", "e", 1, 100)

	ts = mustLockRangeSuccess(c, l, "a", "e", 2)
	c.Assert(ts, check.Equals, uint64(100))

	spans := mustLockRangeStale(c, l, "a", "e", 2)
	c.Assert(len(spans), check.Equals, 0)

	wait := mustLockRangeWait(c, l, "a", "h", 3)
	unlockRange(l, "a", "e", 2, 110)
	res := wait()
	c.Assert(res.Status, check.Equals, LockRangeResultSuccess)
	c.Assert(res.CheckpointTs, check.Equals, uint64(110))
}
