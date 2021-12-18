// Copyright 2021 PingCAP, Inc.
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

package kv

import (
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type rtsHeapSuite struct {
}

var _ = check.Suite(&rtsHeapSuite{})

func checkRegionTsInfoWithoutEvTime(c *check.C, obtained, expected *regionTsInfo) {
	c.Assert(obtained.regionID, check.Equals, expected.regionID)
	c.Assert(obtained.index, check.Equals, expected.index)
	c.Assert(obtained.ts.resolvedTs, check.Equals, expected.ts.resolvedTs)
	c.Assert(obtained.ts.sortByEvTime, check.IsFalse)
}

func (s *rtsHeapSuite) TestRegionTsManagerResolvedTs(c *check.C) {
	defer testleak.AfterTest(c)()
	mgr := newRegionTsManager()
	initRegions := []*regionTsInfo{
		{regionID: 102, ts: newResolvedTsItem(1040)},
		{regionID: 100, ts: newResolvedTsItem(1000)},
		{regionID: 101, ts: newResolvedTsItem(1020)},
	}
	for _, rts := range initRegions {
		mgr.Upsert(rts)
	}
	c.Assert(mgr.Len(), check.Equals, 3)
	rts := mgr.Pop()
	checkRegionTsInfoWithoutEvTime(c, rts, &regionTsInfo{regionID: 100, ts: newResolvedTsItem(1000), index: -1})

	// resolved ts is not updated
	mgr.Upsert(rts)
	rts = mgr.Pop()
	checkRegionTsInfoWithoutEvTime(c, rts, &regionTsInfo{regionID: 100, ts: newResolvedTsItem(1000), index: -1})

	// resolved ts updated
	rts.ts.resolvedTs = 1001
	mgr.Upsert(rts)
	mgr.Upsert(&regionTsInfo{regionID: 100, ts: newResolvedTsItem(1100)})

	rts = mgr.Pop()
	checkRegionTsInfoWithoutEvTime(c, rts, &regionTsInfo{regionID: 101, ts: newResolvedTsItem(1020), index: -1})
	rts = mgr.Pop()
	checkRegionTsInfoWithoutEvTime(c, rts, &regionTsInfo{regionID: 102, ts: newResolvedTsItem(1040), index: -1})
	rts = mgr.Pop()
	checkRegionTsInfoWithoutEvTime(c, rts, &regionTsInfo{regionID: 100, ts: newResolvedTsItem(1100), index: -1})
	rts = mgr.Pop()
	c.Assert(rts, check.IsNil)
}

func (s *rtsHeapSuite) TestRegionTsManagerPenalty(c *check.C) {
	defer testleak.AfterTest(c)()
	mgr := newRegionTsManager()
	initRegions := []*regionTsInfo{
		{regionID: 100, ts: newResolvedTsItem(1000)},
	}
	for _, rts := range initRegions {
		mgr.Upsert(rts)
	}
	c.Assert(mgr.Len(), check.Equals, 1)

	// test penalty increases if resolved ts keeps unchanged
	for i := 0; i < 6; i++ {
		rts := &regionTsInfo{regionID: 100, ts: newResolvedTsItem(1000)}
		mgr.Upsert(rts)
	}
	rts := mgr.Pop()
	c.Assert(rts.ts.resolvedTs, check.Equals, uint64(1000))
	c.Assert(rts.ts.penalty, check.Equals, 6)

	// test penalty is cleared to zero if resolved ts is advanced
	mgr.Upsert(rts)
	rtsNew := &regionTsInfo{regionID: 100, ts: newResolvedTsItem(2000)}
	mgr.Upsert(rtsNew)
	rts = mgr.Pop()
	c.Assert(rts.ts.penalty, check.DeepEquals, 0)
	c.Assert(rts.ts.resolvedTs, check.DeepEquals, uint64(2000))
}

func (s *rtsHeapSuite) TestRegionTsManagerPenaltyForFallBackEvent(c *check.C) {
	defer testleak.AfterTest(c)()
	mgr := newRegionTsManager()
	initRegions := []*regionTsInfo{
		{regionID: 100, ts: newResolvedTsItem(1000)},
	}
	for _, rts := range initRegions {
		mgr.Upsert(rts)
	}
	c.Assert(mgr.Len(), check.Equals, 1)

	// test penalty increases if we meet a fallback event
	for i := 0; i < 6; i++ {
		rts := &regionTsInfo{regionID: 100, ts: newResolvedTsItem(uint64(1000 - i))}
		mgr.Upsert(rts)
	}
	rts := mgr.Pop()
	// original resolvedTs will remain unchanged
	c.Assert(rts.ts.resolvedTs, check.Equals, uint64(1000))
	c.Assert(rts.ts.penalty, check.Equals, 6)

	// test penalty is cleared to zero if resolved ts is advanced
	mgr.Upsert(rts)
	rtsNew := &regionTsInfo{regionID: 100, ts: newResolvedTsItem(2000)}
	mgr.Upsert(rtsNew)
	rts = mgr.Pop()
	c.Assert(rts.ts.penalty, check.DeepEquals, 0)
	c.Assert(rts.ts.resolvedTs, check.DeepEquals, uint64(2000))
}

func (s *rtsHeapSuite) TestRegionTsManagerEvTime(c *check.C) {
	defer testleak.AfterTest(c)()
	mgr := newRegionTsManager()
	initRegions := []*regionTsInfo{
		{regionID: 100, ts: newEventTimeItem()},
		{regionID: 101, ts: newEventTimeItem()},
	}
	for _, item := range initRegions {
		mgr.Upsert(item)
	}
	info := mgr.Remove(101)
	c.Assert(info.regionID, check.Equals, uint64(101))

	ts := time.Now()
	mgr.Upsert(&regionTsInfo{regionID: 100, ts: newEventTimeItem()})
	info = mgr.Pop()
	c.Assert(info.regionID, check.Equals, uint64(100))
	c.Assert(ts.Before(info.ts.eventTime), check.IsTrue)
	c.Assert(time.Now().After(info.ts.eventTime), check.IsTrue)
	info = mgr.Pop()
	c.Assert(info, check.IsNil)
}
