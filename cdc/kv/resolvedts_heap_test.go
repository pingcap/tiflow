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
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type rtsHeapSuite struct {
}

var _ = check.Suite(&rtsHeapSuite{})

func (s *rtsHeapSuite) TestResolvedTsManager(c *check.C) {
	defer testleak.AfterTest(c)()
	mgr := newRegionTsManager()
	initRegions := []*regionTsInfo{
		{regionID: 102, ts: tsItem{resolvedTs: 1040}},
		{regionID: 100, ts: tsItem{resolvedTs: 1000}},
		{regionID: 101, ts: tsItem{resolvedTs: 1020}},
	}
	for _, rts := range initRegions {
		mgr.Upsert(rts)
	}
	c.Assert(mgr.Len(), check.Equals, 3)
	rts := mgr.Pop()
	c.Assert(rts, check.DeepEquals, &regionTsInfo{regionID: 100, ts: newResolvedTsItem(1000), index: -1})

	// resolved ts is not updated
	mgr.Upsert(rts)
	rts = mgr.Pop()
	c.Assert(rts, check.DeepEquals, &regionTsInfo{regionID: 100, ts: newResolvedTsItem(1000), index: -1})

	// resolved ts updated
	rts.ts.resolvedTs = 1001
	mgr.Upsert(rts)
	mgr.Upsert(&regionTsInfo{regionID: 100, ts: newResolvedTsItem(1100)})

	rts = mgr.Pop()
	c.Assert(rts, check.DeepEquals, &regionTsInfo{regionID: 101, ts: newResolvedTsItem(1020), index: -1})
	rts = mgr.Pop()
	c.Assert(rts, check.DeepEquals, &regionTsInfo{regionID: 102, ts: newResolvedTsItem(1040), index: -1})
	rts = mgr.Pop()
	c.Assert(rts, check.DeepEquals, &regionTsInfo{regionID: 100, ts: newResolvedTsItem(1100), index: -1})
	rts = mgr.Pop()
	c.Assert(rts, check.IsNil)
}
