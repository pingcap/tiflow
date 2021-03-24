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
	"math/rand"
	"sync"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type regionWorkerSuite struct{}

var _ = check.Suite(&regionWorkerSuite{})

func (s *regionWorkerSuite) TestRegionStateManager(c *check.C) {
	defer testleak.AfterTest(c)()
	rsm := newRegionStateManager(4)

	regionID := uint64(1000)
	_, ok := rsm.getState(regionID)
	c.Assert(ok, check.IsFalse)

	rsm.setState(regionID, &regionFeedState{requestID: 2})
	state, ok := rsm.getState(regionID)
	c.Assert(ok, check.IsTrue)
	c.Assert(state.requestID, check.Equals, uint64(2))
}

func (s *regionWorkerSuite) TestRegionStateManagerThreadSafe(c *check.C) {
	defer testleak.AfterTest(c)()
	rsm := newRegionStateManager(4)
	regionCount := 100
	regionIDs := make([]uint64, regionCount)
	for i := 0; i < regionCount; i++ {
		regionID := uint64(1000 + i)
		regionIDs[i] = regionID
		rsm.setState(regionID, &regionFeedState{requestID: uint64(i + 1), lastResolvedTs: uint64(1000)})
	}

	var wg sync.WaitGroup
	concurrency := 20
	wg.Add(concurrency * 2)
	for j := 0; j < concurrency; j++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				idx := rand.Intn(regionCount)
				regionID := regionIDs[idx]
				s, ok := rsm.getState(regionID)
				c.Assert(ok, check.IsTrue)
				s.lock.RLock()
				c.Assert(s.requestID, check.Equals, uint64(idx+1))
				s.lock.RUnlock()
			}
		}()
	}
	for j := 0; j < concurrency; j++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				// simulate write less than read
				if i%5 != 0 {
					continue
				}
				regionID := regionIDs[rand.Intn(regionCount)]
				s, ok := rsm.getState(regionID)
				c.Assert(ok, check.IsTrue)
				s.lock.Lock()
				s.lastResolvedTs += 10
				s.lock.Unlock()
				rsm.setState(regionID, s)
			}
		}()
	}
	wg.Wait()

	totalResolvedTs := uint64(0)
	for _, regionID := range regionIDs {
		s, ok := rsm.getState(regionID)
		c.Assert(ok, check.IsTrue)
		c.Assert(s.lastResolvedTs, check.Greater, uint64(1000))
		totalResolvedTs += s.lastResolvedTs
	}
	// 100 regions, initial resolved ts 1000;
	// 2000 * resolved ts forward, increased by 10 each time, routine number is `concurrency`.
	c.Assert(totalResolvedTs, check.Equals, uint64(100*1000+2000*10*concurrency))
}

func (s *regionWorkerSuite) TestRegionStateManagerBucket(c *check.C) {
	defer testleak.AfterTest(c)()
	rsm := newRegionStateManager(-1)
	c.Assert(rsm.bucket, check.GreaterEqual, minRegionStateBucket)
	c.Assert(rsm.bucket, check.LessEqual, maxRegionStateBucket)

	bucket := rsm.bucket * 2
	rsm = newRegionStateManager(bucket)
	c.Assert(rsm.bucket, check.Equals, bucket)
}
