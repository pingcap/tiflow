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

package model

import (
	"context"
	"sync"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type mounterSuite struct{}

var _ = check.Suite(&mounterSuite{})

func (s *mounterSuite) TestPolymorphicEvent(c *check.C) {
	defer testleak.AfterTest(c)()
	raw := &RawKVEntry{
		StartTs:  99,
		CRTs:     100,
		OpType:   OpTypePut,
		RegionID: 2,
	}
	resolved := &RawKVEntry{
		OpType: OpTypeResolved,
		CRTs:   101,
	}

	polyEvent := NewPolymorphicEvent(raw)
	c.Assert(polyEvent.RawKV, check.DeepEquals, raw)
	c.Assert(polyEvent.CRTs, check.Equals, raw.CRTs)
	c.Assert(polyEvent.StartTs, check.Equals, raw.StartTs)
	c.Assert(polyEvent.RegionID(), check.Equals, raw.RegionID)

	rawResolved := &RawKVEntry{CRTs: resolved.CRTs, OpType: OpTypeResolved}
	polyEvent = NewPolymorphicEvent(resolved)
	c.Assert(polyEvent.RawKV, check.DeepEquals, rawResolved)
	c.Assert(polyEvent.CRTs, check.Equals, resolved.CRTs)
	c.Assert(polyEvent.StartTs, check.Equals, uint64(0))
}

func (s *mounterSuite) TestPolymorphicEventPrepare(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	polyEvent := NewPolymorphicEvent(&RawKVEntry{OpType: OpTypeResolved})
	c.Assert(polyEvent.WaitPrepare(ctx), check.IsNil)

	polyEvent = NewPolymorphicEvent(&RawKVEntry{OpType: OpTypePut})
	polyEvent.SetUpFinishedChan()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := polyEvent.WaitPrepare(ctx)
		c.Assert(err, check.IsNil)
	}()
	polyEvent.PrepareFinished()
	wg.Wait()

	cctx, cancel := context.WithCancel(ctx)
	polyEvent = NewPolymorphicEvent(&RawKVEntry{OpType: OpTypePut})
	polyEvent.SetUpFinishedChan()
	cancel()
	err := polyEvent.WaitPrepare(cctx)
	c.Assert(err, check.Equals, context.Canceled)
}
