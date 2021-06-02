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
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

type tokenRegionSuite struct {
}

var _ = check.Suite(&tokenRegionSuite{})

func (s *tokenRegionSuite) TestRouter(c *check.C) {
	defer testleak.AfterTest(c)()
	limit := 10
	metric := prometheus.NewGauge(prometheus.GaugeOpts{})
	r := NewSizedRegionRouter(limit, metric)
	for i := 0; i < limit; i++ {
		r.AddRegion(singleRegionInfo{ts: uint64(i)})
	}
	regions := make([]singleRegionInfo, 0, limit)
	// limit is less than regionScanLimitPerTable
	for i := 0; i < limit; i++ {
		select {
		case sri := <-r.Chan():
			c.Assert(sri.ts, check.Equals, uint64(i))
			r.UseToken()
			regions = append(regions, sri)
		default:
			c.Error("expect region info from router")
		}
	}
	c.Assert(r.tokenUsed, check.Equals, limit)
	for range regions {
		r.RevokeToken()
	}
	c.Assert(r.tokenUsed, check.Equals, 0)
}

func (s *tokenRegionSuite) TestRouterWithFastConsumer(c *check.C) {
	defer testleak.AfterTest(c)()
	s.testRouterWithConsumer(c, func() {})
}

func (s *tokenRegionSuite) TestRouterWithSlowConsumer(c *check.C) {
	defer testleak.AfterTest(c)()
	s.testRouterWithConsumer(c, func() { time.Sleep(time.Millisecond * 15) })
}

func (s *tokenRegionSuite) testRouterWithConsumer(c *check.C, funcDoSth func()) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limit := 20
	metric := prometheus.NewGauge(prometheus.GaugeOpts{})
	r := NewSizedRegionRouter(limit, metric)
	for i := 0; i < limit*2; i++ {
		r.AddRegion(singleRegionInfo{ts: uint64(i)})
	}
	received := uint64(0)
	for i := 0; i < regionRouterChanSize; i++ {
		<-r.Chan()
		atomic.AddUint64(&received, 1)
		r.UseToken()
	}

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return r.Run(ctx)
	})

	wg.Go(func() error {
		for i := 0; i < regionRouterChanSize; i++ {
			r.RevokeToken()
		}
		return nil
	})

	wg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-r.Chan():
				r.UseToken()
				atomic.AddUint64(&received, 1)
				r.RevokeToken()
				funcDoSth()
				if atomic.LoadUint64(&received) == uint64(limit*4) {
					cancel()
				}
			}
		}
	})

	for i := 0; i < limit*2; i++ {
		r.AddRegion(singleRegionInfo{ts: uint64(i)})
	}

	err := wg.Wait()
	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	c.Assert(r.tokenUsed, check.Equals, 0)
}
