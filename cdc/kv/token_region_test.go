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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/tikv/client-go/v2/tikv"
	"golang.org/x/sync/errgroup"
)

type tokenRegionSuite struct{}

var _ = check.Suite(&tokenRegionSuite{})

func (s *tokenRegionSuite) TestRouter(c *check.C) {
	defer testleak.AfterTest(c)()
	store := "store-1"
	limit := 10
	r := NewSizedRegionRouter(context.Background(), limit)
	sessionId := "session"
	for i := 0; i < limit; i++ {
		r.AddRegion(sessionId, singleRegionInfo{ts: uint64(i), rpcCtx: &tikv.RPCContext{Addr: store}})
	}
	regions := make([]singleRegionInfo, 0, limit)
	// limit is less than regionScanLimitPerTable
	for i := 0; i < limit; i++ {
		select {
		case sri := <-r.Chan(sessionId):
			c.Assert(sri.ts, check.Equals, uint64(i))
			r.Acquire(sessionId, store)
			regions = append(regions, sri)
		default:
			c.Error("expect region info from router")
		}
	}
	c.Assert(r.tokens[store], check.Equals, limit)
	for range regions {
		r.Release(sessionId, store)
	}
	c.Assert(r.tokens[store], check.Equals, 0)
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

	store := "store-1"
	limit := 20
	r := NewSizedRegionRouter(context.Background(), limit)
	sessionId := "sessionId"
	for i := 0; i < limit*2; i++ {
		r.AddRegion(sessionId, singleRegionInfo{ts: uint64(i), rpcCtx: &tikv.RPCContext{Addr: store}})
	}
	received := uint64(0)
	for i := 0; i < regionRouterChanSize; i++ {
		<-r.Chan(sessionId)
		atomic.AddUint64(&received, 1)
		r.Acquire(sessionId, store)
	}

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return r.Run(ctx)
	})

	wg.Go(func() error {
		for i := 0; i < regionRouterChanSize; i++ {
			r.Release(sessionId, store)
		}
		return nil
	})

	wg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-r.Chan(sessionId):
				r.Acquire(sessionId, store)
				atomic.AddUint64(&received, 1)
				r.Release(sessionId, store)
				funcDoSth()
				if atomic.LoadUint64(&received) == uint64(limit*4) {
					cancel()
				}
			}
		}
	})

	for i := 0; i < limit*2; i++ {
		r.AddRegion(sessionId, singleRegionInfo{ts: uint64(i), rpcCtx: &tikv.RPCContext{Addr: store}})
	}

	err := wg.Wait()
	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	c.Assert(r.tokens[store], check.Equals, 0)
}

func (s *tokenRegionSuite) TestRouterWithMultiStores(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storeN := 5
	stores := make([]string, 0, storeN)
	for i := 0; i < storeN; i++ {
		stores = append(stores, fmt.Sprintf("store-%d", i))
	}
	limit := 20
	r := NewSizedRegionRouter(context.Background(), limit)
	sessionId := "sessionId"

	for _, store := range stores {
		for j := 0; j < limit*2; j++ {
			r.AddRegion(sessionId, singleRegionInfo{ts: uint64(j), rpcCtx: &tikv.RPCContext{Addr: store}})
		}
	}
	received := uint64(0)
	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return r.Run(ctx)
	})

	for _, store := range stores {
		store := store
		wg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-r.Chan(sessionId):
					r.Acquire(sessionId, store)
					atomic.AddUint64(&received, 1)
					r.Release(sessionId, store)
					if atomic.LoadUint64(&received) == uint64(limit*4*storeN) {
						cancel()
					}
				}
			}
		})
	}

	for _, store := range stores {
		for i := 0; i < limit*2; i++ {
			r.AddRegion(sessionId, singleRegionInfo{ts: uint64(i), rpcCtx: &tikv.RPCContext{Addr: store}})
		}
	}

	err := wg.Wait()
	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	for _, store := range stores {
		c.Assert(r.tokens[store], check.Equals, 0)
	}
}
