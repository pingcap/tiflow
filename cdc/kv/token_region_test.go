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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"golang.org/x/sync/errgroup"
)

func TestRouter(t *testing.T) {
	t.Parallel()
	store := "store-1"
	limit := 10
	r := NewSizedRegionRouter(context.Background(), limit)
	for i := 0; i < limit; i++ {
		r.AddRegion(singleRegionInfo{ts: uint64(i), rpcCtx: &tikv.RPCContext{Addr: store}})
	}
	regions := make([]singleRegionInfo, 0, limit)
	// limit is less than regionScanLimitPerTable
	for i := 0; i < limit; i++ {
		select {
		case sri := <-r.Chan():
			require.Equal(t, uint64(i), sri.ts)
			r.Acquire(store)
			regions = append(regions, sri)
		default:
			t.Error("expect region info from router")
		}
	}
	require.Equal(t, limit, r.tokens[store])
	for range regions {
		r.Release(store)
	}
	require.Equal(t, 0, r.tokens[store])
}

func TestRouterWithFastConsumer(t *testing.T) {
	t.Parallel()
	testRouterWithConsumer(t, func() {})
}

func TestRouterWithSlowConsumer(t *testing.T) {
	t.Parallel()
	testRouterWithConsumer(t, func() { time.Sleep(time.Millisecond * 15) })
}

func testRouterWithConsumer(t *testing.T, funcDoSth func()) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := "store-1"
	limit := 20
	r := NewSizedRegionRouter(context.Background(), limit)
	for i := 0; i < limit*2; i++ {
		r.AddRegion(singleRegionInfo{ts: uint64(i), rpcCtx: &tikv.RPCContext{Addr: store}})
	}
	received := uint64(0)
	for i := 0; i < regionRouterChanSize; i++ {
		<-r.Chan()
		atomic.AddUint64(&received, 1)
		r.Acquire(store)
	}

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return r.Run(ctx)
	})

	wg.Go(func() error {
		for i := 0; i < regionRouterChanSize; i++ {
			r.Release(store)
		}
		return nil
	})

	wg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-r.Chan():
				r.Acquire(store)
				atomic.AddUint64(&received, 1)
				r.Release(store)
				funcDoSth()
				if atomic.LoadUint64(&received) == uint64(limit*4) {
					cancel()
				}
			}
		}
	})

	for i := 0; i < limit*2; i++ {
		r.AddRegion(singleRegionInfo{ts: uint64(i), rpcCtx: &tikv.RPCContext{Addr: store}})
	}

	err := wg.Wait()
	require.Equal(t, context.Canceled, errors.Cause(err))
	require.Equal(t, 0, r.tokens[store])
}

func TestRouterWithMultiStores(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storeN := 5
	stores := make([]string, 0, storeN)
	for i := 0; i < storeN; i++ {
		stores = append(stores, fmt.Sprintf("store-%d", i))
	}
	limit := 20
	r := NewSizedRegionRouter(context.Background(), limit)

	for _, store := range stores {
		for j := 0; j < limit*2; j++ {
			r.AddRegion(singleRegionInfo{ts: uint64(j), rpcCtx: &tikv.RPCContext{Addr: store}})
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
				case <-r.Chan():
					r.Acquire(store)
					atomic.AddUint64(&received, 1)
					r.Release(store)
					if atomic.LoadUint64(&received) == uint64(limit*4*storeN) {
						cancel()
					}
				}
			}
		})
	}

	for _, store := range stores {
		for i := 0; i < limit*2; i++ {
			r.AddRegion(singleRegionInfo{ts: uint64(i), rpcCtx: &tikv.RPCContext{Addr: store}})
		}
	}

	err := wg.Wait()
	require.Equal(t, context.Canceled, errors.Cause(err))
	for _, store := range stores {
		require.Equal(t, 0, r.tokens[store])
	}
}
