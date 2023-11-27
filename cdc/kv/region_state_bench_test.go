// Copyright 2022 PingCAP, Inc.
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
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/tikv/client-go/v2/tikv"
)

func TestSyncRegionFeedStateMapConcurrentAccess(t *testing.T) {
	t.Parallel()

	m := newSyncRegionFeedStateMap()
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			m.setByRequestID(1, &regionFeedState{sri: singleRegionInfo{lockedRange: &regionspan.LockedRange{}}})
			m.setByRequestID(2, &regionFeedState{sri: singleRegionInfo{lockedRange: &regionspan.LockedRange{}}})
			m.setByRequestID(3, &regionFeedState{sri: singleRegionInfo{lockedRange: &regionspan.LockedRange{}}})
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			m.iter(func(requestID uint64, state *regionFeedState) bool {
				state.isInitialized()
				return true
			})
		}
	}()
	time.Sleep(time.Second)
	cancel()
	wg.Wait()
}

// regionStateManagerWithSyncMap is only used for benchmark to compare the performance
// between customized `syncRegionStateMap` and `sync.Map`.
type regionStateManagerWithSyncMap struct {
	bucket int
	states []*sync.Map
}

func newRegionStateManagerWithSyncMap(bucket int) *regionStateManagerWithSyncMap {
	if bucket <= 0 {
		bucket = runtime.NumCPU()
		if bucket > maxRegionStateBucket {
			bucket = maxRegionStateBucket
		}
		if bucket < minRegionStateBucket {
			bucket = minRegionStateBucket
		}
	}
	rsm := &regionStateManagerWithSyncMap{
		bucket: bucket,
		states: make([]*sync.Map, bucket),
	}
	for i := range rsm.states {
		rsm.states[i] = new(sync.Map)
	}
	return rsm
}

func (rsm *regionStateManagerWithSyncMap) getBucket(regionID uint64) int {
	return int(regionID) % rsm.bucket
}

func (rsm *regionStateManagerWithSyncMap) getState(regionID uint64) (*regionFeedState, bool) {
	bucket := rsm.getBucket(regionID)
	if val, ok := rsm.states[bucket].Load(regionID); ok {
		return val.(*regionFeedState), true
	}
	return nil, false
}

func (rsm *regionStateManagerWithSyncMap) setState(regionID uint64, state *regionFeedState) {
	bucket := rsm.getBucket(regionID)
	rsm.states[bucket].Store(regionID, state)
}

func (rsm *regionStateManagerWithSyncMap) delState(regionID uint64) {
	bucket := rsm.getBucket(regionID)
	rsm.states[bucket].Delete(regionID)
}

func benchmarkGetRegionState(b *testing.B, bench func(b *testing.B, sm regionStateManagerInterface, count int)) {
	span := regionspan.Span{Start: []byte{}, End: regionspan.UpperBoundKey}
	state := newRegionFeedState(newSingleRegionInfo(
		tikv.RegionVerID{},
		regionspan.ToComparableSpan(span),
		&tikv.RPCContext{}), 0)
	state.sri.lockedRange = &regionspan.LockedRange{}

	regionCount := []int{100, 1000, 10000, 20000, 40000, 80000, 160000, 320000}
	for _, count := range regionCount {
		sm := newRegionStateManagerWithSyncMap(-1)
		// uncomment the code below to enable the benchmark for `syncRegionStateMap`
		// sm := newRegionStateManager(-1)
		for i := 0; i < count; i++ {
			sm.setState(uint64(i+10000), state)
		}
		b.ResetTimer()
		bench(b, sm, count)
		b.StopTimer()
	}
}

func BenchmarkGetRegionState(b *testing.B) {
	benchmarkGetRegionState(b, func(b *testing.B, sm regionStateManagerInterface, count int) {
		b.Run(fmt.Sprintf("%d regions", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for j := 0; j < count; j++ {
					_, _ = sm.getState(uint64(j + 10000))
				}
			}
		})
	})
}
