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

package memory

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sorter"
	"github.com/stretchr/testify/require"
)

func TestEventSorter(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		input      []*model.RawKVEntry
		resolvedTs uint64
		expect     []*model.RawKVEntry
	}{
		{
			input: []*model.RawKVEntry{
				{CRTs: 1, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 4, OpType: model.OpTypeDelete},
				{CRTs: 2, OpType: model.OpTypeDelete},
			},
			resolvedTs: 0,
			expect: []*model.RawKVEntry{
				{CRTs: 0, OpType: model.OpTypeResolved},
			},
		},
		{
			input: []*model.RawKVEntry{
				{CRTs: 3, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 5, OpType: model.OpTypePut},
			},
			resolvedTs: 3,
			expect: []*model.RawKVEntry{
				{CRTs: 1, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypeDelete},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 3, OpType: model.OpTypePut},
				{CRTs: 3, OpType: model.OpTypeResolved},
			},
		},
		{
			input:      []*model.RawKVEntry{},
			resolvedTs: 3,
			expect:     []*model.RawKVEntry{},
		},
		{
			input: []*model.RawKVEntry{
				{CRTs: 7, OpType: model.OpTypePut},
			},
			resolvedTs: 6,
			expect: []*model.RawKVEntry{
				{CRTs: 4, OpType: model.OpTypeDelete},
				{CRTs: 5, OpType: model.OpTypePut},
				{CRTs: 6, OpType: model.OpTypeResolved},
			},
		},
		{
			input:      []*model.RawKVEntry{{CRTs: 7, OpType: model.OpTypeDelete}},
			resolvedTs: 6,
			expect:     []*model.RawKVEntry{},
		},
		{
			input:      []*model.RawKVEntry{{CRTs: 7, OpType: model.OpTypeDelete}},
			resolvedTs: 8,
			expect: []*model.RawKVEntry{
				{CRTs: 7, OpType: model.OpTypeDelete},
				{CRTs: 7, OpType: model.OpTypeDelete},
				{CRTs: 7, OpType: model.OpTypePut},
				{CRTs: 8, OpType: model.OpTypeResolved},
			},
		},
		{
			input:      []*model.RawKVEntry{},
			resolvedTs: 15,
			expect: []*model.RawKVEntry{
				{CRTs: 15, OpType: model.OpTypeResolved},
			},
		},
	}

	es := New(context.Background())
	var nextToFetch sorter.Position
	for i, tc := range testCases {
		for _, entry := range tc.input {
			es.Add(0, model.NewPolymorphicEvent(entry))
		}
		es.Add(0, model.NewResolvedPolymorphicEvent(0, tc.resolvedTs))
		iter := es.FetchAllTables(nextToFetch)
		for j, expect := range tc.expect {
			event, err := iter.Next()
			require.Nil(t, err)
			require.NotNil(t, event)
			require.Equal(t, expect, event.RawKV)
			if event.IsResolved() {
				nextToFetch.CommitTs = event.CRTs + 1
			}
		}
	}
}

func TestEventSorterRandomly(t *testing.T) {
	t.Parallel()
	es := New(context.Background())

	maxTs := uint64(1000000)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for resolvedTs := uint64(1); resolvedTs <= maxTs; resolvedTs += 400 {
			var opType model.OpType
			if rand.Intn(2) == 0 {
				opType = model.OpTypePut
			} else {
				opType = model.OpTypeDelete
			}
			for i := 0; i < 1000; i++ {
				entry := &model.RawKVEntry{
					CRTs:   uint64(int64(resolvedTs) + rand.Int63n(int64(maxTs-resolvedTs))),
					OpType: opType,
				}
				es.Add(0, model.NewPolymorphicEvent(entry))
			}
			es.Add(0, model.NewResolvedPolymorphicEvent(0, resolvedTs))
		}
		es.Add(0, model.NewResolvedPolymorphicEvent(0, maxTs))
	}()

	var lastTs uint64
	var nextToFetch sorter.Position
	lastOpType := model.OpTypePut
	for {
		iter := es.FetchAllTables(nextToFetch)
		for {
			entry, _ := iter.Next()
			if entry == nil {
				break
			}

			require.GreaterOrEqual(t, entry.CRTs, lastTs)
			require.GreaterOrEqual(t, entry.CRTs, nextToFetch.CommitTs)

			if lastOpType == model.OpTypePut && entry.RawKV.OpType == model.OpTypeDelete {
				require.Greater(t, entry.CRTs, lastTs)
			}
			lastTs = entry.CRTs
			lastOpType = entry.RawKV.OpType
			if entry.IsResolved() {
				nextToFetch.CommitTs = entry.CRTs + 1
			}
		}
		if nextToFetch.CommitTs > maxTs {
			break
		}
	}

	wg.Wait()
}

func TestEventLess(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		order    int
		i        *model.PolymorphicEvent
		j        *model.PolymorphicEvent
		expected bool
	}{
		{
			0,
			&model.PolymorphicEvent{
				CRTs: 1,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypePut,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypePut,
				},
			},
			true,
		},
		{
			1,
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeDelete,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeDelete,
				},
			},
			false,
		},
		{
			2,
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeResolved,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeResolved,
				},
			},
			false,
		},
		{
			3,
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeResolved,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeDelete,
				},
			},
			false,
		},
		{
			4,
			&model.PolymorphicEvent{
				CRTs: 3,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeDelete,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeResolved,
				},
			},
			false,
		},
	}

	for i, tc := range testCases {
		require.Equal(t, tc.expected, eventLess(tc.i, tc.j), "case %d", i)
	}
}

func BenchmarkSorter(b *testing.B) {
	es := New(context.Background())
	esResolved := make(chan model.Ts, 128)
	es.OnResolve(func(_ model.TableID, ts model.Ts) { esResolved <- ts })

	maxTs := uint64(10000000)
	b.ResetTimer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for resolvedTs := uint64(1); resolvedTs <= maxTs; resolvedTs += 400 {
			var opType model.OpType
			if rand.Intn(2) == 0 {
				opType = model.OpTypePut
			} else {
				opType = model.OpTypeDelete
			}
			for i := 0; i < 100000; i++ {
				entry := &model.RawKVEntry{
					CRTs:   uint64(int64(resolvedTs) + rand.Int63n(1000)),
					OpType: opType,
				}
				es.Add(0, model.NewPolymorphicEvent(entry))
			}
			es.Add(0, model.NewResolvedPolymorphicEvent(0, resolvedTs))
		}
		es.Add(0, model.NewResolvedPolymorphicEvent(0, maxTs))
	}()

	var nextToFetch sorter.Position
	for {
		_ = <-esResolved
		iter := es.FetchAllTables(nextToFetch)
		for {
			entry, _ := iter.Next()
			if entry == nil {
				break
			}
			if entry.IsResolved() {
				nextToFetch.CommitTs = entry.CRTs + 1
			}
			if nextToFetch.CommitTs > maxTs {
				break
			}
		}
	}

	wg.Wait()
}
