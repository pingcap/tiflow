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

package memory

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestEntrySorter(t *testing.T) {
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
			expect:     []*model.RawKVEntry{{CRTs: 3, OpType: model.OpTypeResolved}},
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
			expect: []*model.RawKVEntry{
				{CRTs: 6, OpType: model.OpTypeResolved},
			},
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
	es := NewEntrySorter()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := es.Run(ctx)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()
	for _, tc := range testCases {
		for _, entry := range tc.input {
			es.AddEntry(ctx, model.NewPolymorphicEvent(entry))
		}
		es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(0, tc.resolvedTs))
		for i := 0; i < len(tc.expect); i++ {
			e := <-es.Output()
			require.Equal(t, tc.expect[i], e.RawKV)
		}
	}
	cancel()
	wg.Wait()
}

func TestEntrySorterNonBlocking(t *testing.T) {
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
			expect:     []*model.RawKVEntry{{CRTs: 3, OpType: model.OpTypeResolved}},
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
			expect: []*model.RawKVEntry{
				{CRTs: 6, OpType: model.OpTypeResolved},
			},
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
	es := NewEntrySorter()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := es.Run(ctx)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()
	for _, tc := range testCases {
		for _, entry := range tc.input {
			added, err := es.TryAddEntry(ctx, model.NewPolymorphicEvent(entry))
			require.True(t, added)
			require.Nil(t, err)
		}
		added, err := es.TryAddEntry(ctx, model.NewResolvedPolymorphicEvent(0, tc.resolvedTs))
		require.True(t, added)
		require.Nil(t, err)
		for i := 0; i < len(tc.expect); i++ {
			e := <-es.Output()
			require.Equal(t, tc.expect[i], e.RawKV)
		}
	}
	cancel()
	wg.Wait()
}

func TestEntrySorterRandomly(t *testing.T) {
	t.Parallel()
	es := NewEntrySorter()
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := es.Run(ctx)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	maxTs := uint64(1000000)
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
				es.AddEntry(ctx, model.NewPolymorphicEvent(entry))
			}
			es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(0, resolvedTs))
		}
		es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(0, maxTs))
	}()
	var lastTs uint64
	var resolvedTs uint64
	lastOpType := model.OpTypePut
	for entry := range es.Output() {
		require.GreaterOrEqual(t, entry.CRTs, lastTs)
		require.Greater(t, entry.CRTs, resolvedTs)
		if lastOpType == model.OpTypePut && entry.RawKV.OpType == model.OpTypeDelete {
			require.Greater(t, entry.CRTs, lastTs)
		}
		lastTs = entry.CRTs
		lastOpType = entry.RawKV.OpType
		if entry.IsResolved() {
			resolvedTs = entry.CRTs
		}
		if resolvedTs == maxTs {
			break
		}
	}
	cancel()
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

func TestMergeEvents(t *testing.T) {
	t.Parallel()
	events1 := []*model.PolymorphicEvent{
		{
			CRTs: 1,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeDelete,
			},
		},
		{
			CRTs: 2,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypePut,
			},
		},
		{
			CRTs: 3,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypePut,
			},
		},
		{
			CRTs: 4,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypePut,
			},
		},
		{
			CRTs: 5,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeDelete,
			},
		},
	}
	events2 := []*model.PolymorphicEvent{
		{
			CRTs: 3,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
			},
		},
		{
			CRTs: 4,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypePut,
			},
		},
		{
			CRTs: 4,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
			},
		},
		{
			CRTs: 7,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypePut,
			},
		},
		{
			CRTs: 9,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeDelete,
			},
		},
	}

	var outputResults []*model.PolymorphicEvent
	output := func(event *model.PolymorphicEvent) {
		outputResults = append(outputResults, event)
	}

	expectedResults := append(events1, events2...)
	sort.Slice(expectedResults, func(i, j int) bool {
		return eventLess(expectedResults[i], expectedResults[j])
	})

	mergeEvents(events1, events2, output)
	require.Equal(t, expectedResults, outputResults)
}

func TestEntrySorterClosed(t *testing.T) {
	t.Parallel()
	es := NewEntrySorter()
	atomic.StoreInt32(&es.closed, 1)
	added, err := es.TryAddEntry(context.TODO(), model.NewResolvedPolymorphicEvent(0, 1))
	require.False(t, added)
	require.True(t, cerror.ErrSorterClosed.Equal(err))
}

func BenchmarkSorter(b *testing.B) {
	es := NewEntrySorter()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := es.Run(ctx)
		if errors.Cause(err) != context.Canceled {
			panic(errors.Annotate(err, "unexpected error"))
		}
	}()

	maxTs := uint64(10000000)
	b.ResetTimer()
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
				es.AddEntry(ctx, model.NewPolymorphicEvent(entry))
			}
			es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(0, resolvedTs))
		}
		es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(0, maxTs))
	}()
	var resolvedTs uint64
	for entry := range es.Output() {
		if entry.IsResolved() {
			resolvedTs = entry.CRTs
		}
		if resolvedTs == maxTs {
			break
		}
	}
	cancel()
	wg.Wait()
}
