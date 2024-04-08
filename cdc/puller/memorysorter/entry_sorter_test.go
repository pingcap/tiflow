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

package memorysorter

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestEntrySorter(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		input     []*model.RawKVEntry
		watermark uint64
		expect    []*model.RawKVEntry
	}{
		{
			input: []*model.RawKVEntry{
				{CRTs: 1, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 4, OpType: model.OpTypeDelete},
				{CRTs: 2, OpType: model.OpTypeDelete},
			},
			watermark: 0,
			expect: []*model.RawKVEntry{
				{CRTs: 0, OpType: model.OpTypeWatermark},
			},
		},
		{
			input: []*model.RawKVEntry{
				{CRTs: 3, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 5, OpType: model.OpTypePut},
			},
			watermark: 3,
			expect: []*model.RawKVEntry{
				{CRTs: 1, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypeDelete},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 3, OpType: model.OpTypePut},
				{CRTs: 3, OpType: model.OpTypeWatermark},
			},
		},
		{
			input:     []*model.RawKVEntry{},
			watermark: 3,
			expect:    []*model.RawKVEntry{{CRTs: 3, OpType: model.OpTypeWatermark}},
		},
		{
			input: []*model.RawKVEntry{
				{CRTs: 7, OpType: model.OpTypePut},
			},
			watermark: 6,
			expect: []*model.RawKVEntry{
				{CRTs: 4, OpType: model.OpTypeDelete},
				{CRTs: 5, OpType: model.OpTypePut},
				{CRTs: 6, OpType: model.OpTypeWatermark},
			},
		},
		{
			input:     []*model.RawKVEntry{{CRTs: 7, OpType: model.OpTypeDelete}},
			watermark: 6,
			expect: []*model.RawKVEntry{
				{CRTs: 6, OpType: model.OpTypeWatermark},
			},
		},
		{
			input:     []*model.RawKVEntry{{CRTs: 7, OpType: model.OpTypeDelete}},
			watermark: 8,
			expect: []*model.RawKVEntry{
				{CRTs: 7, OpType: model.OpTypeDelete},
				{CRTs: 7, OpType: model.OpTypeDelete},
				{CRTs: 7, OpType: model.OpTypePut},
				{CRTs: 8, OpType: model.OpTypeWatermark},
			},
		},
		{
			input:     []*model.RawKVEntry{},
			watermark: 15,
			expect: []*model.RawKVEntry{
				{CRTs: 15, OpType: model.OpTypeWatermark},
			},
		},
	}
	es := NewEntrySorter(model.ChangeFeedID4Test("test", "test-cf"))
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
		es.AddEntry(ctx, model.NewWatermarkPolymorphicEvent(0, tc.watermark))
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
	es := NewEntrySorter(model.ChangeFeedID4Test("test", "test-cf"))
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
		for watermark := uint64(1); watermark <= maxTs; watermark += 400 {
			var opType model.OpType
			if rand.Intn(2) == 0 {
				opType = model.OpTypePut
			} else {
				opType = model.OpTypeDelete
			}
			for i := 0; i < 1000; i++ {
				entry := &model.RawKVEntry{
					CRTs:   uint64(int64(watermark) + rand.Int63n(int64(maxTs-watermark))),
					OpType: opType,
				}
				es.AddEntry(ctx, model.NewPolymorphicEvent(entry))
			}
			es.AddEntry(ctx, model.NewWatermarkPolymorphicEvent(0, watermark))
		}
		es.AddEntry(ctx, model.NewWatermarkPolymorphicEvent(0, maxTs))
	}()
	var lastTs uint64
	var watermark uint64
	lastOpType := model.OpTypePut
	for entry := range es.Output() {
		require.GreaterOrEqual(t, entry.CRTs, lastTs)
		require.Greater(t, entry.CRTs, watermark)
		if lastOpType == model.OpTypePut && entry.RawKV.OpType == model.OpTypeDelete {
			require.Greater(t, entry.CRTs, lastTs)
		}
		lastTs = entry.CRTs
		lastOpType = entry.RawKV.OpType
		if entry.IsResolved() {
			watermark = entry.CRTs
		}
		if watermark == maxTs {
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
					OpType: model.OpTypeWatermark,
				},
			},
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeWatermark,
				},
			},
			false,
		},
		{
			3,
			&model.PolymorphicEvent{
				CRTs: 2,
				RawKV: &model.RawKVEntry{
					OpType: model.OpTypeWatermark,
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
					OpType: model.OpTypeWatermark,
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
				OpType: model.OpTypeWatermark,
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
				OpType: model.OpTypeWatermark,
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

func BenchmarkSorter(b *testing.B) {
	es := NewEntrySorter(model.ChangeFeedID4Test("test", "test-cf"))
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
		for watermark := uint64(1); watermark <= maxTs; watermark += 400 {
			var opType model.OpType
			if rand.Intn(2) == 0 {
				opType = model.OpTypePut
			} else {
				opType = model.OpTypeDelete
			}
			for i := 0; i < 100000; i++ {
				entry := &model.RawKVEntry{
					CRTs:   uint64(int64(watermark) + rand.Int63n(1000)),
					OpType: opType,
				}
				es.AddEntry(ctx, model.NewPolymorphicEvent(entry))
			}
			es.AddEntry(ctx, model.NewWatermarkPolymorphicEvent(0, watermark))
		}
		es.AddEntry(ctx, model.NewWatermarkPolymorphicEvent(0, maxTs))
	}()
	var watermark uint64
	for entry := range es.Output() {
		if entry.IsResolved() {
			watermark = entry.CRTs
		}
		if watermark == maxTs {
			break
		}
	}
	cancel()
	wg.Wait()
}
