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

package keyspan

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestRegionCountSplitSpan(t *testing.T) {
	t.Parallel()

	cache := NewMockRegionCache()
	cache.regions.ReplaceOrInsert(tablepb.Span{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")}, 1)
	cache.regions.ReplaceOrInsert(tablepb.Span{StartKey: []byte("t1_1"), EndKey: []byte("t1_2")}, 2)
	cache.regions.ReplaceOrInsert(tablepb.Span{StartKey: []byte("t1_2"), EndKey: []byte("t1_3")}, 3)
	cache.regions.ReplaceOrInsert(tablepb.Span{StartKey: []byte("t1_3"), EndKey: []byte("t1_4")}, 4)
	cache.regions.ReplaceOrInsert(tablepb.Span{StartKey: []byte("t1_4"), EndKey: []byte("t2_2")}, 5)
	cache.regions.ReplaceOrInsert(tablepb.Span{StartKey: []byte("t2_2"), EndKey: []byte("t2_3")}, 6)

	cases := []struct {
		totalCaptures int
		span          tablepb.Span
		expectSpans   []tablepb.Span
	}{
		{
			totalCaptures: 7,
			span:          tablepb.Span{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			expectSpans: []tablepb.Span{
				{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t1_1")},   // 1 region
				{TableID: 1, StartKey: []byte("t1_1"), EndKey: []byte("t1_2")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_2"), EndKey: []byte("t1_3")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_3"), EndKey: []byte("t1_4")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_4"), EndKey: []byte("t2")},   // 1 region
			},
		},
		{
			totalCaptures: 6,
			span:          tablepb.Span{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			expectSpans: []tablepb.Span{
				{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t1_1")},   // 1 region
				{TableID: 1, StartKey: []byte("t1_1"), EndKey: []byte("t1_2")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_2"), EndKey: []byte("t1_3")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_3"), EndKey: []byte("t1_4")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_4"), EndKey: []byte("t2")},   // 1 region
			},
		},
		{
			totalCaptures: 5,
			span:          tablepb.Span{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			expectSpans: []tablepb.Span{
				{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t1_1")},   // 1 region
				{TableID: 1, StartKey: []byte("t1_1"), EndKey: []byte("t1_2")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_2"), EndKey: []byte("t1_3")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_3"), EndKey: []byte("t1_4")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_4"), EndKey: []byte("t2")},   // 1 region
			},
		},
		{
			totalCaptures: 4,
			span:          tablepb.Span{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			expectSpans: []tablepb.Span{
				{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t1_2")},   // 2 region
				{TableID: 1, StartKey: []byte("t1_2"), EndKey: []byte("t1_3")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_3"), EndKey: []byte("t1_4")}, // 1 region
				{TableID: 1, StartKey: []byte("t1_4"), EndKey: []byte("t2")},   // 1 region
			},
		},
		{
			totalCaptures: 3,
			span:          tablepb.Span{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			expectSpans: []tablepb.Span{
				{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t1_2")},   // 2 region
				{TableID: 1, StartKey: []byte("t1_2"), EndKey: []byte("t1_4")}, // 2 region
				{TableID: 1, StartKey: []byte("t1_4"), EndKey: []byte("t2")},   // 1 region
			},
		},
		{
			totalCaptures: 2,
			span:          tablepb.Span{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			expectSpans: []tablepb.Span{
				{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t1_3")}, // 3 region
				{TableID: 1, StartKey: []byte("t1_3"), EndKey: []byte("t2")}, // 2 region
			},
		},
		{
			totalCaptures: 1,
			span:          tablepb.Span{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			expectSpans: []tablepb.Span{
				{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")}, // 5 region
			},
		},
	}

	for i, cs := range cases {
		splitter := newRegionCountSplitter(model.ChangeFeedID{}, cache)
		cfg := &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			RegionThreshold:        1,
		}
		spans := splitter.split(context.Background(), cs.span, cs.totalCaptures, cfg)
		require.Equalf(t, cs.expectSpans, spans, "%d %s", i, &cs.span)
	}
}

func TestRegionCountEvenlySplitSpan(t *testing.T) {
	t.Parallel()

	cache := NewMockRegionCache()
	totalRegion := 1000
	for i := 0; i < totalRegion; i++ {
		cache.regions.ReplaceOrInsert(tablepb.Span{
			StartKey: []byte(fmt.Sprintf("t1_%09d", i)),
			EndKey:   []byte(fmt.Sprintf("t1_%09d", i+1)),
		}, uint64(i+1))
	}

	cases := []struct {
		totalCaptures  int
		expectSpansMin int
		expectSpansMax int
	}{
		{
			totalCaptures:  0,
			expectSpansMin: 1000,
			expectSpansMax: 1000,
		},
		{
			totalCaptures:  1,
			expectSpansMin: 1000,
			expectSpansMax: 1000,
		},
		{
			totalCaptures:  3,
			expectSpansMin: 333,
			expectSpansMax: 334,
		},
		{
			totalCaptures:  7,
			expectSpansMin: 142,
			expectSpansMax: 143,
		},
		{
			totalCaptures:  999,
			expectSpansMin: 1,
			expectSpansMax: 2,
		},
		{
			totalCaptures:  1000,
			expectSpansMin: 1,
			expectSpansMax: 1,
		},
		{
			totalCaptures:  2000,
			expectSpansMin: 1,
			expectSpansMax: 1,
		},
	}
	for i, cs := range cases {
		splitter := newRegionCountSplitter(model.ChangeFeedID{}, cache)
		cfg := &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			RegionThreshold:        1,
		}
		spans := splitter.split(
			context.Background(),
			tablepb.Span{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")},
			cs.totalCaptures,
			cfg,
		)
		if cs.totalCaptures == 0 {
			require.Equalf(t, 1, len(spans), "%d %v", i, cs)
		} else if cs.totalCaptures <= 1000 {
			require.Equalf(t, cs.totalCaptures, len(spans), "%d %v", i, cs)
		} else {
			require.Equalf(t, 1000, len(spans), "%d %v", i, cs)
		}

		for _, span := range spans {
			start, end := 0, 1000
			if len(span.StartKey) > len("t1") {
				_, err := fmt.Sscanf(string(span.StartKey), "t1_%d", &start)
				require.Nil(t, err, "%d %v %s", i, cs, span.StartKey)
			}
			if len(span.EndKey) > len("t2") {
				_, err := fmt.Sscanf(string(span.EndKey), "t1_%d", &end)
				require.Nil(t, err, "%d %v %s", i, cs, span.EndKey)
			}
			require.GreaterOrEqual(t, end-start, cs.expectSpansMin, "%d %v", i, cs)
			require.LessOrEqual(t, end-start, cs.expectSpansMax, "%d %v", i, cs)
		}
	}
}

func TestSplitSpanRegionOutOfOrder(t *testing.T) {
	t.Parallel()

	cache := NewMockRegionCache()
	cache.regions.ReplaceOrInsert(tablepb.Span{StartKey: []byte("t1_0"), EndKey: []byte("t1_1")}, 1)
	cache.regions.ReplaceOrInsert(tablepb.Span{StartKey: []byte("t1_1"), EndKey: []byte("t1_4")}, 2)
	cache.regions.ReplaceOrInsert(tablepb.Span{StartKey: []byte("t1_2"), EndKey: []byte("t1_3")}, 3)

	splitter := newRegionCountSplitter(model.ChangeFeedID{}, cache)
	cfg := &config.ChangefeedSchedulerConfig{
		EnableTableAcrossNodes: true,
		RegionThreshold:        1,
	}
	span := tablepb.Span{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")}
	spans := splitter.split(context.Background(), span, 1, cfg)
	require.Equal(
		t, []tablepb.Span{{TableID: 1, StartKey: []byte("t1"), EndKey: []byte("t2")}}, spans)
}
