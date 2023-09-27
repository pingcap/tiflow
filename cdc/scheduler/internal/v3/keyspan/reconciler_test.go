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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/compat"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/member"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/v3/replication"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func prepareSpanCache(
	t *testing.T, ss [][3]uint8, // table ID, start key suffix, end key suffix.
) ([]tablepb.Span, *mockCache) {
	cache := NewMockRegionCache()
	allSpan := make([]tablepb.Span, 0)
	for i, s := range ss {
		tableSpan := spanz.TableIDToComparableSpan(int64(s[0]))
		span := tableSpan
		if s[1] != 0 {
			span.StartKey = append(tableSpan.StartKey, s[1])
		}
		if s[2] != 4 {
			span.EndKey = append(tableSpan.StartKey, s[2])
		}
		t.Logf("insert span %s", &span)
		cache.regions.ReplaceOrInsert(span, uint64(i+1))
		allSpan = append(allSpan, span)
	}
	return allSpan, cache
}

func TestReconcile(t *testing.T) {
	t.Parallel()
	// 1. Changefeed initialization or owner switch.
	// 2. Owner switch after some captures fail.
	// 3. Add table by DDL.
	// 4. Drop table by DDL.
	// 5. Some captures fail, does NOT affect spans.

	allSpan, cache := prepareSpanCache(t, [][3]uint8{
		{1, 0, 1}, // table ID, start key suffix, end key suffix.
		{1, 1, 2},
		{1, 2, 3},
		{1, 3, 4},
		{2, 0, 2},
		{2, 2, 4},
	})

	cfg := &config.SchedulerConfig{
		ChangefeedSettings: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			RegionThreshold:        1,
		},
	}
	compat := compat.New(cfg, map[string]*model.CaptureInfo{})
	captures := map[model.CaptureID]*member.CaptureStatus{
		"1": nil,
		"2": nil,
		"3": nil,
		"4": nil,
	}
	ctx := context.Background()

	// Test 1. changefeed initialization.
	reps := spanz.NewBtreeMap[*replication.ReplicationSet]()
	reconciler := NewReconcilerForTests(cache, cfg.ChangefeedSettings)
	currentTables := &replication.TableRanges{}
	currentTables.UpdateTables([]model.TableID{1})
	spans := reconciler.Reconcile(ctx, currentTables, reps, captures, compat)
	require.Equal(t, allSpan[:4], spans)
	require.Equal(t, allSpan[:4], reconciler.tableSpans[1].spans)
	require.Equal(t, 1, len(reconciler.tableSpans))

	// Test 1. owner switch no capture fails.
	for _, span := range reconciler.tableSpans[1].spans {
		reps.ReplaceOrInsert(span, nil)
	}
	reconciler = NewReconcilerForTests(cache, cfg.ChangefeedSettings)
	currentTables.UpdateTables([]model.TableID{1})
	spans = reconciler.Reconcile(ctx, currentTables, reps, captures, compat)
	require.Equal(t, allSpan[:4], spans)
	require.Equal(t, allSpan[:4], reconciler.tableSpans[1].spans)
	require.Equal(t, 1, len(reconciler.tableSpans))

	// Test 3. add table 2.
	currentTables.UpdateTables([]model.TableID{1, 2})
	spans = reconciler.Reconcile(ctx, currentTables, reps, captures, compat)
	spanz.Sort(spans)
	require.Equal(t, allSpan, spans)
	require.Equal(t, allSpan[:4], reconciler.tableSpans[1].spans)
	require.Equal(t, allSpan[4:], reconciler.tableSpans[2].spans)
	require.Equal(t, 2, len(reconciler.tableSpans))

	// Test 4. drop table 2.
	for _, span := range reconciler.tableSpans[2].spans {
		reps.ReplaceOrInsert(span, nil)
	}
	currentTables.UpdateTables([]model.TableID{1})
	spans = reconciler.Reconcile(ctx, currentTables, reps, captures, compat)
	require.Equal(t, allSpan[:4], spans)
	require.Equal(t, allSpan[:4], reconciler.tableSpans[1].spans)
	require.Equal(t, 1, len(reconciler.tableSpans))

	// Test 2. Owner switch and some captures fail.
	// Start span is missing.
	reps.Delete(allSpan[0])
	currentTables.UpdateTables([]model.TableID{1})
	spans = reconciler.Reconcile(ctx, currentTables, reps, captures, compat)
	spanz.Sort(spans)
	require.Equal(t, allSpan[:4], spans)
	spanz.Sort(reconciler.tableSpans[1].spans)
	require.Equal(t, allSpan[:4], reconciler.tableSpans[1].spans)
	require.Equal(t, 1, len(reconciler.tableSpans))

	// End spans is missing.
	reps.ReplaceOrInsert(allSpan[0], nil)
	reps.Delete(allSpan[3])
	currentTables.UpdateTables([]model.TableID{1})
	spans = reconciler.Reconcile(ctx, currentTables, reps, captures, compat)
	spanz.Sort(spans)
	require.Equal(t, allSpan[:4], spans)
	spanz.Sort(reconciler.tableSpans[1].spans)
	require.Equal(t, allSpan[:4], reconciler.tableSpans[1].spans)
	require.Equal(t, 1, len(reconciler.tableSpans))

	// 2 middle spans are missing.
	reps.ReplaceOrInsert(allSpan[3], nil)
	reps.Delete(allSpan[1])
	reps.Delete(allSpan[2])
	currentTables.UpdateTables([]model.TableID{1})
	spans = reconciler.Reconcile(ctx, currentTables, reps, captures, compat)
	expectedSpan := allSpan[:1]
	expectedSpan = append(expectedSpan, tablepb.Span{
		TableID:  1,
		StartKey: allSpan[1].StartKey,
		EndKey:   allSpan[2].EndKey,
	})
	expectedSpan = append(expectedSpan, allSpan[3])
	spanz.Sort(spans)
	require.Equal(t, expectedSpan, spans)
	spanz.Sort(reconciler.tableSpans[1].spans)
	require.Equal(t, expectedSpan, reconciler.tableSpans[1].spans)
	require.Equal(t, 1, len(reconciler.tableSpans))
}

func TestCompatDisable(t *testing.T) {
	t.Parallel()

	allSpan, cache := prepareSpanCache(t, [][3]uint8{
		{1, 0, 1}, // table ID, start key suffix, end key suffix.
		{1, 1, 2},
		{1, 2, 3},
		{1, 3, 4},
		{2, 0, 2},
		{2, 2, 4},
	})

	// changefeed initialization with span replication disabled.
	cfg := &config.SchedulerConfig{
		ChangefeedSettings: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			RegionThreshold:        1,
		},
	}
	cm := compat.New(cfg, map[string]*model.CaptureInfo{
		"1": {Version: "4.0.0"},
	})
	captures := map[model.CaptureID]*member.CaptureStatus{
		"1": nil,
	}
	require.False(t, cm.CheckSpanReplicationEnabled())
	ctx := context.Background()
	reps := spanz.NewBtreeMap[*replication.ReplicationSet]()
	reconciler := NewReconcilerForTests(cache, cfg.ChangefeedSettings)
	currentTables := &replication.TableRanges{}
	currentTables.UpdateTables([]model.TableID{1})
	spans := reconciler.Reconcile(ctx, currentTables, reps, captures, cm)
	require.Equal(t, []tablepb.Span{spanz.TableIDToComparableSpan(1)}, spans)
	require.Equal(t, 1, len(reconciler.tableSpans))
	reps.ReplaceOrInsert(spanz.TableIDToComparableSpan(1), nil)

	// add table 2 after span replication is enabled.
	cm.UpdateCaptureInfo(map[string]*model.CaptureInfo{
		"2": {Version: compat.SpanReplicationMinVersion.String()},
	})
	captures["2"] = nil
	require.True(t, cm.CheckSpanReplicationEnabled())
	currentTables.UpdateTables([]model.TableID{1, 2})
	spans = reconciler.Reconcile(ctx, currentTables, reps, captures, cm)
	spanz.Sort(spans)
	require.Equal(t, spanz.TableIDToComparableSpan(1), spans[0])
	require.Equal(t, allSpan[4:], spans[1:])
	require.Len(t, spans, 3)
}

func TestBatchAddRateLimit(t *testing.T) {
	t.Parallel()

	allSpan, cache := prepareSpanCache(t, [][3]uint8{
		{2, 0, 2},
		{2, 2, 3},
		{2, 3, 4},
	})

	cfg := &config.SchedulerConfig{
		ChangefeedSettings: &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: true,
			RegionThreshold:        1,
		},
	}
	compat := compat.New(cfg, map[string]*model.CaptureInfo{})
	captures := map[model.CaptureID]*member.CaptureStatus{
		"1": nil,
		"2": nil,
		"3": nil,
	}
	ctx := context.Background()

	// Add table 2.
	reps := spanz.NewBtreeMap[*replication.ReplicationSet]()
	reconciler := NewReconcilerForTests(cache, cfg.ChangefeedSettings)
	currentTables := &replication.TableRanges{}
	currentTables.UpdateTables([]model.TableID{2})
	spans := reconciler.Reconcile(ctx, currentTables, reps, captures, compat)
	require.Equal(t, allSpan, spans)
	require.Equal(t, allSpan, reconciler.tableSpans[2].spans)
	require.Equal(t, 1, len(reconciler.tableSpans))

	// Simulate batch add rate limited
	currentTables.UpdateTables([]model.TableID{2})
	spans = reconciler.Reconcile(ctx, currentTables, reps, captures, compat)
	require.Equal(t, allSpan, spans)
	require.Equal(t, allSpan, reconciler.tableSpans[2].spans)
	require.Equal(t, 1, len(reconciler.tableSpans))

	reps.ReplaceOrInsert(allSpan[0], nil)
	currentTables.UpdateTables([]model.TableID{2})
	spans = reconciler.Reconcile(ctx, currentTables, reps, captures, compat)
	require.Equal(t, allSpan, spans)
	require.Equal(t, allSpan, reconciler.tableSpans[2].spans)
	require.Equal(t, 1, len(reconciler.tableSpans))
}

func TestGetSpansNumber(t *testing.T) {
	tc := []struct {
		regionCount int
		captureNum  int
		expected    int
	}{
		{1, 10, 1},
		{100, 2, 6},
		{100, 3, 9},
		{100, 5, 20},
		{10000, 11, 100},
	}
	for _, c := range tc {
		require.Equal(t, c.expected, getSpansNumber(c.regionCount, c.captureNum))
	}
}
