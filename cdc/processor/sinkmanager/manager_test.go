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

package sinkmanager

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

func getChangefeedInfo() *model.ChangeFeedInfo {
	return &model.ChangeFeedInfo{
		Error:   nil,
		SinkURI: "blackhole://",
		Config:  config.GetDefaultReplicaConfig(),
	}
}

// nolint:unparam
// It is ok to use the same tableID in test.
func addTableAndAddEventsToSortEngine(
	t *testing.T,
	engine engine.SortEngine,
	span tablepb.Span,
) {
	engine.AddTable(span)
	events := []*model.PolymorphicEvent{
		{
			StartTs: 1,
			CRTs:    1,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    1,
			},
			Row: genRowChangedEvent(1, 1, span),
		},
		{
			StartTs: 1,
			CRTs:    2,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    2,
			},
			Row: genRowChangedEvent(1, 2, span),
		},
		{
			StartTs: 1,
			CRTs:    3,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    3,
			},
			Row: genRowChangedEvent(1, 3, span),
		},
		{
			StartTs: 2,
			CRTs:    4,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 2,
				CRTs:    4,
			},
			Row: genRowChangedEvent(2, 4, span),
		},
		{
			CRTs: 4,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
				CRTs:   4,
			},
		},
	}
	for _, event := range events {
		engine.Add(span, event)
	}
}

func TestAddTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, _, _ := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	span := spanz.TableIDToComparableSpan(1)
	manager.AddTable(span, 1, 100)
	tableSink, ok := manager.tableSinks.Load(span)
	require.True(t, ok)
	require.NotNil(t, tableSink)
	require.Equal(t, 0, manager.sinkProgressHeap.len(), "Not started table shout not in progress heap")
	err := manager.StartTable(span, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(0x7ffffffffffbffff), tableSink.(*tableSinkWrapper).replicateTs)

	progress := manager.sinkProgressHeap.pop()
	require.Equal(t, span, progress.span)
	require.Equal(t, uint64(0), progress.nextLowerBoundPos.StartTs)
	require.Equal(t, uint64(2), progress.nextLowerBoundPos.CommitTs)
}

func TestRemoveTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, _, e := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	span := spanz.TableIDToComparableSpan(1)
	manager.AddTable(span, 1, 100)
	tableSink, ok := manager.tableSinks.Load(span)
	require.True(t, ok)
	require.NotNil(t, tableSink)
	err := manager.StartTable(span, 0)
	require.NoError(t, err)
	addTableAndAddEventsToSortEngine(t, e, span)
	manager.UpdateBarrierTs(4, nil)
	manager.UpdateReceivedSorterResolvedTs(span, 5)
	manager.schemaStorage.AdvanceResolvedTs(5)
	// Check all the events are sent to sink and record the memory usage.
	require.Eventually(t, func() bool {
		return manager.sinkMemQuota.GetUsedBytes() == 872
	}, 5*time.Second, 10*time.Millisecond)

	manager.AsyncStopTable(span)
	require.Eventually(t, func() bool {
		state, ok := manager.GetTableState(span)
		require.True(t, ok)
		return state == tablepb.TableStateStopped
	}, 5*time.Second, 10*time.Millisecond)

	manager.RemoveTable(span)

	_, ok = manager.tableSinks.Load(span)
	require.False(t, ok)
	require.Equal(t, uint64(0), manager.sinkMemQuota.GetUsedBytes(), "After remove table, the memory usage should be 0.")
}

func TestGenerateTableSinkTaskWithBarrierTs(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, _, e := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	span := spanz.TableIDToComparableSpan(1)
	manager.AddTable(span, 1, 100)
	addTableAndAddEventsToSortEngine(t, e, span)
	manager.UpdateBarrierTs(4, nil)
	manager.UpdateReceivedSorterResolvedTs(span, 5)
	manager.schemaStorage.AdvanceResolvedTs(5)
	err := manager.StartTable(span, 0)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		tableSink, ok := manager.tableSinks.Load(span)
		require.True(t, ok)
		checkpointTS := tableSink.(*tableSinkWrapper).getCheckpointTs()
		return checkpointTS.ResolvedMark() == 4
	}, 5*time.Second, 10*time.Millisecond)
}

func TestGenerateTableSinkTaskWithResolvedTs(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, _, e := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	span := spanz.TableIDToComparableSpan(1)
	manager.AddTable(span, 1, 100)
	addTableAndAddEventsToSortEngine(t, e, span)
	// This would happen when the table just added to this node and redo log is enabled.
	// So there is possibility that the resolved ts is smaller than the global barrier ts.
	manager.UpdateBarrierTs(4, nil)
	manager.UpdateReceivedSorterResolvedTs(span, 3)
	manager.schemaStorage.AdvanceResolvedTs(4)
	err := manager.StartTable(span, 0)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		tableSink, ok := manager.tableSinks.Load(span)
		require.True(t, ok)
		checkpointTS := tableSink.(*tableSinkWrapper).getCheckpointTs()
		return checkpointTS.ResolvedMark() == 3
	}, 5*time.Second, 10*time.Millisecond)
}

func TestGetTableStatsToReleaseMemQuota(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, _, e := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	span := spanz.TableIDToComparableSpan(1)
	manager.AddTable(span, 1, 100)
	addTableAndAddEventsToSortEngine(t, e, span)

	manager.UpdateBarrierTs(4, nil)
	manager.UpdateReceivedSorterResolvedTs(span, 5)
	manager.schemaStorage.AdvanceResolvedTs(5)
	err := manager.StartTable(span, 0)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		s := manager.GetTableStats(span)
		return manager.sinkMemQuota.GetUsedBytes() == 0 && s.CheckpointTs == 4
	}, 5*time.Second, 10*time.Millisecond)
}

func TestDoNotGenerateTableSinkTaskWhenTableIsNotReplicating(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, _, e := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	span := spanz.TableIDToComparableSpan(1)
	manager.AddTable(span, 1, 100)
	addTableAndAddEventsToSortEngine(t, e, span)
	manager.UpdateBarrierTs(4, nil)
	manager.UpdateReceivedSorterResolvedTs(span, 5)

	require.Equal(t, uint64(0), manager.sinkMemQuota.GetUsedBytes())
	tableSink, ok := manager.tableSinks.Load(span)
	require.True(t, ok)
	require.NotNil(t, tableSink)
	require.Equal(t, uint64(1), tableSink.(*tableSinkWrapper).getCheckpointTs().Ts)
}

func TestClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, _, _ := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))

	err := manager.Close()
	require.NoError(t, err)
}

// This could happen when closing the sink manager and source manager.
// We close the sink manager first, and then close the source manager.
// So probably the source manager calls the sink manager to update the resolved ts to a removed table.
func TestUpdateReceivedSorterResolvedTsOfNonExistTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, _, _ := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()

	manager.UpdateReceivedSorterResolvedTs(spanz.TableIDToComparableSpan(1), 1)
}
