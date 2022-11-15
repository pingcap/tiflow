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
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/memory"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// nolint:revive
// In test it is ok move the ctx to the second parameter.
func createManager(
	t *testing.T,
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	changefeedInfo *model.ChangeFeedInfo,
	errChan chan error,
) *ManagerImpl {
	sortEngine := memory.New(context.Background())
	manager, err := New(ctx, changefeedID, changefeedInfo, nil, sortEngine, errChan, prometheus.NewCounter(prometheus.CounterOpts{}))
	require.NoError(t, err)
	return manager.(*ManagerImpl)
}

func getChangefeedInfo() *model.ChangeFeedInfo {
	return &model.ChangeFeedInfo{
		Error:   nil,
		SinkURI: "blackhole://",
		Config: &config.ReplicaConfig{
			MemoryQuota: 1024 * 1024 * 1024,
		},
	}
}

// nolint:unparam
// It is ok to use the same tableID in test.
func addTableAndAddEventsToSortEngine(
	t *testing.T,
	engine engine.SortEngine,
	tableID model.TableID,
) {
	engine.AddTable(tableID)
	events := []*model.PolymorphicEvent{
		{
			StartTs: 1,
			CRTs:    1,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    1,
			},
			Row: genRowChangedEvent(1, 1, tableID),
		},
		{
			StartTs: 1,
			CRTs:    2,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    2,
			},
			Row: genRowChangedEvent(1, 2, tableID),
		},
		{
			StartTs: 1,
			CRTs:    3,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    3,
			},
			Row: genRowChangedEvent(1, 3, tableID),
		},
		{
			StartTs: 2,
			CRTs:    4,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 2,
				CRTs:    4,
			},
			Row: genRowChangedEvent(2, 4, tableID),
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
		err := engine.Add(tableID, event)
		require.NoError(t, err)
	}
}

func TestAddTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager := createManager(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	tableSink, ok := manager.tableSinks.Load(tableID)
	require.True(t, ok)
	require.NotNil(t, tableSink)
	require.Equal(t, 0, manager.sinkProgressHeap.len(), "Not started table shout not in progress heap")
	manager.StartTable(tableID)
	require.Equal(t, &progress{
		tableID: tableID,
		nextLowerBoundPos: engine.Position{
			StartTs:  0,
			CommitTs: 1,
		},
	}, manager.sinkProgressHeap.pop())
}

func TestRemoveTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager := createManager(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	tableSink, ok := manager.tableSinks.Load(tableID)
	require.True(t, ok)
	require.NotNil(t, tableSink)
	manager.StartTable(tableID)
	addTableAndAddEventsToSortEngine(t, manager.sortEngine, tableID)
	manager.UpdateBarrierTs(4)
	manager.UpdateReceivedSorterResolvedTs(tableID, 5)

	// Check all the events are sent to sink and record the memory usage.
	require.Eventually(t, func() bool {
		return manager.memQuota.getUsedBytes() == 872
	}, 5*time.Second, 10*time.Millisecond)

	err := manager.RemoveTable(tableID)
	require.NoError(t, err)

	_, ok = manager.tableSinks.Load(tableID)
	require.False(t, ok)
	require.Equal(t, uint64(0), manager.memQuota.getUsedBytes(), "After remove table, the memory usage should be 0.")
}

func TestUpdateBarrierTs(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager := createManager(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	manager.UpdateBarrierTs(100)
	require.Equal(t, uint64(100), manager.lastBarrierTs.Load())
	manager.UpdateBarrierTs(50)
	require.Equal(t, uint64(100), manager.lastBarrierTs.Load())
}

func TestGenerateTableSinkTaskWithBarrierTs(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager := createManager(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	addTableAndAddEventsToSortEngine(t, manager.sortEngine, tableID)
	manager.UpdateBarrierTs(4)
	manager.UpdateReceivedSorterResolvedTs(tableID, 5)
	manager.StartTable(tableID)

	require.Eventually(t, func() bool {
		tableSink, ok := manager.tableSinks.Load(tableID)
		require.True(t, ok)
		checkpointTS := tableSink.(*tableSinkWrapper).getCheckpointTs()
		return checkpointTS == model.NewResolvedTs(4)
	}, 5*time.Second, 10*time.Millisecond)
}

func TestGenerateTableSinkTaskWithResolvedTs(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager := createManager(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	addTableAndAddEventsToSortEngine(t, manager.sortEngine, tableID)
	// This would happen when the table just added to this node and redo log is enabled.
	// So there is possibility that the resolved ts is smaller than the global barrier ts.
	manager.UpdateBarrierTs(4)
	manager.UpdateReceivedSorterResolvedTs(tableID, 3)
	manager.StartTable(tableID)

	require.Eventually(t, func() bool {
		tableSink, ok := manager.tableSinks.Load(tableID)
		require.True(t, ok)
		checkpointTS := tableSink.(*tableSinkWrapper).getCheckpointTs()
		return checkpointTS == model.NewResolvedTs(3)
	}, 5*time.Second, 10*time.Millisecond)
}

func TestGetTableStatsToReleaseMemQuota(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager := createManager(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	addTableAndAddEventsToSortEngine(t, manager.sortEngine, tableID)

	manager.UpdateBarrierTs(4)
	manager.UpdateReceivedSorterResolvedTs(tableID, 5)
	manager.StartTable(tableID)

	require.Eventually(t, func() bool {
		s, err := manager.GetTableStats(tableID)
		require.NoError(t, err)
		return manager.memQuota.getUsedBytes() == 0 && s.CheckpointTs == 4
	}, 5*time.Second, 10*time.Millisecond)
}

func TestDoNotGenerateTableSinkTaskWhenTableIsNotReplicating(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager := createManager(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	addTableAndAddEventsToSortEngine(t, manager.sortEngine, tableID)
	manager.UpdateBarrierTs(4)
	manager.UpdateReceivedSorterResolvedTs(tableID, 5)

	require.Equal(t, uint64(0), manager.memQuota.getUsedBytes())
	tableSink, ok := manager.tableSinks.Load(tableID)
	require.True(t, ok)
	require.NotNil(t, tableSink)
	require.Equal(t, uint64(0), tableSink.(*tableSinkWrapper).getCheckpointTs().Ts)
}

func TestClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager := createManager(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))

	err := manager.Close()
	require.NoError(t, err)
}
