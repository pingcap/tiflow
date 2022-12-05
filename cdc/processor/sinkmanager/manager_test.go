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
	"math"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/memory"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type mockPD struct {
	pd.Client
	ts int64
}

func (p *mockPD) GetTS(_ context.Context) (int64, int64, error) {
	if p.ts != 0 {
		return p.ts, p.ts, nil
	}
	return math.MaxInt64, math.MaxInt64, nil
}

// nolint:revive
// In test it is ok move the ctx to the second parameter.
func createManagerWithMemEngine(
	t *testing.T,
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	changefeedInfo *model.ChangeFeedInfo,
	errChan chan error,
) (*SinkManager, engine.SortEngine) {
	sortEngine := memory.New(context.Background())
	up := upstream.NewUpstream4Test(&mockPD{})
	sm := sourcemanager.New(changefeedID, up, &entry.MockMountGroup{}, sortEngine, errChan)
	manager, err := New(
		ctx, changefeedID, changefeedInfo, up,
		nil, sm,
		errChan, prometheus.NewCounter(prometheus.CounterOpts{}))
	require.NoError(t, err)
	return manager, sortEngine
}

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
	manager, _ := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
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
	err := manager.StartTable(tableID, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(0x7ffffffffffbffff), tableSink.(*tableSinkWrapper).replicateTs)
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
	manager, e := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	tableSink, ok := manager.tableSinks.Load(tableID)
	require.True(t, ok)
	require.NotNil(t, tableSink)
	err := manager.StartTable(tableID, 0)
	require.NoError(t, err)
	addTableAndAddEventsToSortEngine(t, e, tableID)
	manager.UpdateBarrierTs(4)
	manager.UpdateReceivedSorterResolvedTs(tableID, 5)

	// Check all the events are sent to sink and record the memory usage.
	require.Eventually(t, func() bool {
		return manager.memQuota.getUsedBytes() == 872
	}, 5*time.Second, 10*time.Millisecond)

	manager.AsyncStopTable(tableID)
	require.Eventually(t, func() bool {
		state, ok := manager.GetTableState(tableID)
		require.True(t, ok)
		return state == tablepb.TableStateStopped
	}, 5*time.Second, 10*time.Millisecond)

	manager.RemoveTable(tableID)

	_, ok = manager.tableSinks.Load(tableID)
	require.False(t, ok)
	require.Equal(t, uint64(0), manager.memQuota.getUsedBytes(), "After remove table, the memory usage should be 0.")
}

func TestUpdateBarrierTs(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, _ := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
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
	manager, e := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	addTableAndAddEventsToSortEngine(t, e, tableID)
	manager.UpdateBarrierTs(4)
	manager.UpdateReceivedSorterResolvedTs(tableID, 5)
	err := manager.StartTable(tableID, 0)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		tableSink, ok := manager.tableSinks.Load(tableID)
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
	manager, e := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	addTableAndAddEventsToSortEngine(t, e, tableID)
	// This would happen when the table just added to this node and redo log is enabled.
	// So there is possibility that the resolved ts is smaller than the global barrier ts.
	manager.UpdateBarrierTs(4)
	manager.UpdateReceivedSorterResolvedTs(tableID, 3)
	err := manager.StartTable(tableID, 0)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		tableSink, ok := manager.tableSinks.Load(tableID)
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
	manager, e := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() {
		err := manager.Close()
		require.NoError(t, err)
	}()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	addTableAndAddEventsToSortEngine(t, e, tableID)

	manager.UpdateBarrierTs(4)
	manager.UpdateReceivedSorterResolvedTs(tableID, 5)
	err := manager.StartTable(tableID, 0)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		s := manager.GetTableStats(tableID)
		return manager.memQuota.getUsedBytes() == 0 && s.CheckpointTs == 4
	}, 5*time.Second, 10*time.Millisecond)
}

func TestDoNotGenerateTableSinkTaskWhenTableIsNotReplicating(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, e := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	addTableAndAddEventsToSortEngine(t, e, tableID)
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
	manager, _ := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))

	err := manager.Close()
	require.NoError(t, err)
}
