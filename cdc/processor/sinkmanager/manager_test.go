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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
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
	sm := sourcemanager.New(changefeedID, up, &entry.MockMountGroup{}, sortEngine, errChan, false)
	manager, err := New(
		ctx, changefeedID, changefeedInfo, up,
		&entry.MockSchemaStorage{Resolved: math.MaxUint64},
		nil, sm,
		errChan, errChan, prometheus.NewCounter(prometheus.CounterOpts{}))
	require.NoError(t, err)
	return manager, sortEngine
}

func getChangefeedInfo() *model.ChangeFeedInfo {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Consistent.MemoryUsage.MemoryQuotaPercentage = 75
	replicaConfig.Consistent.MemoryUsage.EventCachePercentage = 50
	return &model.ChangeFeedInfo{
		Error:   nil,
		SinkURI: "blackhole://",
		Config:  replicaConfig,
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
		{
			CRTs: 6,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
				CRTs:   6,
			},
		},
	}
	for _, event := range events {
		engine.Add(tableID, event)
	}
}

func TestAddTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, _ := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() { manager.Close() }()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	tableSink, ok := manager.tableSinks.Load(tableID)
	require.True(t, ok)
	require.NotNil(t, tableSink)
	require.Equal(t, 0, manager.sinkProgressHeap.len(), "Not started table shout not in progress heap")
	err := manager.StartTable(tableID, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(0x7ffffffffffbffff), tableSink.(*tableSinkWrapper).replicateTs)

	progress := manager.sinkProgressHeap.pop()
	require.Equal(t, tableID, progress.tableID)
	require.Equal(t, uint64(0), progress.nextLowerBoundPos.StartTs)
	require.Equal(t, uint64(2), progress.nextLowerBoundPos.CommitTs)
}

func TestRemoveTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, e := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() { manager.Close() }()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	tableSink, ok := manager.tableSinks.Load(tableID)
	require.True(t, ok)
	require.NotNil(t, tableSink)
	err := manager.StartTable(tableID, 0)
	require.NoError(t, err)
	addTableAndAddEventsToSortEngine(t, e, tableID)
	manager.UpdateBarrierTs(4, nil)
	manager.UpdateReceivedSorterResolvedTs(tableID, 5)
	manager.schemaStorage.AdvanceResolvedTs(5)
	// Check all the events are sent to sink and record the memory usage.
	require.Eventually(t, func() bool {
		return manager.sinkMemQuota.GetUsedBytes() == 872
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
	require.Equal(t, uint64(0), manager.sinkMemQuota.GetUsedBytes(), "After remove table, the memory usage should be 0.")
}

func TestGenerateTableSinkTaskWithBarrierTs(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, e := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() { manager.Close() }()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	addTableAndAddEventsToSortEngine(t, e, tableID)
	manager.UpdateBarrierTs(4, nil)
	manager.UpdateReceivedSorterResolvedTs(tableID, 5)
	manager.schemaStorage.AdvanceResolvedTs(5)
	err := manager.StartTable(tableID, 0)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		tableSink, ok := manager.tableSinks.Load(tableID)
		require.True(t, ok)
		s := manager.GetTableStats(tableID)
		checkpointTS := tableSink.(*tableSinkWrapper).getCheckpointTs()
		return checkpointTS.ResolvedMark() == 4 && s.LastSyncedTs == 4
	}, 5*time.Second, 10*time.Millisecond)

	manager.UpdateBarrierTs(6, nil)
	manager.UpdateReceivedSorterResolvedTs(tableID, 6)
	manager.schemaStorage.AdvanceResolvedTs(6)
	require.Eventually(t, func() bool {
		s := manager.GetTableStats(tableID)
		return s.CheckpointTs == 6 && s.LastSyncedTs == 4
	}, 5*time.Second, 10*time.Millisecond)
}

func TestGenerateTableSinkTaskWithResolvedTs(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, e := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() { manager.Close() }()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	addTableAndAddEventsToSortEngine(t, e, tableID)
	// This would happen when the table just added to this node and redo log is enabled.
	// So there is possibility that the resolved ts is smaller than the global barrier ts.
	manager.UpdateBarrierTs(4, nil)
	manager.UpdateReceivedSorterResolvedTs(tableID, 3)
	manager.schemaStorage.AdvanceResolvedTs(4)
	err := manager.StartTable(tableID, 0)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		tableSink, ok := manager.tableSinks.Load(tableID)
		require.True(t, ok)
		checkpointTS := tableSink.(*tableSinkWrapper).getCheckpointTs()
		s := manager.GetTableStats(tableID)
		return checkpointTS.ResolvedMark() == 3 && s.LastSyncedTs == 3
	}, 5*time.Second, 10*time.Millisecond)
}

func TestGetTableStatsToReleaseMemQuota(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, e := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() { manager.Close() }()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	addTableAndAddEventsToSortEngine(t, e, tableID)

	manager.UpdateBarrierTs(4, nil)
	manager.UpdateReceivedSorterResolvedTs(tableID, 5)
	manager.schemaStorage.AdvanceResolvedTs(5)
	err := manager.StartTable(tableID, 0)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		s := manager.GetTableStats(tableID)
		return manager.sinkMemQuota.GetUsedBytes() == 0 && s.CheckpointTs == 4 && s.LastSyncedTs == 4
	}, 5*time.Second, 10*time.Millisecond)
}

func TestDoNotGenerateTableSinkTaskWhenTableIsNotReplicating(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, e := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() { manager.Close() }()
	tableID := model.TableID(1)
	manager.AddTable(tableID, 1, 100)
	addTableAndAddEventsToSortEngine(t, e, tableID)
	manager.UpdateBarrierTs(4, nil)
	manager.UpdateReceivedSorterResolvedTs(tableID, 5)

	require.Equal(t, uint64(0), manager.sinkMemQuota.GetUsedBytes())
	tableSink, ok := manager.tableSinks.Load(tableID)
	require.True(t, ok)
	require.NotNil(t, tableSink)
	checkpointTS := tableSink.(*tableSinkWrapper).getCheckpointTs()
	require.Equal(t, uint64(1), checkpointTS.Ts)
}

func TestClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, _ := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))

	manager.Close()
}

// This could happen when closing the sink manager and source manager.
// We close the sink manager first, and then close the source manager.
// So probably the source manager calls the sink manager to update the resolved ts to a removed table.
func TestUpdateReceivedSorterResolvedTsOfNonExistTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, _ := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, make(chan error, 1))
	defer func() { manager.Close() }()

	manager.UpdateReceivedSorterResolvedTs(model.TableID(1), 1)
}

// Sink worker errors should cancel the sink manager correctly.
func TestSinkManagerRunWithErrors(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 16)
	changefeedInfo := getChangefeedInfo()
	manager, source := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, errCh)
	defer func() { manager.Close() }()

	_ = failpoint.Enable("github.com/pingcap/tiflow/cdc/processor/sinkmanager/SinkWorkerTaskError", "return")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/processor/sinkmanager/SinkWorkerTaskError")
	}()

	source.AddTable(1)
	manager.AddTable(1, 100, math.MaxUint64)
	manager.StartTable(1, 100)
	source.Add(1, model.NewResolvedPolymorphicEvent(0, 101))
	manager.UpdateReceivedSorterResolvedTs(1, 101)
	manager.UpdateBarrierTs(101, nil)

	timer := time.NewTimer(5 * time.Second)
	select {
	case <-errCh:
		if !timer.Stop() {
			<-timer.C
		}
		return
	case <-timer.C:
		log.Panic("must get an error instead of a timeout")
	}
}

func TestSinkManagerNeedsStuckCheck(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 16)
	changefeedInfo := getChangefeedInfo()
	manager, _ := createManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, errCh)
	defer func() {
		cancel()
		manager.Close()
	}()

	require.False(t, manager.needsStuckCheck())
}

func TestSinkManagerRestartTableSinks(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tiflow/cdc/processor/sinkmanager/SinkWorkerTaskHandlePause", "return")
	defer failpoint.Disable("github.com/pingcap/tiflow/cdc/processor/sinkmanager/SinkWorkerTaskHandlePause")

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 16)
	changefeedInfo := getChangefeedInfo()
	manager, _ := createManagerWithMemEngine(t, ctx, model.ChangeFeedID{}, changefeedInfo, errCh)
	defer func() {
		cancel()
		manager.Close()
	}()

	manager.AddTable(1, 1, 100)
	require.Nil(t, manager.StartTable(1, 2))
	table, exists := manager.tableSinks.Load(model.TableID(1))
	require.True(t, exists)

	table.(*tableSinkWrapper).updateReceivedSorterResolvedTs(4)
	table.(*tableSinkWrapper).updateBarrierTs(4)
	select {
	case task := <-manager.sinkTaskChan:
		require.Equal(t, engine.Position{StartTs: 0, CommitTs: 3}, task.lowerBound)
		task.callback(engine.Position{StartTs: 3, CommitTs: 4})
	case <-time.After(2 * time.Second):
		panic("should always get a sink task")
	}

	// With the failpoint blackhole/WriteEventsFail enabled, sink manager should restarts
	// the table sink at its checkpoint.
	failpoint.Enable("github.com/pingcap/tiflow/cdc/sinkv2/eventsink/blackhole/WriteEventsFail", "1*return")
	defer failpoint.Disable("github.com/pingcap/tiflow/cdc/sinkv2/eventsink/blackhole/WriteEventsFail")
	select {
	case task := <-manager.sinkTaskChan:
		require.Equal(t, engine.Position{StartTs: 2, CommitTs: 2}, task.lowerBound)
		task.callback(engine.Position{StartTs: 3, CommitTs: 4})
	case <-time.After(2 * time.Second):
		panic("should always get a sink task")
	}
}
