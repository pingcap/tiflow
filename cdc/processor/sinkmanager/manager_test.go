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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func getChangefeedInfo() *model.ChangeFeedInfo {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Consistent.MemoryUsage.MemoryQuotaPercentage = 75
	return &model.ChangeFeedInfo{
		Error:   nil,
		SinkURI: "blackhole://",
		Config:  replicaConfig,
	}
}

// nolint:unparam
// It is ok to use the same tableID in test.
func addTableAndAddEventsToSortEngine(
	_ *testing.T,
	engine sorter.SortEngine,
	span tablepb.Span,
) uint64 {
	engine.AddTable(span, 0)
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
		{
			CRTs: 6,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
				CRTs:   6,
			},
		},
	}
	size := uint64(0)
	for _, event := range events {
		if event.Row != nil {
			size += uint64(event.Row.ApproximateBytes())
		}
		engine.Add(span, event)
	}
	return size
}

func TestAddTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	changefeedInfo := getChangefeedInfo()
	manager, _, _ := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		cancel()
		manager.Close()
	}()

	span := spanz.TableIDToComparableSpan(1)
	manager.AddTable(span, 1, 100)
	tableSink, ok := manager.tableSinks.Load(span)
	require.True(t, ok)
	require.NotNil(t, tableSink)
	require.Equal(t, 0, manager.sinkProgressHeap.len(), "Not started table shout not in progress heap")
	err := manager.StartTable(span, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(0x7ffffffffffbffff), tableSink.(*tableSinkWrapper).replicateTs.Load())

	progress := manager.sinkProgressHeap.pop()
	require.Equal(t, span, progress.span)
	require.Equal(t, uint64(0), progress.nextLowerBoundPos.StartTs)
	require.Equal(t, uint64(2), progress.nextLowerBoundPos.CommitTs)
}

func TestRemoveTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	changefeedInfo := getChangefeedInfo()
	manager, _, e := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		cancel()
		manager.Close()
	}()

	span := spanz.TableIDToComparableSpan(1)
	manager.AddTable(span, 1, 100)
	tableSink, ok := manager.tableSinks.Load(span)
	require.True(t, ok)
	require.NotNil(t, tableSink)
	err := manager.StartTable(span, 0)
	require.NoError(t, err)
	totalEventSize := addTableAndAddEventsToSortEngine(t, e, span)
	manager.UpdateBarrierTs(4, nil)
	manager.UpdateReceivedSorterResolvedTs(span, 5)
	manager.schemaStorage.AdvanceResolvedTs(5)
	// Check all the events are sent to sink and record the memory usage.
	require.Eventually(t, func() bool {
		return manager.sinkMemQuota.GetUsedBytes() == totalEventSize
	}, 5*time.Second, 10*time.Millisecond)

	// Call this function times to test the idempotence.
	manager.AsyncStopTable(span)
	manager.AsyncStopTable(span)
	manager.AsyncStopTable(span)
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
	ctx, cancel := context.WithCancel(context.Background())
	changefeedInfo := getChangefeedInfo()
	manager, _, e := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		cancel()
		manager.Close()
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
		return s.CheckpointTs == 4 && s.LastSyncedTs == 4
	}, 5*time.Second, 10*time.Millisecond)

	manager.UpdateBarrierTs(6, nil)
	manager.UpdateReceivedSorterResolvedTs(span, 6)
	manager.schemaStorage.AdvanceResolvedTs(6)
	require.Eventually(t, func() bool {
		s := manager.GetTableStats(span)
		log.Info("checkpoint ts", zap.Uint64("checkpointTs", s.CheckpointTs), zap.Uint64("lastSyncedTs", s.LastSyncedTs))
		fmt.Printf("debug checkpoint ts %d, lastSyncedTs %d\n", s.CheckpointTs, s.LastSyncedTs)
		return s.CheckpointTs == 6 && s.LastSyncedTs == 4
	}, 5*time.Second, 10*time.Millisecond)
}

func TestGenerateTableSinkTaskWithResolvedTs(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	changefeedInfo := getChangefeedInfo()
	manager, _, e := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		cancel()
		manager.Close()
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
		s := manager.GetTableStats(span)
		return s.CheckpointTs == 3 && s.LastSyncedTs == 3
	}, 5*time.Second, 10*time.Millisecond)
}

func TestGetTableStatsToReleaseMemQuota(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	changefeedInfo := getChangefeedInfo()
	manager, _, e := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		cancel()
		manager.Close()
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
		return manager.sinkMemQuota.GetUsedBytes() == 0 && s.CheckpointTs == 4 && s.LastSyncedTs == 4
	}, 5*time.Second, 10*time.Millisecond)
}

func TestDoNotGenerateTableSinkTaskWhenTableIsNotReplicating(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	changefeedInfo := getChangefeedInfo()
	manager, _, e := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		cancel()
		manager.Close()
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
	checkpointTS := tableSink.(*tableSinkWrapper).getCheckpointTs()
	require.Equal(t, uint64(1), checkpointTS.Ts)
}

func TestClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeedInfo := getChangefeedInfo()
	manager, _, _ := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))

	cancel()
	manager.Close()
}

// This could happen when closing the sink manager and source manager.
// We close the sink manager first, and then close the source manager.
// So probably the source manager calls the sink manager to update the resolved ts to a removed table.
func TestUpdateReceivedSorterResolvedTsOfNonExistTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	changefeedInfo := getChangefeedInfo()
	manager, _, _ := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"),
		changefeedInfo, make(chan error, 1))
	defer func() {
		cancel()
		manager.Close()
	}()

	manager.UpdateReceivedSorterResolvedTs(spanz.TableIDToComparableSpan(1), 1)
}

// Sink worker errors should cancel the sink manager correctly.
func TestSinkManagerRunWithErrors(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 16)
	changefeedInfo := getChangefeedInfo()
	manager, source, _ := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, errCh)
	defer func() {
		cancel()
		manager.Close()
	}()

	_ = failpoint.Enable("github.com/pingcap/tiflow/cdc/processor/sinkmanager/SinkWorkerTaskError", "return")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/processor/sinkmanager/SinkWorkerTaskError")
	}()

	span := spanz.TableIDToComparableSpan(1)

	source.AddTable(span, "test", 100, func() model.Ts { return 0 })
	manager.AddTable(span, 100, math.MaxUint64)
	manager.StartTable(span, 100)
	source.Add(span, model.NewResolvedPolymorphicEvent(0, 101))
	manager.UpdateReceivedSorterResolvedTs(span, 101)
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
	manager, _, _ := CreateManagerWithMemEngine(t, ctx, model.DefaultChangeFeedID("1"), changefeedInfo, errCh)
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
	manager, _, _ := CreateManagerWithMemEngine(t, ctx, model.ChangeFeedID{}, changefeedInfo, errCh)
	defer func() {
		cancel()
		manager.Close()
	}()

	span := tablepb.Span{TableID: 1}
	manager.AddTable(span, 1, 100)
	require.Nil(t, manager.StartTable(span, 2))
	table, exists := manager.tableSinks.Load(span)
	require.True(t, exists)

	table.(*tableSinkWrapper).updateReceivedSorterResolvedTs(4)
	table.(*tableSinkWrapper).updateBarrierTs(4)
	select {
	case task := <-manager.sinkTaskChan:
		require.Equal(t, sorter.Position{StartTs: 0, CommitTs: 3}, task.lowerBound)
		task.callback(sorter.Position{StartTs: 3, CommitTs: 4})
	case <-time.After(2 * time.Second):
		panic("should always get a sink task")
	}

	// With the failpoint blackhole/WriteEventsFail enabled, sink manager should restarts
	// the table sink at its checkpoint.
	failpoint.Enable("github.com/pingcap/tiflow/cdc/sink/dmlsink/blackhole/WriteEventsFail", "1*return")
	defer failpoint.Disable("github.com/pingcap/tiflow/cdc/sink/dmlsink/blackhole/WriteEventsFail")
	select {
	case task := <-manager.sinkTaskChan:
		require.Equal(t, sorter.Position{StartTs: 2, CommitTs: 2}, task.lowerBound)
		task.callback(sorter.Position{StartTs: 3, CommitTs: 4})
	case <-time.After(2 * time.Second):
		panic("should always get a sink task")
	}
}
