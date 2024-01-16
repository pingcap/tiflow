// Copyright 2023 PingCAP, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type tableSinkAdvancerSuite struct {
	suite.Suite
	testChangefeedID    model.ChangeFeedID
	testSpan            tablepb.Span
	defaultTestMemQuota uint64
}

func (suite *tableSinkAdvancerSuite) SetupSuite() {
	requestMemSize = 256
	maxUpdateIntervalSize = 512
	suite.testChangefeedID = model.DefaultChangeFeedID("1")
	suite.testSpan = spanz.TableIDToComparableSpan(1)
	suite.defaultTestMemQuota = 1024
}

func (suite *tableSinkAdvancerSuite) SetupTest() {
	// reset batchID
	// We set batchID to 1 because we want to test the case that
	// the first batchID is 1. Normally, the first batchID should
	// never be 0.
	batchID.Store(1)
}

func (suite *tableSinkAdvancerSuite) TearDownSuite() {
	requestMemSize = defaultRequestMemSize
	maxUpdateIntervalSize = defaultMaxUpdateIntervalSize
}

func TestTableSinkAdvancerSuite(t *testing.T) {
	suite.Run(t, new(tableSinkAdvancerSuite))
}

func (suite *tableSinkAdvancerSuite) genSinkTask() (*sinkTask, *mockSink) {
	wrapper, sink := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
	task := &sinkTask{
		span:      suite.testSpan,
		tableSink: wrapper,
	}

	return task, sink
}

func (suite *tableSinkAdvancerSuite) genMemQuota(initMemQuota uint64) *memquota.MemQuota {
	memoryQuota := memquota.NewMemQuota(suite.testChangefeedID, suite.defaultTestMemQuota, "sink")
	memoryQuota.ForceAcquire(initMemQuota)
	memoryQuota.AddTable(suite.testSpan)
	return memoryQuota
}

func (suite *tableSinkAdvancerSuite) TestNeedEmitAndAdvance() {
	for _, tc := range []struct {
		name             string
		splitTxn         bool
		committedTxnSize uint64
		pendingTxnSize   uint64
		expected         bool
	}{
		{
			name:             "split txn and not reach maxUpdateIntervalSize",
			splitTxn:         true,
			committedTxnSize: maxUpdateIntervalSize - 2,
			pendingTxnSize:   1,
			expected:         false,
		},
		{
			name:             "split txn and reach maxUpdateIntervalSize",
			splitTxn:         true,
			committedTxnSize: maxUpdateIntervalSize,
			pendingTxnSize:   1,
			expected:         true,
		},
		{
			name:             "not split txn and not reach maxUpdateIntervalSize",
			splitTxn:         false,
			committedTxnSize: maxUpdateIntervalSize - 1,
			// Do not care about pendingTxnSize
			pendingTxnSize: maxUpdateIntervalSize + 100,
			expected:       false,
		},
		{
			name:             "not split txn and reach maxUpdateIntervalSize",
			splitTxn:         false,
			committedTxnSize: maxUpdateIntervalSize + 100,
			// Do not care about pendingTxnSize
			pendingTxnSize: maxUpdateIntervalSize + 100,
			expected:       true,
		},
	} {
		suite.Run(tc.name, func() {
			require.Equal(suite.T(), tc.expected,
				needEmitAndAdvance(tc.splitTxn, tc.committedTxnSize, tc.pendingTxnSize))
		})
	}
}

func (suite *tableSinkAdvancerSuite) TestAdvanceTableSinkWithBatchID() {
	task, _ := suite.genSinkTask()
	memoryQuota := suite.genMemQuota(512)
	defer memoryQuota.Close()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 512)
	require.NotNil(suite.T(), advancer)

	err := advanceTableSinkWithBatchID(task, 2, 256, 1, memoryQuota)
	require.NoError(suite.T(), err)

	expectedResolvedTs := model.NewResolvedTs(2)
	expectedResolvedTs.Mode = model.BatchResolvedMode
	expectedResolvedTs.BatchID = 1
	checkpointTs := task.tableSink.getCheckpointTs()
	require.Equal(suite.T(), expectedResolvedTs, checkpointTs)
}

func (suite *tableSinkAdvancerSuite) TestAdvanceTableSink() {
	task, _ := suite.genSinkTask()
	memoryQuota := suite.genMemQuota(512)
	defer memoryQuota.Close()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 512)
	require.NotNil(suite.T(), advancer)

	err := advanceTableSink(task, 2, 256, memoryQuota)
	require.NoError(suite.T(), err)

	expectedResolvedTs := model.NewResolvedTs(2)
	checkpointTs := task.tableSink.getCheckpointTs()
	require.Equal(suite.T(), expectedResolvedTs, checkpointTs)
}

func (suite *tableSinkAdvancerSuite) TestNewTableSinkAdvancer() {
	task, _ := suite.genSinkTask()
	memoryQuota := suite.genMemQuota(512)
	defer memoryQuota.Close()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 512)
	require.NotNil(suite.T(), advancer)
	require.Equal(suite.T(), uint64(512), advancer.availableMem)
}

func (suite *tableSinkAdvancerSuite) TestHasEnoughMem() {
	memoryQuota := suite.genMemQuota(512)
	defer memoryQuota.Close()
	task, _ := suite.genSinkTask()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 512)
	require.NotNil(suite.T(), advancer)
	require.True(suite.T(), advancer.hasEnoughMem())
	for i := 0; i < 6; i++ {
		// 6 * 256 = 1536 > 1024
		advancer.appendEvents([]*model.RowChangedEvent{{}}, 256)
	}
	require.False(suite.T(), advancer.hasEnoughMem(),
		"hasEnoughMem should return false when usedMem > availableMem")
}

func (suite *tableSinkAdvancerSuite) TestCleanup() {
	memoryQuota := suite.genMemQuota(512)
	defer memoryQuota.Close()
	task, _ := suite.genSinkTask()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 512)
	require.NotNil(suite.T(), advancer)
	require.Equal(suite.T(), uint64(512), advancer.availableMem)
	require.Equal(suite.T(), uint64(0), advancer.usedMem)
	require.Equal(suite.T(), uint64(512), memoryQuota.GetUsedBytes())
	advancer.cleanup()
	require.Equal(suite.T(), uint64(0), memoryQuota.GetUsedBytes(),
		"memory quota should be released after cleanup")
}

func (suite *tableSinkAdvancerSuite) TestAppendEvents() {
	memoryQuota := suite.genMemQuota(512)
	defer memoryQuota.Close()
	task, _ := suite.genSinkTask()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 512)
	require.NotNil(suite.T(), advancer)
	require.True(suite.T(), advancer.hasEnoughMem())
	for i := 0; i < 2; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{{}}, 256)
	}
	require.Equal(suite.T(), uint64(512), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(512), advancer.usedMem)
	require.False(suite.T(), advancer.hasEnoughMem())
	require.Len(suite.T(), advancer.events, 2)
}

func (suite *tableSinkAdvancerSuite) TestTryMoveMoveToNextTxn() {
	memoryQuota := suite.genMemQuota(512)
	defer memoryQuota.Close()
	task, _ := suite.genSinkTask()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 512)
	require.NotNil(suite.T(), advancer)

	// Initial state.
	require.Equal(suite.T(), uint64(0), advancer.committedTxnSize)
	require.Equal(suite.T(), uint64(0), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(0), advancer.currTxnCommitTs)

	// Append 1 event with commit ts 1
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 1},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(1)
	require.Equal(suite.T(), uint64(256), advancer.committedTxnSize)
	require.Equal(suite.T(), uint64(0), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(1), advancer.currTxnCommitTs)

	// Append 2 events with commit ts 2
	for i := 0; i < 2; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 2},
		}, 256)
	}
	require.Equal(suite.T(), uint64(768), advancer.usedMem)
	require.Equal(suite.T(), uint64(256), advancer.committedTxnSize)
	require.Equal(suite.T(), uint64(0), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(512), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(1), advancer.currTxnCommitTs)

	// Try to move to next txn.
	advancer.tryMoveToNextTxn(2)
	require.Equal(suite.T(), uint64(768), advancer.committedTxnSize)
	require.Equal(suite.T(), uint64(1), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(2), advancer.currTxnCommitTs)
}

// Test Scenario:
// When we meet a commit fence, we should flush all the events and advance the
// table sink with the commit ts of the commit fence.
func (suite *tableSinkAdvancerSuite) TestAdvanceTheSameCommitTsEventsWithCommitFence() {
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, sink := suite.genSinkTask()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 768)
	require.NotNil(suite.T(), advancer)

	// 1. append 1 event with commit ts 1
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 1},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(1)

	// 2. append 2 events with commit ts 2
	for i := 0; i < 2; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 2},
		}, 256)
	}
	advancer.tryMoveToNextTxn(2)

	// 3. advance with commit fence
	// Last pos is a commit fence.
	advancer.lastPos = sorter.Position{
		StartTs:  1,
		CommitTs: 2,
	}
	err := advancer.advance(false)
	require.NoError(suite.T(), err)

	require.Len(suite.T(), sink.GetEvents(), 3)
	sink.AckAllEvents()
	require.Eventually(suite.T(), func() bool {
		checkpointTs := task.tableSink.getCheckpointTs()
		return checkpointTs == model.NewResolvedTs(2)
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(suite.T(), uint64(0), advancer.committedTxnSize)
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
}

// Test Scenario:
// When we do not meet a commit fence, we should flush all the events and advance the
// table sink with the commit ts and batch ID.
func (suite *tableSinkAdvancerSuite) TestAdvanceTheSameCommitTsEventsWithoutCommitFence() {
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, sink := suite.genSinkTask()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 768)
	require.NotNil(suite.T(), advancer)

	// 1. append 1 event with commit ts 1
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 1},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(1)

	// 2. append 2 events with commit ts 3
	for i := 0; i < 2; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 3},
		}, 256)
	}
	advancer.tryMoveToNextTxn(3)

	// 3. advance without commit fence
	// Last pos is **not** a commit fence.
	advancer.lastPos = sorter.Position{
		StartTs:  1,
		CommitTs: 3,
	}
	err := advancer.advance(false)
	require.NoError(suite.T(), err)

	require.Len(suite.T(), sink.GetEvents(), 3)
	sink.AckAllEvents()
	require.Eventually(suite.T(), func() bool {
		expectedResolvedTs := model.NewResolvedTs(3)
		expectedResolvedTs.Mode = model.BatchResolvedMode
		expectedResolvedTs.BatchID = 1
		checkpointTs := task.tableSink.getCheckpointTs()
		return checkpointTs == expectedResolvedTs
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(suite.T(), uint64(0), advancer.committedTxnSize)
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(2), batchID.Load(), "batch ID should be increased")
}

// Test Scenario:
// When we meet a different commit ts event, and we support split txn,
// we should flush all the events and advance the
// table sink with the current commit ts and batch ID.
func (suite *tableSinkAdvancerSuite) TestAdvanceDifferentCommitTsEventsWithSplitTxn() {
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, sink := suite.genSinkTask()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 768)
	require.NotNil(suite.T(), advancer)

	// 1. append 1 event with commit ts 2
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 2},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(2)

	// 2. meet a txn finished event
	advancer.lastPos = sorter.Position{
		StartTs:  1,
		CommitTs: 2,
	}

	// 3. append 2 events with commit ts 3
	for i := 0; i < 2; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 3},
		}, 256)
	}
	require.Equal(suite.T(), uint64(768), advancer.usedMem)
	advancer.tryMoveToNextTxn(3)

	// 4. advance without commit fence and with split txn
	err := advancer.advance(false)
	require.NoError(suite.T(), err)

	require.Len(suite.T(), sink.GetEvents(), 3)
	sink.AckAllEvents()
	require.Eventually(suite.T(), func() bool {
		expectedResolvedTs := model.NewResolvedTs(3)
		expectedResolvedTs.Mode = model.BatchResolvedMode
		expectedResolvedTs.BatchID = 1
		checkpointTs := task.tableSink.getCheckpointTs()
		return checkpointTs == expectedResolvedTs
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(suite.T(), uint64(0), advancer.committedTxnSize)
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(2), batchID.Load(), "batch ID should be increased")
}

// Test Scenario:
// When we meet a different commit ts event, and we do **not** support split txn,
// we should flush all the events and advance the
// table sink with the current commit of the last event.
func (suite *tableSinkAdvancerSuite) TestAdvanceDifferentCommitTsEventsWithoutSplitTxn() {
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, sink := suite.genSinkTask()
	// Do not split txn.
	advancer := newTableSinkAdvancer(task, false, memoryQuota, 768)
	require.NotNil(suite.T(), advancer)

	// 1. append 1 event with commit ts 2
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 2},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(2)

	// 2. meet a txn finished event
	advancer.lastPos = sorter.Position{
		StartTs:  1,
		CommitTs: 2,
	}

	// 3. append 1 event with commit ts 3
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 3},
	}, 256)
	require.Equal(suite.T(), uint64(512), advancer.usedMem)
	advancer.tryMoveToNextTxn(3)

	// 4. append 1 event with commit ts 3
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 3},
	}, 256)
	require.Equal(suite.T(), uint64(768), advancer.usedMem)
	advancer.tryMoveToNextTxn(3)

	// 5. advance without commit fence and split txn
	err := advancer.advance(false)
	require.NoError(suite.T(), err)

	require.Len(suite.T(), sink.GetEvents(), 1)
	sink.AckAllEvents()
	require.Eventually(suite.T(), func() bool {
		expectedResolvedTs := model.NewResolvedTs(2)
		checkpointTs := task.tableSink.getCheckpointTs()
		return checkpointTs == expectedResolvedTs
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(suite.T(), uint64(0), advancer.committedTxnSize)
	require.Equal(suite.T(), uint64(256), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(1), batchID.Load(), "batch ID should not be increased")
}

// Test Scenario:
// When we meet a different commit ts event, and we do **not** support split txn,
// we should flush all the events and advance the
// table sink with the current commit of the last event. Also we should clear the
// pending txn size.
func (suite *tableSinkAdvancerSuite) TestLastTimeAdvanceDifferentCommitTsEventsWithoutSplitTxn() {
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, sink := suite.genSinkTask()
	// Do not split txn.
	advancer := newTableSinkAdvancer(task, false, memoryQuota, 768)
	require.NotNil(suite.T(), advancer)

	// 1. append 1 event with commit ts 2
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 2},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(2)

	// 2. meet a txn finished event
	advancer.lastPos = sorter.Position{
		StartTs:  1,
		CommitTs: 2,
	}

	// 3. append 1 event with commit ts 3
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 3},
	}, 256)
	require.Equal(suite.T(), uint64(512), advancer.usedMem)
	advancer.tryMoveToNextTxn(3)

	// 4. append 1 event with commit ts 3
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 3},
	}, 256)
	require.Equal(suite.T(), uint64(768), advancer.usedMem)
	advancer.tryMoveToNextTxn(3)

	// 5. advance without commit fence and split txn
	err := advancer.lastTimeAdvance()
	require.NoError(suite.T(), err)

	require.Len(suite.T(), sink.GetEvents(), 1)
	sink.AckAllEvents()
	require.Eventually(suite.T(), func() bool {
		expectedResolvedTs := model.NewResolvedTs(2)
		checkpointTs := task.tableSink.getCheckpointTs()
		return checkpointTs == expectedResolvedTs
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(suite.T(), uint64(0), advancer.committedTxnSize)
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize,
		"Last time advance should clear pending txn size,"+
			"otherwise the memory quota will be leaked.")
	require.Equal(suite.T(), uint64(1), batchID.Load())
}

// Test Scenario:
// We receive some events and exceed the available memory quota.
// We should advance the table sink and also make up the difference
// between the available memory quota and the used memory quota.
func (suite *tableSinkAdvancerSuite) TestTryAdvanceWhenExceedAvailableMem() {
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, sink := suite.genSinkTask()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 768)
	require.NotNil(suite.T(), advancer)

	// 1. append 1 event with commit ts 2
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 2},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(2)

	// 2. append 3 events with commit ts 3
	for i := 0; i < 3; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 3},
		}, 256)
	}
	require.Equal(suite.T(), uint64(1024), advancer.usedMem)
	advancer.tryMoveToNextTxn(3)

	// 3. Last pos is a commit fence.
	advancer.lastPos = sorter.Position{
		StartTs:  2,
		CommitTs: 3,
	}

	require.Equal(suite.T(), uint64(768), memoryQuota.GetUsedBytes())
	// 4. Try advance with txn is finished.
	err := advancer.tryAdvanceAndAcquireMem(
		false,
		true,
	)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), uint64(1024), memoryQuota.GetUsedBytes(),
		"Memory quota should be force acquired when exceed available memory.",
	)

	require.Len(suite.T(), sink.GetEvents(), 4)
	sink.AckAllEvents()
	require.Eventually(suite.T(), func() bool {
		expectedResolvedTs := model.NewResolvedTs(3)
		checkpointTs := task.tableSink.getCheckpointTs()
		return checkpointTs == expectedResolvedTs
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(suite.T(), uint64(0), advancer.committedTxnSize)
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(1), batchID.Load())
}

// Test Scenario:
// We receive some events and reach the max update interval size.
// We should advance the table sink.
func (suite *tableSinkAdvancerSuite) TestTryAdvanceWhenReachTheMaxUpdateIntSizeAndTxnNotFinished() {
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, sink := suite.genSinkTask()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 768)
	require.NotNil(suite.T(), advancer)

	// 1. append 1 event with commit ts 2
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 2},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(2)

	// 2. append 2 events with commit ts 3
	for i := 0; i < 2; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 3},
		}, 256)
	}
	require.Equal(suite.T(), uint64(768), advancer.usedMem)
	advancer.tryMoveToNextTxn(3)

	// 3. Last pos is a commit fence.
	advancer.lastPos = sorter.Position{
		StartTs:  2,
		CommitTs: 3,
	}

	// 4. Try advance with txn is not finished.
	err := advancer.tryAdvanceAndAcquireMem(
		false,
		false,
	)
	require.NoError(suite.T(), err)
	require.Len(suite.T(), sink.GetEvents(), 3)
	sink.AckAllEvents()
	require.Eventually(suite.T(), func() bool {
		expectedResolvedTs := model.NewResolvedTs(3)
		checkpointTs := task.tableSink.getCheckpointTs()
		return checkpointTs == expectedResolvedTs
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(suite.T(), uint64(0), advancer.committedTxnSize)
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(1), batchID.Load())
}

// Test Scenario:
// We receive some events and the task is finished.
// We should advance the table sink.
func (suite *tableSinkAdvancerSuite) TestFinish() {
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, sink := suite.genSinkTask()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 768)
	require.NotNil(suite.T(), advancer)

	// 1. append 1 event with commit ts 2
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 2},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(2)

	// 2. append 2 events with commit ts 3
	for i := 0; i < 2; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 3},
		}, 256)
	}
	require.Equal(suite.T(), uint64(768), advancer.usedMem)
	advancer.tryMoveToNextTxn(3)

	require.Equal(suite.T(), uint64(2), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(3), advancer.currTxnCommitTs)
	// 3. Try finish.
	finishedPos := sorter.Position{
		StartTs:  3,
		CommitTs: 4,
	}
	err := advancer.finish(finishedPos)
	require.NoError(suite.T(), err)

	// All events should be flushed and the last pos should be updated.
	require.Equal(suite.T(), finishedPos, advancer.lastPos)
	require.Equal(suite.T(), uint64(4), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(4), advancer.currTxnCommitTs)

	require.Len(suite.T(), sink.GetEvents(), 3)
	sink.AckAllEvents()
	require.Eventually(suite.T(), func() bool {
		expectedResolvedTs := model.NewResolvedTs(4)
		checkpointTs := task.tableSink.getCheckpointTs()
		return checkpointTs == expectedResolvedTs
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(suite.T(), uint64(0), advancer.committedTxnSize)
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(1), batchID.Load())
}

// Test Scenario:
// We receive some events and do not support split txn.
// We should advance the table sink and force acquire memory for next txn.
func (suite *tableSinkAdvancerSuite) TestTryAdvanceAndForceAcquireWithoutSplitTxn() {
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, sink := suite.genSinkTask()
	advancer := newTableSinkAdvancer(task, false, memoryQuota, 768)
	require.NotNil(suite.T(), advancer)

	// 1. append 1 event with commit ts 2
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 2},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(2)

	// 2. append 3 events with commit ts 3, this will exceed the memory quota.
	for i := 0; i < 3; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 3},
		}, 256)
	}
	require.Equal(suite.T(), uint64(1024), advancer.usedMem)
	advancer.tryMoveToNextTxn(3)

	// 3. Last pos is a commit fence.
	advancer.lastPos = sorter.Position{
		StartTs:  2,
		CommitTs: 3,
	}

	// 4. Try advance.
	err := advancer.tryAdvanceAndAcquireMem(
		false,
		false,
	)
	require.NoError(suite.T(), err)
	require.Len(suite.T(), sink.GetEvents(), 4)
	sink.AckAllEvents()
	require.Eventually(suite.T(), func() bool {
		expectedResolvedTs := model.NewResolvedTs(3)
		checkpointTs := task.tableSink.getCheckpointTs()
		return checkpointTs == expectedResolvedTs
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(suite.T(), uint64(0), advancer.committedTxnSize)
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(1), batchID.Load())
}

// Test Scenario:
// We receive some events and support split txn.
// We should advance the table sink and block acquire memory for next txn.
func (suite *tableSinkAdvancerSuite) TestTryAdvanceAndBlockAcquireWithSplitTxn() {
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, sink := suite.genSinkTask()
	advancer := newTableSinkAdvancer(task, true, memoryQuota, 768)
	require.NotNil(suite.T(), advancer)

	// 1. append 1 event with commit ts 2
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 2},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(2)

	// 2. append 3 events with commit ts 3, this will exceed the memory quota.
	for i := 0; i < 3; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 3},
		}, 256)
	}
	require.Equal(suite.T(), uint64(1024), advancer.usedMem)
	advancer.tryMoveToNextTxn(3)

	// 3. Last pos is a commit fence.
	advancer.lastPos = sorter.Position{
		StartTs:  2,
		CommitTs: 3,
	}

	down := make(chan struct{})
	go func() {
		// 4. Try advance and block acquire.
		err := advancer.tryAdvanceAndAcquireMem(
			false,
			false,
		)
		require.ErrorIs(suite.T(), err, context.Canceled)
		down <- struct{}{}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// Wait all events are flushed.
		require.Eventually(suite.T(), func() bool {
			return len(sink.GetEvents()) == 4
		}, 5*time.Second, 10*time.Millisecond)
		sink.AckAllEvents()
		// After ack, abort the blocked acquire.
		memoryQuota.Close()
		// Wait the blocked acquire is aborted, otherwise the test data race.
		<-down
		require.Eventually(suite.T(), func() bool {
			expectedResolvedTs := model.NewResolvedTs(3)
			checkpointTs := task.tableSink.getCheckpointTs()
			return checkpointTs == expectedResolvedTs
		}, 5*time.Second, 10*time.Millisecond)
		require.Equal(suite.T(), uint64(0), advancer.committedTxnSize)
		require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
		wg.Done()
	}()
	wg.Wait()
}
