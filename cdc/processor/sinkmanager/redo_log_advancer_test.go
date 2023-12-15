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
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var _ redo.DMLManager = &mockRedoDMLManager{}

type mockRedoDMLManager struct {
	mu                  sync.Mutex
	events              map[int64][]*model.RowChangedEvent
	resolvedTss         map[int64]model.Ts
	releaseRowsMemories map[int64]func()
}

func newMockRedoDMLManager() *mockRedoDMLManager {
	return &mockRedoDMLManager{
		events:              make(map[int64][]*model.RowChangedEvent),
		resolvedTss:         make(map[int64]model.Ts),
		releaseRowsMemories: make(map[int64]func()),
	}
}

func (m *mockRedoDMLManager) Enabled() bool {
	panic("unreachable")
}

func (m *mockRedoDMLManager) Run(ctx context.Context, _ ...chan<- error) error {
	panic("unreachable")
}

func (m *mockRedoDMLManager) WaitForReady(_ context.Context) {
	panic("unreachable")
}

func (m *mockRedoDMLManager) Close() {
	panic("unreachable")
}

func (m *mockRedoDMLManager) AddTable(span tablepb.Span, startTs uint64) {
	panic("unreachable")
}

func (m *mockRedoDMLManager) RemoveTable(span tablepb.Span) {
	panic("unreachable")
}

func (m *mockRedoDMLManager) UpdateResolvedTs(ctx context.Context,
	span tablepb.Span, resolvedTs uint64,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resolvedTss[span.TableID] = resolvedTs
	return nil
}

func (m *mockRedoDMLManager) StartTable(span tablepb.Span, resolvedTs uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resolvedTss[span.TableID] = resolvedTs
}

func (m *mockRedoDMLManager) GetResolvedTs(span tablepb.Span) model.Ts {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resolvedTss[span.TableID]
}

func (m *mockRedoDMLManager) EmitRowChangedEvents(ctx context.Context,
	span tablepb.Span, releaseRowsMemory func(), rows ...*model.RowChangedEvent,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.events[span.TableID]; !ok {
		m.events[span.TableID] = make([]*model.RowChangedEvent, 0)
	}
	m.events[span.TableID] = append(m.events[span.TableID], rows...)
	m.releaseRowsMemories[span.TableID] = releaseRowsMemory

	return nil
}

func (m *mockRedoDMLManager) getEvents(span tablepb.Span) []*model.RowChangedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.events[span.TableID]
}

func (m *mockRedoDMLManager) releaseRowsMemory(span tablepb.Span) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseRowsMemories[span.TableID]()
}

type redoLogAdvancerSuite struct {
	suite.Suite
	testChangefeedID    model.ChangeFeedID
	testSpan            tablepb.Span
	defaultTestMemQuota uint64
}

func (suite *redoLogAdvancerSuite) SetupSuite() {
	requestMemSize = 256
	maxUpdateIntervalSize = 512
	suite.testChangefeedID = model.DefaultChangeFeedID("1")
	suite.testSpan = spanz.TableIDToComparableSpan(1)
	suite.defaultTestMemQuota = 1024
}

func (suite *redoLogAdvancerSuite) TearDownSuite() {
	requestMemSize = defaultRequestMemSize
	maxUpdateIntervalSize = defaultMaxUpdateIntervalSize
}

func TestRedoLogAdvancerSuite(t *testing.T) {
	suite.Run(t, new(redoLogAdvancerSuite))
}

func (suite *redoLogAdvancerSuite) genRedoTaskAndRedoDMLManager() (*redoTask, *mockRedoDMLManager) {
	redoDMLManager := newMockRedoDMLManager()
	wrapper, _ := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)

	task := &redoTask{
		span:      suite.testSpan,
		tableSink: wrapper,
	}
	return task, redoDMLManager
}

func (suite *redoLogAdvancerSuite) genMemQuota(initMemQuota uint64) *memquota.MemQuota {
	memoryQuota := memquota.NewMemQuota(suite.testChangefeedID, suite.defaultTestMemQuota, "sink")
	memoryQuota.ForceAcquire(initMemQuota)
	memoryQuota.AddTable(suite.testSpan)
	return memoryQuota
}

func (suite *redoLogAdvancerSuite) TestNewRedoLogAdvancer() {
	task, manager := suite.genRedoTaskAndRedoDMLManager()
	memoryQuota := suite.genMemQuota(512)
	defer memoryQuota.Close()
	advancer := newRedoLogAdvancer(task, memoryQuota, 512, manager)
	require.NotNil(suite.T(), advancer)
	require.Equal(suite.T(), uint64(512), advancer.availableMem)
}

func (suite *redoLogAdvancerSuite) TestHasEnoughMem() {
	memoryQuota := suite.genMemQuota(512)
	defer memoryQuota.Close()
	task, manager := suite.genRedoTaskAndRedoDMLManager()
	advancer := newRedoLogAdvancer(task, memoryQuota, 512, manager)
	require.NotNil(suite.T(), advancer)
	require.True(suite.T(), advancer.hasEnoughMem())
	for i := 0; i < 6; i++ {
		// 6 * 256 = 1536 > 1024
		advancer.appendEvents([]*model.RowChangedEvent{{}}, 256)
	}
	require.False(suite.T(), advancer.hasEnoughMem(),
		"hasEnoughMem should return false when usedMem > availableMem")
}

func (suite *redoLogAdvancerSuite) TestCleanup() {
	memoryQuota := suite.genMemQuota(512)
	defer memoryQuota.Close()
	task, manager := suite.genRedoTaskAndRedoDMLManager()
	advancer := newRedoLogAdvancer(task, memoryQuota, 512, manager)
	require.NotNil(suite.T(), advancer)
	require.Equal(suite.T(), uint64(512), advancer.availableMem)
	require.Equal(suite.T(), uint64(0), advancer.usedMem)
	require.Equal(suite.T(), uint64(512), memoryQuota.GetUsedBytes())
	advancer.cleanup()
	require.Equal(suite.T(), uint64(0), memoryQuota.GetUsedBytes(),
		"memory quota should be released after cleanup")
}

func (suite *redoLogAdvancerSuite) TestAppendEvents() {
	memoryQuota := suite.genMemQuota(512)
	defer memoryQuota.Close()
	task, manager := suite.genRedoTaskAndRedoDMLManager()
	advancer := newRedoLogAdvancer(task, memoryQuota, 512, manager)
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

func (suite *redoLogAdvancerSuite) TestTryMoveMoveToNextTxn() {
	memoryQuota := suite.genMemQuota(512)
	defer memoryQuota.Close()
	task, manager := suite.genRedoTaskAndRedoDMLManager()
	advancer := newRedoLogAdvancer(task, memoryQuota, 512, manager)
	require.NotNil(suite.T(), advancer)

	// Initial state.
	require.Equal(suite.T(), uint64(0), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(0), advancer.currTxnCommitTs)
	pos := sorter.Position{StartTs: 1, CommitTs: 3}
	// Append 1 event with commit ts 1
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 1},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(1, pos)
	require.Equal(suite.T(), uint64(0), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(1), advancer.currTxnCommitTs)

	// Append 2 events with commit ts 2
	for i := 0; i < 2; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 2},
		}, 256)
	}
	require.Equal(suite.T(), uint64(768), advancer.usedMem)
	require.Equal(suite.T(), uint64(0), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(1), advancer.currTxnCommitTs)

	// Try to move to next txn.
	advancer.tryMoveToNextTxn(2, pos)
	require.Equal(suite.T(), uint64(1), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(2), advancer.currTxnCommitTs)

	// Set pos to a commit fence
	pos = sorter.Position{
		StartTs:  2,
		CommitTs: 3,
	}
	// Try to move to next txn.
	advancer.tryMoveToNextTxn(2, pos)
	require.Equal(suite.T(), uint64(2), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(2), advancer.currTxnCommitTs)
}

func (suite *redoLogAdvancerSuite) TestAdvance() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, manager := suite.genRedoTaskAndRedoDMLManager()
	advancer := newRedoLogAdvancer(task, memoryQuota, 768, manager)
	require.NotNil(suite.T(), advancer)

	pos := sorter.Position{StartTs: 1, CommitTs: 3}
	// 1. append 1 event with commit ts 1
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 1},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(1, pos)

	// 2. append 2 events with commit ts 2
	for i := 0; i < 2; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 2},
		}, 256)
	}
	advancer.tryMoveToNextTxn(2, pos)

	require.Equal(suite.T(), uint64(768), advancer.pendingTxnSize)
	err := advancer.advance(ctx, 256)
	require.NoError(suite.T(), err)

	require.Len(suite.T(), manager.getEvents(suite.testSpan), 3)
	require.Equal(suite.T(), uint64(1), manager.GetResolvedTs(suite.testSpan))
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(768), memoryQuota.GetUsedBytes())
	manager.releaseRowsMemory(suite.testSpan)
	require.Equal(suite.T(), uint64(256), memoryQuota.GetUsedBytes(),
		"memory quota should be released after releaseRowsMemory is called")
}

func (suite *redoLogAdvancerSuite) TestTryAdvanceWhenExceedAvailableMem() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, manager := suite.genRedoTaskAndRedoDMLManager()
	advancer := newRedoLogAdvancer(task, memoryQuota, 768, manager)
	require.NotNil(suite.T(), advancer)

	pos := sorter.Position{StartTs: 1, CommitTs: 2}
	// 1. append 1 event with commit ts 2
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 2},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(2, pos)

	// 2. append 3 events with commit ts 3
	for i := 0; i < 3; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 3},
		}, 256)
	}
	require.Equal(suite.T(), uint64(1024), advancer.usedMem)
	pos = sorter.Position{StartTs: 2, CommitTs: 3}
	advancer.tryMoveToNextTxn(3, pos)

	require.Equal(suite.T(), uint64(1024), advancer.pendingTxnSize)
	require.Equal(suite.T(), uint64(768), memoryQuota.GetUsedBytes())
	// 3. Try advance with txn is finished.
	advanced, err := advancer.tryAdvanceAndAcquireMem(
		ctx,
		256,
		false,
		true,
	)
	require.NoError(suite.T(), err)
	require.True(suite.T(), advanced)
	require.Equal(suite.T(), uint64(1024), memoryQuota.GetUsedBytes(),
		"Memory quota should be force acquired when exceed available memory.",
	)

	require.Len(suite.T(), manager.getEvents(suite.testSpan), 4)
	require.Equal(suite.T(), uint64(3), manager.GetResolvedTs(suite.testSpan))
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
	manager.releaseRowsMemory(suite.testSpan)
	require.Equal(suite.T(), uint64(256), memoryQuota.GetUsedBytes(),
		"memory quota should be released after releaseRowsMemory is called")
}

func (suite *redoLogAdvancerSuite) TestTryAdvanceWhenReachTheMaxUpdateIntSizeAndTxnNotFinished() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, manager := suite.genRedoTaskAndRedoDMLManager()
	advancer := newRedoLogAdvancer(task, memoryQuota, 768, manager)
	require.NotNil(suite.T(), advancer)

	pos := sorter.Position{StartTs: 1, CommitTs: 2}
	// 1. append 1 event with commit ts 2
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 2},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(2, pos)

	// 2. append 2 events with commit ts 3
	for i := 0; i < 2; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 3},
		}, 256)
	}
	require.Equal(suite.T(), uint64(768), advancer.usedMem)
	pos = sorter.Position{StartTs: 1, CommitTs: 3}
	advancer.tryMoveToNextTxn(3, pos)

	// 3. Try advance with txn is not finished.
	advanced, err := advancer.tryAdvanceAndAcquireMem(
		ctx,
		256,
		false,
		false,
	)
	require.NoError(suite.T(), err)
	require.True(suite.T(), advanced)
	require.Len(suite.T(), manager.getEvents(suite.testSpan), 3)
	require.Equal(suite.T(), uint64(2), manager.GetResolvedTs(suite.testSpan))
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
	manager.releaseRowsMemory(suite.testSpan)
	require.Equal(suite.T(), uint64(512), memoryQuota.GetUsedBytes(),
		"memory quota should be released after releaseRowsMemory is called")
}

func (suite *redoLogAdvancerSuite) TestFinish() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, manager := suite.genRedoTaskAndRedoDMLManager()
	advancer := newRedoLogAdvancer(task, memoryQuota, 768, manager)
	require.NotNil(suite.T(), advancer)

	pos := sorter.Position{StartTs: 1, CommitTs: 2}
	// 1. append 1 event with commit ts 2
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 2},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(2, pos)

	// 2. append 2 events with commit ts 3
	for i := 0; i < 2; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 3},
		}, 256)
	}
	require.Equal(suite.T(), uint64(768), advancer.usedMem)
	pos = sorter.Position{StartTs: 1, CommitTs: 3}
	advancer.tryMoveToNextTxn(3, pos)

	require.Equal(suite.T(), uint64(2), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(3), advancer.currTxnCommitTs)
	// 3. Try finish.
	err := advancer.finish(
		ctx,
		256,
		pos,
	)
	require.NoError(suite.T(), err)

	// All events should be flushed and the last pos should be updated.
	require.Equal(suite.T(), pos, advancer.lastPos)
	require.Equal(suite.T(), uint64(3), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(3), advancer.currTxnCommitTs)

	require.Len(suite.T(), manager.getEvents(suite.testSpan), 3)
	require.Equal(suite.T(), uint64(3), manager.GetResolvedTs(suite.testSpan))
	require.Equal(suite.T(), uint64(0), advancer.pendingTxnSize)
	manager.releaseRowsMemory(suite.testSpan)
	require.Equal(suite.T(), uint64(256), memoryQuota.GetUsedBytes(),
		"memory quota should be released after releaseRowsMemory is called")
}

func (suite *redoLogAdvancerSuite) TestTryAdvanceAndBlockAcquireWithSplitTxn() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	memoryQuota := suite.genMemQuota(768)
	defer memoryQuota.Close()
	task, manager := suite.genRedoTaskAndRedoDMLManager()
	advancer := newRedoLogAdvancer(task, memoryQuota, 768, manager)
	require.NotNil(suite.T(), advancer)

	pos := sorter.Position{StartTs: 1, CommitTs: 2}
	// 1. append 1 event with commit ts 2
	advancer.appendEvents([]*model.RowChangedEvent{
		{CommitTs: 2},
	}, 256)
	require.Equal(suite.T(), uint64(256), advancer.usedMem)
	advancer.tryMoveToNextTxn(2, pos)

	// 2. append 3 events with commit ts 3, this will exceed the memory quota.
	for i := 0; i < 3; i++ {
		advancer.appendEvents([]*model.RowChangedEvent{
			{CommitTs: 3},
		}, 256)
	}
	require.Equal(suite.T(), uint64(1024), advancer.usedMem)
	pos = sorter.Position{StartTs: 1, CommitTs: 3}
	advancer.tryMoveToNextTxn(3, pos)

	// 3. Last pos is a commit fence.
	advancer.lastPos = sorter.Position{
		StartTs:  2,
		CommitTs: 3,
	}

	down := make(chan struct{})
	go func() {
		// 4. Try advance and block acquire.
		advanced, err := advancer.tryAdvanceAndAcquireMem(
			ctx,
			256,
			false,
			false,
		)
		require.False(suite.T(), advanced)
		require.ErrorIs(suite.T(), err, context.Canceled)
		down <- struct{}{}
	}()

	// Wait all events are flushed.
	require.Eventually(suite.T(), func() bool {
		return len(manager.getEvents(suite.testSpan)) == 4
	}, 5*time.Second, 10*time.Millisecond)
	// After ack, abort the blocked acquire.
	memoryQuota.Close()
	<-down
}
