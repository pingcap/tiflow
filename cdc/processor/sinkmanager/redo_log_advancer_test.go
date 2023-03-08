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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var _ redo.DMLManager = &mockRedoDMLManager{}

type mockRedoDMLManager struct{}

func (m *mockRedoDMLManager) Enabled() bool {
	panic("unreachable")
}

func (m *mockRedoDMLManager) Run(ctx context.Context) error {
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
	panic("unreachable")
}

func (m *mockRedoDMLManager) GetResolvedTs(span tablepb.Span) model.Ts {
	panic("unreachable")
}

func (m *mockRedoDMLManager) EmitRowChangedEvents(ctx context.Context,
	span tablepb.Span, releaseRowsMemory func(), rows ...*model.RowChangedEvent,
) error {
	panic("unreachable")
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

func (suite *redoLogAdvancerSuite) genRedoTaskAndRedoDMLManager() (*redoTask, redo.DMLManager) {
	redoDMLManager := &mockRedoDMLManager{}
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
	pos := engine.Position{StartTs: 1, CommitTs: 3}
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
	pos = engine.Position{
		StartTs:  2,
		CommitTs: 3,
	}
	// Try to move to next txn.
	advancer.tryMoveToNextTxn(2, pos)
	require.Equal(suite.T(), uint64(2), advancer.lastTxnCommitTs)
	require.Equal(suite.T(), uint64(2), advancer.currTxnCommitTs)
}
