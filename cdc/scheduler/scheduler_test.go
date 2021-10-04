// Copyright 2021 PingCAP, Inc.
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

package scheduler

import (
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/scheduler/util"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockScheduleDispatcherCommunicator struct {
	mock.Mock
	addTableRecords    map[model.CaptureID][]model.TableID
	removeTableRecords map[model.CaptureID][]model.TableID
}

func NewMockScheduleDispatcherCommunicator() *mockScheduleDispatcherCommunicator {
	return &mockScheduleDispatcherCommunicator{
		addTableRecords:    map[model.CaptureID][]model.TableID{},
		removeTableRecords: map[model.CaptureID][]model.TableID{},
	}
}

func (m *mockScheduleDispatcherCommunicator) Reset() {
	m.addTableRecords = map[model.CaptureID][]model.TableID{}
	m.removeTableRecords = map[model.CaptureID][]model.TableID{}
	m.Mock.ExpectedCalls = nil
	m.Mock.Calls = nil
}

func (m *mockScheduleDispatcherCommunicator) DispatchTable(
	ctx cdcContext.Context,
	changeFeedID model.ChangeFeedID,
	tableID model.TableID,
	captureID model.CaptureID,
	boundaryTs model.Ts,
	isDelete bool,
) (done bool, err error) {
	log.Info("dispatch table called",
		zap.String("changefeed-id", changeFeedID),
		zap.Int64("table-id", tableID),
		zap.String("capture-id", captureID),
		zap.Uint64("boundary-ts", boundaryTs),
		zap.Bool("is-delete", isDelete))
	if !isDelete {
		m.addTableRecords[captureID] = append(m.addTableRecords[captureID], tableID)
	} else {
		m.removeTableRecords[captureID] = append(m.removeTableRecords[captureID], tableID)
	}
	args := m.Called(ctx, changeFeedID, tableID, captureID, boundaryTs, isDelete)
	return args.Bool(0), args.Error(1)
}

func (m *mockScheduleDispatcherCommunicator) Announce(
	ctx cdcContext.Context,
	changeFeedID model.ChangeFeedID,
	captureID model.CaptureID,
) (done bool, err error) {
	args := m.Called(ctx, changeFeedID, captureID)
	return args.Bool(0), args.Error(1)
}

func TestDispatchTable(t *testing.T) {
	defer testleak.AfterTestT(t)()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()

	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	communicator.On("Announce", mock.Anything, "cf-1", "capture-1").Return(true, nil)
	communicator.On("Announce", mock.Anything, "cf-1", "capture-2").Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "DispatchTable", mock.Anything)

	dispatcher.OnAgentSyncTaskStatuses("capture-1", []model.TableID{}, []model.TableID{}, []model.TableID{})
	dispatcher.OnAgentSyncTaskStatuses("capture-2", []model.TableID{}, []model.TableID{}, []model.TableID{})

	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(1), mock.Anything, model.Ts(1000), false).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(2), mock.Anything, model.Ts(1000), false).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(3), mock.Anything, model.Ts(1000), false).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce", mock.Anything)
	require.NotEqual(t, 0, len(communicator.addTableRecords["capture-1"]))
	require.NotEqual(t, 0, len(communicator.addTableRecords["capture-2"]))
	require.Equal(t, 0, len(communicator.removeTableRecords["capture-1"]))
	require.Equal(t, 0, len(communicator.removeTableRecords["capture-2"]))

	dispatcher.OnAgentCheckpoint("capture-1", 2000, 2000)
	dispatcher.OnAgentCheckpoint("capture-1", 2001, 2001)

	communicator.ExpectedCalls = nil
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")
	communicator.AssertNotCalled(t, "DispatchTable")

	for captureID, tables := range communicator.addTableRecords {
		for _, tableID := range tables {
			dispatcher.OnAgentFinishedTableOperation(captureID, tableID)
		}
	}

	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1000), checkpointTs)
	require.Equal(t, model.Ts(1000), resolvedTs)
	communicator.AssertNotCalled(t, "Announce")
	communicator.AssertNotCalled(t, "DispatchTable")

	dispatcher.OnAgentCheckpoint("capture-1", 1100, 1400)
	dispatcher.OnAgentCheckpoint("capture-2", 1200, 1300)
	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1100), checkpointTs)
	require.Equal(t, model.Ts(1300), resolvedTs)
	communicator.AssertNotCalled(t, "Announce")
	communicator.AssertNotCalled(t, "DispatchTable")
}

func TestRemoveTable(t *testing.T) {
	defer testleak.AfterTestT(t)()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-2",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    runningTable,
	})

	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1500), checkpointTs)
	require.Equal(t, model.Ts(1500), resolvedTs)
	communicator.AssertNotCalled(t, "Announce")
	communicator.AssertNotCalled(t, "DispatchTable")

	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(3), "capture-1", model.Ts(1500), true).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")

	dispatcher.OnAgentFinishedTableOperation("capture-1", 3)
	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1500), checkpointTs)
	require.Equal(t, model.Ts(1500), resolvedTs)
	communicator.AssertNotCalled(t, "Announce")
	communicator.AssertNotCalled(t, "DispatchTable")
}

func TestCaptureGone(t *testing.T) {
	defer testleak.AfterTestT(t)()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		// capture-2 is gone
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-2",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    runningTable,
	})

	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(2), "capture-1", model.Ts(1500), false).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")
}

func TestCaptureRestarts(t *testing.T) {
	defer testleak.AfterTestT(t)()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-2",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    runningTable,
	})

	dispatcher.OnAgentSyncTaskStatuses("capture-2", []model.TableID{}, []model.TableID{}, []model.TableID{})
	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(2), "capture-2", model.Ts(1500), false).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")
}

func TestManualMoveTable(t *testing.T) {
	defer testleak.AfterTestT(t)()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-2",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    runningTable,
	})

	dispatcher.MoveTable(1, "capture-2")
	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(1), "capture-1", model.Ts(1300), true).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")

	dispatcher.OnAgentFinishedTableOperation("capture-1", 1)
	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(1), "capture-2", model.Ts(1300), false).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedTableOperation("capture-2", 1)
	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1300), checkpointTs)
	require.Equal(t, model.Ts(1550), resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")
	communicator.AssertNotCalled(t, "DispatchTable")
}

func TestCaptureGoneWhileMovingTable(t *testing.T) {
	defer testleak.AfterTestT(t)()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-2",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    runningTable,
	})

	dispatcher.MoveTable(1, "capture-2")
	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(1), "capture-1", model.Ts(1300), true).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")

	delete(mockCaptureInfos, "capture-2")
	dispatcher.OnAgentFinishedTableOperation("capture-1", 1)
	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(1), mock.Anything, model.Ts(1300), false).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(2), mock.Anything, model.Ts(1300), false).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
}

func TestManualRebalance(t *testing.T) {
	defer testleak.AfterTestT(t)()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
		"capture-3": {
			ID:            "capture-3",
			AdvertiseAddr: "fakeip:3",
		},
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
		},
		"capture-3": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1400,
			ResolvedTs:   1650,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-2",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   4,
		CaptureID: "capture-2",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   5,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   6,
		CaptureID: "capture-2",
		Status:    runningTable,
	})

	dispatcher.Rebalance()
	communicator.On("DispatchTable", mock.Anything, "cf-1", mock.Anything, "capture-1", model.Ts(1300), true).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything, "cf-1", mock.Anything, "capture-2", model.Ts(1300), true).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")

	communicator.ExpectedCalls = nil
	for captureID, tables := range communicator.removeTableRecords {
		for _, tableID := range tables {
			dispatcher.OnAgentFinishedTableOperation(captureID, tableID)
			communicator.On("DispatchTable", mock.Anything, "cf-1", tableID, "capture-3", model.Ts(1300), false).
				Return(true, nil)
		}
	}
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")
}

func TestRebalanceReentrant(t *testing.T) {
	defer testleak.AfterTestT(t)()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
		"capture-3": {
			ID:            "capture-3",
			AdvertiseAddr: "fakeip:3",
		},
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
		},
		"capture-3": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1400,
			ResolvedTs:   1650,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-2",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   4,
		CaptureID: "capture-2",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   5,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   6,
		CaptureID: "capture-2",
		Status:    runningTable,
	})

	dispatcher.Rebalance()
	communicator.On("DispatchTable", mock.Anything, "cf-1", mock.Anything, mock.Anything, model.Ts(1300), true).
		Return(false, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNumberOfCalls(t, "DispatchTable", 1)
	communicator.AssertNotCalled(t, "Announce")

	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything, "cf-1", mock.Anything, mock.Anything, model.Ts(1300), true).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertNumberOfCalls(t, "DispatchTable", 2)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")
}

func TestIgnoreEmptyCapture(t *testing.T) {
	defer testleak.AfterTestT(t)()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
		"capture-3": {
			ID:            "capture-3",
			AdvertiseAddr: "fakeip:3",
		},
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
		},
		"capture-3": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 900,
			ResolvedTs:   1650,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-2",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   4,
		CaptureID: "capture-2",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   5,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   6,
		CaptureID: "capture-2",
		Status:    runningTable,
	})

	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1300), checkpointTs)
	require.Equal(t, model.Ts(1550), resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "DispatchTable")
	communicator.AssertNotCalled(t, "Announce")
}

func TestRebalanceWhileAddingTable(t *testing.T) {
	defer testleak.AfterTestT(t)()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   4,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   5,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   6,
		CaptureID: "capture-1",
		Status:    runningTable,
	})

	communicator.On("DispatchTable", mock.Anything, "cf-1",  model.TableID(7), "capture-2", model.Ts(1300), false).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6, 7}, mockCaptureInfos)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")

	dispatcher.Rebalance()
	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6, 7}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")
	communicator.AssertNotCalled(t, "DispatchTable")

	dispatcher.OnAgentFinishedTableOperation("capture-2", model.TableID(7))
	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything, "cf-1", mock.Anything, mock.Anything, model.Ts(1300), true).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6, 7}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertNumberOfCalls(t, "DispatchTable", 2)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")
}

func TestManualMoveTableWhileAddingTable(t *testing.T) {
	defer testleak.AfterTestT(t)()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher("cf-1", communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-2",
		Status:    runningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    runningTable,
	})

	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(1), "capture-2", model.Ts(1300), true).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, mockCaptureInfos)


	dispatcher.MoveTable(1, "capture-2")
	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(1), "capture-1", model.Ts(1300), true).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")

	dispatcher.OnAgentFinishedTableOperation("capture-1", 1)
	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(1), "capture-2", model.Ts(1300), false).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedTableOperation("capture-2", 1)
	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1300), checkpointTs)
	require.Equal(t, model.Ts(1550), resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNotCalled(t, "Announce")
	communicator.AssertNotCalled(t, "DispatchTable")
}
