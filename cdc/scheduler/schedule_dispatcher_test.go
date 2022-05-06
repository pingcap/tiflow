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
	"fmt"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/util"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var _ ScheduleDispatcherCommunicator = (*mockScheduleDispatcherCommunicator)(nil)

const (
	defaultEpoch = "default-epoch"
	nextEpoch    = "next-epoch"
)

type mockScheduleDispatcherCommunicator struct {
	mock.Mock
	addTableRecords    map[model.CaptureID][]model.TableID
	removeTableRecords map[model.CaptureID][]model.TableID

	isBenchmark bool
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
	isDelete bool,
	epoch model.ProcessorEpoch,
) (done bool, err error) {
	if !m.isBenchmark {
		log.Info("dispatch table called",
			zap.String("changefeed", changeFeedID.ID),
			zap.Int64("tableID", tableID),
			zap.String("captureID", captureID),
			zap.Bool("isDelete", isDelete),
			zap.String("epoch", epoch))
		if !isDelete {
			m.addTableRecords[captureID] = append(m.addTableRecords[captureID], tableID)
		} else {
			m.removeTableRecords[captureID] = append(m.removeTableRecords[captureID], tableID)
		}
	}
	args := m.Called(ctx, changeFeedID, tableID, captureID, isDelete, epoch)
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

// read-only variable
var defaultMockCaptureInfos = map[model.CaptureID]*model.CaptureInfo{
	"capture-1": {
		ID:            "capture-1",
		AdvertiseAddr: "fakeip:1",
	},
	"capture-2": {
		ID:            "capture-2",
		AdvertiseAddr: "fakeip:2",
	},
}

func TestDispatchTable(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)

	communicator.On("Announce", mock.Anything,
		cf1, "capture-1").Return(true, nil)
	communicator.On("Announce", mock.Anything,
		cf1, "capture-2").Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentSyncTaskStatuses("capture-1", defaultEpoch, []model.TableID{}, []model.TableID{}, []model.TableID{})
	dispatcher.OnAgentSyncTaskStatuses("capture-2", defaultEpoch, []model.TableID{}, []model.TableID{}, []model.TableID{})

	communicator.Reset()
	// Injects a dispatch table failure
	communicator.On("DispatchTable", mock.Anything,
		cf1, mock.Anything, mock.Anything, false, defaultEpoch).
		Return(false, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything, cf1,
		model.TableID(1), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything, cf1,
		model.TableID(2), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything, cf1,
		model.TableID(3), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	require.NotEqual(t, 0, len(communicator.addTableRecords["capture-1"]))
	require.NotEqual(t, 0, len(communicator.addTableRecords["capture-2"]))
	require.Equal(t, 0, len(communicator.removeTableRecords["capture-1"]))
	require.Equal(t, 0, len(communicator.removeTableRecords["capture-2"]))

	dispatcher.OnAgentCheckpoint("capture-1", 2000, 2000)
	dispatcher.OnAgentCheckpoint("capture-1", 2001, 2001)

	communicator.ExpectedCalls = nil
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	communicator.AssertExpectations(t)

	for captureID, tables := range communicator.addTableRecords {
		for _, tableID := range tables {
			dispatcher.OnAgentFinishedTableOperation(captureID, tableID, defaultEpoch)
		}
	}

	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1000), checkpointTs)
	require.Equal(t, model.Ts(1000), resolvedTs)

	dispatcher.OnAgentCheckpoint("capture-1", 1100, 1400)
	dispatcher.OnAgentCheckpoint("capture-2", 1200, 1300)
	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1100), checkpointTs)
	require.Equal(t, model.Ts(1300), resolvedTs)
}

func TestSyncCaptures(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{} // empty capture status
	communicator.On("Announce", mock.Anything, cf1, "capture-1").
		Return(false, nil)
	communicator.On("Announce", mock.Anything, cf1, "capture-2").
		Return(false, nil)

	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3, 4, 5}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	communicator.Reset()
	communicator.On("Announce", mock.Anything, cf1, "capture-1").
		Return(true, nil)
	communicator.On("Announce", mock.Anything, cf1, "capture-2").
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3, 4, 5}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	dispatcher.OnAgentSyncTaskStatuses("capture-1", defaultEpoch, []model.TableID{1, 2, 3}, []model.TableID{4, 5}, []model.TableID{6, 7})
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3, 4, 5}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	communicator.Reset()
	dispatcher.OnAgentFinishedTableOperation("capture-1", 4, defaultEpoch)
	dispatcher.OnAgentFinishedTableOperation("capture-1", 5, defaultEpoch)
	dispatcher.OnAgentSyncTaskStatuses("capture-2", defaultEpoch, []model.TableID(nil), []model.TableID(nil), []model.TableID(nil))
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3, 4, 5}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	communicator.Reset()
	dispatcher.OnAgentFinishedTableOperation("capture-1", 6, defaultEpoch)
	dispatcher.OnAgentFinishedTableOperation("capture-1", 7, defaultEpoch)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3, 4, 5}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1500), checkpointTs)
	require.Equal(t, model.Ts(1500), resolvedTs)
}

func TestSyncUnknownCapture(t *testing.T) {
	t.Parallel()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher(
		model.DefaultChangeFeedID("cf-1"), communicator,
		1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{} // empty capture status

	// Sends a sync from an unknown capture
	dispatcher.OnAgentSyncTaskStatuses("capture-1", defaultEpoch, []model.TableID{1, 2, 3}, []model.TableID{4, 5}, []model.TableID{6, 7})

	// We expect the `Sync` to be ignored.
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3, 4, 5}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
}

func TestRemoveTable(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
			Epoch:        defaultEpoch,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})

	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1500), checkpointTs)
	require.Equal(t, model.Ts(1500), resolvedTs)

	// Inject a dispatch table failure
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(3), "capture-1", true, defaultEpoch).
		Return(false, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(3), "capture-1", true, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedTableOperation("capture-1", 3, defaultEpoch)
	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1500), checkpointTs)
	require.Equal(t, model.Ts(1500), resolvedTs)
}

func TestCaptureGone(t *testing.T) {
	t.Parallel()

	mockCaptureInfos := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		// capture-2 is gone
	}

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
			Epoch:        defaultEpoch,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})

	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(2), "capture-1", false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
}

func TestCaptureRestarts(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1500,
			Epoch:        defaultEpoch,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})

	dispatcher.OnAgentSyncTaskStatuses("capture-2", nextEpoch, []model.TableID{}, []model.TableID{}, []model.TableID{})
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(2), "capture-2", false, nextEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
}

func TestCaptureGoneWhileMovingTable(t *testing.T) {
	t.Parallel()

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
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
			Epoch:        defaultEpoch,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})

	dispatcher.MoveTable(1, "capture-2")
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(1), "capture-1", true, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	delete(mockCaptureInfos, "capture-2")
	dispatcher.OnAgentFinishedTableOperation("capture-1", 1, defaultEpoch)
	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(1), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(2), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
}

func TestRebalance(t *testing.T) {
	t.Parallel()

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
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
			Epoch:        defaultEpoch,
		},
		"capture-3": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1400,
			ResolvedTs:   1650,
			Epoch:        defaultEpoch,
		},
	}
	for i := 1; i <= 6; i++ {
		dispatcher.tables.AddTableRecord(&util.TableRecord{
			TableID:   model.TableID(i),
			CaptureID: fmt.Sprintf("capture-%d", (i+1)%2+1),
			Status:    util.RunningTable,
		})
	}

	dispatcher.Rebalance()
	communicator.On("DispatchTable", mock.Anything,
		cf1, mock.Anything, mock.Anything, true, defaultEpoch).
		Return(false, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNumberOfCalls(t, "DispatchTable", 1)

	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, mock.Anything, mock.Anything, true, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertNumberOfCalls(t, "DispatchTable", 2)
	communicator.AssertExpectations(t)
}

func TestIgnoreEmptyCapture(t *testing.T) {
	t.Parallel()

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
	dispatcher := NewBaseScheduleDispatcher(
		model.DefaultChangeFeedID("cf-1"),
		communicator, 1000)
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
	for i := 1; i <= 6; i++ {
		dispatcher.tables.AddTableRecord(&util.TableRecord{
			TableID:   model.TableID(i),
			CaptureID: fmt.Sprintf("capture-%d", (i+1)%2+1),
			Status:    util.RunningTable,
		})
	}

	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1300), checkpointTs)
	require.Equal(t, model.Ts(1550), resolvedTs)
	communicator.AssertExpectations(t)
}

func TestIgnoreDeadCapture(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher(
		model.DefaultChangeFeedID("cf-1"), communicator,
		1000)
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
	for i := 1; i <= 6; i++ {
		dispatcher.tables.AddTableRecord(&util.TableRecord{
			TableID:   model.TableID(i),
			CaptureID: fmt.Sprintf("capture-%d", (i+1)%2+1),
			Status:    util.RunningTable,
		})
	}

	// A dead capture sends very old watermarks.
	// They should be ignored.
	dispatcher.OnAgentCheckpoint("capture-3", 1000, 1000)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1300), checkpointTs)
	require.Equal(t, model.Ts(1550), resolvedTs)
	communicator.AssertExpectations(t)
}

func TestIgnoreUnsyncedCaptures(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	dispatcher := NewBaseScheduleDispatcher(
		model.DefaultChangeFeedID("cf-1"),
		communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncSent, // not synced
			CheckpointTs: 1400,
			ResolvedTs:   1500,
			Epoch:        "garbage",
		},
	}

	for i := 1; i <= 6; i++ {
		dispatcher.tables.AddTableRecord(&util.TableRecord{
			TableID:   model.TableID(i),
			CaptureID: fmt.Sprintf("capture-%d", (i+1)%2+1),
			Status:    util.RunningTable,
		})
	}

	dispatcher.OnAgentCheckpoint("capture-2", 1000, 1000)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	communicator.Reset()
	dispatcher.OnAgentSyncTaskStatuses("capture-2", defaultEpoch, []model.TableID{2, 4, 6}, []model.TableID{}, []model.TableID{})
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1300), checkpointTs)
	require.Equal(t, model.Ts(1500), resolvedTs)
	communicator.AssertExpectations(t)
}

func TestRebalanceWhileAddingTable(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
			Epoch:        defaultEpoch,
		},
	}
	for i := 1; i <= 6; i++ {
		dispatcher.tables.AddTableRecord(&util.TableRecord{
			TableID:   model.TableID(i),
			CaptureID: "capture-1",
			Status:    util.RunningTable,
		})
	}

	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(7), "capture-2", false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6, 7}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.Rebalance()
	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6, 7}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedTableOperation("capture-2", model.TableID(7), defaultEpoch)
	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, mock.Anything, mock.Anything, true, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6, 7}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertNumberOfCalls(t, "DispatchTable", 2)
	communicator.AssertExpectations(t)
}

func TestDrainingCaptureWhileMoveTable(t *testing.T) {
	t.Parallel()
}

func TestRebalanceWhileDrainingCapture(t *testing.T) {
	t.Parallel()
}

func TestDrainingCaptureWhileRebalance(t *testing.T) {
	t.Parallel()
}

func TestDrainingCapture(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
			Epoch:        defaultEpoch,
		},
		"capture-3": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1400,
			ResolvedTs:   1450,
			Epoch:        defaultEpoch,
		},
	}
	// initialize each capture with 2 tables.
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(0),
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(1),
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(2),
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(3),
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(4),
		CaptureID: "capture-3",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(5),
		CaptureID: "capture-3",
		Status:    util.RunningTable,
	})

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

	// drain the `capture-1`
	err := dispatcher.DrainCapture("capture-1")
	require.Nil(t, err)
	// inject `DispatchTable` method
	communicator.On("DispatchTable", mock.Anything, "cf-1", mock.Anything, mock.Anything, mock.Anything, defaultEpoch).Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3, 4, 5}, mockCaptureInfos)
	require.Nil(t, err)
	// some table is `AddingTable`, so that cannot make progress at the moment
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	require.ElementsMatch(t, []model.CaptureID{"capture-2", "capture-3"}, dispatcher.balancerCandidates)

	tablesByCapture := dispatcher.tables.GetAllTablesGroupedByCaptures()
	require.Equal(t, 2, len(tablesByCapture["capture-1"]))
	require.Equal(t, 2, len(tablesByCapture["capture-2"]))
	require.Equal(t, 2, len(tablesByCapture["capture-3"]))

	for _, record := range tablesByCapture["capture-1"] {
		require.Equal(t, record.Status, util.RemovingTable)
	}
	communicator.AssertExpectations(t)

	// receive ack message from agents.
	for _, record := range tablesByCapture["capture-1"] {
		dispatcher.OnAgentFinishedTableOperation("capture-1", record.TableID, defaultEpoch)
	}

	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3, 4, 5}, mockCaptureInfos)
	require.Nil(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	tablesByCapture = dispatcher.tables.GetAllTablesGroupedByCaptures()
	for captureID, tables := range tablesByCapture {
		require.Equal(t, 3, len(tables))
		for _, record := range tables {
			if record.Status == util.AddingTable {
				dispatcher.OnAgentFinishedTableOperation(captureID, record.TableID, defaultEpoch)
			}
		}
	}

	// captures should send checkpoint back to the dispatcher
	dispatcher.OnAgentCheckpoint("capture-2", 1300, 1550)
	dispatcher.OnAgentCheckpoint("capture-3", 1300, 1450)

	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3, 4, 5}, mockCaptureInfos)
	require.Nil(t, err)
	require.Equal(t, uint64(1300), checkpointTs)
	require.Equal(t, uint64(1450), resolvedTs)

	// after one capture drain finished,
	// a new one should be online, and the old drained one should be offline.
	delete(mockCaptureInfos, "capture-1")
	mockCaptureInfos["capture-4"] = &model.CaptureInfo{
		ID: "capture-4",
		// only ID changed, keep the `AdvertiseAddr` the same.
		AdvertiseAddr: "fakeip:1",
	}

	// drain the `capture-2`
	err = dispatcher.DrainCapture("capture-2")
	require.Nil(t, err)
	communicator.On("Announce", mock.Anything, "cf-1", "capture-4").Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3, 4, 5}, mockCaptureInfos)
	require.Nil(t, err)
	// new capture does not sync status to the dispatcher, so does not make progress.
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	require.ElementsMatch(t, []model.CaptureID{"capture-3", "capture-4"}, dispatcher.balancerCandidates)
	dispatcher.OnAgentSyncTaskStatuses("capture-4", defaultEpoch, []model.TableID{}, []model.TableID{}, []model.TableID{})
	// this tick should handle draining capture-2
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3, 4, 5}, mockCaptureInfos)
	require.Nil(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	// all capture-2's tables should be in `RemovingTable`
	tablesByCapture = dispatcher.tables.GetAllTablesGroupedByCaptures()
	require.Equal(t, 3, len(tablesByCapture["capture-2"]))
	for _, record := range tablesByCapture["capture-2"] {
		require.Equal(t, util.RemovingTable, record.Status)
		dispatcher.OnAgentFinishedTableOperation("capture-2", record.TableID, defaultEpoch)
	}
	// this tick should add tables to other captures
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3, 4, 5}, mockCaptureInfos)
	require.Nil(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	// all capture-2's table should be dispatched to the new capture-4
	require.Equal(t, dispatcher.tables.CountTableByCaptureID("capture-4"), 3)
	// receive ack message from agent
	tables := dispatcher.tables.GetAllTablesGroupedByCaptures()["capture-4"]
	for tableID, record := range tables {
		require.Equal(t, util.AddingTable, record.Status)
		dispatcher.OnAgentFinishedTableOperation("capture-4", tableID, defaultEpoch)
	}
	// capture-4 inherit all tables from capture-2, so the checkpointTs / resolvedTs should identical to capture-2's.
	dispatcher.OnAgentCheckpoint("capture-4", 1300, 1450)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3, 4, 5}, mockCaptureInfos)
	require.Nil(t, err)
	require.Equal(t, uint64(1300), checkpointTs)
	require.Equal(t, uint64(1450), resolvedTs)

	// the whole rolling upgrade process should be complete after the capture-3 drained, but that
	// process is identical draining capture-2, so ignore it at the moment.
}

func TestDrainingCaptureCrashed(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
			Epoch:        defaultEpoch,
		},
	}

	// initialize each capture with 2 tables.
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(0),
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(1),
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(2),
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(3),
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})

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

	communicator.On("DispatchTable", mock.Anything, "cf-1", mock.Anything, mock.Anything, mock.Anything, defaultEpoch).
		Return(true, nil)

	// 1. drain one capture
	err := dispatcher.DrainCapture("capture-2")
	require.Nil(t, err)

	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	tablesByCapture := dispatcher.tables.GetAllTablesGroupedByCaptures()
	tables4Capture2 := tablesByCapture["capture-2"]
	require.Equal(t, 2, len(tables4Capture2))
	for _, record := range tables4Capture2 {
		require.Equal(t, util.RemovingTable, record.Status)
	}

	// draining target capture crashed before send ack message.
	delete(mockCaptureInfos, "capture-2")
	// `capture-2` crashed, cannot make progress, and add capture-2's tables to capture-1.
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3}, mockCaptureInfos)
	require.Nil(t, err)
	require.Equal(t, dispatcher.drainTarget, captureIDNotDraining)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	tablesByCapture = dispatcher.tables.GetAllTablesGroupedByCaptures()
	tables4Capture1 := tablesByCapture["capture-1"]
	require.Equal(t, 4, len(tables4Capture1))
	require.Equal(t, util.AddingTable, tables4Capture1[2].Status)
	require.Equal(t, util.AddingTable, tables4Capture1[3].Status)

	_, ok := tablesByCapture["capture-2"]
	require.Equal(t, ok, false)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedTableOperation("capture-1", 2, defaultEpoch)
	dispatcher.OnAgentFinishedTableOperation("capture-1", 3, defaultEpoch)

	dispatcher.OnAgentCheckpoint("capture-1", 1300, 1550)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3}, mockCaptureInfos)
	require.Nil(t, err)
	require.Equal(t, uint64(1300), checkpointTs)
	require.Equal(t, uint64(1550), resolvedTs)
	communicator.AssertExpectations(t)
}

func TestDrainingOnlyOneCapture(t *testing.T) {
	t.Parallel()

	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
			Epoch:        defaultEpoch,
		},
	}

	// initialize each capture with 2 tables.
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(0),
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(1),
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	err := dispatcher.DrainCapture("capture-2")
	require.Error(t, err, cerror.ErrSchedulerDrainCaptureNotAllowed)
}

func TestDrainingOwnerOtherCrashed(t *testing.T) {
	t.Parallel()
}

func TestDrainingCaptureOwnerCrashed(t *testing.T) {
	t.Parallel()
}

func TestDrainingCaptureOtherCrashed(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
			Epoch:        defaultEpoch,
		},
	}

	// initialize each capture with 2 tables.
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(0),
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(1),
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(2),
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(3),
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})

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

	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1300), checkpointTs)
	require.Equal(t, model.Ts(1550), resolvedTs)
	communicator.AssertExpectations(t)

	communicator.On("DispatchTable", mock.Anything, "cf-1", mock.Anything, mock.Anything, mock.Anything, defaultEpoch).
		Return(true, nil)

	// 1. drain the `capture-2`
	err = dispatcher.DrainCapture("capture-2")
	require.Nil(t, err)

	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	tablesByCapture := dispatcher.tables.GetAllTablesGroupedByCaptures()
	tables4Capture2 := tablesByCapture["capture-2"]
	require.Equal(t, 2, len(tables4Capture2))
	for _, record := range tables4Capture2 {
		require.Equal(t, util.RemovingTable, record.Status)
	}

	// 2. before send ack back to the dispatcher, `capture-1` crashed.
	delete(mockCaptureInfos, "capture-1")
	// `capture-2` crashed, cannot make progress, and add capture-2's tables to capture-1.
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3}, mockCaptureInfos)
	require.Nil(t, err)
	require.Equal(t, dispatcher.drainTarget, "capture-2")
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	// since `capture-2` is draining, capture-1's tables cannot be dispatched to capture-2
	tablesByCapture = dispatcher.tables.GetAllTablesGroupedByCaptures()
	// `capture-1` crashed, no tables
	_, ok := tablesByCapture["capture-1"]
	require.False(t, ok)
	tables4Capture2 = tablesByCapture["capture-2"]
	require.Equal(t, 2, len(tables4Capture2))
	for _, record := range tables4Capture2 {
		require.Equal(t, util.RemovingTable, record.Status)
		dispatcher.OnAgentFinishedTableOperation("capture-2", record.TableID, defaultEpoch)
	}

	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3}, mockCaptureInfos)
	require.Nil(t, err)
	// since `capture-2` has no table, so the draining process finished.
	require.Equal(t, dispatcher.drainTarget, captureIDNotDraining)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	// all tables dispatched to `capture-2` back, to prevent no table dispatched.
	tables4Capture2 = dispatcher.tables.GetAllTablesGroupedByCaptures()["capture-2"]
	for _, record := range tables4Capture2 {
		require.Equal(t, util.AddingTable, record.Status)
		dispatcher.OnAgentFinishedTableOperation("capture-2", record.TableID, defaultEpoch)
	}

	dispatcher.OnAgentCheckpoint("capture-2", 1300, 1550)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3}, mockCaptureInfos)
	require.Nil(t, err)
	require.Equal(t, uint64(1300), checkpointTs)
	require.Equal(t, uint64(1550), resolvedTs)
	communicator.AssertExpectations(t)
}

func TestDrainingCaptureWhileAddingTable(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
			Epoch:        defaultEpoch,
		},
	}

	// initialize each capture with 2 tables.
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(0),
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(1),
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(2),
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(3),
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})

	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1300), checkpointTs)
	require.Equal(t, model.Ts(1550), resolvedTs)
	communicator.AssertExpectations(t)

	communicator.On("DispatchTable", mock.Anything, "cf-1", mock.Anything, mock.Anything, mock.Anything, defaultEpoch).
		Return(true, nil)

	// 1. drain one capture
	err = dispatcher.DrainCapture("capture-2")
	require.Nil(t, err)

	// 2. add a new table
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3, 4}, defaultMockCaptureInfos)
	require.Nil(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	// new table `table-4` should be adding to `capture-1`
	tablesByCapture := dispatcher.tables.GetAllTablesGroupedByCaptures()
	tables4Capture1 := tablesByCapture["capture-1"]
	require.Equal(t, 3, len(tables4Capture1))
	require.Equal(t, util.AddingTable, tables4Capture1[4].Status)
	dispatcher.OnAgentFinishedTableOperation("capture-1", 4, defaultEpoch)

	// tables in the `capture-2` should be in `RemovingTable` status
	tables4Capture2 := tablesByCapture["capture-2"]
	require.Equal(t, 2, len(tables4Capture2))
	for _, record := range tables4Capture2 {
		require.Equal(t, util.RemovingTable, record.Status)
		dispatcher.OnAgentFinishedTableOperation("capture-2", record.TableID, defaultEpoch)
	}

	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3, 4}, defaultMockCaptureInfos)
	require.Nil(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	// all tables should in capture-1, and 2 in `AddingTable` status
	tablesByCapture = dispatcher.tables.GetAllTablesGroupedByCaptures()
	require.Equal(t, 0, len(tablesByCapture["capture-2"]))
	tables4Capture1 = tablesByCapture["capture-1"]
	count := 0
	for _, record := range tables4Capture1 {
		if record.Status == util.AddingTable {
			dispatcher.OnAgentFinishedTableOperation("capture-1", record.TableID, defaultEpoch)
			count += 1
		}
	}
	require.Equal(t, count, 2)

	dispatcher.OnAgentCheckpoint("capture-1", 1300, 1550)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3, 4}, defaultMockCaptureInfos)
	require.Nil(t, err)
	require.Equal(t, model.Ts(1300), checkpointTs)
	require.Equal(t, model.Ts(1550), resolvedTs)
}

func TestManualMoveTableWhileDrainingCapture(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
			Epoch:        defaultEpoch,
		},
		"capture-3": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1400,
			ResolvedTs:   1450,
			Epoch:        defaultEpoch,
		},
	}
	// initialize each capture with 2 tables.
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(0),
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(1),
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(2),
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(3),
		CaptureID: "capture-2",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(4),
		CaptureID: "capture-3",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   model.TableID(5),
		CaptureID: "capture-3",
		Status:    util.RunningTable,
	})

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

	communicator.On("DispatchTable", mock.Anything, "cf-1", mock.Anything, mock.Anything, mock.Anything, defaultEpoch).Return(true, nil)

	// manually move table: `table-1` to `capture-2`
	dispatcher.MoveTable(1, "capture-2")
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3, 4, 5}, mockCaptureInfos)
	require.Nil(t, err)
	// cannot make progress, since `table-1` is removing from `capture-1`.
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	// `table-1` is not adding to `capture-2` yet, now drain the `capture-2`
	err = dispatcher.DrainCapture("capture-2")
	require.Error(t, err, cerror.ErrSchedulerDrainCaptureNotAllowed)

	// `table-1` is removed from `capture-1`
	dispatcher.OnAgentFinishedTableOperation("capture-1", 1, defaultEpoch)

	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3, 4, 5}, mockCaptureInfos)
	require.Nil(t, err)
	// cannot make progress, since `table-1` is adding to `capture-2`
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	err = dispatcher.DrainCapture("capture-2")
	require.Error(t, err, cerror.ErrSchedulerDrainCaptureNotAllowed)

	// move `table-1` from `capture-1` to `capture-2` finished
	dispatcher.OnAgentFinishedTableOperation("capture-2", 1, defaultEpoch)

	err = dispatcher.DrainCapture("capture-2")
	require.Nil(t, err)

	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{0, 1, 2, 3, 4, 5}, mockCaptureInfos)
	require.Nil(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	// capture is draining now, `MoveTable` should be a no-op.
	dispatcher.MoveTable(5, "capture-1")
	require.False(t, dispatcher.moveTableManager.HaveJobsByCaptureID("capture-1"))
}

func TestManualMoveTableWhileAddingTable(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
			Epoch:        defaultEpoch,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})

	// add table-1 to the capture-2
	communicator.On("DispatchTable", mock.Anything, "cf-1", model.TableID(1), "capture-2", false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	// move table-1 to the capture-1
	dispatcher.MoveTable(1, "capture-1")
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedTableOperation("capture-2", 1, defaultEpoch)
	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(1), "capture-2", true, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedTableOperation("capture-2", 1, defaultEpoch)
	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(1), "capture-1", false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
}

func TestAutoRebalanceOnCaptureOnline(t *testing.T) {
	// This test case tests the following scenario:
	// 1. Capture-1 and Capture-2 are online.
	// 2. Owner dispatches three tables to these two captures.
	// 3. While the pending dispatches are in progress, Capture-3 goes online.
	// 4. Capture-1 and Capture-2 finish the dispatches.
	//
	// We expect that the workload is eventually balanced by migrating
	// a table to Capture-3.

	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)

	captureList := map[model.CaptureID]*model.CaptureInfo{
		"capture-1": {
			ID:            "capture-1",
			AdvertiseAddr: "fakeip:1",
		},
		"capture-2": {
			ID:            "capture-2",
			AdvertiseAddr: "fakeip:2",
		},
	}

	communicator.On("Announce", mock.Anything,
		cf1, "capture-1").Return(true, nil)
	communicator.On("Announce", mock.Anything,
		cf1, "capture-2").Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentSyncTaskStatuses("capture-1", defaultEpoch, []model.TableID{}, []model.TableID{}, []model.TableID{})
	dispatcher.OnAgentSyncTaskStatuses("capture-2", defaultEpoch, []model.TableID{}, []model.TableID{}, []model.TableID{})

	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(1), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(2), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(3), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	require.NotEqual(t, 0, len(communicator.addTableRecords["capture-1"]))
	require.NotEqual(t, 0, len(communicator.addTableRecords["capture-2"]))
	require.Equal(t, 0, len(communicator.removeTableRecords["capture-1"]))
	require.Equal(t, 0, len(communicator.removeTableRecords["capture-2"]))

	dispatcher.OnAgentCheckpoint("capture-1", 2000, 2000)
	dispatcher.OnAgentCheckpoint("capture-1", 2001, 2001)

	communicator.ExpectedCalls = nil
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	communicator.AssertExpectations(t)

	captureList["capture-3"] = &model.CaptureInfo{
		ID:            "capture-3",
		AdvertiseAddr: "fakeip:3",
	}
	communicator.ExpectedCalls = nil
	communicator.On("Announce", mock.Anything,
		cf1, "capture-3").Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	communicator.ExpectedCalls = nil
	dispatcher.OnAgentSyncTaskStatuses("capture-3", defaultEpoch, []model.TableID{}, []model.TableID{}, []model.TableID{})
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	for captureID, tables := range communicator.addTableRecords {
		for _, tableID := range tables {
			dispatcher.OnAgentFinishedTableOperation(captureID, tableID, defaultEpoch)
		}
	}

	communicator.Reset()
	var removeTableFromCapture model.CaptureID
	communicator.On("DispatchTable", mock.Anything,
		cf1, mock.Anything, mock.Anything, true, defaultEpoch).
		Return(true, nil).Run(func(args mock.Arguments) {
		removeTableFromCapture = args.Get(3).(model.CaptureID)
	})
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	removedTableID := communicator.removeTableRecords[removeTableFromCapture][0]

	dispatcher.OnAgentFinishedTableOperation(removeTableFromCapture, removedTableID, defaultEpoch)
	dispatcher.OnAgentCheckpoint("capture-1", 1100, 1400)
	dispatcher.OnAgentCheckpoint("capture-2", 1200, 1300)
	communicator.ExpectedCalls = nil
	communicator.On("DispatchTable", mock.Anything,
		cf1, removedTableID, "capture-3", false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
}

func TestInvalidFinishedTableOperation(t *testing.T) {
	t.Parallel()

	ctx := cdcContext.NewBackendContext4Test(false)
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
			Epoch:        defaultEpoch,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1500,
			ResolvedTs:   1550,
			Epoch:        defaultEpoch,
		},
	}
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   2,
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   3,
		CaptureID: "capture-1",
		Status:    util.RunningTable,
	})

	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(1), "capture-2", false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)

	// Invalid epoch
	dispatcher.OnAgentFinishedTableOperation("capture-2", model.TableID(1), "invalid-epoch")
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	record, ok := dispatcher.tables.GetTableRecord(model.TableID(1))
	require.True(t, ok)
	require.Equal(t, record.Status, util.AddingTable)

	// Invalid capture
	dispatcher.OnAgentFinishedTableOperation("capture-invalid", model.TableID(1), defaultEpoch)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	record, ok = dispatcher.tables.GetTableRecord(model.TableID(1))
	require.True(t, ok)
	require.Equal(t, record.Status, util.AddingTable)

	// Invalid table
	dispatcher.OnAgentFinishedTableOperation("capture-1", model.TableID(999), defaultEpoch)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, CheckpointCannotProceed, checkpointTs)
	require.Equal(t, CheckpointCannotProceed, resolvedTs)
	record, ok = dispatcher.tables.GetTableRecord(model.TableID(1))
	require.True(t, ok)
	require.Equal(t, record.Status, util.AddingTable)

	// Capture not matching
	require.Panics(t, func() {
		dispatcher.OnAgentFinishedTableOperation("capture-1", model.TableID(1), defaultEpoch)
	})
}

func BenchmarkAddTable(b *testing.B) {
	ctx := cdcContext.NewBackendContext4Test(false)

	communicator := NewMockScheduleDispatcherCommunicator()
	communicator.isBenchmark = true

	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	communicator.On("DispatchTable", mock.Anything, mock.Anything, mock.Anything, mock.Anything, false).
		Return(true, nil)

	dispatcher.captures = defaultMockCaptureInfos
	dispatcher.captureStatus["capture-1"] = &captureStatus{
		SyncStatus:   captureSyncFinished,
		CheckpointTs: 100,
		ResolvedTs:   100,
	}
	dispatcher.captureStatus["capture-2"] = &captureStatus{
		SyncStatus:   captureSyncFinished,
		CheckpointTs: 100,
		ResolvedTs:   100,
	}
	dispatcher.captureStatus["capture-3"] = &captureStatus{
		SyncStatus:   captureSyncFinished,
		CheckpointTs: 100,
		ResolvedTs:   100,
	}
	// Use a no-op logger to save IO cost
	dispatcher.logger = zap.NewNop()

	for i := 0; i < b.N; i++ {
		done, err := dispatcher.addTable(ctx, model.TableID(i))
		if !done || err != nil {
			b.Fatalf("addTable failed")
		}
	}
}
