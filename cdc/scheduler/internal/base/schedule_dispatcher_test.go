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

package base

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/base/protocol"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/util"
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
	ctx context.Context,
	changeFeedID model.ChangeFeedID,
	tableID model.TableID,
	startTs model.Ts,
	captureID model.CaptureID,
	isDelete bool,
	epoch protocol.ProcessorEpoch,
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
	args := m.Called(ctx, changeFeedID, tableID, startTs, captureID, isDelete, epoch)
	return args.Bool(0), args.Error(1)
}

func (m *mockScheduleDispatcherCommunicator) Announce(
	ctx context.Context,
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	communicator := NewMockScheduleDispatcherCommunicator()
	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)

	communicator.On("Announce", mock.Anything,
		cf1, "capture-1").Return(true, nil)
	communicator.On("Announce", mock.Anything,
		cf1, "capture-2").Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentSyncTaskStatuses("capture-1", defaultEpoch, []model.TableID{}, []model.TableID{}, []model.TableID{})
	dispatcher.OnAgentSyncTaskStatuses("capture-2", defaultEpoch, []model.TableID{}, []model.TableID{}, []model.TableID{})

	communicator.Reset()
	// Injects a dispatch table failure
	communicator.On("DispatchTable", mock.Anything,
		cf1, mock.Anything, mock.Anything, mock.Anything, false, defaultEpoch).
		Return(false, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything, cf1,
		model.TableID(1), model.Ts(1000), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything, cf1,
		model.TableID(2), model.Ts(1000), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything, cf1,
		model.TableID(3), model.Ts(1000), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
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
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)

	communicator.Reset()
	communicator.On("Announce", mock.Anything, cf1, "capture-1").
		Return(true, nil)
	communicator.On("Announce", mock.Anything, cf1, "capture-2").
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3, 4, 5}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)

	dispatcher.OnAgentSyncTaskStatuses("capture-1", defaultEpoch, []model.TableID{1, 2, 3}, []model.TableID{4, 5}, []model.TableID{6, 7})
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3, 4, 5}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)

	communicator.Reset()
	dispatcher.OnAgentFinishedTableOperation("capture-1", 4, defaultEpoch)
	dispatcher.OnAgentFinishedTableOperation("capture-1", 5, defaultEpoch)
	dispatcher.OnAgentSyncTaskStatuses("capture-2", defaultEpoch, []model.TableID(nil), []model.TableID(nil), []model.TableID(nil))
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3, 4, 5}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
}

func TestRemoveTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
		cf1, model.TableID(3), model.Ts(0), "capture-1", true, defaultEpoch).
		Return(false, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(3), model.Ts(0), "capture-1", true, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1500, []model.TableID{1, 2}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
		cf1, model.TableID(2), model.Ts(1500), "capture-1", false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
}

func TestCaptureRestarts(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
		cf1, model.TableID(2), model.Ts(1500), "capture-2", false, nextEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1500, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
		cf1, model.TableID(1), model.Ts(0), "capture-1", true, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	delete(mockCaptureInfos, "capture-2")
	dispatcher.OnAgentFinishedTableOperation("capture-1", 1, defaultEpoch)
	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(1), model.Ts(1300), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(2), model.Ts(1300), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
		cf1, mock.Anything, mock.Anything, mock.Anything, true, defaultEpoch).
		Return(false, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
	communicator.AssertNumberOfCalls(t, "DispatchTable", 1)

	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, mock.Anything, mock.Anything, mock.Anything, true, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6}, mockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
		cf1, model.TableID(7), model.Ts(1300), "capture-2", false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6, 7}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.Rebalance()
	communicator.Reset()
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6, 7}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedTableOperation("capture-2", model.TableID(7), defaultEpoch)
	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, mock.Anything, mock.Anything, mock.Anything, true, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3, 4, 5, 6, 7}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertNumberOfCalls(t, "DispatchTable", 2)
	communicator.AssertExpectations(t)
}

func TestManualMoveTableWhileAddingTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
		cf1, model.TableID(1), model.Ts(1300), "capture-2", false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)

	dispatcher.MoveTable(1, "capture-1")
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedTableOperation("capture-2", 1, defaultEpoch)
	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(1), model.Ts(0), "capture-2", true, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentFinishedTableOperation("capture-2", 1, defaultEpoch)
	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(1), model.Ts(1300), "capture-1", false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	dispatcher.OnAgentSyncTaskStatuses("capture-1", defaultEpoch, []model.TableID{}, []model.TableID{}, []model.TableID{})
	dispatcher.OnAgentSyncTaskStatuses("capture-2", defaultEpoch, []model.TableID{}, []model.TableID{}, []model.TableID{})

	communicator.Reset()
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(1), model.Ts(1000), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(2), model.Ts(1000), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	communicator.On("DispatchTable", mock.Anything,
		cf1, model.TableID(3), model.Ts(1000), mock.Anything, false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
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
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)

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
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	communicator.ExpectedCalls = nil
	dispatcher.OnAgentSyncTaskStatuses("capture-3", defaultEpoch, []model.TableID{}, []model.TableID{}, []model.TableID{})
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	for captureID, tables := range communicator.addTableRecords {
		for _, tableID := range tables {
			dispatcher.OnAgentFinishedTableOperation(captureID, tableID, defaultEpoch)
		}
	}

	communicator.Reset()
	var removeTableFromCapture model.CaptureID
	communicator.On("DispatchTable", mock.Anything,
		cf1, mock.Anything, mock.Anything, mock.Anything, true, defaultEpoch).
		Return(true, nil).Run(func(args mock.Arguments) {
		removeTableFromCapture = args.Get(4).(model.CaptureID)
	})
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)

	removedTableID := communicator.removeTableRecords[removeTableFromCapture][0]

	dispatcher.OnAgentFinishedTableOperation(removeTableFromCapture, removedTableID, defaultEpoch)
	dispatcher.OnAgentCheckpoint("capture-1", 1100, 1400)
	dispatcher.OnAgentCheckpoint("capture-2", 1200, 1300)
	communicator.ExpectedCalls = nil
	communicator.On("DispatchTable", mock.Anything,
		cf1, removedTableID, model.Ts(1000), "capture-3", false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1000, []model.TableID{1, 2, 3}, captureList)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	communicator.AssertExpectations(t)
}

func TestInvalidFinishedTableOperation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
		cf1, model.TableID(1), model.Ts(1300), "capture-2", false, defaultEpoch).
		Return(true, nil)
	checkpointTs, resolvedTs, err := dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)

	// Invalid epoch
	dispatcher.OnAgentFinishedTableOperation("capture-2", model.TableID(1), "invalid-epoch")
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	record, ok := dispatcher.tables.GetTableRecord(model.TableID(1))
	require.True(t, ok)
	require.Equal(t, record.Status, util.AddingTable)

	// Invalid capture
	dispatcher.OnAgentFinishedTableOperation("capture-invalid", model.TableID(1), defaultEpoch)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	record, ok = dispatcher.tables.GetTableRecord(model.TableID(1))
	require.True(t, ok)
	require.Equal(t, record.Status, util.AddingTable)

	// Invalid table
	dispatcher.OnAgentFinishedTableOperation("capture-1", model.TableID(999), defaultEpoch)
	checkpointTs, resolvedTs, err = dispatcher.Tick(ctx, 1300, []model.TableID{1, 2, 3}, defaultMockCaptureInfos)
	require.NoError(t, err)
	require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
	require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	record, ok = dispatcher.tables.GetTableRecord(model.TableID(1))
	require.True(t, ok)
	require.Equal(t, record.Status, util.AddingTable)

	// Capture not matching
	require.Panics(t, func() {
		dispatcher.OnAgentFinishedTableOperation("capture-1", model.TableID(1), defaultEpoch)
	})
}

func BenchmarkAddTable(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	communicator := NewMockScheduleDispatcherCommunicator()
	communicator.isBenchmark = true

	cf1 := model.DefaultChangeFeedID("cf-1")
	dispatcher := NewBaseScheduleDispatcher(cf1, communicator, 1000)
	communicator.On("DispatchTable", mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, false).
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
		done, err := dispatcher.addTable(ctx, model.TableID(i), 100)
		if !done || err != nil {
			b.Fatalf("addTable failed")
		}
	}
}
