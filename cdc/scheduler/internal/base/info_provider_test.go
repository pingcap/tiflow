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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/util"
	"github.com/stretchr/testify/require"
)

// Asserts that BaseSchedulerDispatcher implements InfoProvider interface.
var _ internal.InfoProvider = (*ScheduleDispatcher)(nil)

func injectSchedulerStateForInfoProviderTest(dispatcher *ScheduleDispatcher) {
	dispatcher.captureStatus = map[model.CaptureID]*captureStatus{
		"capture-1": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1400,
			ResolvedTs:   1500,
		},
		"capture-2": {
			SyncStatus:   captureSyncFinished,
			CheckpointTs: 1300,
			ResolvedTs:   1600,
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
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   4,
		CaptureID: "capture-2",
		Status:    util.AddingTable,
	})
	dispatcher.tables.AddTableRecord(&util.TableRecord{
		TableID:   5,
		CaptureID: "capture-1",
		Status:    util.RemovingTable,
	})
}

func TestInfoProviderTaskStatus(t *testing.T) {
	dispatcher := NewBaseScheduleDispatcher(model.DefaultChangeFeedID("cf-1"), nil, 1300)
	injectSchedulerStateForInfoProviderTest(dispatcher)

	taskStatus, err := dispatcher.GetTaskStatuses()
	require.NoError(t, err)
	require.Equal(t, map[model.CaptureID]*model.TaskStatus{
		"capture-1": {
			Tables: map[model.TableID]*model.TableReplicaInfo{
				1: {},
				3: {},
				5: {},
			},
			Operation: map[model.TableID]*model.TableOperation{
				5: {
					Delete: true,
					Status: model.OperDispatched,
				},
			},
		},
		"capture-2": {
			Tables: map[model.TableID]*model.TableReplicaInfo{
				2: {},
				4: {},
			},
			Operation: map[model.TableID]*model.TableOperation{
				4: {
					Delete: false,
					Status: model.OperDispatched,
				},
			},
		},
	}, taskStatus)
}

func TestInfoProviderTaskPosition(t *testing.T) {
	dispatcher := NewBaseScheduleDispatcher(model.DefaultChangeFeedID("cf-1"), nil, 1300)
	injectSchedulerStateForInfoProviderTest(dispatcher)

	taskPosition, err := dispatcher.GetTaskPositions()
	require.NoError(t, err)
	require.Equal(t, map[model.CaptureID]*model.TaskPosition{
		"capture-1": {
			CheckPointTs: 1400,
			ResolvedTs:   1500,
		},
		"capture-2": {
			CheckPointTs: 1300,
			ResolvedTs:   1600,
		},
	}, taskPosition)
}

func TestInfoProviderTableCounts(t *testing.T) {
	dispatcher := NewBaseScheduleDispatcher(model.DefaultChangeFeedID("cf-1"), nil, 1300)
	injectSchedulerStateForInfoProviderTest(dispatcher)

	require.Equal(t, map[model.CaptureID]int{
		"capture-1": 3,
		"capture-2": 2,
	}, dispatcher.GetTotalTableCounts())

	require.Equal(t, map[model.CaptureID]int{
		"capture-1": 1,
		"capture-2": 1,
	}, dispatcher.GetPendingTableCounts())
}
