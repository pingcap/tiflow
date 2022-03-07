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

package owner

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/stretchr/testify/require"
)

type schedulerTester struct {
	changefeedID model.ChangeFeedID
	state        *orchestrator.ChangefeedReactorState
	tester       *orchestrator.ReactorStateTester
	captures     map[model.CaptureID]*model.CaptureInfo
	scheduler    *oldScheduler
}

func (s *schedulerTester) reset(t *testing.T) {
	s.changefeedID = fmt.Sprintf("test-changefeed-%x", rand.Uint32())
	s.state = orchestrator.NewChangefeedReactorState("test-changefeed")
	s.tester = orchestrator.NewReactorStateTester(t, s.state, nil)
	s.scheduler = newSchedulerV1().(*schedulerV1CompatWrapper).inner
	s.captures = make(map[model.CaptureID]*model.CaptureInfo)
	s.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return &model.ChangeFeedStatus{}, true, nil
	})
	s.tester.MustApplyPatches()
}

func (s *schedulerTester) addCapture(captureID model.CaptureID) {
	captureInfo := &model.CaptureInfo{
		ID: captureID,
	}
	s.captures[captureID] = captureInfo
	s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		return &model.TaskStatus{}, true, nil
	})
	s.tester.MustApplyPatches()
}

func (s *schedulerTester) finishTableOperation(captureID model.CaptureID, tableIDs ...model.TableID) {
	s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		for _, tableID := range tableIDs {
			status.Operation[tableID].Status = model.OperFinished
		}
		return status, true, nil
	})
	s.state.PatchTaskWorkload(captureID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		if workload == nil {
			workload = make(model.TaskWorkload)
		}
		for _, tableID := range tableIDs {
			if s.state.TaskStatuses[captureID].Operation[tableID].Delete {
				delete(workload, tableID)
			} else {
				workload[tableID] = model.WorkloadInfo{
					Workload: 1,
				}
			}
		}
		return workload, true, nil
	})
	s.tester.MustApplyPatches()
}

func TestScheduleOneCapture(t *testing.T) {
	s := &schedulerTester{}
	s.reset(t)
	captureID := "test-capture-0"
	s.addCapture(captureID)

	_, _ = s.scheduler.Tick(s.state, []model.TableID{}, s.captures)

	// Manually simulate the scenario where the corresponding key was deleted in the etcd
	key := &etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeTaskStatus,
		CaptureID:    captureID,
		ChangefeedID: s.state.ID,
	}
	s.tester.MustUpdate(key.String(), nil)
	s.tester.MustApplyPatches()

	s.reset(t)
	captureID = "test-capture-1"
	s.addCapture(captureID)

	// add three tables
	shouldUpdateState, err := s.scheduler.Tick(s.state, []model.TableID{1, 2, 3, 4}, s.captures)
	require.Nil(t, err)
	require.False(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	require.Equal(t, s.state.TaskStatuses[captureID].Tables, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0}, 2: {StartTs: 0}, 3: {StartTs: 0}, 4: {StartTs: 0},
	})
	require.Equal(t, s.state.TaskStatuses[captureID].Operation, map[model.TableID]*model.TableOperation{
		1: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		3: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2, 3, 4}, s.captures)
	require.Nil(t, err)
	require.True(t, shouldUpdateState)
	s.tester.MustApplyPatches()

	// two tables finish adding operation
	s.finishTableOperation(captureID, 2, 3)

	// remove table 1,2 and add table 4,5
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	require.Nil(t, err)
	require.False(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	require.Equal(t, s.state.TaskStatuses[captureID].Tables, map[model.TableID]*model.TableReplicaInfo{
		3: {StartTs: 0}, 4: {StartTs: 0}, 5: {StartTs: 0},
	})
	require.Equal(t, s.state.TaskStatuses[captureID].Operation, map[model.TableID]*model.TableOperation{
		1: {Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		5: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	// move a non exist table to a non exist capture
	s.scheduler.MoveTable(2, "fake-capture")
	// move tables to a non exist capture
	s.scheduler.MoveTable(3, "fake-capture")
	s.scheduler.MoveTable(4, "fake-capture")
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	require.Nil(t, err)
	require.False(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	require.Equal(t, s.state.TaskStatuses[captureID].Tables, map[model.TableID]*model.TableReplicaInfo{
		4: {StartTs: 0}, 5: {StartTs: 0},
	})
	require.Equal(t, s.state.TaskStatuses[captureID].Operation, map[model.TableID]*model.TableOperation{
		1: {Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		3: {Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		5: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	// finish all operations
	s.finishTableOperation(captureID, 1, 2, 3, 4, 5)

	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	require.Nil(t, err)
	require.True(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	require.Equal(t, s.state.TaskStatuses[captureID].Tables, map[model.TableID]*model.TableReplicaInfo{
		4: {StartTs: 0}, 5: {StartTs: 0},
	})
	require.Equal(t, s.state.TaskStatuses[captureID].Operation, map[model.TableID]*model.TableOperation{})

	// table 3 is missing by expected, because the table was trying to move to a invalid capture
	// and the move will failed, the table 3 will be add in next tick
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	require.Nil(t, err)
	require.False(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	require.Equal(t, s.state.TaskStatuses[captureID].Tables, map[model.TableID]*model.TableReplicaInfo{
		4: {StartTs: 0}, 5: {StartTs: 0},
	})
	require.Equal(t, s.state.TaskStatuses[captureID].Operation, map[model.TableID]*model.TableOperation{})

	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	require.Nil(t, err)
	require.False(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	require.Equal(t, s.state.TaskStatuses[captureID].Tables, map[model.TableID]*model.TableReplicaInfo{
		3: {StartTs: 0}, 4: {StartTs: 0}, 5: {StartTs: 0},
	})
	require.Equal(t, s.state.TaskStatuses[captureID].Operation, map[model.TableID]*model.TableOperation{
		3: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})
}

func TestScheduleMoveTable(t *testing.T) {
	s := &schedulerTester{}
	s.reset(t)
	captureID1 := "test-capture-1"
	captureID2 := "test-capture-2"
	s.addCapture(captureID1)

	// add a table
	shouldUpdateState, err := s.scheduler.Tick(s.state, []model.TableID{1}, s.captures)
	require.Nil(t, err)
	require.False(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	require.Equal(t, s.state.TaskStatuses[captureID1].Tables, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0},
	})
	require.Equal(t, s.state.TaskStatuses[captureID1].Operation, map[model.TableID]*model.TableOperation{
		1: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	s.finishTableOperation(captureID1, 1)
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1}, s.captures)
	require.Nil(t, err)
	require.True(t, shouldUpdateState)
	s.tester.MustApplyPatches()

	s.addCapture(captureID2)

	// add a table
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2}, s.captures)
	require.Nil(t, err)
	require.False(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	require.Equal(t, s.state.TaskStatuses[captureID1].Tables, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0},
	})
	require.Equal(t, s.state.TaskStatuses[captureID1].Operation, map[model.TableID]*model.TableOperation{})
	require.Equal(t, s.state.TaskStatuses[captureID2].Tables, map[model.TableID]*model.TableReplicaInfo{
		2: {StartTs: 0},
	})
	require.Equal(t, s.state.TaskStatuses[captureID2].Operation, map[model.TableID]*model.TableOperation{
		2: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	s.finishTableOperation(captureID2, 2)

	s.scheduler.MoveTable(2, captureID1)
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2}, s.captures)
	require.Nil(t, err)
	require.False(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	require.Equal(t, s.state.TaskStatuses[captureID1].Tables, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0},
	})
	require.Equal(t, s.state.TaskStatuses[captureID1].Operation, map[model.TableID]*model.TableOperation{})
	require.Equal(t, s.state.TaskStatuses[captureID2].Tables, map[model.TableID]*model.TableReplicaInfo{})
	require.Equal(t, s.state.TaskStatuses[captureID2].Operation, map[model.TableID]*model.TableOperation{
		2: {Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
	})

	s.finishTableOperation(captureID2, 2)

	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2}, s.captures)
	require.Nil(t, err)
	require.True(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	require.Equal(t, s.state.TaskStatuses[captureID1].Tables, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0},
	})
	require.Equal(t, s.state.TaskStatuses[captureID1].Operation, map[model.TableID]*model.TableOperation{})
	require.Equal(t, s.state.TaskStatuses[captureID2].Tables, map[model.TableID]*model.TableReplicaInfo{})
	require.Equal(t, s.state.TaskStatuses[captureID2].Operation, map[model.TableID]*model.TableOperation{})

	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2}, s.captures)
	require.Nil(t, err)
	require.False(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	require.Equal(t, s.state.TaskStatuses[captureID1].Tables, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0}, 2: {StartTs: 0},
	})
	require.Equal(t, s.state.TaskStatuses[captureID1].Operation, map[model.TableID]*model.TableOperation{
		2: {Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})
	require.Equal(t, s.state.TaskStatuses[captureID2].Tables, map[model.TableID]*model.TableReplicaInfo{})
	require.Equal(t, s.state.TaskStatuses[captureID2].Operation, map[model.TableID]*model.TableOperation{})
}

func TestScheduleRebalance(t *testing.T) {
	s := &schedulerTester{}
	s.reset(t)
	captureID1 := "test-capture-1"
	captureID2 := "test-capture-2"
	captureID3 := "test-capture-3"
	s.addCapture(captureID1)
	s.addCapture(captureID2)
	s.addCapture(captureID3)

	s.state.PatchTaskStatus(captureID1, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Tables = make(map[model.TableID]*model.TableReplicaInfo)
		status.Tables[1] = &model.TableReplicaInfo{StartTs: 1}
		status.Tables[2] = &model.TableReplicaInfo{StartTs: 1}
		status.Tables[3] = &model.TableReplicaInfo{StartTs: 1}
		status.Tables[4] = &model.TableReplicaInfo{StartTs: 1}
		status.Tables[5] = &model.TableReplicaInfo{StartTs: 1}
		status.Tables[6] = &model.TableReplicaInfo{StartTs: 1}
		return status, true, nil
	})
	s.tester.MustApplyPatches()

	// rebalance table
	shouldUpdateState, err := s.scheduler.Tick(s.state, []model.TableID{1, 2, 3, 4, 5, 6}, s.captures)
	require.Nil(t, err)
	require.False(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	// 4 tables remove in capture 1, this 4 tables will be added to another capture in next tick
	require.Len(t, s.state.TaskStatuses[captureID1].Tables, 2)
	require.Len(t, s.state.TaskStatuses[captureID2].Tables, 0)
	require.Len(t, s.state.TaskStatuses[captureID3].Tables, 0)

	s.state.PatchTaskStatus(captureID1, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		for _, opt := range status.Operation {
			opt.Status = model.OperFinished
		}
		return status, true, nil
	})
	s.state.PatchTaskWorkload(captureID1, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		require.Nil(t, workload)
		workload = make(model.TaskWorkload)
		for tableID := range s.state.TaskStatuses[captureID1].Tables {
			workload[tableID] = model.WorkloadInfo{Workload: 1}
		}
		return workload, true, nil
	})
	s.tester.MustApplyPatches()

	// clean finished operation
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2, 3, 4, 5, 6}, s.captures)
	require.Nil(t, err)
	require.True(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	// 4 tables add to another capture in this tick
	require.Len(t, s.state.TaskStatuses[captureID1].Operation, 0)

	// rebalance table
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2, 3, 4, 5, 6}, s.captures)
	require.Nil(t, err)
	require.False(t, shouldUpdateState)
	s.tester.MustApplyPatches()
	// 4 tables add to another capture in this tick
	require.Len(t, s.state.TaskStatuses[captureID1].Tables, 2)
	require.Len(t, s.state.TaskStatuses[captureID2].Tables, 2)
	require.Len(t, s.state.TaskStatuses[captureID3].Tables, 2)
	tableIDs := make(map[model.TableID]struct{})
	for _, status := range s.state.TaskStatuses {
		for tableID := range status.Tables {
			tableIDs[tableID] = struct{}{}
		}
	}
	require.Equal(t, tableIDs, map[model.TableID]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}})
}
