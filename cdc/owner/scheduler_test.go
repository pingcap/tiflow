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
<<<<<<< HEAD
	"encoding/json"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/kv"
=======
	"fmt"
	"math/rand"

	"github.com/pingcap/check"
>>>>>>> e495f785 (new_owner: a table task scheduler for the owner (#1820))
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

<<<<<<< HEAD
type schedulerTestSuite struct {
}

var _ = check.Suite(&schedulerTestSuite{})

func setUp(c *check.C) (*schedulerImpl, *orchestrator.ReactorStateTester) {
	ownerState := newCDCReactorState()
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{})
	return newScheduler(ownerState, "cf-1"), tester
}

func addCapture(c *check.C, tester *orchestrator.ReactorStateTester, captureID model.CaptureID) {
	captureInfo := &model.CaptureInfo{
		ID: captureID,
	}
	captureInfoJSON, err := json.Marshal(captureInfo)
	c.Assert(err, check.IsNil)

	err = tester.UpdateKeys(map[string][]byte{
		kv.GetEtcdKeyCaptureInfo(captureID): captureInfoJSON,
	})
	c.Assert(err, check.IsNil)
}

func removeCapture(c *check.C, tester *orchestrator.ReactorStateTester, captureID model.CaptureID) {
	err := tester.UpdateKeys(map[string][]byte{
		kv.GetEtcdKeyCaptureInfo(captureID): nil,
	})
	c.Assert(err, check.IsNil)
}

func mockProcessorTick(c *check.C, tester *orchestrator.ReactorStateTester, captureID model.CaptureID) {
	var taskStatus model.TaskStatus
	if jsonStr, ok := tester.KVEntries()[kv.GetEtcdKeyTaskStatus("cf-1", captureID)]; ok {
		err := json.Unmarshal([]byte(jsonStr), &taskStatus)
		c.Assert(err, check.IsNil)
	}

	for tableID, op := range taskStatus.Operation {
		if op.Delete {
			delete(taskStatus.Tables, tableID)
			op.Status = model.OperFinished
			op.Done = true
		} else {
			if taskStatus.Tables == nil {
				taskStatus.Tables = make(map[model.TableID]*model.TableReplicaInfo)
			}
			taskStatus.Tables[tableID] = &model.TableReplicaInfo{
				StartTs: op.BoundaryTs,
			}
			op.Status = model.OperFinished
			op.Done = true
		}
	}

	newBytes, err := json.Marshal(&taskStatus)
	c.Assert(err, check.IsNil)

	err = tester.UpdateKeys(map[string][]byte{
		kv.GetEtcdKeyTaskStatus("cf-1", captureID): newBytes,
	})
	c.Assert(err, check.IsNil)
}

func readTaskStatus(c *check.C, tester *orchestrator.ReactorStateTester, captureID model.CaptureID) *model.TaskStatus {
	var taskStatus model.TaskStatus
	err := json.Unmarshal([]byte(tester.KVEntries()[kv.GetEtcdKeyTaskStatus("cf-1", captureID)]), &taskStatus)
	c.Assert(err, check.IsNil)
	return &taskStatus
}

func (s *schedulerTestSuite) TestPutTaskAddRemove(c *check.C) {
	defer testleak.AfterTest(c)()
	scheduler, tester := setUp(c)
	addCapture(c, tester, "capture-1")

	tasks := map[model.TableID]*tableTask{
		1: {
			TableID:      1,
			CheckpointTs: 1000,
			ResolvedTs:   1200,
		},
	}

	scheduler.PutTasks(tasks)
	err := tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	taskStatus := readTaskStatus(c, tester, "capture-1")
	c.Assert(taskStatus.Operation, check.HasKey, model.TableID(1))
	c.Assert(taskStatus.Operation[1], check.DeepEquals, &model.TableOperation{
		Delete:     false,
		BoundaryTs: 1001,
		Done:       false,
		Status:     model.OperDispatched,
	})

	mockProcessorTick(c, tester, "capture-1")

	tasks[2] = &tableTask{
		TableID:      2,
		CheckpointTs: 1100,
		ResolvedTs:   1000,
	}

	scheduler.PutTasks(tasks)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	// delay the mocked processor for one tick to test idempotency
	scheduler.PutTasks(tasks)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	taskStatus = readTaskStatus(c, tester, "capture-1")
	c.Assert(taskStatus.Operation, check.HasKey, model.TableID(2))
	c.Assert(taskStatus.Operation[2], check.DeepEquals, &model.TableOperation{
		Delete:     false,
		BoundaryTs: 1101,
		Done:       false,
		Status:     model.OperDispatched,
	})

	mockProcessorTick(c, tester, "capture-1")

	delete(tasks, 1)
	scheduler.PutTasks(tasks)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	taskStatus = readTaskStatus(c, tester, "capture-1")
	c.Assert(taskStatus.Operation, check.HasKey, model.TableID(1))
	c.Assert(taskStatus.Operation[1], check.DeepEquals, &model.TableOperation{
		Delete:     true,
		BoundaryTs: 0, // we don't have a global resolved Ts in the mocked environment
		Done:       false,
		Status:     model.OperDispatched,
	})

	mockProcessorTick(c, tester, "capture-1")

	delete(tasks, 2)
	scheduler.PutTasks(tasks)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	taskStatus = readTaskStatus(c, tester, "capture-1")
	c.Assert(taskStatus.Operation, check.HasKey, model.TableID(2))
	c.Assert(taskStatus.Operation[2], check.DeepEquals, &model.TableOperation{
		Delete:     true,
		BoundaryTs: 0, // we don't have a global resolved Ts in the mocked environment
		Done:       false,
		Status:     model.OperDispatched,
	})

	// delay the mocked processor for one tick to test idempotency
	scheduler.PutTasks(tasks)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	mockProcessorTick(c, tester, "capture-1")
	taskStatus = readTaskStatus(c, tester, "capture-1")
	c.Assert(taskStatus.Tables, check.HasLen, 0)
}

func (s *schedulerTestSuite) TestPutTaskRebalance(c *check.C) {
	defer testleak.AfterTest(c)()
	scheduler, tester := setUp(c)
	addCapture(c, tester, "capture-1")

	// Add two tables
	tasks := map[model.TableID]*tableTask{
		1: {
			TableID:      1,
			CheckpointTs: 1000,
			ResolvedTs:   1200,
		},
		2: {
			TableID:      2,
			CheckpointTs: 1100,
			ResolvedTs:   1210,
		},
	}
	scheduler.PutTasks(tasks)
	err := tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	taskStatus := readTaskStatus(c, tester, "capture-1")
	c.Assert(taskStatus.Operation, check.HasKey, model.TableID(1))
	c.Assert(taskStatus.Operation[1], check.DeepEquals, &model.TableOperation{
		Delete:     false,
		BoundaryTs: 1001,
		Done:       false,
		Status:     model.OperDispatched,
	})
	c.Assert(taskStatus.Operation, check.HasKey, model.TableID(2))
	c.Assert(taskStatus.Operation[2], check.DeepEquals, &model.TableOperation{
		Delete:     false,
		BoundaryTs: 1101,
		Done:       false,
		Status:     model.OperDispatched,
	})

	mockProcessorTick(c, tester, "capture-1")
	addCapture(c, tester, "capture-2")
	scheduler.PutTasks(tasks)
	// rebalance should have been triggered
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	taskStatus1 := readTaskStatus(c, tester, "capture-1")
	c.Assert(taskStatus1.Operation, check.HasLen, 1)
	var victimID model.TableID
	for tableID := range taskStatus1.Operation {
		// record the victim ID since it is generated randomly
		victimID = tableID
	}
	c.Assert(taskStatus1.Operation[victimID].Delete, check.IsTrue)
	c.Assert(taskStatus1.Operation[victimID].Status, check.Equals, model.OperDispatched)

	mockProcessorTick(c, tester, "capture-1")
	// capture-1 has processed the delete request
	taskStatus1 = readTaskStatus(c, tester, "capture-1")
	c.Assert(taskStatus1.Tables, check.HasLen, 1)
	c.Assert(taskStatus1.Operation[victimID].Status, check.Equals, model.OperFinished)

	tasks[victimID].CheckpointTs = 1500
	// scheduler will redispatch the victim here
	scheduler.PutTasks(tasks)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	taskStatus1 = readTaskStatus(c, tester, "capture-1")
	c.Assert(taskStatus1.Tables, check.HasLen, 1)
	c.Assert(taskStatus1.Operation, check.HasLen, 0)

	taskStatus2 := readTaskStatus(c, tester, "capture-2")
	c.Assert(taskStatus2.Tables, check.HasLen, 1)
	c.Assert(taskStatus2.Operation, check.HasLen, 1)
	c.Assert(taskStatus2.Operation[victimID].BoundaryTs, check.Equals, uint64(1501))
}

func (s *schedulerTestSuite) TestPutTaskAddAfterDelete(c *check.C) {
	defer testleak.AfterTest(c)()
	scheduler, tester := setUp(c)
	addCapture(c, tester, "capture-1")
	addCapture(c, tester, "capture-2")

	// Add two tables
	tasks := map[model.TableID]*tableTask{
		1: {
			TableID:      1,
			CheckpointTs: 1000,
			ResolvedTs:   1200,
		},
		2: {
			TableID:      2,
			CheckpointTs: 1100,
			ResolvedTs:   1210,
		},
	}
	scheduler.PutTasks(tasks)
	err := tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	mockProcessorTick(c, tester, "capture-1")
	mockProcessorTick(c, tester, "capture-2")

	// wait for one tick for the state to stabilize
	scheduler.PutTasks(tasks)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	taskStatus1 := readTaskStatus(c, tester, "capture-1")
	c.Assert(taskStatus1.Tables, check.HasLen, 1)
	c.Assert(taskStatus1.Operation, check.HasLen, 0)

	taskStatus2 := readTaskStatus(c, tester, "capture-2")
	c.Assert(taskStatus2.Tables, check.HasLen, 1)
	c.Assert(taskStatus2.Operation, check.HasLen, 0)

	// delete the two tables
	scheduler.PutTasks(map[model.TableID]*tableTask{})
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	// add back the two tables
	scheduler.PutTasks(tasks)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	mockProcessorTick(c, tester, "capture-1")
	mockProcessorTick(c, tester, "capture-2")

	// wait for one tick for the state to stabilize
	scheduler.PutTasks(tasks)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	mockProcessorTick(c, tester, "capture-1")
	mockProcessorTick(c, tester, "capture-2")

	taskStatus1 = readTaskStatus(c, tester, "capture-1")
	c.Assert(taskStatus1.Tables, check.HasLen, 1)
	c.Assert(taskStatus1.Operation, check.HasLen, 1)

	taskStatus2 = readTaskStatus(c, tester, "capture-2")
	c.Assert(taskStatus2.Tables, check.HasLen, 1)
	c.Assert(taskStatus2.Operation, check.HasLen, 1)
}

func (s *schedulerTestSuite) TestPutTaskWithAffinity(c *check.C) {
	defer testleak.AfterTest(c)()
	scheduler, tester := setUp(c)
	addCapture(c, tester, "capture-1")
	addCapture(c, tester, "capture-2")

	// Add two tables
	tasks := map[model.TableID]*tableTask{
		1: {
			TableID:      1,
			CheckpointTs: 1000,
			ResolvedTs:   1200,
		},
		2: {
			TableID:      2,
			CheckpointTs: 1100,
			ResolvedTs:   1210,
		},
	}

	scheduler.SetAffinity(1, "capture-1", 10)
	scheduler.SetAffinity(2, "capture-1", 10)

	scheduler.PutTasks(tasks)
	err := tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	mockProcessorTick(c, tester, "capture-1")
	mockProcessorTick(c, tester, "capture-2")

	taskStatus1 := readTaskStatus(c, tester, "capture-1")
	c.Assert(taskStatus1.Tables, check.HasLen, 2)
	c.Assert(taskStatus1.Operation, check.HasLen, 2)

	taskStatus2 := readTaskStatus(c, tester, "capture-2")
	c.Assert(taskStatus2.Tables, check.HasLen, 0)
	c.Assert(taskStatus2.Operation, check.HasLen, 0)

	removeCapture(c, tester, "capture-1")
	c.Assert(scheduler.IsReady(), check.IsFalse)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(scheduler.IsReady(), check.IsTrue)

	scheduler.PutTasks(tasks)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	mockProcessorTick(c, tester, "capture-2")
	taskStatus2 = readTaskStatus(c, tester, "capture-2")
	c.Assert(taskStatus2.Tables, check.HasLen, 2)
	c.Assert(taskStatus2.Operation, check.HasLen, 2)
}

// TODO add test cases for 1) affinity expiring 2) adding table when there is no capture.
=======
var _ = check.Suite(&schedulerSuite{})

type schedulerSuite struct {
	changefeedID model.ChangeFeedID
	state        *model.ChangefeedReactorState
	tester       *orchestrator.ReactorStateTester
	captures     map[model.CaptureID]*model.CaptureInfo
	scheduler    *scheduler
}

func (s *schedulerSuite) reset(c *check.C) {
	s.changefeedID = fmt.Sprintf("test-changefeed-%x", rand.Uint32())
	s.state = model.NewChangefeedReactorState("test-changefeed")
	s.tester = orchestrator.NewReactorStateTester(c, s.state, nil)
	s.scheduler = newScheduler()
	s.captures = make(map[model.CaptureID]*model.CaptureInfo)
	s.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return &model.ChangeFeedStatus{}, true, nil
	})
	s.tester.MustApplyPatches()
}

func (s *schedulerSuite) addCapture(captureID model.CaptureID) {
	captureInfo := &model.CaptureInfo{
		ID: captureID,
	}
	s.captures[captureID] = captureInfo
	s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		return &model.TaskStatus{}, true, nil
	})
	s.tester.MustApplyPatches()
}

func (s *schedulerSuite) finishTableOperation(captureID model.CaptureID, tableIDs ...model.TableID) {
	s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		for _, tableID := range tableIDs {
			status.Operation[tableID].Done = true
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

func (s *schedulerSuite) TestScheduleOneCapture(c *check.C) {
	defer testleak.AfterTest(c)()
	s.reset(c)
	captureID := "test-capture-1"
	s.addCapture(captureID)

	// add three tables
	shouldUpdateState, err := s.scheduler.Tick(s.state, []model.TableID{1, 2, 3, 4}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0}, 2: {StartTs: 0}, 3: {StartTs: 0}, 4: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		1: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		3: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2, 3, 4}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()

	// two tables finish adding operation
	s.finishTableOperation(captureID, 2, 3)

	// remove table 1,2 and add table 4,5
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		3: {StartTs: 0}, 4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		1: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		5: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	// move a non exist table to a non exist capture
	s.scheduler.MoveTable(2, "fake-capture")
	// move tables to a non exist capture
	s.scheduler.MoveTable(3, "fake-capture")
	s.scheduler.MoveTable(4, "fake-capture")
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		1: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		3: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
		5: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	// finish all operations
	s.finishTableOperation(captureID, 1, 2, 3, 4, 5)

	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})

	// table 3 is missing by expected, because the table was trying to move to a invalid capture
	// and the move will failed, the table 3 will be add in next tick
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})

	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		3: {StartTs: 0}, 4: {StartTs: 0}, 5: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		3: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})
}

func (s *schedulerSuite) TestScheduleMoveTable(c *check.C) {
	defer testleak.AfterTest(c)()
	s.reset(c)
	captureID1 := "test-capture-1"
	captureID2 := "test-capture-2"
	s.addCapture(captureID1)

	// add a table
	shouldUpdateState, err := s.scheduler.Tick(s.state, []model.TableID{1}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		1: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	s.finishTableOperation(captureID1, 1)
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()

	s.addCapture(captureID2)

	// add a table
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})
	c.Assert(s.state.TaskStatuses[captureID2].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		2: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID2].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		2: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})

	s.finishTableOperation(captureID2, 2)

	s.scheduler.MoveTable(2, captureID1)
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})
	c.Assert(s.state.TaskStatuses[captureID2].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{})
	c.Assert(s.state.TaskStatuses[captureID2].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		2: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
	})

	s.finishTableOperation(captureID2, 2)

	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})
	c.Assert(s.state.TaskStatuses[captureID2].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{})
	c.Assert(s.state.TaskStatuses[captureID2].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})

	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID1].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 0}, 2: {StartTs: 0},
	})
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		2: {Done: false, Delete: false, BoundaryTs: 0, Status: model.OperDispatched},
	})
	c.Assert(s.state.TaskStatuses[captureID2].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{})
	c.Assert(s.state.TaskStatuses[captureID2].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})
}

func (s *schedulerSuite) TestScheduleRebalance(c *check.C) {
	defer testleak.AfterTest(c)()
	s.reset(c)
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
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	// 4 tables remove in capture 1, this 4 tables will be added to another capture in next tick
	c.Assert(s.state.TaskStatuses[captureID1].Tables, check.HasLen, 2)
	c.Assert(s.state.TaskStatuses[captureID2].Tables, check.HasLen, 0)
	c.Assert(s.state.TaskStatuses[captureID3].Tables, check.HasLen, 0)

	s.state.PatchTaskStatus(captureID1, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		for _, opt := range status.Operation {
			opt.Done = true
			opt.Status = model.OperFinished
		}
		return status, true, nil
	})
	s.state.PatchTaskWorkload(captureID1, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		c.Assert(workload, check.IsNil)
		workload = make(model.TaskWorkload)
		for tableID := range s.state.TaskStatuses[captureID1].Tables {
			workload[tableID] = model.WorkloadInfo{Workload: 1}
		}
		return workload, true, nil
	})
	s.tester.MustApplyPatches()

	// clean finished operation
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2, 3, 4, 5, 6}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsTrue)
	s.tester.MustApplyPatches()
	// 4 tables add to another capture in this tick
	c.Assert(s.state.TaskStatuses[captureID1].Operation, check.HasLen, 0)

	// rebalance table
	shouldUpdateState, err = s.scheduler.Tick(s.state, []model.TableID{1, 2, 3, 4, 5, 6}, s.captures)
	c.Assert(err, check.IsNil)
	c.Assert(shouldUpdateState, check.IsFalse)
	s.tester.MustApplyPatches()
	// 4 tables add to another capture in this tick
	c.Assert(s.state.TaskStatuses[captureID1].Tables, check.HasLen, 2)
	c.Assert(s.state.TaskStatuses[captureID2].Tables, check.HasLen, 2)
	c.Assert(s.state.TaskStatuses[captureID3].Tables, check.HasLen, 2)
	tableIDs := make(map[model.TableID]struct{})
	for _, status := range s.state.TaskStatuses {
		for tableID := range status.Tables {
			tableIDs[tableID] = struct{}{}
		}
	}
	c.Assert(tableIDs, check.DeepEquals, map[model.TableID]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}})
}
>>>>>>> e495f785 (new_owner: a table task scheduler for the owner (#1820))
