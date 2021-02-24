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
	"encoding/json"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

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
