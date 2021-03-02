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
	"reflect"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

func Test(t *testing.T) { check.TestingT(t) }

type ownerStateTestSuite struct {
}

var _ = check.Suite(&ownerStateTestSuite{})

func (s *ownerStateTestSuite) TestUpdateCapture(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{})

	// Test adding a capture
	captureInfo := &model.CaptureInfo{
		ID:            "capture-1",
		AdvertiseAddr: "127.0.0.1:8083",
	}
	captureInfoJSON, err := json.Marshal(captureInfo)
	c.Assert(err, check.IsNil)

	captureHandlerCalled := false
	ownerState.SetNewCaptureHandler(func(id model.CaptureID) {
		captureHandlerCalled = true
		c.Assert(id, check.Equals, "capture-1")
	})

	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/capture/capture-1": captureInfoJSON,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.Captures["capture-1"], check.DeepEquals, captureInfo)
	c.Assert(captureHandlerCalled, check.IsTrue)

	// Test updating a capture
	captureInfo.AdvertiseAddr = "127.0.0.1:8084"
	captureInfoJSON, err = json.Marshal(captureInfo)
	c.Assert(err, check.IsNil)

	captureHandlerCalled = false
	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/capture/capture-1": captureInfoJSON,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.Captures["capture-1"], check.DeepEquals, captureInfo)
	c.Assert(captureHandlerCalled, check.IsFalse)

	// Test deleting a capture
	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/capture/capture-1": nil,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.Captures, check.Not(check.HasKey), "capture-1")
	c.Assert(captureHandlerCalled, check.IsFalse)
}

func (s *ownerStateTestSuite) TestUpdateChangeFeedStatus(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{})

	// Test adding a changeFeed
	changeFeedStatus := &model.ChangeFeedStatus{
		ResolvedTs:   1000,
		CheckpointTs: 800,
		AdminJobType: 0,
	}

	changeFeedStatusJSON, err := json.Marshal(changeFeedStatus)
	c.Assert(err, check.IsNil)

	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/job/cf-1": changeFeedStatusJSON,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.ChangeFeedStatuses["cf-1"], check.DeepEquals, changeFeedStatus)

	// Test updating a changeFeed
	changeFeedStatus.ResolvedTs = 1200
	changeFeedStatusJSON, err = json.Marshal(changeFeedStatus)
	c.Assert(err, check.IsNil)

	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/job/cf-1": changeFeedStatusJSON,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.ChangeFeedStatuses["cf-1"], check.DeepEquals, changeFeedStatus)

	// Test deleting a changeFeed
	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/job/cf-1": nil,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.ChangeFeedStatuses, check.Not(check.HasKey), "cf-1")
}

func (s *ownerStateTestSuite) TestUpdateTaskPosition(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{})

	// Test adding a position
	taskPosition := &model.TaskPosition{
		CheckPointTs: 800,
		ResolvedTs:   1000,
	}

	taskPositionJSON, err := json.Marshal(taskPosition)
	c.Assert(err, check.IsNil)

	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/task/position/capture-1/cf-1": taskPositionJSON,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.TaskPositions["cf-1"]["capture-1"], check.DeepEquals, taskPosition)

	// Test updating a position
	taskPosition.ResolvedTs = 1200
	taskPositionJSON, err = json.Marshal(taskPosition)
	c.Assert(err, check.IsNil)

	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/task/position/capture-1/cf-1": taskPositionJSON,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.TaskPositions["cf-1"]["capture-1"], check.DeepEquals, taskPosition)

	// Test deleting a position
	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/task/position/capture-1/cf-1": nil,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.TaskPositions["cf-1"], check.Not(check.HasKey), "capture-1")
}

func (s *ownerStateTestSuite) TestUpdateTaskStatus(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{})

	// Test adding a status
	taskStatus := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{1: {
			StartTs:     100,
			MarkTableID: 0,
		}},
		Operation: nil,
	}

	taskStatusJSON, err := json.Marshal(taskStatus)
	c.Assert(err, check.IsNil)

	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/task/status/capture-1/cf-1": taskStatusJSON,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.TaskStatuses["cf-1"]["capture-1"], check.DeepEquals, taskStatus)

	// Test updating a status
	taskStatus.Tables[2] = &model.TableReplicaInfo{StartTs: 200}
	taskStatusJSON, err = json.Marshal(taskStatus)
	c.Assert(err, check.IsNil)

	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/task/status/capture-1/cf-1": taskStatusJSON,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.TaskStatuses["cf-1"]["capture-1"], check.DeepEquals, taskStatus)

	// Test deleting a status
	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/task/status/capture-1/cf-1": nil,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.TaskStatuses["cf-1"], check.Not(check.HasKey), "capture-1")
}

func (s *ownerStateTestSuite) TestUpdateChangeFeedInfo(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{})

	// Test adding a changeFeed
	changeFeedInfo := &model.ChangeFeedInfo{
		SinkURI: "blackhole:///",
	}

	changeFeedInfoJSON, err := json.Marshal(changeFeedInfo)
	c.Assert(err, check.IsNil)

	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/changefeed/info/cf-1": changeFeedInfoJSON,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.ChangeFeedInfos["cf-1"], check.DeepEquals, changeFeedInfo)

	// Test updating a changeFeed
	changeFeedInfo.State = model.StateFailed
	changeFeedInfoJSON, err = json.Marshal(changeFeedInfo)
	c.Assert(err, check.IsNil)

	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/changefeed/info/cf-1": changeFeedInfoJSON,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.ChangeFeedInfos["cf-1"], check.DeepEquals, changeFeedInfo)

	// Test deleting a changeFeed
	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/changefeed/info/cf-1": nil,
	})
	c.Assert(err, check.IsNil)
	c.Assert(ownerState.ChangeFeedInfos, check.Not(check.HasKey), "cf-1")
}

func (s *ownerStateTestSuite) TestPatchChangeFeedStatus(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	changeFeedStatus := &model.ChangeFeedStatus{
		ResolvedTs:   800,
		CheckpointTs: 1000,
		AdminJobType: 0,
	}
	changeFeedStatusJSON, err := json.Marshal(changeFeedStatus)
	c.Assert(err, check.IsNil)
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{})

	// test the creation case
	ownerState.UpdateChangeFeedStatus("cf-1", 800, 1000)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(tester.KVEntries()["/tidb/cdc/job/cf-1"], check.Equals, string(changeFeedStatusJSON))

	// test the normal case
	ownerState.UpdateChangeFeedStatus("cf-1", 1200, 1000)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(tester.KVEntries()["/tidb/cdc/job/cf-1"], check.NotNil)

	targetChangeFeedStatus := &model.ChangeFeedStatus{
		ResolvedTs:   1200,
		CheckpointTs: 1000,
		AdminJobType: 0,
	}
	targetChangeFeedStatusJSON, err := json.Marshal(targetChangeFeedStatus)
	c.Assert(err, check.IsNil)
	c.Assert(tester.KVEntries()["/tidb/cdc/job/cf-1"], check.Equals, string(targetChangeFeedStatusJSON))

	// test the zero cases
	c.Assert(func() {
		ownerState.UpdateChangeFeedStatus("cf-1", 0, 1000)
	}, check.PanicMatches, ".*illegal changeFeedStatus.*")

	c.Assert(func() {
		ownerState.UpdateChangeFeedStatus("cf-1", 1200, 0)
	}, check.PanicMatches, ".*illegal changeFeedStatus.*")

	// test the regression case
	c.Assert(func() {
		ownerState.UpdateChangeFeedStatus("cf-1", 1200, 900)
		_ = tester.ApplyPatches()
	}, check.PanicMatches, ".*checkpointTs regressed.*")
}

func (s *ownerStateTestSuite) TestDispatchTable(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	replicaInfo := model.TableReplicaInfo{
		StartTs:     1000,
		MarkTableID: 0,
	}
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{})

	// test the creation case
	ownerState.DispatchTable("cf-1", "capture-1", 1, replicaInfo)
	err := tester.ApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(tester.KVEntries()["/tidb/cdc/task/status/capture-1/cf-1"], check.NotNil)

	var status model.TaskStatus
	err = json.Unmarshal([]byte(tester.KVEntries()["/tidb/cdc/task/status/capture-1/cf-1"]), &status)
	c.Assert(err, check.IsNil)
	c.Assert(status.Tables[1], check.DeepEquals, &replicaInfo)
	c.Assert(status.Operation[1].Status, check.Equals, model.OperDispatched)
	c.Assert(status.Operation[1].Delete, check.IsFalse)
	c.Assert(status.Operation[1].BoundaryTs, check.Equals, uint64(1000))

	// test the normal case
	ownerState.DispatchTable("cf-1", "capture-1", 2, replicaInfo)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(tester.KVEntries()["/tidb/cdc/task/status/capture-1/cf-1"], check.NotNil)

	err = json.Unmarshal([]byte(tester.KVEntries()["/tidb/cdc/task/status/capture-1/cf-1"]), &status)
	c.Assert(err, check.IsNil)
	c.Assert(status.Tables[2], check.DeepEquals, &replicaInfo)
	c.Assert(status.Operation[2].Status, check.Equals, model.OperDispatched)
	c.Assert(status.Operation[2].Delete, check.IsFalse)
	c.Assert(status.Operation[2].BoundaryTs, check.Equals, uint64(1000))

	c.Assert(status.Tables[1], check.NotNil)
	c.Assert(status.Operation[2], check.NotNil)

	// test the duplication case
	oldJSON := tester.KVEntries()["/tidb/cdc/task/status/capture-1/cf-1"]
	ownerState.DispatchTable("cf-1", "capture-1", 2, replicaInfo)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(tester.KVEntries()["/tidb/cdc/task/status/capture-1/cf-1"], check.Equals, oldJSON)
}

func (s *ownerStateTestSuite) TestStartDeletingTableCase(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	taskStatus := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{
			1: {
				StartTs:     2000,
				MarkTableID: 0,
			},
			2: {
				StartTs:     3000,
				MarkTableID: 0,
			},
		},
		Operation: nil,
	}
	ownerState.ChangeFeedStatuses["cf-1"] = &model.ChangeFeedStatus{
		ResolvedTs:   5000,
		CheckpointTs: 4500,
		AdminJobType: 0,
	}
	ownerState.TaskStatuses["cf-1"] = map[model.CaptureID]*model.TaskStatus{
		"capture-1": taskStatus,
	}
	taskStatusJSON, err := json.Marshal(taskStatus)
	c.Assert(err, check.IsNil)
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{
		"/tidb/cdc/task/status/capture-1/cf-1": string(taskStatusJSON),
	})

	// test the normal case
	ownerState.StartDeletingTable("cf-1", "capture-1", 1)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	var newTaskStatus model.TaskStatus
	err = json.Unmarshal([]byte(tester.KVEntries()["/tidb/cdc/task/status/capture-1/cf-1"]), &newTaskStatus)
	c.Assert(err, check.IsNil)
	c.Assert(newTaskStatus.Tables, check.DeepEquals, taskStatus.Tables)
	c.Assert(newTaskStatus.Operation[1].Status, check.Equals, model.OperDispatched)
	c.Assert(newTaskStatus.Operation[1].BoundaryTs, check.Equals, uint64(5000))
	c.Assert(newTaskStatus.Operation[1].Delete, check.IsTrue)

	// test the duplication case
	ownerState.StartDeletingTable("cf-1", "capture-1", 1)
	c.Assert(func() {
		_ = tester.ApplyPatches()
	}, check.PanicMatches, ".*repeated deletion.*")

	// test the inconsistent case
	ownerState.StartDeletingTable("cf-1", "capture-1", 2)
	// simulate a Etcd txn conflict
	delete(tester.KVEntries(), "/tidb/cdc/task/status/capture-1/cf-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.ErrorMatches, ".*TaskStatus deleted.*")

	// test the panic cases
	delete(ownerState.TaskStatuses["cf-1"], "capture-1")
	c.Assert(func() {
		ownerState.StartDeletingTable("cf-1", "capture-1", 2)
	}, check.PanicMatches, ".*capture not found.*")

	delete(ownerState.TaskStatuses, "cf-1")
	c.Assert(func() {
		ownerState.StartDeletingTable("cf-1", "capture-1", 2)
	}, check.PanicMatches, ".*changeFeedState not found.*")
}

func (s *ownerStateTestSuite) TestCleanOperation(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	taskStatus := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{
			2: {
				StartTs:     3000,
				MarkTableID: 0,
			},
		},
		Operation: map[model.TableID]*model.TableOperation{
			1: {
				Delete:     false,
				BoundaryTs: 4500,
				Done:       true,
				Status:     model.OperFinished,
			},
			2: {
				Delete:     false,
				BoundaryTs: 4500,
				Done:       false,
				Status:     model.OperProcessed,
			},
		},
	}
	ownerState.ChangeFeedStatuses["cf-1"] = &model.ChangeFeedStatus{
		ResolvedTs:   5000,
		CheckpointTs: 4500,
		AdminJobType: 0,
	}
	ownerState.TaskStatuses["cf-1"] = map[model.CaptureID]*model.TaskStatus{
		"capture-1": taskStatus,
	}
	taskStatusJSON, err := json.Marshal(taskStatus)
	c.Assert(err, check.IsNil)
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{
		"/tidb/cdc/task/status/capture-1/cf-1": string(taskStatusJSON),
	})

	// test the normal case
	ownerState.CleanOperation("cf-1", "capture-1", 1)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	var newTaskStatus model.TaskStatus
	err = json.Unmarshal([]byte(tester.KVEntries()["/tidb/cdc/task/status/capture-1/cf-1"]), &newTaskStatus)
	c.Assert(err, check.IsNil)
	c.Assert(newTaskStatus.Tables, check.DeepEquals, taskStatus.Tables)
	c.Assert(newTaskStatus.Operation[2], check.DeepEquals, taskStatus.Operation[2])
	c.Assert(newTaskStatus.Operation, check.Not(check.HasKey), model.TableID(1))

	// test the inconsistent case
	ownerState.CleanOperation("cf-1", "capture-1", 1)
	delete(tester.KVEntries(), "/tidb/cdc/task/status/capture-1/cf-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.ErrorMatches, ".*TaskStatus deleted.*")

	// test the table-not-cleaned case
	taskStatus.Tables[1] = &model.TableReplicaInfo{
		StartTs:     2000,
		MarkTableID: 0,
	}
	taskStatus.Operation[1].Delete = true
	taskStatusJSON, err = json.Marshal(taskStatus)
	c.Assert(err, check.IsNil)
	tester = orchestrator.NewReactorStateTester(ownerState, map[string]string{
		"/tidb/cdc/task/status/capture-1/cf-1": string(taskStatusJSON),
	})
	ownerState.CleanOperation("cf-1", "capture-1", 1)
	c.Assert(func() {
		_ = tester.ApplyPatches()
	}, check.PanicMatches, ".*table not cleaned.*")

	// test the panic cases
	delete(ownerState.TaskStatuses["cf-1"], "capture-1")
	c.Assert(func() {
		ownerState.CleanOperation("cf-1", "capture-1", 2)
	}, check.PanicMatches, ".*capture not found.*")

	delete(ownerState.TaskStatuses, "cf-1")
	c.Assert(func() {
		ownerState.CleanOperation("cf-1", "capture-1", 2)
	}, check.PanicMatches, ".*changeFeedState not found.*")
}

func (s *ownerStateTestSuite) TestAlterChangeFeedRuntimeState(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	changeFeedInfo := &model.ChangeFeedInfo{
		SinkURI:      "blackhole:///",
		AdminJobType: model.AdminNone,
		State:        model.StateNormal,
		ErrorHis:     nil,
	}
	changeFeedInfoJSON, err := json.Marshal(changeFeedInfo)
	c.Assert(err, check.IsNil)

	ownerState.ChangeFeedInfos["cf-1"] = changeFeedInfo
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{
		"/tidb/cdc/changefeed/info/cf-1": string(changeFeedInfoJSON),
	})

	// test the normal case
	runningErr := &model.RunningError{
		Addr:    "127.0.0.1:8083",
		Code:    "",
		Message: "",
	}
	ts := time.Now().UnixNano()
	ownerState.AlterChangeFeedRuntimeState("cf-1", model.AdminStop, model.StateFailed, runningErr, ts)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(tester.KVEntries()["/tidb/cdc/changefeed/info/cf-1"], check.NotNil)

	var newChangeFeedInfo model.ChangeFeedInfo
	err = json.Unmarshal([]byte(tester.KVEntries()["/tidb/cdc/changefeed/info/cf-1"]), &newChangeFeedInfo)
	c.Assert(err, check.IsNil)
	c.Assert(newChangeFeedInfo.Error, check.DeepEquals, runningErr)
	c.Assert(newChangeFeedInfo.ErrorHis, check.DeepEquals, []int64{ts})
	c.Assert(newChangeFeedInfo.AdminJobType, check.Equals, model.AdminStop)
	c.Assert(newChangeFeedInfo.State, check.Equals, model.StateFailed)

	// test that error is NOT cleared
	// reset states
	ownerState.ChangeFeedInfos["cf-1"] = &newChangeFeedInfo
	ownerState.AlterChangeFeedRuntimeState("cf-1", model.AdminNone, model.StateNormal, nil, 0)
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	err = json.Unmarshal([]byte(tester.KVEntries()["/tidb/cdc/changefeed/info/cf-1"]), &newChangeFeedInfo)
	c.Assert(err, check.IsNil)
	c.Assert(newChangeFeedInfo.Error, check.DeepEquals, runningErr)
	c.Assert(newChangeFeedInfo.ErrorHis, check.DeepEquals, []int64{ts})
	c.Assert(newChangeFeedInfo.AdminJobType, check.Equals, model.AdminNone)
	c.Assert(newChangeFeedInfo.State, check.Equals, model.StateNormal)

	// test changeFeedInfo-gone case: we should survive it
	ownerState.AlterChangeFeedRuntimeState("cf-1", model.AdminNone, model.StateNormal, nil, 0)
	delete(tester.KVEntries(), "/tidb/cdc/changefeed/info/cf-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
}

func (s *ownerStateTestSuite) TestCleanUpTaskStatus(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	taskStatus := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{
			2: {
				StartTs:     3000,
				MarkTableID: 0,
			},
		},
		Operation: map[model.TableID]*model.TableOperation{
			1: {
				Delete:     false,
				BoundaryTs: 4500,
				Done:       true,
				Status:     model.OperFinished,
			},
			2: {
				Delete:     false,
				BoundaryTs: 4500,
				Done:       false,
				Status:     model.OperProcessed,
			},
		},
	}
	ownerState.TaskStatuses["cf-1"] = map[model.CaptureID]*model.TaskStatus{"capture-1": taskStatus}

	taskStatusJSON, err := json.Marshal(taskStatus)
	c.Assert(err, check.IsNil)
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{
		"/tidb/cdc/task/status/capture-1/cf-1": string(taskStatusJSON),
	})

	// test the normal case
	ownerState.CleanUpTaskStatus("cf-1", "capture-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(tester.KVEntries(), check.Not(check.HasKey), "/tidb/cdc/task/status/capture-1/cf-1")

	// test the idempotent case
	delete(ownerState.TaskStatuses["cf-1"], "capture-1")
	ownerState.CleanUpTaskStatus("cf-1", "capture-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(tester.KVEntries(), check.Not(check.HasKey), "/tidb/cdc/task/status/capture-1/cf-1")

	// test the no-changefeed case
	delete(ownerState.TaskStatuses, "cf-1")
	ownerState.CleanUpTaskStatus("cf-1", "capture-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	// test the conflict case
	ownerState.TaskStatuses["cf-1"] = map[model.CaptureID]*model.TaskStatus{"capture-1": taskStatus}
	tester.KVEntries()["/tidb/cdc/task/status/capture-1/cf-1"] = string(taskStatusJSON)
	ownerState.CleanUpTaskStatus("cf-1", "capture-1")
	delete(tester.KVEntries(), "/tidb/cdc/task/status/capture-1/cf-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
}

func (s *ownerStateTestSuite) TestCleanUpTaskPosition(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	taskPosition := &model.TaskPosition{
		CheckPointTs: 1000,
		ResolvedTs:   2000,
	}
	ownerState.TaskPositions["cf-1"] = map[model.CaptureID]*model.TaskPosition{"capture-1": taskPosition}

	taskPositionJSON, err := json.Marshal(taskPosition)
	c.Assert(err, check.IsNil)
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{
		"/tidb/cdc/task/position/capture-1/cf-1": string(taskPositionJSON),
	})

	// test the normal case
	ownerState.CleanUpTaskPosition("cf-1", "capture-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(tester.KVEntries(), check.Not(check.HasKey), "/tidb/cdc/task/position/capture-1/cf-1")

	// test the idempotent case
	delete(ownerState.TaskPositions["cf-1"], "capture-1")
	ownerState.CleanUpTaskPosition("cf-1", "capture-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(tester.KVEntries(), check.Not(check.HasKey), "/tidb/cdc/task/position/capture-1/cf-1")

	// test the no-changefeed case
	delete(ownerState.TaskPositions, "cf-1")
	ownerState.CleanUpTaskPosition("cf-1", "capture-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	// test the conflict case
	ownerState.TaskPositions["cf-1"] = map[model.CaptureID]*model.TaskPosition{"capture-1": taskPosition}
	tester.KVEntries()["/tidb/cdc/task/position/capture-1/cf-1"] = string(taskPositionJSON)
	ownerState.CleanUpTaskPosition("cf-1", "capture-1")
	delete(tester.KVEntries(), "/tidb/cdc/task/position/capture-1/cf-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
}

func (s *ownerStateTestSuite) TestGetCaptureTables(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	taskStatus := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{
			1: {
				StartTs:     2000,
				MarkTableID: 0,
			},
			2: {
				StartTs:     3000,
				MarkTableID: 0,
			},
		},
		Operation: map[model.TableID]*model.TableOperation{
			2: {
				Delete:     false,
				BoundaryTs: 4500,
				Done:       true,
				Status:     model.OperFinished,
			},
			3: {
				Delete:     false,
				BoundaryTs: 4500,
				Done:       false,
				Status:     model.OperProcessed,
			},
		},
	}
	ownerState.TaskStatuses["cf-1"] = map[model.CaptureID]*model.TaskStatus{"capture-1": taskStatus}
	ownerState.Captures["capture-1"] = &model.CaptureInfo{}

	// test normal case 1
	tables := ownerState.GetCaptureTables("cf-1", "capture-1")
	if !tableIDListMatch(tables, []model.TableID{1, 2, 3}) {
		c.Fatal(tables)
	}

	// test normal case 2
	ownerState.TaskStatuses["cf-1"]["capture-1"].Operation[3].Delete = true
	ownerState.TaskStatuses["cf-1"]["capture-1"].Operation[3].Status = model.OperFinished
	tables = ownerState.GetCaptureTables("cf-1", "capture-1")
	if !tableIDListMatch(tables, []model.TableID{1, 2}) {
		c.Fatal(tables)
	}

	// test normal case 3
	ownerState.TaskStatuses["cf-1"]["capture-1"].Tables[3] = &model.TableReplicaInfo{
		StartTs:     3000,
		MarkTableID: 0,
	}

	tables = ownerState.GetCaptureTables("cf-1", "capture-1")
	if !tableIDListMatch(tables, []model.TableID{1, 2, 3}) {
		c.Fatal(tables)
	}

	// test no task status case 1
	delete(ownerState.TaskStatuses["cf-1"], "capture-1")
	tables = ownerState.GetCaptureTables("cf-1", "capture-1")
	c.Assert(tables, check.HasLen, 0)

	// test no task status case 2
	delete(ownerState.TaskStatuses, "cf-1")
	tables = ownerState.GetCaptureTables("cf-1", "capture-1")
	c.Assert(tables, check.HasLen, 0)

	// test capture gone case
	delete(ownerState.Captures, "capture-1")
	tables = ownerState.GetCaptureTables("cf-1", "capture-1")
	c.Assert(tables, check.HasLen, 0)
}

func tableIDListMatch(a, b []model.TableID) bool {
	if len(a) != len(b) {
		return false
	}

	setA := make(map[model.TableID]struct{})
	for _, tableID := range a {
		setA[tableID] = struct{}{}
	}

	setB := make(map[model.TableID]struct{})
	for _, tableID := range b {
		setB[tableID] = struct{}{}
	}

	return reflect.DeepEqual(setA, setB)
}

func (s *ownerStateTestSuite) TestCleanUpChangeFeedErrorHistory(c *check.C) {
	defer testleak.AfterTest(c)()
	now := time.Now()
	ownerState := newCDCReactorState()
	changeFeedInfo := &model.ChangeFeedInfo{
		SinkURI:      "blackhole:///",
		AdminJobType: model.AdminNone,
		State:        model.StateNormal,
		ErrorHis: []int64{
			now.Add(-time.Hour).UnixNano() / 1e6, now.Add(-time.Minute*20).UnixNano() / 1e6,
			now.Add(-time.Minute*5).UnixNano() / 1e6, now.Add(-time.Minute*3).UnixNano() / 1e6,
		},
	}
	changeFeedInfoJSON, err := json.Marshal(changeFeedInfo)
	c.Assert(err, check.IsNil)

	ownerState.ChangeFeedInfos["cf-1"] = changeFeedInfo
	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{
		"/tidb/cdc/changefeed/info/cf-1": string(changeFeedInfoJSON),
	})

	// test the normal case
	ownerState.CleanUpChangeFeedErrorHistory("cf-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)

	var newChangeFeedInfo model.ChangeFeedInfo
	err = json.Unmarshal([]byte(tester.KVEntries()["/tidb/cdc/changefeed/info/cf-1"]), &newChangeFeedInfo)
	c.Assert(err, check.IsNil)
	c.Assert(newChangeFeedInfo.ErrorHis, check.DeepEquals,
		[]int64{now.Add(-time.Minute*5).UnixNano() / 1e6, now.Add(-time.Minute*3).UnixNano() / 1e6})

	// test the already cleaned case
	oldJSON := tester.KVEntries()["/tidb/cdc/changefeed/info/cf-1"]
	ownerState.CleanUpChangeFeedErrorHistory("cf-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(tester.KVEntries()["/tidb/cdc/changefeed/info/cf-1"], check.Equals, oldJSON)

	// test changeFeedInfo gone case
	ownerState.CleanUpChangeFeedErrorHistory("cf-1")
	delete(tester.KVEntries(), "/tidb/cdc/changefeed/info/cf-1")
	err = tester.ApplyPatches()
	c.Assert(err, check.IsNil)
}

func (s *ownerStateTestSuite) TestGetTableToCaptureMap(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	taskStatus1 := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{
			1: {
				StartTs:     2000,
				MarkTableID: 0,
			},
			2: {
				StartTs:     3000,
				MarkTableID: 0,
			},
		},
	}
	taskStatus2 := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{
			3: {
				StartTs:     2000,
				MarkTableID: 0,
			},
			4: {
				StartTs:     3000,
				MarkTableID: 0,
			},
		},
	}
	ownerState.TaskStatuses["cf-1"] = map[model.CaptureID]*model.TaskStatus{
		"capture-1": taskStatus1, "capture-2": taskStatus2,
	}
	ownerState.Captures["capture-1"] = &model.CaptureInfo{}
	ownerState.Captures["capture-2"] = &model.CaptureInfo{}

	// test the basic case
	tableToCaptureMap := ownerState.GetTableToCaptureMap("cf-1")
	c.Assert(tableToCaptureMap, check.DeepEquals, map[model.TableID]model.CaptureID{
		1: "capture-1",
		2: "capture-1",
		3: "capture-2",
		4: "capture-2",
	})

	// test the operation case
	ownerState.TaskStatuses["cf-1"]["capture-1"].Operation = make(map[model.TableID]*model.TableOperation)
	ownerState.TaskStatuses["cf-1"]["capture-1"].Operation[5] = &model.TableOperation{
		Delete:     false,
		BoundaryTs: 5000,
		Done:       false,
		Status:     model.OperDispatched,
	}
	ownerState.TaskStatuses["cf-1"]["capture-2"].Operation = make(map[model.TableID]*model.TableOperation)
	ownerState.TaskStatuses["cf-1"]["capture-2"].Operation[6] = &model.TableOperation{
		Delete:     true,
		BoundaryTs: 5000,
		Done:       true,
		Status:     model.OperFinished,
	}
	tableToCaptureMap = ownerState.GetTableToCaptureMap("cf-1")
	c.Assert(tableToCaptureMap, check.DeepEquals, map[model.TableID]model.CaptureID{
		1: "capture-1",
		2: "capture-1",
		3: "capture-2",
		4: "capture-2",
		5: "capture-1",
	})
}

func (s *ownerStateTestSuite) TestGetTableProgressAndActiveTables(c *check.C) {
	defer testleak.AfterTest(c)()
	ownerState := newCDCReactorState()
	taskStatus1 := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{
			1: {
				StartTs:     2000,
				MarkTableID: 0,
			},
			2: {
				StartTs:     3000,
				MarkTableID: 0,
			},
		},
	}
	taskStatus1JSON, err := json.Marshal(taskStatus1)
	c.Assert(err, check.IsNil)

	taskStatus2 := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{
			3: {
				StartTs:     2000,
				MarkTableID: 0,
			},
			4: {
				StartTs:     3000,
				MarkTableID: 0,
			},
		},
	}
	taskStatus2JSON, err := json.Marshal(taskStatus2)
	c.Assert(err, check.IsNil)

	taskPosition1 := &model.TaskPosition{
		CheckPointTs: 3500,
		ResolvedTs:   4000,
	}
	taskPosition1JSON, err := json.Marshal(taskPosition1)
	c.Assert(err, check.IsNil)

	taskPosition2 := &model.TaskPosition{
		CheckPointTs: 3600,
		ResolvedTs:   3900,
	}
	taskPosition2JSON, err := json.Marshal(taskPosition2)
	c.Assert(err, check.IsNil)

	captureInfo1 := &model.CaptureInfo{
		ID:            "127.0.0.1:8081",
		AdvertiseAddr: "127.0.0.1:8081",
	}
	captureInfo1JSON, err := json.Marshal(captureInfo1)
	c.Assert(err, check.IsNil)

	captureInfo2 := &model.CaptureInfo{
		ID:            "127.0.0.1:8082",
		AdvertiseAddr: "127.0.0.1:8082",
	}
	captureInfo2JSON, err := json.Marshal(captureInfo2)
	c.Assert(err, check.IsNil)

	tester := orchestrator.NewReactorStateTester(ownerState, map[string]string{})
	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/task/status/capture-1/cf-1":   taskStatus1JSON,
		"/tidb/cdc/task/status/capture-2/cf-1":   taskStatus2JSON,
		"/tidb/cdc/task/position/capture-1/cf-1": taskPosition1JSON,
		"/tidb/cdc/task/position/capture-2/cf-1": taskPosition2JSON,
		"/tidb/cdc/capture/capture-1":            captureInfo1JSON,
		"/tidb/cdc/capture/capture-2":            captureInfo2JSON,
	})
	c.Assert(err, check.IsNil)

	// test the basic case
	progress := ownerState.GetTableProgress("cf-1", 1)
	c.Assert(progress, check.DeepEquals, &tableProgress{
		resolvedTs:   4000,
		checkpointTs: 3500,
	})

	progress = ownerState.GetTableProgress("cf-1", 2)
	c.Assert(progress, check.DeepEquals, &tableProgress{
		resolvedTs:   4000,
		checkpointTs: 3500,
	})

	progress = ownerState.GetTableProgress("cf-1", 3)
	c.Assert(progress, check.DeepEquals, &tableProgress{
		resolvedTs:   3900,
		checkpointTs: 3600,
	})

	progress = ownerState.GetTableProgress("cf-1", 4)
	c.Assert(progress, check.DeepEquals, &tableProgress{
		resolvedTs:   3900,
		checkpointTs: 3600,
	})

	activeTables := ownerState.GetChangeFeedActiveTables("cf-1")
	if !tableIDListMatch(activeTables, []model.TableID{1, 2, 3, 4}) {
		c.Fatal(activeTables)
	}

	// test stopping table
	delete(taskStatus1.Tables, 1)
	taskStatus1JSON, err = json.Marshal(taskStatus1)
	c.Assert(err, check.IsNil)

	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/task/status/capture-1/cf-1": taskStatus1JSON,
	})
	c.Assert(err, check.IsNil)

	progress = ownerState.GetTableProgress("cf-1", 1)
	c.Assert(progress, check.IsNil)

	activeTables = ownerState.GetChangeFeedActiveTables("cf-1")
	if !tableIDListMatch(activeTables, []model.TableID{2, 3, 4}) {
		c.Fatal(activeTables)
	}

	// test capture gone case
	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/capture/capture-2": nil,
	})
	c.Assert(err, check.IsNil)

	progress = ownerState.GetTableProgress("cf-1", 3)
	c.Assert(progress, check.IsNil)

	activeTables = ownerState.GetChangeFeedActiveTables("cf-1")
	if !tableIDListMatch(activeTables, []model.TableID{2}) {
		c.Fatal(activeTables)
	}

	// test position gone case
	err = tester.UpdateKeys(map[string][]byte{
		"/tidb/cdc/task/position/capture-1/cf-1": nil,
	})
	c.Assert(err, check.IsNil)

	progress = ownerState.GetTableProgress("cf-1", 2)
	c.Assert(progress, check.IsNil)

	activeTables = ownerState.GetChangeFeedActiveTables("cf-1")
	if !tableIDListMatch(activeTables, []model.TableID{2}) {
		c.Fatal(activeTables)
	}
}
