// Copyright 2020 PingCAP, Inc.
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

package model

import (
	"math"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type ownerCommonSuite struct{}

var _ = check.Suite(&ownerCommonSuite{})

func (s *ownerCommonSuite) TestAdminJobType(c *check.C) {
	defer testleak.AfterTest(c)()
	names := map[AdminJobType]string{
		AdminNone:         "noop",
		AdminStop:         "stop changefeed",
		AdminResume:       "resume changefeed",
		AdminRemove:       "remove changefeed",
		AdminFinish:       "finish changefeed",
		AdminJobType(100): "unknown",
	}
	for job, name := range names {
		c.Assert(job.String(), check.Equals, name)
	}

	isStopped := map[AdminJobType]bool{
		AdminNone:   false,
		AdminStop:   true,
		AdminResume: false,
		AdminRemove: true,
		AdminFinish: true,
	}
	for job, stopped := range isStopped {
		c.Assert(job.IsStopState(), check.Equals, stopped)
	}
}

func (s *ownerCommonSuite) TestDDLStateString(c *check.C) {
	defer testleak.AfterTest(c)()
	names := map[ChangeFeedDDLState]string{
		ChangeFeedSyncDML:          "SyncDML",
		ChangeFeedWaitToExecDDL:    "WaitToExecDDL",
		ChangeFeedExecDDL:          "ExecDDL",
		ChangeFeedDDLExecuteFailed: "DDLExecuteFailed",
		ChangeFeedDDLState(100):    "Unknown",
	}
	for state, name := range names {
		c.Assert(state.String(), check.Equals, name)
	}
}

func (s *ownerCommonSuite) TestTaskPositionMarshal(c *check.C) {
	defer testleak.AfterTest(c)()
	pos := &TaskPosition{
		ResolvedTs:   420875942036766723,
		CheckPointTs: 420875940070686721,
	}
	expected := `{"checkpoint-ts":420875940070686721,"resolved-ts":420875942036766723,"count":0,"error":null}`

	data, err := pos.Marshal()
	c.Assert(err, check.IsNil)
	c.Assert(data, check.DeepEquals, expected)
	c.Assert(pos.String(), check.Equals, expected)

	newPos := &TaskPosition{}
	err = newPos.Unmarshal([]byte(data))
	c.Assert(err, check.IsNil)
	c.Assert(newPos, check.DeepEquals, pos)
}

func (s *ownerCommonSuite) TestChangeFeedStatusMarshal(c *check.C) {
	defer testleak.AfterTest(c)()
	status := &ChangeFeedStatus{
		ResolvedTs:   420875942036766723,
		CheckpointTs: 420875940070686721,
	}
	expected := `{"resolved-ts":420875942036766723,"checkpoint-ts":420875940070686721,"admin-job-type":0}`

	data, err := status.Marshal()
	c.Assert(err, check.IsNil)
	c.Assert(data, check.DeepEquals, expected)

	newStatus := &ChangeFeedStatus{}
	err = newStatus.Unmarshal([]byte(data))
	c.Assert(err, check.IsNil)
	c.Assert(newStatus, check.DeepEquals, status)
}

func (s *ownerCommonSuite) TestTableOperationState(c *check.C) {
	defer testleak.AfterTest(c)()
	processedMap := map[uint64]bool{
		OperDispatched: false,
		OperProcessed:  true,
		OperFinished:   true,
	}
	appliedMap := map[uint64]bool{
		OperDispatched: false,
		OperProcessed:  false,
		OperFinished:   true,
	}
	o := &TableOperation{}

	for status, processed := range processedMap {
		o.Status = status
		c.Assert(o.TableProcessed(), check.Equals, processed)
	}
	for status, applied := range appliedMap {
		o.Status = status
		c.Assert(o.TableApplied(), check.Equals, applied)
	}

	// test clone nil operation. no-nil clone will be tested in `TestShouldBeDeepCopy`
	var nilTableOper *TableOperation
	c.Assert(nilTableOper.Clone(), check.IsNil)
}

func (s *ownerCommonSuite) TestTaskWorkloadMarshal(c *check.C) {
	defer testleak.AfterTest(c)()
	workload := &TaskWorkload{
		12: WorkloadInfo{Workload: uint64(1)},
		15: WorkloadInfo{Workload: uint64(3)},
	}
	expected := `{"12":{"workload":1},"15":{"workload":3}}`

	data, err := workload.Marshal()
	c.Assert(err, check.IsNil)
	c.Assert(data, check.Equals, expected)

	newWorkload := &TaskWorkload{}
	err = newWorkload.Unmarshal([]byte(data))
	c.Assert(err, check.IsNil)
	c.Assert(newWorkload, check.DeepEquals, workload)

	workload = nil
	data, err = workload.Marshal()
	c.Assert(err, check.IsNil)
	c.Assert(data, check.Equals, "{}")
}

type taskStatusSuite struct{}

var _ = check.Suite(&taskStatusSuite{})

func (s *taskStatusSuite) TestShouldBeDeepCopy(c *check.C) {
	defer testleak.AfterTest(c)()
	info := TaskStatus{

		Tables: map[TableID]*TableReplicaInfo{
			1: {StartTs: 100},
			2: {StartTs: 100},
			3: {StartTs: 100},
			4: {StartTs: 100},
		},
		Operation: map[TableID]*TableOperation{
			5: {
				Delete: true, BoundaryTs: 6, Done: true,
			},
			6: {
				Delete: false, BoundaryTs: 7, Done: false,
			},
		},
		AdminJobType: AdminStop,
	}

	clone := info.Clone()
	assertIsSnapshot := func() {
		c.Assert(clone.Tables, check.DeepEquals, map[TableID]*TableReplicaInfo{
			1: {StartTs: 100},
			2: {StartTs: 100},
			3: {StartTs: 100},
			4: {StartTs: 100},
		})
		c.Assert(clone.Operation, check.DeepEquals, map[TableID]*TableOperation{
			5: {
				Delete: true, BoundaryTs: 6, Done: true,
			},
			6: {
				Delete: false, BoundaryTs: 7, Done: false,
			},
		})
		c.Assert(clone.AdminJobType, check.Equals, AdminStop)
	}

	assertIsSnapshot()

	info.Tables[7] = &TableReplicaInfo{StartTs: 100}
	info.Operation[7] = &TableOperation{Delete: true, BoundaryTs: 7, Done: true}

	info.Operation[5].BoundaryTs = 8
	info.Tables[1].StartTs = 200

	assertIsSnapshot()
}

func (s *taskStatusSuite) TestProcSnapshot(c *check.C) {
	defer testleak.AfterTest(c)()
	info := TaskStatus{
		Tables: map[TableID]*TableReplicaInfo{
			10: {StartTs: 100},
		},
	}
	cfID := "changefeed-1"
	captureID := "capture-1"
	snap := info.Snapshot(cfID, captureID, 200)
	c.Assert(snap.CfID, check.Equals, cfID)
	c.Assert(snap.CaptureID, check.Equals, captureID)
	c.Assert(snap.Tables, check.HasLen, 1)
	c.Assert(snap.Tables[10], check.DeepEquals, &TableReplicaInfo{StartTs: 200})
}

func (s *taskStatusSuite) TestTaskStatusMarshal(c *check.C) {
	defer testleak.AfterTest(c)()
	status := &TaskStatus{
		Tables: map[TableID]*TableReplicaInfo{
			1: {StartTs: 420875942036766723},
		},
	}
	expected := `{"tables":{"1":{"start-ts":420875942036766723,"mark-table-id":0}},"operation":null,"admin-job-type":0}`

	data, err := status.Marshal()
	c.Assert(err, check.IsNil)
	c.Assert(data, check.DeepEquals, expected)
	c.Assert(status.String(), check.Equals, expected)

	newStatus := &TaskStatus{}
	err = newStatus.Unmarshal([]byte(data))
	c.Assert(err, check.IsNil)
	c.Assert(newStatus, check.DeepEquals, status)
}

func (s *taskStatusSuite) TestAddTable(c *check.C) {
	defer testleak.AfterTest(c)()
	ts := uint64(420875942036766723)
	expected := &TaskStatus{
		Tables: map[TableID]*TableReplicaInfo{
			1: {StartTs: ts},
		},
		Operation: map[TableID]*TableOperation{
			1: {
				BoundaryTs: ts,
				Status:     OperDispatched,
			},
		},
	}
	status := &TaskStatus{}
	status.AddTable(1, &TableReplicaInfo{StartTs: ts}, ts)
	c.Assert(status, check.DeepEquals, expected)

	// add existing table does nothing
	status.AddTable(1, &TableReplicaInfo{StartTs: 1}, 1)
	c.Assert(status, check.DeepEquals, expected)
}

func (s *taskStatusSuite) TestTaskStatusApplyState(c *check.C) {
	defer testleak.AfterTest(c)()
	ts1 := uint64(420875042036766723)
	ts2 := uint64(420876783269969921)
	status := &TaskStatus{}
	status.AddTable(1, &TableReplicaInfo{StartTs: ts1}, ts1)
	status.AddTable(2, &TableReplicaInfo{StartTs: ts2}, ts2)
	c.Assert(status.SomeOperationsUnapplied(), check.IsTrue)
	c.Assert(status.AppliedTs(), check.Equals, ts1)

	status.Operation[1].Status = OperFinished
	status.Operation[2].Status = OperFinished
	c.Assert(status.SomeOperationsUnapplied(), check.IsFalse)
	c.Assert(status.AppliedTs(), check.Equals, uint64(math.MaxUint64))
}

type removeTableSuite struct{}

var _ = check.Suite(&removeTableSuite{})

func (s *removeTableSuite) TestMoveTable(c *check.C) {
	defer testleak.AfterTest(c)()
	info := TaskStatus{
		Tables: map[TableID]*TableReplicaInfo{
			1: {StartTs: 100},
			2: {StartTs: 200},
		},
	}

	replicaInfo, found := info.RemoveTable(2, 300, true)
	c.Assert(found, check.IsTrue)
	c.Assert(replicaInfo, check.DeepEquals, &TableReplicaInfo{StartTs: 200})
	c.Assert(info.Tables, check.HasKey, int64(1))
	c.Assert(info.Tables, check.Not(check.HasKey), int64(2))
	expectedFlag := uint64(1) // OperFlagMoveTable
	c.Assert(info.Operation, check.DeepEquals, map[int64]*TableOperation{
		2: {
			Delete:     true,
			Flag:       expectedFlag,
			BoundaryTs: 300,
			Status:     OperDispatched,
		},
	})
}

func (s *removeTableSuite) TestShouldReturnRemovedTable(c *check.C) {
	defer testleak.AfterTest(c)()
	info := TaskStatus{
		Tables: map[TableID]*TableReplicaInfo{
			1: {StartTs: 100},
			2: {StartTs: 200},
			3: {StartTs: 300},
			4: {StartTs: 400},
		},
	}

	replicaInfo, found := info.RemoveTable(2, 666, false)
	c.Assert(found, check.IsTrue)
	c.Assert(replicaInfo, check.DeepEquals, &TableReplicaInfo{StartTs: 200})
}

func (s *removeTableSuite) TestShouldHandleTableNotFound(c *check.C) {
	defer testleak.AfterTest(c)()
	info := TaskStatus{}
	_, found := info.RemoveTable(404, 666, false)
	c.Assert(found, check.IsFalse)

	info = TaskStatus{
		Tables: map[TableID]*TableReplicaInfo{
			1: {StartTs: 100},
		},
	}
	_, found = info.RemoveTable(404, 666, false)
	c.Assert(found, check.IsFalse)
}
