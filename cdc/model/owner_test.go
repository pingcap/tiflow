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
	"testing"

	"github.com/pingcap/check"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type taskStatusSuite struct{}

var _ = check.Suite(&taskStatusSuite{})

func (s *taskStatusSuite) TestShouldBeDeepCopy(c *check.C) {
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
		})
		c.Assert(clone.AdminJobType, check.Equals, AdminStop)
	}

	assertIsSnapshot()

	info.Tables[6] = &TableReplicaInfo{StartTs: 100}
	info.Operation = map[TableID]*TableOperation{
		6: {Delete: true, BoundaryTs: 6, Done: true},
	}

	assertIsSnapshot()
}

func (s *taskStatusSuite) TestProcSnapshot(c *check.C) {
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

type removeTableSuite struct{}

var _ = check.Suite(&removeTableSuite{})

func (s *removeTableSuite) TestShouldReturnRemovedTable(c *check.C) {
	info := TaskStatus{
		Tables: map[TableID]*TableReplicaInfo{
			1: {StartTs: 100},
			2: {StartTs: 200},
			3: {StartTs: 300},
			4: {StartTs: 400},
		},
	}

	replicaInfo, found := info.RemoveTable(2, 666)
	c.Assert(found, check.IsTrue)
	c.Assert(replicaInfo, check.DeepEquals, &TableReplicaInfo{StartTs: 200})
}

func (s *removeTableSuite) TestShouldHandleTableNotFoundCorrectly(c *check.C) {
	info := TaskStatus{}
	_, found := info.RemoveTable(404, 666)
	c.Assert(found, check.IsFalse)
}
