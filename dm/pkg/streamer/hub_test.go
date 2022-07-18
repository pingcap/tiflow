// Copyright 2019 PingCAP, Inc.
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

package streamer

import (
	"path/filepath"

	. "github.com/pingcap/check"
)

var _ = Suite(&testHubSuite{})

type testHubSuite struct{}

func (t *testHubSuite) TestRelayLogInfo(c *C) {
	rli1 := RelayLogInfo{}
	rli1.SubDirSuffix = 1
	rli2 := RelayLogInfo{}
	rli2.SubDirSuffix = 2

	// compare by UUIDSuffix
	c.Assert(rli1.Earlier(&rli1), IsFalse)
	c.Assert(rli1.Earlier(&rli2), IsTrue)
	c.Assert(rli2.Earlier(&rli1), IsFalse)
	c.Assert(rli2.Earlier(&rli2), IsFalse)

	// compare by filename
	rli3 := RelayLogInfo{}
	rli3.SubDirSuffix = 1

	rli1.Filename = "mysql-bin.000001"
	rli3.Filename = "mysql-bin.000001" // equal
	c.Assert(rli1.Earlier(&rli3), IsFalse)
	c.Assert(rli3.Earlier(&rli1), IsFalse)

	rli3.Filename = "mysql-bin.000003"
	c.Assert(rli1.Earlier(&rli3), IsTrue)
	c.Assert(rli3.Earlier(&rli1), IsFalse)

	// string representation
	rli3.SubDir = "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000001"
	c.Assert(rli3.String(), Equals, filepath.Join(rli3.SubDir, rli3.Filename))
}

func (t *testHubSuite) TestRelayLogInfoHub(c *C) {
	rlih := newRelayLogInfoHub()
	taskName, earliest := rlih.earliest()
	c.Assert(taskName, Equals, "")
	c.Assert(earliest, IsNil)

	// update with invalid args
	err := rlih.update("invalid-task", "invalid.uuid", "mysql-bin.000006")
	c.Assert(err, NotNil)
	err = rlih.update("invalid-task", "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002", "invalid.binlog")
	c.Assert(err, NotNil)
	taskName, earliest = rlih.earliest()
	c.Assert(taskName, Equals, "")
	c.Assert(earliest, IsNil)

	cases := []struct {
		taskName string
		uuid     string
		filename string
	}{
		{"task-1", "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000004", "mysql-bin.000001"},
		{"task-2", "e9540a0d-f16d-11e8-8cb7-0242ac130008.000003", "mysql-bin.000002"},
		{"task-3", "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002", "mysql-bin.000003"},
		{"task-3", "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002", "mysql-bin.000002"},
	}

	// update from later to earlier
	for _, cs := range cases {
		err = rlih.update(cs.taskName, cs.uuid, cs.filename)
		c.Assert(err, IsNil)
		taskName, earliest = rlih.earliest()
		c.Assert(taskName, Equals, cs.taskName)
		c.Assert(earliest.SubDir, Equals, cs.uuid)
		c.Assert(earliest.Filename, Equals, cs.filename)
	}
	c.Assert(len(rlih.logs), Equals, 3)

	// remove earliest
	cs := cases[3]
	rlih.remove(cs.taskName)
	c.Assert(len(rlih.logs), Equals, 2)

	taskName, earliest = rlih.earliest()
	cs = cases[1]
	c.Assert(taskName, Equals, cs.taskName)
	c.Assert(earliest.SubDir, Equals, cs.uuid)
	c.Assert(earliest.Filename, Equals, cs.filename)

	// remove non-earliest
	cs = cases[0]
	rlih.remove(cs.taskName)
	c.Assert(len(rlih.logs), Equals, 1)

	taskName, earliest = rlih.earliest()
	cs = cases[1]
	c.Assert(taskName, Equals, cs.taskName)
	c.Assert(earliest.SubDir, Equals, cs.uuid)
	c.Assert(earliest.Filename, Equals, cs.filename)

	// all removed
	cs = cases[1]
	rlih.remove(cs.taskName)
	c.Assert(len(rlih.logs), Equals, 0)

	taskName, earliest = rlih.earliest()
	c.Assert(taskName, Equals, "")
	c.Assert(earliest, IsNil)
}

func (t *testHubSuite) TestReaderHub(c *C) {
	h := GetReaderHub()
	c.Assert(h, NotNil)

	h2 := GetReaderHub()
	c.Assert(h2, NotNil)
	c.Assert(h2, Equals, h)

	// no earliest
	erli := h.EarliestActiveRelayLog()
	c.Assert(erli, IsNil)

	// update one
	err := h.UpdateActiveRelayLog("task-1", "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000004", "mysql-bin.000001")
	c.Assert(err, IsNil)

	// the only one is the earliest
	erli = h.EarliestActiveRelayLog()
	c.Assert(erli, NotNil)
	c.Assert(erli.SubDir, Equals, "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000004")
	c.Assert(erli.Filename, Equals, "mysql-bin.000001")

	// update an earlier one
	err = h.UpdateActiveRelayLog("task-2", "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002", "mysql-bin.000002")
	c.Assert(err, IsNil)

	// the earlier one is the earliest
	erli = h.EarliestActiveRelayLog()
	c.Assert(erli, NotNil)
	c.Assert(erli.SubDir, Equals, "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002")
	c.Assert(erli.Filename, Equals, "mysql-bin.000002")

	// remove the earlier one
	h.RemoveActiveRelayLog("task-2")

	// the only one is the earliest
	erli = h.EarliestActiveRelayLog()
	c.Assert(erli, NotNil)
	c.Assert(erli.SubDir, Equals, "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000004")
	c.Assert(erli.Filename, Equals, "mysql-bin.000001")

	// remove the only one
	h.RemoveActiveRelayLog("task-1")

	// no earliest
	erli = h.EarliestActiveRelayLog()
	c.Assert(erli, IsNil)
}
