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

package ha

import (
	. "github.com/pingcap/check"
)

func (t *testForEtcd) TestWorkerInfoJSON(c *C) {
	i1 := NewWorkerInfo("dm-worker-1", "192.168.0.100:8262")

	j, err := i1.toJSON()
	c.Assert(err, IsNil)
	c.Assert(j, Equals, `{"name":"dm-worker-1","addr":"192.168.0.100:8262"}`)
	c.Assert(j, Equals, i1.String())

	i2, err := workerInfoFromJSON(j)
	c.Assert(err, IsNil)
	c.Assert(i2, DeepEquals, i1)
}

func (t *testForEtcd) TestWorkerInfoEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		worker1 = "dm-worker-1"
		worker2 = "dm-worker-2"
		info1   = NewWorkerInfo(worker1, "192.168.0.100:8262")
		info2   = NewWorkerInfo(worker2, "192.168.0.101:8262")
	)

	// get without info.
	ifm, _, err := GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 0)

	// put two info.
	rev1, err := PutWorkerInfo(etcdTestCli, info1)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))
	rev2, err := PutWorkerInfo(etcdTestCli, info2)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// get again, with two info.
	ifm, rev3, err := GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(ifm, HasLen, 2)
	c.Assert(ifm[worker1], DeepEquals, info1)
	c.Assert(ifm[worker2], DeepEquals, info2)

	// delete info1.
	rev4, err := DeleteWorkerInfoRelayConfig(etcdTestCli, worker1)
	c.Assert(err, IsNil)
	c.Assert(rev4, Greater, rev3)

	// get again, with only one info.
	ifm, rev5, err := GetAllWorkerInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, rev4)
	c.Assert(ifm, HasLen, 1)
	c.Assert(ifm[worker2], DeepEquals, info2)
}
