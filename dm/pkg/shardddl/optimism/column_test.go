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

package optimism

import (
	. "github.com/pingcap/check"
)

type testColumn struct{}

var _ = Suite(&testColumn{})

func (t *testColumn) TestColumnETCD(c *C) {
	defer clearTestInfoOperation(c)

	var (
		task       = "test"
		downSchema = "shardddl"
		downTable  = "tb"
		source1    = "mysql-replica-1"
		source2    = "mysql-replica-2"
		upSchema1  = "shardddl1"
		upTable1   = "tb1"
		upSchema2  = "shardddl2"
		upTable2   = "tb2"
		info1      = NewInfo(task, source1, upSchema1, upTable1, downSchema, downTable, nil, nil, nil)
		lockID     = genDDLLockID(info1)
	)
	rev1, putted, err := PutDroppedColumn(etcdTestCli, lockID, "a", source1, upSchema1, upTable1, DropNotDone)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	rev2, putted, err := PutDroppedColumn(etcdTestCli, lockID, "b", source1, upSchema1, upTable1, DropNotDone)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(rev2, Greater, rev1)
	rev3, putted, err := PutDroppedColumn(etcdTestCli, lockID, "b", source1, upSchema2, upTable2, DropNotDone)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(rev3, Greater, rev2)
	rev4, putted, err := PutDroppedColumn(etcdTestCli, lockID, "b", source2, upSchema1, upTable1, DropNotDone)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(rev4, Greater, rev3)

	expectedColm := map[string]map[string]map[string]map[string]map[string]DropColumnStage{
		lockID: {
			"a": {source1: {upSchema1: {upTable1: DropNotDone}}},
			"b": {
				source1: {
					upSchema1: {upTable1: DropNotDone},
					upSchema2: {upTable2: DropNotDone},
				},
				source2: {upSchema1: {upTable1: DropNotDone}},
			},
		},
	}
	colm, rev5, err := GetAllDroppedColumns(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(colm, DeepEquals, expectedColm)
	c.Assert(rev5, Equals, rev4)

	rev6, deleted, err := DeleteDroppedColumns(etcdTestCli, lockID, "b")
	c.Assert(err, IsNil)
	c.Assert(deleted, IsTrue)
	c.Assert(rev6, Greater, rev5)

	delete(expectedColm[lockID], "b")
	colm, rev7, err := GetAllDroppedColumns(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(colm, DeepEquals, expectedColm)
	c.Assert(rev7, Equals, rev6)
}
