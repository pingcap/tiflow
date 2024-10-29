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
	"github.com/pingcap/check"
)

type testColumn struct{}

var _ = check.Suite(&testColumn{})

func (t *testColumn) TestColumnETCD(c *check.C) {
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
	rev1, putted, err := PutDroppedColumns(etcdTestCli, lockID, source1, upSchema1, upTable1, []string{"a"}, DropNotDone)
	c.Assert(err, check.IsNil)
	c.Assert(putted, check.IsTrue)
	rev2, putted, err := PutDroppedColumns(etcdTestCli, lockID, source1, upSchema1, upTable1, []string{"b"}, DropNotDone)
	c.Assert(err, check.IsNil)
	c.Assert(putted, check.IsTrue)
	c.Assert(rev2, check.Greater, rev1)
	rev3, putted, err := PutDroppedColumns(etcdTestCli, lockID, source1, upSchema2, upTable2, []string{"b"}, DropNotDone)
	c.Assert(err, check.IsNil)
	c.Assert(putted, check.IsTrue)
	c.Assert(rev3, check.Greater, rev2)
	rev4, putted, err := PutDroppedColumns(etcdTestCli, lockID, source2, upSchema1, upTable1, []string{"b"}, DropNotDone)
	c.Assert(err, check.IsNil)
	c.Assert(putted, check.IsTrue)
	c.Assert(rev4, check.Greater, rev3)
	rev5, putted, err := PutDroppedColumns(etcdTestCli, lockID, source2, upSchema1, upTable1, []string{"b", "c"}, DropNotDone)
	c.Assert(err, check.IsNil)
	c.Assert(putted, check.IsTrue)
	c.Assert(rev5, check.Greater, rev4)

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
			"c": {
				source2: {upSchema1: {upTable1: DropNotDone}},
			},
		},
	}
	colm, rev6, err := GetAllDroppedColumns(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(colm, check.DeepEquals, expectedColm)
	c.Assert(rev6, check.Equals, rev5)

	rev7, deleted, err := DeleteDroppedColumns(etcdTestCli, lockID, "b", "c")
	c.Assert(err, check.IsNil)
	c.Assert(deleted, check.IsTrue)
	c.Assert(rev7, check.Greater, rev6)

	delete(expectedColm[lockID], "b")
	delete(expectedColm[lockID], "c")
	colm, rev8, err := GetAllDroppedColumns(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(colm, check.DeepEquals, expectedColm)
	c.Assert(rev8, check.Equals, rev7)
}
