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

package pessimism

import (
	"github.com/pingcap/check"
)

func (t *testForEtcd) TestPutOperationDeleteInfo(c *check.C) {
	defer clearTestInfoOperation(c)

	var (
		task   = "test"
		source = "mysql-replica-1"
		DDLs   = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		info   = NewInfo(task, source, "foo", "bar", DDLs)
		op     = NewOperation("test-ID", task, source, DDLs, true, false)
	)

	// put info.
	_, err := PutInfo(etcdTestCli, info)
	c.Assert(err, check.IsNil)

	// verify the info exists.
	ifm, _, err := GetAllInfo(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(ifm, check.HasLen, 1)
	c.Assert(ifm, check.HasKey, task)
	c.Assert(ifm[task][source], check.DeepEquals, info)

	// verify no operations exist.
	opm, _, err := GetAllOperations(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(opm, check.HasLen, 0)

	// put operation & delete info.
	done, _, err := PutOperationDeleteExistInfo(etcdTestCli, op, info)
	c.Assert(err, check.IsNil)
	c.Assert(done, check.IsTrue)

	// verify no info exit.
	ifm, _, err = GetAllInfo(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(ifm, check.HasLen, 0)

	// verify the operation exists.
	opm, _, err = GetAllOperations(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(opm, check.HasLen, 1)
	c.Assert(opm, check.HasKey, task)
	c.Assert(opm[task][source], check.DeepEquals, op)

	// try to put operation & delete info again, succeed(to support reentrant).
	done, _, err = PutOperationDeleteExistInfo(etcdTestCli, op, info)
	c.Assert(err, check.IsNil)
	c.Assert(done, check.IsTrue)

	// PUT info and operation.
	_, err = PutInfo(etcdTestCli, info)
	c.Assert(err, check.IsNil)
	_, _, err = PutOperations(etcdTestCli, true, op)
	c.Assert(err, check.IsNil)

	// verify the info exists.
	ifm, _, err = GetAllInfo(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(ifm, check.HasLen, 1)
	c.Assert(ifm, check.HasKey, task)
	c.Assert(ifm[task][source], check.DeepEquals, info)

	// verify the operation exists.
	opm, _, err = GetAllOperations(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(opm, check.HasLen, 1)
	c.Assert(opm, check.HasKey, task)
	c.Assert(opm[task][source], check.DeepEquals, op)

	// DELETE info and operation.
	_, err = DeleteInfosOperations(etcdTestCli, []Info{info}, []Operation{op})
	c.Assert(err, check.IsNil)

	// verify no info exit.
	ifm, _, err = GetAllInfo(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(ifm, check.HasLen, 0)

	// verify no operations exist.
	opm, _, err = GetAllOperations(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(opm, check.HasLen, 0)

	// put a done operation into etcd and try to delete operation again.
	op.Done = true
	_, _, err = PutOperations(etcdTestCli, true, op)
	op.Done = false
	c.Assert(err, check.IsNil)

	// try to put operation & delete info again, fail(operation not equal).
	done, _, err = PutOperationDeleteExistInfo(etcdTestCli, op, info)
	c.Assert(err, check.IsNil)
	c.Assert(done, check.IsFalse)

	// DELETE info and operation.
	_, err = DeleteInfosOperations(etcdTestCli, []Info{info}, []Operation{op})
	c.Assert(err, check.IsNil)
}
