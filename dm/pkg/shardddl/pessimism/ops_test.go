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

func (t *testForEtcd) TestPutOperationDeleteInfo() {
	defer clearTestInfoOperation(t.T())

	var (
		task   = "test"
		source = "mysql-replica-1"
		DDLs   = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		info   = NewInfo(task, source, "foo", "bar", DDLs)
		op     = NewOperation("test-ID", task, source, DDLs, true, false)
	)

	// put info.
	_, err := PutInfo(etcdTestCli, info)
	t.Require().NoError(err)

	// verify the info exists.
	ifm, _, err := GetAllInfo(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Len(ifm, 1)
	t.Require().Contains(ifm, task)
	t.Require().Equal(info, ifm[task][source])

	// verify no operations exist.
	opm, _, err := GetAllOperations(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Len(opm, 0)

	// put operation & delete info.
	done, _, err := PutOperationDeleteExistInfo(etcdTestCli, op, info)
	t.Require().NoError(err)
	t.Require().True(done)

	// verify no info exit.
	ifm, _, err = GetAllInfo(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Len(ifm, 0)

	// verify the operation exists.
	opm, _, err = GetAllOperations(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Len(opm, 1)
	t.Require().Contains(opm, task)
	t.Require().Equal(op, opm[task][source])

	// try to put operation & delete info again, succeed(to support reentrant).
	done, _, err = PutOperationDeleteExistInfo(etcdTestCli, op, info)
	t.Require().NoError(err)
	t.Require().True(done)

	// PUT info and operation.
	_, err = PutInfo(etcdTestCli, info)
	t.Require().NoError(err)
	_, _, err = PutOperations(etcdTestCli, true, op)
	t.Require().NoError(err)

	// verify the info exists.
	ifm, _, err = GetAllInfo(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Len(ifm, 1)
	t.Require().Contains(ifm, task)
	t.Require().Equal(info, ifm[task][source])

	// verify the operation exists.
	opm, _, err = GetAllOperations(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Len(opm, 1)
	t.Require().Contains(opm, task)
	t.Require().Equal(op, opm[task][source])

	// DELETE info and operation.
	_, err = DeleteInfosOperations(etcdTestCli, []Info{info}, []Operation{op})
	t.Require().NoError(err)

	// verify no info exit.
	ifm, _, err = GetAllInfo(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Len(ifm, 0)

	// verify no operations exist.
	opm, _, err = GetAllOperations(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Len(opm, 0)

	// put a done operation into etcd and try to delete operation again.
	op.Done = true
	_, _, err = PutOperations(etcdTestCli, true, op)
	op.Done = false
	t.Require().NoError(err)

	// try to put operation & delete info again, fail(operation not equal).
	done, _, err = PutOperationDeleteExistInfo(etcdTestCli, op, info)
	t.Require().NoError(err)
	t.Require().False(done)

	// DELETE info and operation.
	_, err = DeleteInfosOperations(etcdTestCli, []Info{info}, []Operation{op})
	t.Require().NoError(err)
}
