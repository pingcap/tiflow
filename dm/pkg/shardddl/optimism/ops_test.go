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

package optimism

import (
	. "github.com/pingcap/check"
)

func (t *testForEtcd) TestDeleteInfosOperationsSchema(c *C) {
	defer clearTestInfoOperation(c)

	var (
		task       = "test"
		source     = "mysql-replica-1"
		upSchema   = "foo-1"
		upTable    = "bar-1"
		downSchema = "foo"
		downTable  = "bar"
		DDLs       = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		info       = NewInfo(task, source, upSchema, upTable, downSchema, downTable, DDLs, nil, nil)
		op         = NewOperation("test-ID", task, source, upSchema, upTable, DDLs, ConflictResolved, "", false, []string{})
		is         = NewInitSchema(task, downSchema, downTable, nil)
	)

	// put info.
	rev, err := PutInfo(etcdTestCli, info)
	c.Assert(err, IsNil)
	ifm, _, err := GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 1)
	infoWithVer := info
	infoWithVer.Version = 1
	infoWithVer.Revision = rev
	c.Assert(ifm[task][source][upSchema][upTable], DeepEquals, infoWithVer)

	// put operation.
	_, _, err = PutOperation(etcdTestCli, false, op, 0)
	c.Assert(err, IsNil)
	opm, _, err := GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(opm, HasLen, 1)
	c.Assert(opm[task][source][upSchema][upTable], DeepEquals, op)

	// put init schema.
	_, _, err = PutInitSchemaIfNotExist(etcdTestCli, is)
	c.Assert(err, IsNil)
	isc, _, err := GetInitSchema(etcdTestCli, is.Task, is.DownSchema, is.DownTable)
	c.Assert(err, IsNil)
	c.Assert(isc, DeepEquals, is)

	// DELETE info and operation with version 0
	_, deleted, err := DeleteInfosOperationsSchemaColumn(etcdTestCli, []Info{info}, []Operation{op}, is)
	c.Assert(err, IsNil)
	c.Assert(deleted, IsFalse)

	// data still exist
	ifm, _, err = GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 1)
	opm, _, err = GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(opm, HasLen, 1)
	isc, _, err = GetInitSchema(etcdTestCli, is.Task, is.DownSchema, is.DownTable)
	c.Assert(err, IsNil)
	c.Assert(isc.IsEmpty(), IsFalse)

	// DELETE info and operation with version 1
	_, deleted, err = DeleteInfosOperationsSchemaColumn(etcdTestCli, []Info{infoWithVer}, []Operation{op}, is)
	c.Assert(err, IsNil)
	c.Assert(deleted, IsTrue)

	// verify no info & operation exist.
	ifm, _, err = GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 0)
	opm, _, err = GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(opm, HasLen, 0)
	isc, _, err = GetInitSchema(etcdTestCli, is.Task, is.DownSchema, is.DownTable)
	c.Assert(err, IsNil)
	c.Assert(isc.IsEmpty(), IsTrue)
}

func (t *testForEtcd) TestSourceTablesInfo(c *C) {
	defer clearTestInfoOperation(c)

	var (
		task       = "task"
		source     = "mysql-replica-1"
		upSchema   = "foo-1"
		upTable    = "bar-1"
		downSchema = "foo"
		downTable  = "bar"
		st1        = NewSourceTables(task, source)
		st2        = NewSourceTables(task, source)
		i11        = NewInfo(task, source, upSchema, upTable, "foo", "bar",
			[]string{"ALTER TABLE bar ADD COLUMN c1 INT"}, nil, nil)
	)

	st1.AddTable("db", "tbl-1", downSchema, downTable)
	st1.AddTable("db", "tbl-2", downSchema, downTable)
	st2.AddTable("db", "tbl-2", downSchema, downTable)
	st2.AddTable("db", "tbl-3", downSchema, downTable)

	// put source tables and info.
	rev1, err := PutSourceTablesInfo(etcdTestCli, st1, i11)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, int64(0))

	stm, rev2, err := GetAllSourceTables(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev2, Equals, rev1)
	c.Assert(stm, HasLen, 1)
	c.Assert(stm[task], HasLen, 1)
	c.Assert(stm[task][source], DeepEquals, st1)

	ifm, rev3, err := GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev1)
	c.Assert(ifm, HasLen, 1)
	c.Assert(ifm[task], HasLen, 1)
	c.Assert(ifm[task][source], HasLen, 1)
	c.Assert(ifm[task][source][upSchema], HasLen, 1)
	i11WithVer := i11
	i11WithVer.Version = 1
	i11WithVer.Revision = rev3
	c.Assert(ifm[task][source][upSchema][upTable], DeepEquals, i11WithVer)

	// put/update source tables and delete info.
	rev4, err := PutSourceTablesDeleteInfo(etcdTestCli, st2, i11)
	c.Assert(err, IsNil)
	c.Assert(rev4, Greater, rev1)

	stm, rev5, err := GetAllSourceTables(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, rev4)
	c.Assert(stm, HasLen, 1)
	c.Assert(stm[task], HasLen, 1)
	c.Assert(stm[task][source], DeepEquals, st2)

	ifm, rev6, err := GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev6, Equals, rev4)
	c.Assert(ifm, HasLen, 0)
}
