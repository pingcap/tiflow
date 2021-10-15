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
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/util/mock"
)

func (t *testForEtcd) TestInitSchemaJSON(c *C) {
	is1 := NewInitSchema("test", "foo", "bar", nil)
	j, err := is1.toJSON()
	c.Assert(err, IsNil)
	c.Assert(j, Equals, `{"task":"test","down-schema":"foo","down-table":"bar","table-info":null}`)
	c.Assert(j, Equals, is1.String())

	is2, err := initSchemaFromJSON(j)
	c.Assert(err, IsNil)
	c.Assert(is2, DeepEquals, is1)
}

func (t *testForEtcd) TestInitSchemaEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		task             = "test-init-schema-etcd"
		downSchema       = "foo"
		downTable        = "bar"
		downTable2       = "bar2"
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		tblI1            = createTableInfo(c, p, se, tblID, "CREATE TABLE bar (id INT PRIMARY KEY)")
		tblI2            = createTableInfo(c, p, se, tblID, "CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)")
		tblI3            = createTableInfo(c, p, se, tblID, "CREATE TABLE bar2 (id INT PRIMARY KEY, c INT)")
		is1              = NewInitSchema(task, downSchema, downTable, tblI1)
		is2              = NewInitSchema(task, downSchema, downTable, tblI2)
		is3              = NewInitSchema(task, downSchema, downTable2, tblI3)
	)

	// try to get, but no one exists.
	isc, rev0, err := GetInitSchema(etcdTestCli, task, downSchema, downTable)
	c.Assert(err, IsNil)
	c.Assert(rev0, Greater, int64(0))
	c.Assert(isc.IsEmpty(), IsTrue)

	// put the init schema.
	rev1, putted, err := PutInitSchemaIfNotExist(etcdTestCli, is1)
	c.Assert(err, IsNil)
	c.Assert(rev1, Greater, rev0)
	c.Assert(putted, IsTrue)

	// get it back.
	isc, rev2, err := GetInitSchema(etcdTestCli, task, downSchema, downTable)
	c.Assert(err, IsNil)
	c.Assert(rev2, Equals, rev1)
	c.Assert(isc, DeepEquals, is1)

	// can't put again if a previous one exist.
	rev3, putted, err := PutInitSchemaIfNotExist(etcdTestCli, is1)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev1)
	c.Assert(putted, IsFalse)
	rev3, putted, err = PutInitSchemaIfNotExist(etcdTestCli, is2)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev1)
	c.Assert(putted, IsFalse)

	// put new init schema with different downstream info.
	rev4, putted, err := PutInitSchemaIfNotExist(etcdTestCli, is3)
	c.Assert(err, IsNil)
	c.Assert(rev4, Greater, rev3)
	c.Assert(putted, IsTrue)

	// get all init schemas.
	initSchemas, rev5, err := GetAllInitSchemas(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, rev4)
	c.Assert(initSchemas[is1.Task][is1.DownSchema][is1.DownTable], DeepEquals, is1)
	c.Assert(initSchemas[is3.Task][is3.DownSchema][is3.DownTable], DeepEquals, is3)

	// delete the schema.
	rev6, deleted, err := DeleteInitSchema(etcdTestCli, is1.Task, is1.DownSchema, is1.DownTable)
	c.Assert(err, IsNil)
	c.Assert(rev6, Greater, rev5)
	c.Assert(deleted, IsTrue)
	rev7, deleted, err := DeleteInitSchema(etcdTestCli, is3.Task, is3.DownSchema, is3.DownTable)
	c.Assert(err, IsNil)
	c.Assert(rev7, Greater, rev6)
	c.Assert(deleted, IsTrue)

	// not exist now.
	initSchemas, rev8, err := GetAllInitSchemas(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev8, Equals, rev7)
	c.Assert(initSchemas, HasLen, 0)
}
