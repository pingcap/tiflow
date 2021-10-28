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
	"context"
	"time"

	. "github.com/pingcap/check"
)

func (t *testForEtcd) TestSourceTablesJSON(c *C) {
	st1 := NewSourceTables("test", "mysql-replica-1")
	st1.AddTable("db1", "tbl1", "db", "tbl")
	j, err := st1.toJSON()
	c.Assert(err, IsNil)
	c.Assert(j, Equals, `{"task":"test","source":"mysql-replica-1","tables":{"db":{"tbl":{"db1":{"tbl1":{}}}}}}`)
	c.Assert(j, Equals, st1.String())

	st2, err := sourceTablesFromJSON(j)
	c.Assert(err, IsNil)
	c.Assert(st2, DeepEquals, st1)
}

func (t *testForEtcd) TestSourceTablesAddRemove(c *C) {
	var (
		task       = "task"
		source     = "mysql-replica-1"
		downSchema = "foo"
		downTable1 = "bar1"
		downTable2 = "bar2"
		st         = NewSourceTables(task, source)
	)

	// no target table exist.
	c.Assert(st.TargetTable(downSchema, downTable1).IsEmpty(), IsTrue)
	c.Assert(st.TargetTable(downSchema, downTable2).IsEmpty(), IsTrue)

	// add a table for downTable1.
	c.Assert(st.AddTable("foo1", "bar1", downSchema, downTable1), IsTrue)
	c.Assert(st.AddTable("foo1", "bar1", downSchema, downTable1), IsFalse)
	c.Assert(st.TargetTable(downSchema, downTable1), DeepEquals,
		newTargetTable(task, source, downSchema, downTable1, map[string]map[string]struct{}{
			"foo1": {"bar1": struct{}{}},
		}))
	c.Assert(st.TargetTable(downSchema, downTable2).IsEmpty(), IsTrue)

	// add a table for downTable2.
	c.Assert(st.AddTable("foo2", "bar2", downSchema, downTable2), IsTrue)
	c.Assert(st.AddTable("foo2", "bar2", downSchema, downTable2), IsFalse)
	c.Assert(st.TargetTable(downSchema, downTable2), DeepEquals,
		newTargetTable(task, source, downSchema, downTable2, map[string]map[string]struct{}{
			"foo2": {"bar2": struct{}{}},
		}))

	// remove a table for downTable1.
	c.Assert(st.RemoveTable("foo1", "bar1", downSchema, downTable1), IsTrue)
	c.Assert(st.RemoveTable("foo1", "bar1", downSchema, downTable1), IsFalse)
	c.Assert(st.TargetTable(downSchema, downTable1).IsEmpty(), IsTrue)

	// remove a table for downTable2.
	c.Assert(st.RemoveTable("foo2", "bar2", downSchema, downTable2), IsTrue)
	c.Assert(st.RemoveTable("foo2", "bar2", downSchema, downTable2), IsFalse)
	c.Assert(st.TargetTable(downSchema, downTable2).IsEmpty(), IsTrue)
}

func (t *testForEtcd) TestSourceTablesEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		watchTimeout = 2 * time.Second
		task         = "task"
		source1      = "mysql-replica-1"
		source2      = "mysql-replica-2"
		downSchema   = "db"
		downTable    = "tbl"
		st1          = NewSourceTables(task, source1)
		st2          = NewSourceTables(task, source2)
	)

	st1.AddTable("db", "tbl-1", downSchema, downTable)
	st1.AddTable("db", "tbl-2", downSchema, downTable)
	st2.AddTable("db", "tbl-1", downSchema, downTable)
	st2.AddTable("db", "tbl-2", downSchema, downTable)

	// put two SourceTables.
	rev1, err := PutSourceTables(etcdTestCli, st1)
	c.Assert(err, IsNil)
	rev2, err := PutSourceTables(etcdTestCli, st2)
	c.Assert(err, IsNil)
	c.Assert(rev2, Greater, rev1)

	// get with two SourceTables.
	stm, rev3, err := GetAllSourceTables(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev3, Equals, rev2)
	c.Assert(stm, HasLen, 1)
	c.Assert(stm[task], HasLen, 2)
	c.Assert(stm[task][source1], DeepEquals, st1)
	c.Assert(stm[task][source2], DeepEquals, st2)

	// watch with an older revision for all SourceTables.
	wch := make(chan SourceTables, 10)
	ech := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchSourceTables(ctx, etcdTestCli, rev1, wch, ech)
	cancel()
	close(wch)
	close(ech)

	// get two source tables.
	c.Assert(len(wch), Equals, 2)
	c.Assert(<-wch, DeepEquals, st1)
	c.Assert(<-wch, DeepEquals, st2)
	c.Assert(len(ech), Equals, 0)

	// delete tow sources tables.
	_, err = DeleteSourceTables(etcdTestCli, st1)
	c.Assert(err, IsNil)
	rev4, err := DeleteSourceTables(etcdTestCli, st2)
	c.Assert(err, IsNil)

	// get without SourceTables.
	stm, rev5, err := GetAllSourceTables(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev5, Equals, rev4)
	c.Assert(stm, HasLen, 0)

	// watch the deletion for SourceTables.
	wch = make(chan SourceTables, 10)
	ech = make(chan error, 10)
	ctx, cancel = context.WithTimeout(context.Background(), watchTimeout)
	WatchSourceTables(ctx, etcdTestCli, rev4, wch, ech)
	cancel()
	close(wch)
	close(ech)
	c.Assert(len(wch), Equals, 1)
	std := <-wch
	c.Assert(std.IsDeleted, IsTrue)
	c.Assert(std.Task, Equals, st2.Task)
	c.Assert(std.Source, Equals, st2.Source)
	c.Assert(len(ech), Equals, 0)
}
