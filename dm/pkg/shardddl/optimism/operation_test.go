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

func (t *testForEtcd) TestOperationJSON(c *C) {
	o1 := NewOperation("test-ID", "test", "mysql-replica-1", "db-1", "tbl-1", []string{
		"ALTER TABLE tbl ADD COLUMN c1 INT",
	}, ConflictDetected, "conflict", true, []string{})

	j, err := o1.toJSON()
	c.Assert(err, IsNil)
	c.Assert(j, Equals, `{"id":"test-ID","task":"test","source":"mysql-replica-1","up-schema":"db-1","up-table":"tbl-1","ddls":["ALTER TABLE tbl ADD COLUMN c1 INT"],"conflict-stage":"detected","conflict-message":"conflict","done":true,"cols":[]}`)
	c.Assert(j, Equals, o1.String())

	o2, err := operationFromJSON(j)
	c.Assert(err, IsNil)
	c.Assert(o2, DeepEquals, o1)
}

func (t *testForEtcd) TestOperationEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		watchTimeout = 2 * time.Second
		task1        = "test1"
		task2        = "test2"
		upSchema     = "foo_1"
		upTable      = "bar_1"
		ID1          = "test1-`foo`.`bar`"
		ID2          = "test2-`foo`.`bar`"
		source1      = "mysql-replica-1"
		DDLs         = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		op11         = NewOperation(ID1, task1, source1, upSchema, upTable, DDLs, ConflictNone, "", false, []string{})
		op21         = NewOperation(ID2, task2, source1, upSchema, upTable, DDLs, ConflictResolved, "", true, []string{})
	)

	// put the same keys twice.
	rev1, succ, err := PutOperation(etcdTestCli, false, op11, 0)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)
	rev2, succ, err := PutOperation(etcdTestCli, false, op11, 0)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)
	c.Assert(rev2, Greater, rev1)

	// start the watcher with the same revision as the last PUT for the specified task and source.
	wch := make(chan Operation, 10)
	ech := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchOperationPut(ctx, etcdTestCli, task1, source1, upSchema, upTable, rev2, wch, ech)
	cancel()
	close(wch)
	close(ech)

	// watch should only get op11.
	c.Assert(len(ech), Equals, 0)
	c.Assert(len(wch), Equals, 1)
	op11.Revision = rev2
	c.Assert(<-wch, DeepEquals, op11)

	// put for another task.
	rev3, succ, err := PutOperation(etcdTestCli, false, op21, 0)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)

	// start the watch with an older revision for all tasks and sources.
	wch = make(chan Operation, 10)
	ech = make(chan error, 10)
	ctx, cancel = context.WithTimeout(context.Background(), watchTimeout)
	WatchOperationPut(ctx, etcdTestCli, "", "", "", "", rev2, wch, ech)
	cancel()
	close(wch)
	close(ech)

	// watch should get 2 operations.
	c.Assert(len(ech), Equals, 0)
	c.Assert(len(wch), Equals, 2)
	c.Assert(<-wch, DeepEquals, op11)
	op21.Revision = rev3
	c.Assert(<-wch, DeepEquals, op21)

	// get all operations.
	opm, rev4, err := GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev4, Equals, rev3)
	c.Assert(opm, HasLen, 2)
	c.Assert(opm, HasKey, task1)
	c.Assert(opm, HasKey, task2)
	c.Assert(opm[task1], HasLen, 1)
	op11.Revision = rev2
	c.Assert(opm[task1][source1][upSchema][upTable], DeepEquals, op11)
	c.Assert(opm[task2], HasLen, 1)
	op21.Revision = rev3
	c.Assert(opm[task2][source1][upSchema][upTable], DeepEquals, op21)

	// put for `skipDone` with `done` in etcd, the operations should not be skipped.
	// case: kv's "the `done` field is not `true`".
	rev5, succ, err := PutOperation(etcdTestCli, true, op11, 0)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)
	c.Assert(rev5, Greater, rev4)

	// delete op11.
	deleteOp := deleteOperationOp(op11)
	_, err = etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	c.Assert(err, IsNil)

	// get again, op11 should be deleted.
	opm, _, err = GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(opm[task1], HasLen, 0)

	// put for `skipDone` with `done` in etcd, the operations should not be skipped.
	// case: kv "not exist".
	rev6, succ, err := PutOperation(etcdTestCli, true, op11, 0)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)

	// get again, op11 should be putted.
	opm, _, err = GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(opm[task1], HasLen, 1)
	op11.Revision = rev6
	c.Assert(opm[task1][source1][upSchema][upTable], DeepEquals, op11)

	// update op11 to `done`.
	op11c := op11
	op11c.Done = true
	rev7, succ, err := PutOperation(etcdTestCli, true, op11c, 0)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)
	c.Assert(rev7, Greater, rev6)

	// put for `skipDone` with `done` in etcd, the operations should not be skipped.
	// case: operation modRevision < info's modRevision
	rev8, succ, err := PutOperation(etcdTestCli, true, op11c, rev7+10)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)
	c.Assert(rev8, Greater, rev7)

	// put for `skipDone` with `done` in etcd, the operations should be skipped.
	// case: kv's ("exist" and "the `done` field is `true`").
	rev9, succ, err := PutOperation(etcdTestCli, true, op11, rev6)
	c.Assert(err, IsNil)
	c.Assert(succ, IsFalse)
	c.Assert(rev9, Equals, rev8)
}
