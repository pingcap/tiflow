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
	"context"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (t *testForEtcd) TestOperationJSON(c *C) {
	o1 := NewOperation("test-ID", "test", "mysql-replica-1", []string{
		"ALTER TABLE bar ADD COLUMN c1 INT",
	}, true, false)

	j, err := o1.toJSON()
	c.Assert(err, IsNil)
	c.Assert(j, Equals, `{"id":"test-ID","task":"test","source":"mysql-replica-1","ddls":["ALTER TABLE bar ADD COLUMN c1 INT"],"exec":true,"done":false}`)
	c.Assert(j, Equals, o1.String())

	o2, err := operationFromJSON(j)
	c.Assert(err, IsNil)
	c.Assert(o2, DeepEquals, o1)
}

func watchExactOperations(
	ctx context.Context, cli *clientv3.Client, watchTp mvccpb.Event_EventType,
	task, source string, revision int64, opCnt int,
) ([]Operation, error) {
	opCh := make(chan Operation, 10)
	errCh := make(chan error, 10)
	done := make(chan struct{})
	subCtx, cancel := context.WithCancel(ctx)
	go func() {
		watchOperation(subCtx, cli, watchTp, task, source, revision, opCh, errCh)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()
	var ops []Operation
	for i := 0; i < opCnt; i++ {
		select {
		case op := <-opCh:
			ops = append(ops, op)
		case err := <-errCh:
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	// Wait 100ms to check if there is unexpected operation.
	select {
	case op := <-opCh:
		return nil, fmt.Errorf("unpexecped operation %s", op.String())
	case <-time.After(time.Millisecond * 100):
	}
	return ops, nil
}

func (t *testForEtcd) TestOperationEtcd(c *C) {
	defer clearTestInfoOperation(c)

	var (
		task1   = "test1"
		task2   = "test2"
		ID1     = "test1-`foo`.`bar`"
		ID2     = "test2-`foo`.`bar`"
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		source3 = "mysql-replica-3"
		DDLs    = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		op11    = NewOperation(ID1, task1, source1, DDLs, true, false)
		op12    = NewOperation(ID1, task1, source2, DDLs, true, false)
		op13    = NewOperation(ID1, task1, source3, DDLs, true, false)
		op21    = NewOperation(ID2, task2, source1, DDLs, false, true)
	)

	// put the same keys twice.
	rev1, succ, err := PutOperations(etcdTestCli, false, op11, op12)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)
	rev2, succ, err := PutOperations(etcdTestCli, false, op11, op12)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)
	c.Assert(rev2, Greater, rev1)

	// start the watcher with the same revision as the last PUT for the specified task and source.
	ops, err := watchExactOperations(context.Background(), etcdTestCli, mvccpb.PUT, task1, source1, rev2, 1)
	c.Assert(err, IsNil)
	// watch should only get op11.
	c.Assert(ops[0], DeepEquals, op11)

	// put for another task.
	rev3, succ, err := PutOperations(etcdTestCli, false, op21)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)

	// start the watch with an older revision for all tasks and sources.
	ops, err = watchExactOperations(context.Background(), etcdTestCli, mvccpb.PUT, "", "", rev2, 3)
	c.Assert(err, IsNil)
	// watch should get 3 operations.
	c.Assert(ops[0], DeepEquals, op11)
	c.Assert(ops[1], DeepEquals, op12)
	c.Assert(ops[2], DeepEquals, op21)

	// get all operations.
	opm, rev4, err := GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(rev4, Equals, rev3)
	c.Assert(opm, HasLen, 2)
	c.Assert(opm, HasKey, task1)
	c.Assert(opm, HasKey, task2)
	c.Assert(opm[task1], HasLen, 2)
	c.Assert(opm[task1][source1], DeepEquals, op11)
	c.Assert(opm[task1][source2], DeepEquals, op12)
	c.Assert(opm[task2], HasLen, 1)
	c.Assert(opm[task2][source1], DeepEquals, op21)

	// put for `skipDone` with `done` in etcd, the operations should not be skipped.
	// case: all of kvs "the `done` field is not `true`".
	rev5, succ, err := PutOperations(etcdTestCli, true, op11, op12)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)
	c.Assert(rev5, Greater, rev4)

	// delete op11.
	rev6, err := DeleteOperations(etcdTestCli, op11)
	c.Assert(err, IsNil)
	c.Assert(rev6, Greater, rev5)

	// start watch with an older revision for the deleted op11.
	ops, err = watchExactOperations(context.Background(), etcdTestCli, mvccpb.DELETE, op11.Task, op11.Source, rev5, 1)
	c.Assert(err, IsNil)
	// watch should got the previous deleted operation.
	op11d := ops[0]
	c.Assert(op11d.IsDeleted, IsTrue)
	op11d.IsDeleted = false // reset to false
	c.Assert(op11d, DeepEquals, op11)

	// get again, op11 should be deleted.
	opm, _, err = GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(opm[task1], HasLen, 1)
	c.Assert(opm[task1][source2], DeepEquals, op12)

	// put for `skipDone` with `done` in etcd, the operations should not be skipped.
	// case: all of kvs "not exist".
	rev7, succ, err := PutOperations(etcdTestCli, true, op11, op13)
	c.Assert(err, IsNil)
	c.Assert(succ, IsTrue)
	c.Assert(rev7, Greater, rev6)

	// get again, op11 and op13 should be putted.
	opm, _, err = GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(opm[task1], HasLen, 3)
	c.Assert(opm[task1][source1], DeepEquals, op11)
	c.Assert(opm[task1][source2], DeepEquals, op12)
	c.Assert(opm[task1][source3], DeepEquals, op13)

	// update op12 to `done`.
	op12c := op12
	op12c.Done = true
	putOp, err := putOperationOp(op12c)
	c.Assert(err, IsNil)
	txnResp, err := etcdTestCli.Txn(context.Background()).Then(putOp).Commit()
	c.Assert(err, IsNil)

	// delete op13.
	rev8, err := DeleteOperations(etcdTestCli, op13)
	c.Assert(err, IsNil)
	c.Assert(rev8, Greater, txnResp.Header.Revision)

	// put for `skipDone` with `done` in etcd, the operations should be skipped.
	// case: any of kvs ("exist" and "the `done` field is `true`").
	rev9, succ, err := PutOperations(etcdTestCli, true, op12, op13)
	c.Assert(err, IsNil)
	c.Assert(succ, IsFalse)
	c.Assert(rev9, Equals, rev8)

	// get again, op13 not putted.
	opm, _, err = GetAllOperations(etcdTestCli)
	c.Assert(err, IsNil)
	c.Assert(opm[task1], HasLen, 2)
	c.Assert(opm[task1][source1], DeepEquals, op11)
	c.Assert(opm[task1][source2], DeepEquals, op12c)

	// FIXME: the right result:
	//   the operations should *NOT* be skipped.
	// case:
	//   - some of kvs "exist" and "the `done` field is not `true`"
	//   - some of kvs "not exist"
	// after FIXED, this test case will fail and need to be updated.
	rev10, succ, err := PutOperations(etcdTestCli, true, op11, op13)
	c.Assert(err, IsNil)
	c.Assert(succ, IsFalse)
	c.Assert(rev10, Equals, rev9)
}
