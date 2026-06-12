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

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (t *testForEtcd) TestOperationJSON() {
	o1 := NewOperation("test-ID", "test", "mysql-replica-1", []string{
		"ALTER TABLE bar ADD COLUMN c1 INT",
	}, true, false)

	j, err := o1.toJSON()
	t.Require().NoError(err)
	t.Require().Equal(`{"id":"test-ID","task":"test","source":"mysql-replica-1","ddls":["ALTER TABLE bar ADD COLUMN c1 INT"],"exec":true,"done":false}`, j)
	t.Require().Equal(o1.String(), j)

	o2, err := operationFromJSON(j)
	t.Require().NoError(err)
	t.Require().Equal(o1, o2)
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

func (t *testForEtcd) TestOperationEtcd() {
	defer clearTestInfoOperation(t.T())

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
	t.Require().NoError(err)
	t.Require().True(succ)
	rev2, succ, err := PutOperations(etcdTestCli, false, op11, op12)
	t.Require().NoError(err)
	t.Require().True(succ)
	t.Require().Greater(rev2, rev1)

	// start the watcher with the same revision as the last PUT for the specified task and source.
	ops, err := watchExactOperations(context.Background(), etcdTestCli, mvccpb.PUT, task1, source1, rev2, 1)
	t.Require().NoError(err)
	// watch should only get op11.
	t.Require().Equal(op11, ops[0])

	// put for another task.
	rev3, succ, err := PutOperations(etcdTestCli, false, op21)
	t.Require().NoError(err)
	t.Require().True(succ)

	// start the watch with an older revision for all tasks and sources.
	ops, err = watchExactOperations(context.Background(), etcdTestCli, mvccpb.PUT, "", "", rev2, 3)
	t.Require().NoError(err)
	// watch should get 3 operations.
	t.Require().Equal(op11, ops[0])
	t.Require().Equal(op12, ops[1])
	t.Require().Equal(op21, ops[2])

	// get all operations.
	opm, rev4, err := GetAllOperations(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Equal(rev3, rev4)
	t.Require().Len(opm, 2)
	t.Require().Contains(opm, task1)
	t.Require().Contains(opm, task2)
	t.Require().Len(opm[task1], 2)
	t.Require().Equal(op11, opm[task1][source1])
	t.Require().Equal(op12, opm[task1][source2])
	t.Require().Len(opm[task2], 1)
	t.Require().Equal(op21, opm[task2][source1])

	// put for `skipDone` with `done` in etcd, the operations should not be skipped.
	// case: all of kvs "the `done` field is not `true`".
	rev5, succ, err := PutOperations(etcdTestCli, true, op11, op12)
	t.Require().NoError(err)
	t.Require().True(succ)
	t.Require().Greater(rev5, rev4)

	// delete op11.
	rev6, err := DeleteOperations(etcdTestCli, op11)
	t.Require().NoError(err)
	t.Require().Greater(rev6, rev5)

	// start watch with an older revision for the deleted op11.
	ops, err = watchExactOperations(context.Background(), etcdTestCli, mvccpb.DELETE, op11.Task, op11.Source, rev5, 1)
	t.Require().NoError(err)
	// watch should got the previous deleted operation.
	op11d := ops[0]
	t.Require().True(op11d.IsDeleted)
	op11d.IsDeleted = false // reset to false
	t.Require().Equal(op11, op11d)

	// get again, op11 should be deleted.
	opm, _, err = GetAllOperations(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Len(opm[task1], 1)
	t.Require().Equal(op12, opm[task1][source2])

	// put for `skipDone` with `done` in etcd, the operations should not be skipped.
	// case: all of kvs "not exist".
	rev7, succ, err := PutOperations(etcdTestCli, true, op11, op13)
	t.Require().NoError(err)
	t.Require().True(succ)
	t.Require().Greater(rev7, rev6)

	// get again, op11 and op13 should be putted.
	opm, _, err = GetAllOperations(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Len(opm[task1], 3)
	t.Require().Equal(op11, opm[task1][source1])
	t.Require().Equal(op12, opm[task1][source2])
	t.Require().Equal(op13, opm[task1][source3])

	// update op12 to `done`.
	op12c := op12
	op12c.Done = true
	putOp, err := putOperationOp(op12c)
	t.Require().NoError(err)
	txnResp, err := etcdTestCli.Txn(context.Background()).Then(putOp).Commit()
	t.Require().NoError(err)

	// delete op13.
	rev8, err := DeleteOperations(etcdTestCli, op13)
	t.Require().NoError(err)
	t.Require().Greater(rev8, txnResp.Header.Revision)

	// put for `skipDone` with `done` in etcd, the operations should be skipped.
	// case: any of kvs ("exist" and "the `done` field is `true`").
	rev9, succ, err := PutOperations(etcdTestCli, true, op12, op13)
	t.Require().NoError(err)
	t.Require().False(succ)
	t.Require().Equal(rev8, rev9)

	// get again, op13 not putted.
	opm, _, err = GetAllOperations(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Len(opm[task1], 2)
	t.Require().Equal(op11, opm[task1][source1])
	t.Require().Equal(op12c, opm[task1][source2])

	// FIXME: the right result:
	//   the operations should *NOT* be skipped.
	// case:
	//   - some of kvs "exist" and "the `done` field is not `true`"
	//   - some of kvs "not exist"
	// after FIXED, this test case will fail and need to be updated.
	rev10, succ, err := PutOperations(etcdTestCli, true, op11, op13)
	t.Require().NoError(err)
	t.Require().False(succ)
	t.Require().Equal(rev9, rev10)
}
