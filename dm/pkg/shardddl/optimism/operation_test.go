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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOperationJSON(t *testing.T) {
	o1 := NewOperation("test-ID", "test", "mysql-replica-1", "db-1", "tbl-1", []string{
		"ALTER TABLE tbl ADD COLUMN c1 INT",
	}, ConflictDetected, "conflict", true, []string{})

	j, err := o1.toJSON()
	require.NoError(t, err)
	require.Equal(t, `{"id":"test-ID","task":"test","source":"mysql-replica-1","up-schema":"db-1","up-table":"tbl-1","ddls":["ALTER TABLE tbl ADD COLUMN c1 INT"],"conflict-stage":"detected","conflict-message":"conflict","done":true,"cols":[]}`, j)
	require.Equal(t, o1.String(), j)

	o2, err := operationFromJSON(j)
	require.NoError(t, err)
	require.Equal(t, o1, o2)
}

func TestOperationEtcd(t *testing.T) {
	defer clearTestInfoOperation(t)

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
	require.NoError(t, err)
	require.True(t, succ)
	rev2, succ, err := PutOperation(etcdTestCli, false, op11, 0)
	require.NoError(t, err)
	require.True(t, succ)
	require.Greater(t, rev2, rev1)

	// start the watcher with the same revision as the last PUT for the specified task and source.
	wch := make(chan Operation, 10)
	ech := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchOperationPut(ctx, etcdTestCli, task1, source1, upSchema, upTable, rev2, wch, ech)
	cancel()
	close(wch)
	close(ech)

	// watch should only get op11.
	require.Equal(t, 0, len(ech))
	require.Equal(t, 1, len(wch))
	op11.Revision = rev2
	require.Equal(t, op11, <-wch)

	// put for another task.
	rev3, succ, err := PutOperation(etcdTestCli, false, op21, 0)
	require.NoError(t, err)
	require.True(t, succ)

	// start the watch with an older revision for all tasks and sources.
	wch = make(chan Operation, 10)
	ech = make(chan error, 10)
	ctx, cancel = context.WithTimeout(context.Background(), watchTimeout)
	WatchOperationPut(ctx, etcdTestCli, "", "", "", "", rev2, wch, ech)
	cancel()
	close(wch)
	close(ech)

	// watch should get 2 operations.
	require.Equal(t, 0, len(ech))
	require.Equal(t, 2, len(wch))
	require.Equal(t, op11, <-wch)
	op21.Revision = rev3
	require.Equal(t, op21, <-wch)

	// get all operations.
	opm, rev4, err := GetAllOperations(etcdTestCli)
	require.NoError(t, err)
	require.Equal(t, rev3, rev4)
	require.Len(t, opm, 2)
	require.Contains(t, opm, task1)
	require.Contains(t, opm, task2)
	require.Len(t, opm[task1], 1)
	op11.Revision = rev2
	require.Equal(t, op11, opm[task1][source1][upSchema][upTable])
	require.Len(t, opm[task2], 1)
	op21.Revision = rev3
	require.Equal(t, op21, opm[task2][source1][upSchema][upTable])

	// put for `skipDone` with `done` in etcd, the operations should not be skipped.
	// case: kv's "the `done` field is not `true`".
	rev5, succ, err := PutOperation(etcdTestCli, true, op11, 0)
	require.NoError(t, err)
	require.True(t, succ)
	require.Greater(t, rev5, rev4)

	// delete op11.
	deleteOp := deleteOperationOp(op11)
	_, err = etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	require.NoError(t, err)

	// get again, op11 should be deleted.
	opm, _, err = GetAllOperations(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, opm[task1], 0)

	// put for `skipDone` with `done` in etcd, the operations should not be skipped.
	// case: kv "not exist".
	rev6, succ, err := PutOperation(etcdTestCli, true, op11, 0)
	require.NoError(t, err)
	require.True(t, succ)

	// get again, op11 should be putted.
	opm, _, err = GetAllOperations(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, opm[task1], 1)
	op11.Revision = rev6
	require.Equal(t, op11, opm[task1][source1][upSchema][upTable])

	// update op11 to `done`.
	op11c := op11
	op11c.Done = true
	rev7, succ, err := PutOperation(etcdTestCli, true, op11c, 0)
	require.NoError(t, err)
	require.True(t, succ)
	require.Greater(t, rev7, rev6)

	// put for `skipDone` with `done` in etcd, the operations should not be skipped.
	// case: operation modRevision < info's modRevision
	rev8, succ, err := PutOperation(etcdTestCli, true, op11c, rev7+10)
	require.NoError(t, err)
	require.True(t, succ)
	require.Greater(t, rev8, rev7)

	// put for `skipDone` with `done` in etcd, the operations should be skipped.
	// case: kv's ("exist" and "the `done` field is `true`").
	rev9, succ, err := PutOperation(etcdTestCli, true, op11, rev6)
	require.NoError(t, err)
	require.False(t, succ)
	require.Equal(t, rev8, rev9)
}
