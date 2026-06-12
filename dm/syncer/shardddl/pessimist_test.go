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

package shardddl

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/pessimism"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

var etcdTestCli *clientv3.Client

func TestShardDDL(t *testing.T) {
	err := log.InitLogger(&log.Config{})
	if err != nil {
		t.Fatal(err)
	}

	integration.BeforeTestExternal(t)
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	t.Run("Pessimist", testPessimist)
	t.Run("Optimist", testOptimist)
}

// clear keys in etcd test cluster.
func clearTestInfoOperation(t *testing.T) {
	clearInfo := clientv3.OpDelete(common.ShardDDLPessimismInfoKeyAdapter.Path(), clientv3.WithPrefix())
	clearOp := clientv3.OpDelete(common.ShardDDLPessimismOperationKeyAdapter.Path(), clientv3.WithPrefix())
	_, err := etcdTestCli.Txn(context.Background()).Then(clearInfo, clearOp).Commit()
	require.NoError(t, err)
}

func testPessimist(t *testing.T) {
	defer clearTestInfoOperation(t)

	var (
		task          = "task"
		source        = "mysql-replicate-1"
		schema, table = "foo", "bar"
		DDLs          = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ID            = "task-`foo`.`bar`"
		op            = pessimism.NewOperation(ID, task, source, DDLs, true, false)

		logger = log.L()
		p      = NewPessimist(&logger, etcdTestCli, task, source)
		info   = p.ConstructInfo(schema, table, DDLs)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// no info and operation in pending
	require.Nil(t, p.PendingInfo())
	require.Nil(t, p.PendingOperation())

	// put shard DDL info.
	rev1, err := p.PutInfo(ctx, info)
	require.NoError(t, err)
	require.Greater(t, rev1, int64(0))

	// have info in pending
	info2 := p.PendingInfo()
	require.NotNil(t, info2)
	require.Equal(t, info, *info2)

	// put the lock operation.
	rev2, putted, err := pessimism.PutOperations(etcdTestCli, false, op)
	require.NoError(t, err)
	require.Greater(t, rev2, rev1)
	require.True(t, putted)

	// wait for the lock operation.
	op2, err := p.GetOperation(ctx, info, rev1)
	require.NoError(t, err)
	require.Equal(t, op, op2)

	// have operation in pending.
	op3 := p.PendingOperation()
	require.NotNil(t, op3)
	require.Equal(t, op, *op3)

	// mark the operation as done and delete the info.
	require.NoError(t, p.DoneOperationDeleteInfo(op, info))
	// make this op reentrant.
	require.NoError(t, p.DoneOperationDeleteInfo(op, info))

	// verify the operation and info.
	opc := op2
	opc.Done = true
	opm, _, err := pessimism.GetAllOperations(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, opm, 1)
	require.Len(t, opm[task], 1)
	require.Equal(t, opc, opm[task][source])
	ifm, _, err := pessimism.GetAllInfo(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, ifm, 0)

	// no info and operation in pending now.
	require.Nil(t, p.PendingInfo())
	require.Nil(t, p.PendingOperation())

	// try to put info again, but timeout because a `done` operation exist in etcd.
	ctx2, cancel2 := context.WithTimeout(ctx, time.Second)
	defer cancel2()
	_, err = p.PutInfo(ctx2, info)
	require.Equal(t, context.DeadlineExceeded, err)

	// start a goroutine to delete the `done` operation in background, then we can put info again.
	go func() {
		time.Sleep(500 * time.Millisecond) // wait `PutInfo` to start watch the deletion of the operation.
		_, err2 := pessimism.DeleteOperations(etcdTestCli, op)
		require.NoError(t, err2)
	}()

	// put info again, but do not complete the flow.
	_, err = p.PutInfo(ctx, info)
	require.NoError(t, err)
	require.NotNil(t, p.PendingInfo())

	// put the lock operation again.
	rev3, _, err := pessimism.PutOperations(etcdTestCli, false, op)
	require.NoError(t, err)
	// wait for the lock operation.
	_, err = p.GetOperation(ctx, info, rev3)
	require.NoError(t, err)
	require.NotNil(t, p.PendingOperation())

	// reset the pessimist.
	p.Reset()
	require.Nil(t, p.PendingInfo())
	require.Nil(t, p.PendingOperation())
}
