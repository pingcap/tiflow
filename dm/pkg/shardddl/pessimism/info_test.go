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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

var etcdTestCli *clientv3.Client

func TestInfo(t *testing.T) {
	integration.BeforeTestExternal(t)
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	suite.Run(t, new(testForEtcd))
}

// clear keys in etcd test cluster.
func clearTestInfoOperation(t require.TestingT) {
	clearInfo := clientv3.OpDelete(common.ShardDDLPessimismInfoKeyAdapter.Path(), clientv3.WithPrefix())
	clearOp := clientv3.OpDelete(common.ShardDDLPessimismOperationKeyAdapter.Path(), clientv3.WithPrefix())
	_, err := etcdTestCli.Txn(context.Background()).Then(clearInfo, clearOp).Commit()
	require.NoError(t, err)
}

type testForEtcd struct {
	suite.Suite
}

func (t *testForEtcd) TestInfoJSON() {
	i1 := NewInfo("test", "mysql-replica-1", "foo", "bar", []string{
		"ALTER TABLE bar ADD COLUMN c1 INT",
		"ALTER TABLE bar ADD COLUMN c2 INT",
	})

	j, err := i1.toJSON()
	t.Require().NoError(err)
	t.Require().Equal(`{"task":"test","source":"mysql-replica-1","schema":"foo","table":"bar","ddls":["ALTER TABLE bar ADD COLUMN c1 INT","ALTER TABLE bar ADD COLUMN c2 INT"]}`, j)
	t.Require().Equal(i1.String(), j)

	i2, err := infoFromJSON(j)
	t.Require().NoError(err)
	t.Require().Equal(i1, i2)
}

func (t *testForEtcd) TestInfoEtcd() {
	defer clearTestInfoOperation(t.T())

	var (
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		task1   = "task-1"
		task2   = "task-2"
		i11     = NewInfo(task1, source1, "foo", "bar", []string{
			"ALTER TABLE bar ADD COLUMN c1 INT",
		})
		i12 = NewInfo(task1, source2, "foo", "bar", []string{
			"ALTER TABLE bar ADD COLUMN c2 INT",
		})
		i21 = NewInfo(task2, source1, "foo", "bar", []string{
			"ALTER TABLE bar ADD COLUMN c3 INT",
		})
	)

	// put the same key twice.
	rev1, err := PutInfo(etcdTestCli, i11)
	t.Require().NoError(err)
	rev2, err := PutInfo(etcdTestCli, i11)
	t.Require().NoError(err)
	t.Require().Greater(rev2, rev1)

	// get with only 1 info.
	ifm, rev3, err := GetAllInfo(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Equal(rev2, rev3)
	t.Require().Len(ifm, 1)
	t.Require().Contains(ifm, task1)
	t.Require().Len(ifm[task1], 1)
	t.Require().Equal(i11, ifm[task1][source1])

	// put another key and get again with 2 info.
	rev4, err := PutInfo(etcdTestCli, i12)
	t.Require().NoError(err)
	ifm, _, err = GetAllInfo(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Len(ifm, 1)
	t.Require().Contains(ifm, task1)
	t.Require().Len(ifm[task1], 2)
	t.Require().Equal(i11, ifm[task1][source1])
	t.Require().Equal(i12, ifm[task1][source2])

	// start the watcher.
	wch := make(chan Info, 10)
	ech := make(chan error, 10)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		WatchInfoPut(ctx, etcdTestCli, rev4+1, wch, ech) // revision+1
		close(wch)                                       // close the chan
		close(ech)
	}()

	// put another key for a different task.
	_, err = PutInfo(etcdTestCli, i21)
	t.Require().NoError(err)
	// wait response of WatchInfoPut, increase waiting time when resource shortage
	utils.WaitSomething(10, 500*time.Millisecond, func() bool {
		return len(wch) != 0
	})
	cancel()
	wg.Wait()

	// watch should only get i21.
	t.Require().Equal(1, len(wch))
	t.Require().Equal(0, len(ech))
	t.Require().Equal(i21, <-wch)

	// delete i12.
	deleteOp := deleteInfoOp(i12)
	_, err = etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	t.Require().NoError(err)

	// get again.
	ifm, _, err = GetAllInfo(etcdTestCli)
	t.Require().NoError(err)
	t.Require().Len(ifm, 2)
	t.Require().Contains(ifm, task1)
	t.Require().Contains(ifm, task2)
	t.Require().Len(ifm[task1], 1)
	t.Require().Equal(i11, ifm[task1][source1])
	t.Require().Len(ifm[task2], 1)
	t.Require().Equal(i21, ifm[task2][source1])
}

func (t *testForEtcd) TestPutInfoIfOpNotDone() {
	defer clearTestInfoOperation(t.T())

	var (
		source = "mysql-replica-1"
		task   = "test-put-info-if-no-op"
		schema = "foo"
		table  = "bar"
		DDLs   = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		ID     = fmt.Sprintf("%s-%s", task, dbutil.TableName(schema, table))
		info   = NewInfo(task, source, schema, table, DDLs)
		op     = NewOperation(ID, task, source, DDLs, false, false)
	)

	// put info success because no operation exist.
	rev1, putted, err := PutInfoIfOpNotDone(etcdTestCli, info)
	t.Require().NoError(err)
	t.Require().Greater(rev1, int64(0))
	t.Require().True(putted)

	// put a non-done operation.
	rev2, putted, err := PutOperations(etcdTestCli, false, op)
	t.Require().NoError(err)
	t.Require().Greater(rev2, rev1)
	t.Require().True(putted)

	// still can put info.
	rev3, putted, err := PutInfoIfOpNotDone(etcdTestCli, info)
	t.Require().NoError(err)
	t.Require().Greater(rev3, rev2)
	t.Require().True(putted)

	// change op to `done` and put it.
	op.Done = true
	rev4, putted, err := PutOperations(etcdTestCli, false, op)
	t.Require().NoError(err)
	t.Require().Greater(rev4, rev3)
	t.Require().True(putted)

	// can't put info anymore.
	rev5, putted, err := PutInfoIfOpNotDone(etcdTestCli, info)
	t.Require().NoError(err)
	t.Require().Equal(rev4, rev5)
	t.Require().False(putted)

	// try put anther info, but still can't put it.
	info.DDLs = []string{"ALTER TABLE bar ADD COLUMN c2 INT"}
	rev6, putted, err := PutInfoIfOpNotDone(etcdTestCli, info)
	t.Require().NoError(err)
	t.Require().Equal(rev5, rev6)
	t.Require().False(putted)
}
