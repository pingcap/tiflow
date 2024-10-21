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

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

var etcdTestCli *clientv3.Client

func TestInfo(t *testing.T) {
	integration.BeforeTestExternal(t)
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	check.TestingT(t)
}

// clear keys in etcd test cluster.
func clearTestInfoOperation(c *check.C) {
	clearInfo := clientv3.OpDelete(common.ShardDDLPessimismInfoKeyAdapter.Path(), clientv3.WithPrefix())
	clearOp := clientv3.OpDelete(common.ShardDDLPessimismOperationKeyAdapter.Path(), clientv3.WithPrefix())
	_, err := etcdTestCli.Txn(context.Background()).Then(clearInfo, clearOp).Commit()
	c.Assert(err, check.IsNil)
}

type testForEtcd struct{}

var _ = check.Suite(&testForEtcd{})

func (t *testForEtcd) TestInfoJSON(c *check.C) {
	i1 := NewInfo("test", "mysql-replica-1", "foo", "bar", []string{
		"ALTER TABLE bar ADD COLUMN c1 INT",
		"ALTER TABLE bar ADD COLUMN c2 INT",
	})

	j, err := i1.toJSON()
	c.Assert(err, check.IsNil)
	c.Assert(j, check.Equals, `{"task":"test","source":"mysql-replica-1","schema":"foo","table":"bar","ddls":["ALTER TABLE bar ADD COLUMN c1 INT","ALTER TABLE bar ADD COLUMN c2 INT"]}`)
	c.Assert(j, check.Equals, i1.String())

	i2, err := infoFromJSON(j)
	c.Assert(err, check.IsNil)
	c.Assert(i2, check.DeepEquals, i1)
}

func (t *testForEtcd) TestInfoEtcd(c *check.C) {
	defer clearTestInfoOperation(c)

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
	c.Assert(err, check.IsNil)
	rev2, err := PutInfo(etcdTestCli, i11)
	c.Assert(err, check.IsNil)
	c.Assert(rev2, check.Greater, rev1)

	// get with only 1 info.
	ifm, rev3, err := GetAllInfo(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(rev3, check.Equals, rev2)
	c.Assert(ifm, check.HasLen, 1)
	c.Assert(ifm, check.HasKey, task1)
	c.Assert(ifm[task1], check.HasLen, 1)
	c.Assert(ifm[task1][source1], check.DeepEquals, i11)

	// put another key and get again with 2 info.
	rev4, err := PutInfo(etcdTestCli, i12)
	c.Assert(err, check.IsNil)
	ifm, _, err = GetAllInfo(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(ifm, check.HasLen, 1)
	c.Assert(ifm, check.HasKey, task1)
	c.Assert(ifm[task1], check.HasLen, 2)
	c.Assert(ifm[task1][source1], check.DeepEquals, i11)
	c.Assert(ifm[task1][source2], check.DeepEquals, i12)

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
	c.Assert(err, check.IsNil)
	// wait response of WatchInfoPut, increase waiting time when resource shortage
	utils.WaitSomething(10, 500*time.Millisecond, func() bool {
		return len(wch) != 0
	})
	cancel()
	wg.Wait()

	// watch should only get i21.
	c.Assert(len(wch), check.Equals, 1)
	c.Assert(len(ech), check.Equals, 0)
	c.Assert(<-wch, check.DeepEquals, i21)

	// delete i12.
	deleteOp := deleteInfoOp(i12)
	_, err = etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	c.Assert(err, check.IsNil)

	// get again.
	ifm, _, err = GetAllInfo(etcdTestCli)
	c.Assert(err, check.IsNil)
	c.Assert(ifm, check.HasLen, 2)
	c.Assert(ifm, check.HasKey, task1)
	c.Assert(ifm, check.HasKey, task2)
	c.Assert(ifm[task1], check.HasLen, 1)
	c.Assert(ifm[task1][source1], check.DeepEquals, i11)
	c.Assert(ifm[task2], check.HasLen, 1)
	c.Assert(ifm[task2][source1], check.DeepEquals, i21)
}

func (t *testForEtcd) TestPutInfoIfOpNotDone(c *check.C) {
	defer clearTestInfoOperation(c)

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
	c.Assert(err, check.IsNil)
	c.Assert(rev1, check.Greater, int64(0))
	c.Assert(putted, check.IsTrue)

	// put a non-done operation.
	rev2, putted, err := PutOperations(etcdTestCli, false, op)
	c.Assert(err, check.IsNil)
	c.Assert(rev2, check.Greater, rev1)
	c.Assert(putted, check.IsTrue)

	// still can put info.
	rev3, putted, err := PutInfoIfOpNotDone(etcdTestCli, info)
	c.Assert(err, check.IsNil)
	c.Assert(rev3, check.Greater, rev2)
	c.Assert(putted, check.IsTrue)

	// change op to `done` and put it.
	op.Done = true
	rev4, putted, err := PutOperations(etcdTestCli, false, op)
	c.Assert(err, check.IsNil)
	c.Assert(rev4, check.Greater, rev3)
	c.Assert(putted, check.IsTrue)

	// can't put info anymore.
	rev5, putted, err := PutInfoIfOpNotDone(etcdTestCli, info)
	c.Assert(err, check.IsNil)
	c.Assert(rev5, check.Equals, rev4)
	c.Assert(putted, check.IsFalse)

	// try put anther info, but still can't put it.
	info.DDLs = []string{"ALTER TABLE bar ADD COLUMN c2 INT"}
	rev6, putted, err := PutInfoIfOpNotDone(etcdTestCli, info)
	c.Assert(err, check.IsNil)
	c.Assert(rev6, check.Equals, rev5)
	c.Assert(putted, check.IsFalse)
}
