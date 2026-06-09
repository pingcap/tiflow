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
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	tiddl "github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

var etcdTestCli *clientv3.Client

func TestMain(m *testing.M) {
	mockCluster := integration.NewClusterV3(&testing.T{}, &integration.ClusterConfig{Size: 1})
	etcdTestCli = mockCluster.RandClient()

	code := m.Run()

	mockCluster.Terminate(&testing.T{})
	os.Exit(code)
}

// clear keys in etcd test cluster.
func clearTestInfoOperation(t *testing.T) {
	require.NoError(t, ClearTestInfoOperationColumn(etcdTestCli))
}

func createTableInfo(t *testing.T, p *parser.Parser, se sessionctx.Context, tableID int64, sql string) *model.TableInfo {
	node, err := p.ParseOneStmt(sql, "utf8mb4", "utf8mb4_bin")
	if err != nil {
		t.Fatalf("fail to parse stmt, %v", err)
	}
	createStmtNode, ok := node.(*ast.CreateTableStmt)
	if !ok {
		t.Fatalf("%s is not a CREATE TABLE statement", sql)
	}
	info, err := tiddl.MockTableInfo(se, createStmtNode, tableID)
	if err != nil {
		t.Fatalf("fail to create table info, %v", err)
	}
	return info
}

func TestInfoJSON(t *testing.T) {
	i1 := NewInfo("test", "mysql-replica-1",
		"db-1", "tbl-1", "db", "tbl", []string{
			"ALTER TABLE tbl ADD COLUMN c1 INT",
			"ALTER TABLE tbl ADD COLUMN c2 INT",
		}, nil, nil)

	j, err := i1.toJSON()
	require.NoError(t, err)
	require.Equal(t, `{"task":"test","source":"mysql-replica-1","up-schema":"db-1","up-table":"tbl-1","down-schema":"db","down-table":"tbl","ddls":["ALTER TABLE tbl ADD COLUMN c1 INT","ALTER TABLE tbl ADD COLUMN c2 INT"],"table-info-before":null,"table-info-after":null,"ignore-conflict":false}`, j)
	require.Equal(t, i1.String(), j)
	j = i1.ShortString()
	require.Equal(t, `{"task":"test","source":"mysql-replica-1","up-schema":"db-1","up-table":"tbl-1","down-schema":"db","down-table":"tbl","ddls":["ALTER TABLE tbl ADD COLUMN c1 INT","ALTER TABLE tbl ADD COLUMN c2 INT"],"table-before":"","table-after":"","is-deleted":false,"version":0,"revision":0,"ignore-conflict":false}`, j)

	i2, err := infoFromJSON(j)
	require.NoError(t, err)
	require.Equal(t, i1, i2)
}

func TestEtcdInfoUpgrade(t *testing.T) {
	defer clearTestInfoOperation(t)

	var (
		source1          = "mysql-replica-1"
		source2          = "mysql-replica-2"
		task1            = "task-1"
		task2            = "task-2"
		upSchema         = "foo_1"
		upTable          = "bar_1"
		downSchema       = "foo"
		downTable        = "bar"
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 222
		tblI1            = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tblI2            = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		tblI3            = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		tblI4            = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT, c3 INT)`)
		i11              = NewInfo(task1, source1, upSchema, upTable, downSchema, downTable, []string{"ALTER TABLE bar ADD COLUMN c1 INT"}, tblI1, []*model.TableInfo{tblI2})
		i12              = NewInfo(task1, source2, upSchema, upTable, downSchema, downTable, []string{"ALTER TABLE bar ADD COLUMN c2 INT"}, tblI2, []*model.TableInfo{tblI3})
		i21              = NewInfo(task2, source1, upSchema, upTable, downSchema, downTable, []string{"ALTER TABLE bar ADD COLUMN c3 INT"}, tblI3, []*model.TableInfo{tblI4})
		oi11             = newOldInfo(task1, source1, upSchema, upTable, downSchema, downTable, []string{"ALTER TABLE bar ADD COLUMN c1 INT"}, tblI1, tblI2)
		oi12             = newOldInfo(task1, source2, upSchema, upTable, downSchema, downTable, []string{"ALTER TABLE bar ADD COLUMN c2 INT"}, tblI2, tblI3)
		oi21             = newOldInfo(task2, source1, upSchema, upTable, downSchema, downTable, []string{"ALTER TABLE bar ADD COLUMN c3 INT"}, tblI3, tblI4)
	)

	// put the oldInfo
	rev1, err := putOldInfo(etcdTestCli, oi11)
	require.NoError(t, err)
	rev2, err := putOldInfo(etcdTestCli, oi11)
	require.NoError(t, err)
	require.Greater(t, rev2, rev1)

	// put another key and get again with 2 info.
	rev3, err := putOldInfo(etcdTestCli, oi12)
	require.NoError(t, err)
	require.Greater(t, rev3, rev2)

	// get all infos.
	ifm, rev4, err := GetAllInfo(etcdTestCli)
	require.NoError(t, err)
	require.Equal(t, rev3, rev4)
	require.Len(t, ifm, 1)
	require.Contains(t, ifm, task1)
	require.Len(t, ifm[task1], 2)
	require.Len(t, ifm[task1][source1], 1)
	require.Len(t, ifm[task1][source1][upSchema], 1)
	require.Len(t, ifm[task1][source2], 1)
	require.Len(t, ifm[task1][source2][upSchema], 1)

	i11WithVer := i11
	i11WithVer.Version = 2
	i11WithVer.Revision = rev2
	i12WithVer := i12
	i12WithVer.Version = 1
	i12WithVer.Revision = rev4
	require.Equal(t, i11WithVer, ifm[task1][source1][upSchema][upTable])
	require.Equal(t, i12WithVer, ifm[task1][source2][upSchema][upTable])

	// start the watcher.
	wch := make(chan Info, 10)
	ech := make(chan error, 10)
	var wg sync.WaitGroup
	wg.Add(1)
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	go func() {
		defer wg.Done()
		WatchInfo(watchCtx, etcdTestCli, rev4+1, wch, ech) // revision+1
	}()

	// put another oldInfo for a different task.
	// version start from 1
	// simulate v2.0.1 worker and v2.0.2 master
	rev5, err := putOldInfo(etcdTestCli, oi21)
	require.NoError(t, err)
	infoWithVer := <-wch
	i21WithVer := i21
	i21WithVer.Version = 1
	i21WithVer.Revision = rev5
	require.Equal(t, i21WithVer, infoWithVer)
	require.Equal(t, 0, len(ech))
}

func TestInfoEtcd(t *testing.T) {
	defer clearTestInfoOperation(t)

	var (
		watchTimeout       = 2 * time.Second
		source1            = "mysql-replica-1"
		source2            = "mysql-replica-2"
		task1              = "task-1"
		task2              = "task-2"
		upSchema           = "foo_1"
		upTable            = "bar_1"
		downSchema         = "foo"
		downTable          = "bar"
		p                  = parser.New()
		se                 = mock.NewContext()
		tblID        int64 = 222
		tblI1              = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tblI2              = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		tblI3              = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		tblI4              = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT, c3 INT)`)
		i11                = NewInfo(task1, source1, upSchema, upTable, downSchema, downTable, []string{"ALTER TABLE bar ADD COLUMN c1 INT"}, tblI1, []*model.TableInfo{tblI2})
		i12                = NewInfo(task1, source2, upSchema, upTable, downSchema, downTable, []string{"ALTER TABLE bar ADD COLUMN c2 INT"}, tblI2, []*model.TableInfo{tblI3})
		i21                = NewInfo(task2, source1, upSchema, upTable, downSchema, downTable, []string{"ALTER TABLE bar ADD COLUMN c3 INT"}, tblI3, []*model.TableInfo{tblI4})
	)

	// put the same key twice.
	rev1, err := PutInfo(etcdTestCli, i11)
	require.NoError(t, err)
	rev2, err := PutInfo(etcdTestCli, i11)
	require.NoError(t, err)
	require.Greater(t, rev2, rev1)

	// get with only 1 info.
	ifm, rev3, err := GetAllInfo(etcdTestCli)
	require.NoError(t, err)
	require.Equal(t, rev2, rev3)
	require.Len(t, ifm, 1)
	require.Contains(t, ifm, task1)
	require.Len(t, ifm[task1], 1)
	require.Len(t, ifm[task1][source1], 1)
	require.Len(t, ifm[task1][source1][upSchema], 1)
	i11WithVer := i11
	i11WithVer.Version = 2
	i11WithVer.Revision = rev2
	require.Equal(t, i11WithVer, ifm[task1][source1][upSchema][upTable])

	// put another key and get again with 2 info.
	rev4, err := PutInfo(etcdTestCli, i12)
	require.NoError(t, err)
	ifm, _, err = GetAllInfo(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, ifm, 1)
	require.Contains(t, ifm, task1)
	require.Len(t, ifm[task1], 2)
	require.Equal(t, i11WithVer, ifm[task1][source1][upSchema][upTable])
	i12WithVer := i12
	i12WithVer.Version = 1
	i12WithVer.Revision = rev4
	require.Equal(t, i12WithVer, ifm[task1][source2][upSchema][upTable])

	// start the watcher.
	wch := make(chan Info, 10)
	ech := make(chan error, 10)
	var wg sync.WaitGroup
	wg.Add(1)
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	go func() {
		defer wg.Done()
		WatchInfo(watchCtx, etcdTestCli, rev4+1, wch, ech) // revision+1
	}()

	// put another key for a different task.
	// version start from 1
	rev5, err := PutInfo(etcdTestCli, i21)
	require.NoError(t, err)
	infoWithVer := <-wch
	i21WithVer := i21
	i21WithVer.Version = 1
	i21WithVer.Revision = rev5
	require.Equal(t, i21WithVer, infoWithVer)
	require.Equal(t, 0, len(ech))

	// put again
	// version increase
	rev6, err := PutInfo(etcdTestCli, i21)
	require.NoError(t, err)
	infoWithVer = <-wch
	i21WithVer.Version++
	i21WithVer.Revision = rev6
	require.Equal(t, i21WithVer, infoWithVer)
	require.Equal(t, 0, len(ech))

	// delete i21.
	deleteOp := deleteInfoOp(i21)
	resp, err := etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	require.NoError(t, err)
	require.True(t, resp.Succeeded)
	select {
	case err2 := <-ech:
		t.Fatal(err2)
	case <-wch:
	}

	// put again
	// version reset to 1
	rev7, err := PutInfo(etcdTestCli, i21)
	require.NoError(t, err)
	infoWithVer = <-wch
	i21WithVer.Version = 1
	i21WithVer.Revision = rev7
	require.Equal(t, i21WithVer, infoWithVer)
	require.Equal(t, 0, len(ech))

	watchCancel()
	wg.Wait()
	close(wch) // close the chan
	close(ech)

	// delete i12.
	deleteOp = deleteInfoOp(i12)
	resp, err = etcdTestCli.Txn(context.Background()).Then(deleteOp).Commit()
	require.NoError(t, err)

	// get again.
	ifm, _, err = GetAllInfo(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, ifm, 2)
	require.Contains(t, ifm, task1)
	require.Contains(t, ifm, task2)
	require.Len(t, ifm[task1], 1)
	i11WithVer.Revision = ifm[task1][source1][upSchema][upTable].Revision
	require.Equal(t, i11WithVer, ifm[task1][source1][upSchema][upTable])
	require.Len(t, ifm[task2], 1)
	i21WithVer.Revision = ifm[task2][source1][upSchema][upTable].Revision
	require.Equal(t, i21WithVer, ifm[task2][source1][upSchema][upTable])

	// watch the deletion for i12.
	wch = make(chan Info, 10)
	ech = make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchInfo(ctx, etcdTestCli, resp.Header.Revision, wch, ech)
	cancel()
	close(wch)
	close(ech)
	require.Equal(t, 1, len(wch))
	info := <-wch
	i12c := i12
	i12c.IsDeleted = true
	require.Equal(t, i12c, info)
	require.Equal(t, 0, len(ech))
}

func newOldInfo(task, source, upSchema, upTable, downSchema, downTable string,
	ddls []string, tableInfoBefore *model.TableInfo, tableInfoAfter *model.TableInfo,
) OldInfo {
	return OldInfo{
		Task:            task,
		Source:          source,
		UpSchema:        upSchema,
		UpTable:         upTable,
		DownSchema:      downSchema,
		DownTable:       downTable,
		DDLs:            ddls,
		TableInfoBefore: tableInfoBefore,
		TableInfoAfter:  tableInfoAfter,
	}
}

func putOldInfo(cli *clientv3.Client, oldInfo OldInfo) (int64, error) {
	data, err := json.Marshal(oldInfo)
	if err != nil {
		return 0, err
	}
	key := common.ShardDDLOptimismInfoKeyAdapter.Encode(oldInfo.Task, oldInfo.Source, oldInfo.UpSchema, oldInfo.UpTable)

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Put(ctx, key, string(data))
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}
