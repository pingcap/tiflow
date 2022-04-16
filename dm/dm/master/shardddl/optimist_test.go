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
	"fmt"
	"testing"
	"time"

	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
)

func TestOptimistSuite(t *testing.T) {
	suite.Run(t, new(testOptimistSuite))
}

type testOptimistSuite struct {
	suite.Suite
	mockCluster *integration.ClusterV3
	etcdTestCli *clientv3.Client
}

func (t *testOptimistSuite) SetupSuite() {
	require.NoError(t.T(), log.InitLogger(&log.Config{}))

	integration.BeforeTestExternal(t.T())
	t.mockCluster = integration.NewClusterV3(t.T(), &integration.ClusterConfig{Size: 1})
	t.etcdTestCli = t.mockCluster.RandClient()
}

func (t *testOptimistSuite) TearDownSuite() {
	t.mockCluster.Terminate(t.T())
}

func (t *testOptimistSuite) TearDownTest() {
	t.clearOptimistTestSourceInfoOperation()
}

// clear keys in etcd test cluster.
func (t *testOptimistSuite) clearOptimistTestSourceInfoOperation() {
	require.NoError(t.T(), optimism.ClearTestInfoOperationColumn(t.etcdTestCli))
}

func createTableInfo(t *testing.T, p *parser.Parser, se sessionctx.Context, tableID int64, sql string) *model.TableInfo {
	t.Helper()
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

func watchExactOneOperation(
	ctx context.Context,
	cli *clientv3.Client,
	task, source, upSchema, upTable string,
	revision int64,
) (optimism.Operation, error) {
	opCh := make(chan optimism.Operation, 10)
	errCh := make(chan error, 10)
	done := make(chan struct{})
	subCtx, cancel := context.WithCancel(ctx)
	go func() {
		optimism.WatchOperationPut(subCtx, cli, task, source, upSchema, upTable, revision, opCh, errCh)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	var op optimism.Operation
	select {
	case op = <-opCh:
	case err := <-errCh:
		return op, err
	case <-ctx.Done():
		return op, ctx.Err()
	}

	// Wait 100ms to check if there is unexpected operation.
	select {
	case extraOp := <-opCh:
		return op, fmt.Errorf("unpexecped operation %s", extraOp)
	case <-time.After(time.Millisecond * 100):
	}
	return op, nil
}

func checkLocks(t *testing.T, o *Optimist, expectedLocks []*pb.DDLLock, task string, sources []string) {
	t.Helper()
	lock, err := o.ShowLocks(task, sources)
	require.NoError(t, err)
	if expectedLocks == nil {
		require.Len(t, lock, 0)
	} else {
		require.Equal(t, expectedLocks, lock)
	}
}

func checkLocksByMap(t *testing.T, o *Optimist, expectedLocks map[string]*pb.DDLLock, sources []string, lockIDs ...string) {
	t.Helper()
	lock, err := o.ShowLocks("", sources)
	require.NoError(t, err)
	require.Len(t, lock, len(lockIDs))
	lockIDMap := make(map[string]struct{})
	for _, lockID := range lockIDs {
		lockIDMap[lockID] = struct{}{}
	}
	for i := range lockIDs {
		_, ok := lockIDMap[lock[i].ID]
		require.True(t, ok)
		delete(lockIDMap, lock[i].ID)
		require.Equal(t, expectedLocks[lock[i].ID], lock[i])
	}
}

func (t *testOptimistSuite) TestOptimistSourceTables() {
	var (
		logger     = log.L()
		o          = NewOptimist(&logger, getDownstreamMeta)
		task       = "task"
		source1    = "mysql-replica-1"
		source2    = "mysql-replica-2"
		downSchema = "db"
		downTable  = "tbl"
		st1        = optimism.NewSourceTables(task, source1)
		st2        = optimism.NewSourceTables(task, source2)
	)

	st1.AddTable("db", "tbl-1", downSchema, downTable)
	st1.AddTable("db", "tbl-2", downSchema, downTable)
	st2.AddTable("db", "tbl-1", downSchema, downTable)
	st2.AddTable("db", "tbl-2", downSchema, downTable)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous kv and no etcd operation.
	require.NoError(t.T(), o.Start(ctx, t.etcdTestCli))
	require.Nil(t.T(), o.tk.FindTables(task, downSchema, downTable))
	o.Close()
	o.Close() // close multiple times.

	// CASE 2: start again without any previous kv.
	require.NoError(t.T(), o.Start(ctx, t.etcdTestCli))
	require.Nil(t.T(), o.tk.FindTables(task, downSchema, downTable))

	// PUT st1, should find tables.
	_, err := optimism.PutSourceTables(t.etcdTestCli, st1)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		tts := o.tk.FindTables(task, downSchema, downTable)
		return len(tts) == 1
	}, 30*100*time.Millisecond, 100*time.Millisecond)
	tts := o.tk.FindTables(task, downSchema, downTable)
	require.Len(t.T(), tts, 1)
	require.Equal(t.T(), st1.TargetTable(downSchema, downTable), tts[0])
	o.Close()

	// CASE 3: start again with previous source tables.
	require.NoError(t.T(), o.Start(ctx, t.etcdTestCli))
	tts = o.tk.FindTables(task, downSchema, downTable)
	require.Len(t.T(), tts, 1)
	require.Equal(t.T(), st1.TargetTable(downSchema, downTable), tts[0])

	// PUT st2, should find more tables.
	_, err = optimism.PutSourceTables(t.etcdTestCli, st2)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		tts = o.tk.FindTables(task, downSchema, downTable)
		return len(tts) == 2
	}, 30*100*time.Millisecond, 100*time.Millisecond)
	tts = o.tk.FindTables(task, downSchema, downTable)
	require.Len(t.T(), tts, 2)
	require.Equal(t.T(), st1.TargetTable(downSchema, downTable), tts[0])
	require.Equal(t.T(), st2.TargetTable(downSchema, downTable), tts[1])
	o.Close()

	// CASE 4: create (not re-start) a new optimist with previous source tables.
	o = NewOptimist(&logger, getDownstreamMeta)
	require.NoError(t.T(), o.Start(ctx, t.etcdTestCli))
	tts = o.tk.FindTables(task, downSchema, downTable)
	require.Len(t.T(), tts, 2)
	require.Equal(t.T(), st1.TargetTable(downSchema, downTable), tts[0])
	require.Equal(t.T(), st2.TargetTable(downSchema, downTable), tts[1])

	// DELETE st1, should find less tables.
	_, err = optimism.DeleteSourceTables(t.etcdTestCli, st1)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		tts = o.tk.FindTables(task, downSchema, downTable)
		return len(tts) == 1
	}, 30*100*time.Millisecond, 100*time.Millisecond)
	tts = o.tk.FindTables(task, downSchema, downTable)
	require.Len(t.T(), tts, 1)
	require.Equal(t.T(), st2.TargetTable(downSchema, downTable), tts[0])
	o.Close()
}

func (t *testOptimistSuite) TestOptimist() {
	t.testOptimist(t.etcdTestCli, noRestart)
	t.testOptimist(t.etcdTestCli, restartOnly)
	t.testOptimist(t.etcdTestCli, restartNewInstance)
	t.testSortInfos(t.etcdTestCli)
}

func (t *testOptimistSuite) testOptimist(cli *clientv3.Client, restart int) {
	defer func() {
		require.NoError(t.T(), optimism.ClearTestInfoOperationColumn(cli))
	}()

	var (
		tick    = 100 * time.Millisecond
		waitFor = 30 * tick
		logger  = log.L()
		o       = NewOptimist(&logger, getDownstreamMeta)

		rebuildOptimist = func(ctx context.Context) {
			switch restart {
			case restartOnly:
				o.Close()
				require.NoError(t.T(), o.Start(ctx, cli))
			case restartNewInstance:
				o.Close()
				o = NewOptimist(&logger, getDownstreamMeta)
				require.NoError(t.T(), o.Start(ctx, cli))
			}
		}

		task             = "task-test-optimist"
		source1          = "mysql-replica-1"
		source2          = "mysql-replica-2"
		downSchema       = "foo"
		downTable        = "bar"
		lockID           = fmt.Sprintf("%s-`%s`.`%s`", task, downSchema, downTable)
		st1              = optimism.NewSourceTables(task, source1)
		st31             = optimism.NewSourceTables(task, source1)
		st32             = optimism.NewSourceTables(task, source2)
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		DDLs2            = []string{"ALTER TABLE bar ADD COLUMN c2 INT"}
		DDLs3            = []string{"ALTER TABLE bar DROP COLUMN c2"}
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		ti3              = ti1
		i11              = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i12              = optimism.NewInfo(task, source1, "foo", "bar-2", downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i21              = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2})
		i23              = optimism.NewInfo(task, source2, "foo-2", "bar-3", downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2})
		i31              = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, DDLs3, ti2, []*model.TableInfo{ti3})
		i33              = optimism.NewInfo(task, source2, "foo-2", "bar-3", downSchema, downTable, DDLs3, ti2, []*model.TableInfo{ti3})
	)

	st1.AddTable("foo", "bar-1", downSchema, downTable)
	st1.AddTable("foo", "bar-2", downSchema, downTable)
	st31.AddTable("foo", "bar-1", downSchema, downTable)
	st32.AddTable("foo-2", "bar-3", downSchema, downTable)

	// put source tables first.
	_, err := optimism.PutSourceTables(cli, st1)
	require.NoError(t.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous shard DDL info.
	require.NoError(t.T(), o.Start(ctx, cli))
	require.Len(t.T(), o.Locks(), 0)
	o.Close()
	o.Close() // close multiple times.

	// CASE 2: start again without any previous shard DDL info.
	require.NoError(t.T(), o.Start(ctx, cli))
	require.Len(t.T(), o.Locks(), 0)

	// PUT i11, will create a lock but not synced.
	rev1, err := optimism.PutInfo(cli, i11)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		return len(o.Locks()) == 1
	}, waitFor, tick)
	require.Contains(t.T(), o.Locks(), lockID)
	synced, remain := o.Locks()[lockID].IsSynced()
	require.False(t.T(), synced)
	require.Equal(t.T(), 1, remain)

	// check ShowLocks.
	expectedLock := []*pb.DDLLock{
		{
			ID:    lockID,
			Task:  task,
			Mode:  config.ShardOptimistic,
			Owner: "",
			DDLs:  nil,
			Synced: []string{
				fmt.Sprintf("%s-%s", i11.Source, dbutil.TableName(i11.UpSchema, i11.UpTable)),
			},
			Unsynced: []string{
				fmt.Sprintf("%s-%s", i12.Source, dbutil.TableName(i12.UpSchema, i12.UpTable)),
			},
		},
	}
	checkLocks(t.T(), o, expectedLock, "", []string{})

	// wait operation for i11 become available.
	op11, err := watchExactOneOperation(ctx, cli, i11.Task, i11.Source, i11.UpSchema, i11.UpTable, rev1)
	require.NoError(t.T(), err)
	require.Equal(t.T(), DDLs1, op11.DDLs)
	require.Equal(t.T(), optimism.ConflictNone, op11.ConflictStage)
	checkLocks(t.T(), o, expectedLock, "", []string{})

	// mark op11 as done.
	op11c := op11
	op11c.Done = true
	_, putted, err := optimism.PutOperation(cli, false, op11c, 0)
	require.NoError(t.T(), err)
	require.True(t.T(), putted)
	require.Eventually(t.T(), func() bool {
		lock := o.Locks()[lockID]
		if lock == nil {
			return false
		}
		return lock.IsDone(op11.Source, op11.UpSchema, op11.UpTable)
	}, waitFor, tick)
	require.False(t.T(), o.Locks()[lockID].IsDone(i12.Source, i12.UpSchema, i12.UpTable))
	checkLocks(t.T(), o, expectedLock, "", []string{})

	// PUT i12, the lock will be synced.
	rev2, err := optimism.PutInfo(cli, i12)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		synced, _ = o.Locks()[lockID].IsSynced()
		return synced
	}, waitFor, tick)

	expectedLock = []*pb.DDLLock{
		{
			ID:    lockID,
			Task:  task,
			Mode:  config.ShardOptimistic,
			Owner: "",
			DDLs:  nil,
			Synced: []string{
				fmt.Sprintf("%s-%s", i11.Source, dbutil.TableName(i11.UpSchema, i11.UpTable)),
				fmt.Sprintf("%s-%s", i12.Source, dbutil.TableName(i12.UpSchema, i12.UpTable)),
			},
			Unsynced: []string{},
		},
	}
	checkLocks(t.T(), o, expectedLock, "", []string{})

	// wait operation for i12 become available.
	op12, err := watchExactOneOperation(ctx, cli, i12.Task, i12.Source, i12.UpSchema, i12.UpTable, rev2)
	require.NoError(t.T(), err)
	require.Equal(t.T(), DDLs1, op12.DDLs)
	require.Equal(t.T(), optimism.ConflictNone, op12.ConflictStage)
	checkLocks(t.T(), o, expectedLock, "", []string{})

	// mark op12 as done, the lock should be resolved.
	op12c := op12
	op12c.Done = true
	_, putted, err = optimism.PutOperation(cli, false, op12c, 0)
	require.NoError(t.T(), err)
	require.True(t.T(), putted)
	require.Eventually(t.T(), func() bool {
		_, ok := o.Locks()[lockID]
		return !ok
	}, waitFor, tick)
	require.Len(t.T(), o.Locks(), 0)
	checkLocks(t.T(), o, nil, "", nil)

	// no shard DDL info or lock operation exists.
	ifm, _, err := optimism.GetAllInfo(cli)
	require.NoError(t.T(), err)
	require.Len(t.T(), ifm, 0)
	opm, _, err := optimism.GetAllOperations(cli)
	require.NoError(t.T(), err)
	require.Len(t.T(), opm, 0)

	// put another table info.
	rev1, err = optimism.PutInfo(cli, i21)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		return len(o.Locks()) == 1
	}, waitFor, tick)
	require.Contains(t.T(), o.Locks(), lockID)
	synced, remain = o.Locks()[lockID].IsSynced()
	require.False(t.T(), synced)
	require.Equal(t.T(), 1, remain)

	// wait operation for i21 become available.
	op21, err := watchExactOneOperation(ctx, cli, i21.Task, i21.Source, i21.UpSchema, i21.UpTable, rev1)
	require.NoError(t.T(), err)
	require.Equal(t.T(), i21.DDLs, op21.DDLs)
	require.Equal(t.T(), optimism.ConflictNone, op12.ConflictStage)

	// CASE 3: start again with some previous shard DDL info and the lock is un-synced.
	rebuildOptimist(ctx)
	require.Len(t.T(), o.Locks(), 1)
	require.Contains(t.T(), o.Locks(), lockID)
	synced, remain = o.Locks()[lockID].IsSynced()
	require.False(t.T(), synced)
	require.Equal(t.T(), 1, remain)

	// put table info for a new table (to simulate `CREATE TABLE`).
	// lock will fetch the new table info from downstream.
	// here will use ti1
	_, err = optimism.PutSourceTables(cli, st32)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		ready := o.Locks()[lockID].Ready()
		return !ready[source2][i23.UpSchema][i23.UpTable]
	}, waitFor, tick)
	synced, remain = o.Locks()[lockID].IsSynced()
	require.False(t.T(), synced)
	require.Equal(t.T(), remain, 2)
	tts := o.tk.FindTables(task, downSchema, downTable)
	require.Len(t.T(), tts, 2)
	require.Equal(t.T(), source2, tts[1].Source)
	require.Contains(t.T(), tts[1].UpTables, i23.UpSchema)
	require.Contains(t.T(), tts[1].UpTables[i23.UpSchema], i23.UpTable)

	// ddl for new table
	rev3, err := optimism.PutInfo(cli, i23)
	require.NoError(t.T(), err)
	// wait operation for i23 become available.
	op23, err := watchExactOneOperation(ctx, cli, i23.Task, i23.Source, i23.UpSchema, i23.UpTable, rev3)
	require.NoError(t.T(), err)
	require.Equal(t.T(), op23.DDLs, i23.DDLs)
	require.Equal(t.T(), op23.ConflictStage, optimism.ConflictNone)

	require.Contains(t.T(), o.Locks(), lockID)
	synced, remain = o.Locks()[lockID].IsSynced()
	require.False(t.T(), synced)
	require.Equal(t.T(), remain, 1)

	// check ShowLocks.
	expectedLock = []*pb.DDLLock{
		{
			ID:    lockID,
			Task:  task,
			Mode:  config.ShardOptimistic,
			Owner: "",
			DDLs:  nil,
			Synced: []string{
				fmt.Sprintf("%s-%s", source1, dbutil.TableName(i21.UpSchema, i21.UpTable)),
				fmt.Sprintf("%s-%s", source2, dbutil.TableName(i23.UpSchema, i23.UpTable)),
			},
			Unsynced: []string{
				fmt.Sprintf("%s-%s", source1, dbutil.TableName(i12.UpSchema, i12.UpTable)),
			},
		},
	}
	checkLocks(t.T(), o, expectedLock, "", []string{})
	checkLocks(t.T(), o, expectedLock, task, []string{})
	checkLocks(t.T(), o, expectedLock, "", []string{source1})
	checkLocks(t.T(), o, expectedLock, "", []string{source2})
	checkLocks(t.T(), o, expectedLock, "", []string{source1, source2})
	checkLocks(t.T(), o, expectedLock, task, []string{source1, source2})
	checkLocks(t.T(), o, nil, "not-exist", []string{})
	checkLocks(t.T(), o, nil, "", []string{"not-exist"})

	// delete i12 for a table (to simulate `DROP TABLE`), the lock should become synced again.
	rev2, err = optimism.PutInfo(cli, i12) // put i12 first to trigger DELETE for i12.
	require.NoError(t.T(), err)
	// wait until operation for i12 ready.
	_, err = watchExactOneOperation(ctx, cli, i12.Task, i12.Source, i12.UpSchema, i12.UpTable, rev2)
	require.NoError(t.T(), err)

	_, err = optimism.PutSourceTables(cli, st31)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		synced, _ = o.Locks()[lockID].IsSynced()
		return synced
	}, waitFor, tick)
	tts = o.tk.FindTables(task, downSchema, downTable)
	require.Len(t.T(), tts, 2)
	require.Equal(t.T(), source1, tts[0].Source)
	require.Len(t.T(), tts[0].UpTables, 1)
	require.Contains(t.T(), tts[0].UpTables[i21.UpSchema], i21.UpTable)
	require.Equal(t.T(), source2, tts[1].Source)
	require.Len(t.T(), tts[1].UpTables, 1)
	require.Contains(t.T(), tts[1].UpTables[i23.UpSchema], i23.UpTable)
	require.False(t.T(), o.Locks()[lockID].IsResolved())
	require.False(t.T(), o.Locks()[lockID].IsDone(i21.Source, i21.UpSchema, i21.UpTable))
	require.False(t.T(), o.Locks()[lockID].IsDone(i23.Source, i23.UpSchema, i23.UpTable))

	// CASE 4: start again with some previous shard DDL info and non-`done` operation.
	rebuildOptimist(ctx)
	require.Len(t.T(), o.Locks(), 1)
	require.Contains(t.T(), o.Locks(), lockID)
	synced, remain = o.Locks()[lockID].IsSynced()
	require.True(t.T(), synced)
	require.Equal(t.T(), 0, remain)
	require.False(t.T(), o.Locks()[lockID].IsDone(i21.Source, i21.UpSchema, i21.UpTable))
	require.False(t.T(), o.Locks()[lockID].IsDone(i23.Source, i23.UpSchema, i23.UpTable))

	// mark op21 as done.
	op21c := op21
	op21c.Done = true
	_, putted, err = optimism.PutOperation(cli, false, op21c, 0)
	require.NoError(t.T(), err)
	require.True(t.T(), putted)
	require.Eventually(t.T(), func() bool {
		return o.Locks()[lockID].IsDone(i21.Source, i21.UpSchema, i21.UpTable)
	}, waitFor, tick)

	// CASE 5: start again with some previous shard DDL info and `done` operation.
	rebuildOptimist(ctx)
	require.Len(t.T(), o.Locks(), 1)
	require.Contains(t.T(), o.Locks(), lockID)
	synced, remain = o.Locks()[lockID].IsSynced()
	require.True(t.T(), synced)
	require.Equal(t.T(), 0, remain)
	require.True(t.T(), o.Locks()[lockID].IsDone(i21.Source, i21.UpSchema, i21.UpTable))
	require.False(t.T(), o.Locks()[lockID].IsDone(i23.Source, i23.UpSchema, i23.UpTable))

	// mark op23 as done.
	op23c := op23
	op23c.Done = true
	_, putted, err = optimism.PutOperation(cli, false, op23c, 0)
	require.NoError(t.T(), err)
	require.True(t.T(), putted)
	require.Eventually(t.T(), func() bool {
		_, ok := o.Locks()[lockID]
		return !ok
	}, waitFor, tick)
	require.Len(t.T(), o.Locks(), 0)

	// PUT i31, will create a lock but not synced (to test `DROP COLUMN`)
	rev1, err = optimism.PutInfo(cli, i31)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		return len(o.Locks()) == 1
	}, waitFor, tick)
	require.Contains(t.T(), o.Locks(), lockID)
	synced, remain = o.Locks()[lockID].IsSynced()
	require.False(t.T(), synced)
	require.Equal(t.T(), 1, remain)
	// check ShowLocks.
	expectedLock = []*pb.DDLLock{
		{
			ID:    lockID,
			Task:  task,
			Mode:  config.ShardOptimistic,
			Owner: "",
			DDLs:  nil,
			Synced: []string{ // for `DROP COLUMN`, un-dropped is synced (the same with the joined schema)
				fmt.Sprintf("%s-%s", i33.Source, dbutil.TableName(i33.UpSchema, i33.UpTable)),
			},
			Unsynced: []string{ // for `DROP COLUMN`, dropped is un-synced (not the same with the joined schema)
				fmt.Sprintf("%s-%s", i31.Source, dbutil.TableName(i31.UpSchema, i31.UpTable)),
			},
		},
	}
	checkLocks(t.T(), o, expectedLock, "", []string{})

	// wait operation for i31 become available.
	op31, err := watchExactOneOperation(ctx, cli, i31.Task, i31.Source, i31.UpSchema, i31.UpTable, rev1)
	require.NoError(t.T(), err)
	require.Equal(t.T(), []string{}, op31.DDLs)
	require.Equal(t.T(), optimism.ConflictNone, op31.ConflictStage)
	checkLocks(t.T(), o, expectedLock, "", []string{})

	// mark op31 as done.
	op31c := op31
	op31c.Done = true
	_, putted, err = optimism.PutOperation(cli, false, op31c, 0)
	require.NoError(t.T(), err)
	require.True(t.T(), putted)
	require.Eventually(t.T(), func() bool {
		return o.Locks()[lockID].IsDone(op31c.Source, op31c.UpSchema, op31c.UpTable)
	}, waitFor, tick)
	checkLocks(t.T(), o, expectedLock, "", []string{})

	// PUT i33, the lock will be synced.
	rev3, err = optimism.PutInfo(cli, i33)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		synced, _ = o.Locks()[lockID].IsSynced()
		return synced
	}, waitFor, tick)

	expectedLock = []*pb.DDLLock{
		{
			ID:    lockID,
			Task:  task,
			Mode:  config.ShardOptimistic,
			Owner: "",
			DDLs:  nil,
			Synced: []string{
				fmt.Sprintf("%s-%s", i31.Source, dbutil.TableName(i31.UpSchema, i31.UpTable)),
				fmt.Sprintf("%s-%s", i33.Source, dbutil.TableName(i33.UpSchema, i33.UpTable)),
			},
			Unsynced: []string{},
		},
	}
	checkLocks(t.T(), o, expectedLock, "", []string{})

	// wait operation for i33 become available.
	op33, err := watchExactOneOperation(ctx, cli, i33.Task, i33.Source, i33.UpSchema, i33.UpTable, rev3)
	require.NoError(t.T(), err)
	require.Equal(t.T(), DDLs3, op33.DDLs)
	require.Equal(t.T(), optimism.ConflictNone, op33.ConflictStage)
	checkLocks(t.T(), o, expectedLock, "", []string{})

	// mark op33 as done, the lock should be resolved.
	op33c := op33
	op33c.Done = true
	_, putted, err = optimism.PutOperation(cli, false, op33c, 0)
	require.NoError(t.T(), err)
	require.True(t.T(), putted)
	require.Eventually(t.T(), func() bool {
		_, ok := o.Locks()[lockID]
		return !ok
	}, waitFor, tick)
	require.Len(t.T(), o.Locks(), 0)
	checkLocks(t.T(), o, nil, "", nil)

	// CASE 6: start again after all shard DDL locks have been resolved.
	rebuildOptimist(ctx)
	require.Len(t.T(), o.Locks(), 0)
	o.Close()
}

func (t *testOptimistSuite) TestOptimistLockConflict() {
	var (
		watchTimeout       = 5 * time.Second
		logger             = log.L()
		o                  = NewOptimist(&logger, getDownstreamMeta)
		task               = "task-test-optimist"
		source1            = "mysql-replica-1"
		downSchema         = "foo"
		downTable          = "bar"
		st1                = optimism.NewSourceTables(task, source1)
		p                  = parser.New()
		se                 = mock.NewContext()
		tblID        int64 = 222
		DDLs1              = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		DDLs2              = []string{"ALTER TABLE bar ADD COLUMN c1 DATETIME"}
		ti0                = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1                = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2                = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME)`)
		ti3                = ti0
		i1                 = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i2                 = optimism.NewInfo(task, source1, "foo", "bar-2", downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti2})
		i3                 = optimism.NewInfo(task, source1, "foo", "bar-2", downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti3})
	)

	st1.AddTable("foo", "bar-1", downSchema, downTable)
	st1.AddTable("foo", "bar-2", downSchema, downTable)

	// put source tables first.
	_, err := optimism.PutSourceTables(t.etcdTestCli, st1)
	require.NoError(t.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		o.Close()
	}()

	require.NoError(t.T(), o.Start(ctx, t.etcdTestCli))
	require.Len(t.T(), o.Locks(), 0)

	// PUT i1, will create a lock but not synced.
	rev1, err := optimism.PutInfo(t.etcdTestCli, i1)
	require.NoError(t.T(), err)
	// wait operation for i1 become available.
	opCh := make(chan optimism.Operation, 10)
	errCh := make(chan error, 10)
	ctx2, cancel2 := context.WithCancel(ctx)
	go optimism.WatchOperationPut(ctx2, t.etcdTestCli, i1.Task, i1.Source, i1.UpSchema, i1.UpTable, rev1, opCh, errCh)
	select {
	case <-time.After(watchTimeout):
		t.T().Fatal("timeout")
	case op1 := <-opCh:
		require.Equal(t.T(), DDLs1, op1.DDLs)
		require.Equal(t.T(), optimism.ConflictNone, op1.ConflictStage)
	}

	cancel2()
	close(opCh)
	close(errCh)
	require.Equal(t.T(), 0, len(opCh))
	require.Equal(t.T(), 0, len(errCh))

	// PUT i2, conflict will be detected.
	rev2, err := optimism.PutInfo(t.etcdTestCli, i2)
	require.NoError(t.T(), err)
	// wait operation for i2 become available.
	opCh = make(chan optimism.Operation, 10)
	errCh = make(chan error, 10)

	ctx2, cancel2 = context.WithCancel(ctx)
	go optimism.WatchOperationPut(ctx2, t.etcdTestCli, i2.Task, i2.Source, i2.UpSchema, i2.UpTable, rev2, opCh, errCh)
	select {
	case <-time.After(watchTimeout):
		t.T().Fatal("timeout")
	case op2 := <-opCh:
		require.Equal(t.T(), []string{}, op2.DDLs)
		require.Equal(t.T(), optimism.ConflictDetected, op2.ConflictStage)
	}

	cancel2()
	close(opCh)
	close(errCh)
	require.Equal(t.T(), 0, len(opCh))
	require.Equal(t.T(), 0, len(errCh))

	// PUT i3, no conflict now.
	// case for handle-error replace
	rev3, err := optimism.PutInfo(t.etcdTestCli, i3)
	require.NoError(t.T(), err)
	// wait operation for i3 become available.
	opCh = make(chan optimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithCancel(ctx)
	go optimism.WatchOperationPut(ctx2, t.etcdTestCli, i3.Task, i3.Source, i3.UpSchema, i3.UpTable, rev3, opCh, errCh)
	select {
	case <-time.After(watchTimeout):
		t.T().Fatal("timeout")
	case op3 := <-opCh:
		require.Equal(t.T(), []string{}, op3.DDLs)
		require.Equal(t.T(), optimism.ConflictNone, op3.ConflictStage)
	}
	cancel2()
	close(opCh)
	close(errCh)
	require.Equal(t.T(), 0, len(opCh))
	require.Equal(t.T(), 0, len(errCh))
}

func (t *testOptimistSuite) TestOptimistLockMultipleTarget() {
	var (
		tick               = 100 * time.Millisecond
		waitFor            = 30 * tick
		watchTimeout       = 5 * time.Second
		logger             = log.L()
		o                  = NewOptimist(&logger, getDownstreamMeta)
		task               = "test-optimist-lock-multiple-target"
		source             = "mysql-replica-1"
		upSchema           = "foo"
		upTables           = []string{"bar-1", "bar-2", "bar-3", "bar-4"}
		downSchema         = "foo"
		downTable1         = "bar"
		downTable2         = "rab"
		lockID1            = fmt.Sprintf("%s-`%s`.`%s`", task, downSchema, downTable1)
		lockID2            = fmt.Sprintf("%s-`%s`.`%s`", task, downSchema, downTable2)
		sts                = optimism.NewSourceTables(task, source)
		p                  = parser.New()
		se                 = mock.NewContext()
		tblID        int64 = 111
		DDLs               = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		ti0                = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1                = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		i11                = optimism.NewInfo(task, source, upSchema, upTables[0], downSchema, downTable1, DDLs, ti0, []*model.TableInfo{ti1})
		i12                = optimism.NewInfo(task, source, upSchema, upTables[1], downSchema, downTable1, DDLs, ti0, []*model.TableInfo{ti1})
		i21                = optimism.NewInfo(task, source, upSchema, upTables[2], downSchema, downTable2, DDLs, ti0, []*model.TableInfo{ti1})
		i22                = optimism.NewInfo(task, source, upSchema, upTables[3], downSchema, downTable2, DDLs, ti0, []*model.TableInfo{ti1})
	)

	sts.AddTable(upSchema, upTables[0], downSchema, downTable1)
	sts.AddTable(upSchema, upTables[1], downSchema, downTable1)
	sts.AddTable(upSchema, upTables[2], downSchema, downTable2)
	sts.AddTable(upSchema, upTables[3], downSchema, downTable2)

	// put source tables first.
	_, err := optimism.PutSourceTables(t.etcdTestCli, sts)
	require.NoError(t.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		o.Close()
	}()

	require.NoError(t.T(), o.Start(ctx, t.etcdTestCli))
	require.Len(t.T(), o.Locks(), 0)

	// PUT i11 and i21, will create two locks but no synced.
	_, err = optimism.PutInfo(t.etcdTestCli, i11)
	require.NoError(t.T(), err)
	_, err = optimism.PutInfo(t.etcdTestCli, i21)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		return len(o.Locks()) == 2
	}, waitFor, tick)
	require.Contains(t.T(), o.Locks(), lockID1)
	require.Contains(t.T(), o.Locks(), lockID2)

	// check ShowLocks
	expectedLock := map[string]*pb.DDLLock{
		lockID1: {
			ID:    lockID1,
			Task:  task,
			Mode:  config.ShardOptimistic,
			Owner: "",
			DDLs:  nil,
			Synced: []string{
				fmt.Sprintf("%s-%s", i11.Source, dbutil.TableName(i11.UpSchema, i11.UpTable)),
			},
			Unsynced: []string{
				fmt.Sprintf("%s-%s", i12.Source, dbutil.TableName(i12.UpSchema, i12.UpTable)),
			},
		},
		lockID2: {
			ID:    lockID2,
			Task:  task,
			Mode:  config.ShardOptimistic,
			Owner: "",
			DDLs:  nil,
			Synced: []string{
				fmt.Sprintf("%s-%s", i21.Source, dbutil.TableName(i21.UpSchema, i21.UpTable)),
			},
			Unsynced: []string{
				fmt.Sprintf("%s-%s", i22.Source, dbutil.TableName(i22.UpSchema, i22.UpTable)),
			},
		},
	}
	checkLocksByMap(t.T(), o, expectedLock, []string{}, lockID1, lockID2)

	// put i12 and i22, both of locks will be synced.
	rev1, err := optimism.PutInfo(t.etcdTestCli, i12)
	require.NoError(t.T(), err)
	rev2, err := optimism.PutInfo(t.etcdTestCli, i22)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		synced1, _ := o.Locks()[lockID1].IsSynced()
		synced2, _ := o.Locks()[lockID2].IsSynced()
		return synced1 && synced2
	}, waitFor, tick)

	expectedLock[lockID1].Synced = []string{
		fmt.Sprintf("%s-%s", i11.Source, dbutil.TableName(i11.UpSchema, i11.UpTable)),
		fmt.Sprintf("%s-%s", i12.Source, dbutil.TableName(i12.UpSchema, i12.UpTable)),
	}
	expectedLock[lockID1].Unsynced = []string{}
	expectedLock[lockID2].Synced = []string{
		fmt.Sprintf("%s-%s", i21.Source, dbutil.TableName(i21.UpSchema, i21.UpTable)),
		fmt.Sprintf("%s-%s", i22.Source, dbutil.TableName(i22.UpSchema, i22.UpTable)),
	}
	expectedLock[lockID2].Unsynced = []string{}
	checkLocksByMap(t.T(), o, expectedLock, []string{}, lockID1, lockID2)

	// wait operation for i12 become available.
	opCh := make(chan optimism.Operation, 10)
	errCh := make(chan error, 10)
	var op12 optimism.Operation
	ctx2, cancel2 := context.WithCancel(ctx)
	go optimism.WatchOperationPut(ctx2, t.etcdTestCli, i12.Task, i12.Source, i12.UpSchema, i12.UpTable, rev1, opCh, errCh)
	select {
	case <-time.After(watchTimeout):
		t.T().Fatal("timeout")
	case op12 = <-opCh:
		require.Equal(t.T(), DDLs, op12.DDLs)
		require.Equal(t.T(), optimism.ConflictNone, op12.ConflictStage)
	}
	cancel2()
	close(opCh)
	close(errCh)
	require.Equal(t.T(), 0, len(opCh))
	require.Equal(t.T(), 0, len(errCh))

	// mark op11 and op12 as done, the lock should be resolved.
	op11c := op12
	op11c.Done = true
	op11c.UpTable = i11.UpTable // overwrite `UpTable`.
	_, putted, err := optimism.PutOperation(t.etcdTestCli, false, op11c, 0)
	require.NoError(t.T(), err)
	require.True(t.T(), putted)
	op12c := op12
	op12c.Done = true
	_, putted, err = optimism.PutOperation(t.etcdTestCli, false, op12c, 0)
	require.NoError(t.T(), err)
	require.True(t.T(), putted)
	require.Eventually(t.T(), func() bool {
		_, ok := o.Locks()[lockID1]
		return !ok
	}, waitFor, tick)
	require.Len(t.T(), o.Locks(), 1)
	checkLocksByMap(t.T(), o, expectedLock, nil, lockID2)

	// wait operation for i22 become available.
	opCh = make(chan optimism.Operation, 10)
	errCh = make(chan error, 10)
	var op22 optimism.Operation
	ctx2, cancel2 = context.WithCancel(ctx)
	go optimism.WatchOperationPut(ctx2, t.etcdTestCli, i22.Task, i22.Source, i22.UpSchema, i22.UpTable, rev2, opCh, errCh)
	select {
	case <-time.After(watchTimeout):
		t.T().Fatal("timeout")
	case op22 = <-opCh:
		require.Equal(t.T(), DDLs, op22.DDLs)
		require.Equal(t.T(), optimism.ConflictNone, op22.ConflictStage)
	}
	cancel2()
	close(opCh)
	close(errCh)
	require.Equal(t.T(), 0, len(opCh))
	require.Equal(t.T(), 0, len(errCh))

	// mark op21 and op22 as done, the lock should be resolved.
	op21c := op22
	op21c.Done = true
	op21c.UpTable = i21.UpTable // overwrite `UpTable`.
	_, putted, err = optimism.PutOperation(t.etcdTestCli, false, op21c, 0)
	require.NoError(t.T(), err)
	require.True(t.T(), putted)
	op22c := op22
	op22c.Done = true
	_, putted, err = optimism.PutOperation(t.etcdTestCli, false, op22c, 0)
	require.NoError(t.T(), err)
	require.True(t.T(), putted)
	require.Eventually(t.T(), func() bool {
		_, ok := o.Locks()[lockID2]
		return !ok
	}, waitFor, tick)
	require.Len(t.T(), o.Locks(), 0)
	checkLocksByMap(t.T(), o, expectedLock, nil)
}

func (t *testOptimistSuite) TestOptimistInitSchema() {
	var (
		tick         = 100 * time.Millisecond
		waitFor      = 30 * tick
		watchTimeout = 5 * time.Second
		logger       = log.L()
		o            = NewOptimist(&logger, getDownstreamMeta)
		task         = "test-optimist-init-schema"
		source       = "mysql-replica-1"
		upSchema     = "foo"
		upTables     = []string{"bar-1", "bar-2"}
		downSchema   = "foo"
		downTable    = "bar"
		st           = optimism.NewSourceTables(task, source)

		p           = parser.New()
		se          = mock.NewContext()
		tblID int64 = 111
		DDLs1       = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		DDLs2       = []string{"ALTER TABLE bar ADD COLUMN c2 INT"}
		ti0         = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1         = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2         = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 INT)`)
		i11         = optimism.NewInfo(task, source, upSchema, upTables[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i12         = optimism.NewInfo(task, source, upSchema, upTables[1], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i21         = optimism.NewInfo(task, source, upSchema, upTables[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2})
	)

	st.AddTable(upSchema, upTables[0], downSchema, downTable)
	st.AddTable(upSchema, upTables[1], downSchema, downTable)

	// put source tables first.
	_, err := optimism.PutSourceTables(t.etcdTestCli, st)
	require.NoError(t.T(), err)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		o.Close()
	}()

	require.NoError(t.T(), o.Start(ctx, t.etcdTestCli))
	require.Len(t.T(), o.Locks(), 0)

	// PUT i11, will creat a lock.
	_, err = optimism.PutInfo(t.etcdTestCli, i11)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		return len(o.Locks()) == 1
	}, waitFor, tick)
	time.Sleep(tick) // sleep one more time to wait for update of init schema.

	// PUT i12, the lock will be synced.
	rev1, err := optimism.PutInfo(t.etcdTestCli, i12)
	require.NoError(t.T(), err)

	// wait operation for i12 become available.
	opCh := make(chan optimism.Operation, 10)
	errCh := make(chan error, 10)
	var op12 optimism.Operation
	ctx2, cancel2 := context.WithCancel(ctx)
	go optimism.WatchOperationPut(ctx2, t.etcdTestCli, i12.Task, i12.Source, i12.UpSchema, i12.UpTable, rev1, opCh, errCh)
	select {
	case <-time.After(watchTimeout):
		t.T().Fatal("timeout")
	case op12 = <-opCh:
		require.Equal(t.T(), DDLs1, op12.DDLs)
		require.Equal(t.T(), optimism.ConflictNone, op12.ConflictStage)
	}
	cancel2()
	close(opCh)
	close(errCh)
	require.Equal(t.T(), 0, len(opCh))
	require.Equal(t.T(), 0, len(errCh))

	// mark op11 and op12 as done, the lock should be resolved.
	op11c := op12
	op11c.Done = true
	op11c.UpTable = i11.UpTable // overwrite `UpTable`.
	_, putted, err := optimism.PutOperation(t.etcdTestCli, false, op11c, 0)
	require.NoError(t.T(), err)
	require.True(t.T(), putted)
	op12c := op12
	op12c.Done = true
	_, putted, err = optimism.PutOperation(t.etcdTestCli, false, op12c, 0)
	require.NoError(t.T(), err)
	require.True(t.T(), putted)
	require.Eventually(t.T(), func() bool {
		return len(o.Locks()) == 0
	}, waitFor, tick)

	// PUT i21 to create the lock again.
	_, err = optimism.PutInfo(t.etcdTestCli, i21)
	require.NoError(t.T(), err)
	require.Eventually(t.T(), func() bool {
		return len(o.Locks()) == 1
	}, waitFor, tick)
	time.Sleep(tick) // sleep one more time to wait for update of init schema.
}

func (t *testOptimistSuite) testSortInfos(cli *clientv3.Client) {
	defer func() {
		require.NoError(t.T(), optimism.ClearTestInfoOperationColumn(cli))
	}()

	var (
		task       = "test-optimist-init-schema"
		sources    = []string{"mysql-replica-1", "mysql-replica-2"}
		upSchema   = "foo"
		upTables   = []string{"bar-1", "bar-2"}
		downSchema = "foo"
		downTable  = "bar"

		p           = parser.New()
		se          = mock.NewContext()
		tblID int64 = 111
		DDLs1       = []string{"ALTER TABLE bar ADD COLUMN c1 TEXT"}
		DDLs2       = []string{"ALTER TABLE bar ADD COLUMN c2 INT"}
		ti0         = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1         = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2         = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 INT)`)
		i11         = optimism.NewInfo(task, sources[0], upSchema, upTables[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i12         = optimism.NewInfo(task, sources[0], upSchema, upTables[1], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i21         = optimism.NewInfo(task, sources[1], upSchema, upTables[1], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2})
	)

	rev1, err := optimism.PutInfo(cli, i11)
	require.NoError(t.T(), err)
	ifm, _, err := optimism.GetAllInfo(cli)
	require.NoError(t.T(), err)
	infos := sortInfos(ifm)
	require.Equal(t.T(), 1, len(infos))
	i11.Version = 1
	i11.Revision = rev1
	require.Equal(t.T(), i11, infos[0])

	rev2, err := optimism.PutInfo(cli, i12)
	require.NoError(t.T(), err)
	ifm, _, err = optimism.GetAllInfo(cli)
	require.NoError(t.T(), err)
	infos = sortInfos(ifm)
	require.Equal(t.T(), 2, len(infos))
	i11.Version = 1
	i11.Revision = rev1
	i12.Version = 1
	i12.Revision = rev2
	require.Equal(t.T(), i11, infos[0])
	require.Equal(t.T(), i12, infos[1])

	rev3, err := optimism.PutInfo(cli, i21)
	require.NoError(t.T(), err)
	rev4, err := optimism.PutInfo(cli, i11)
	require.NoError(t.T(), err)
	ifm, _, err = optimism.GetAllInfo(cli)
	require.NoError(t.T(), err)
	infos = sortInfos(ifm)
	require.Equal(t.T(), 3, len(infos))

	i11.Version = 2
	i11.Revision = rev4
	i12.Version = 1
	i12.Revision = rev2
	i21.Version = 1
	i21.Revision = rev3
	require.Equal(t.T(), i12, infos[0])
	require.Equal(t.T(), i21, infos[1])
	require.Equal(t.T(), i11, infos[2])
}

func (t *testOptimistSuite) TestBuildLockJoinedAndTable() {
	var (
		logger           = log.L()
		o                = NewOptimist(&logger, getDownstreamMeta)
		task             = "task-build-lock-joined-and-table"
		source1          = "mysql-replica-1"
		source2          = "mysql-replica-2"
		downSchema       = "db"
		downTable        = "tbl"
		st1              = optimism.NewSourceTables(task, source1)
		st2              = optimism.NewSourceTables(task, source2)
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		DDLs2            = []string{"ALTER TABLE bar DROP COLUMN c1"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		ti0              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		ti3              = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c2 INT)`)

		i11 = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i21 = optimism.NewInfo(task, source2, "foo", "bar-1", downSchema, downTable, DDLs2, ti2, []*model.TableInfo{ti3})
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		o.Close()
	}()

	st1.AddTable("foo", "bar-1", downSchema, downTable)
	st2.AddTable("foo", "bar-1", downSchema, downTable)

	require.NoError(t.T(), o.Start(ctx, t.etcdTestCli))
	_, err := optimism.PutSourceTables(t.etcdTestCli, st1)
	require.NoError(t.T(), err)
	_, err = optimism.PutSourceTables(t.etcdTestCli, st2)
	require.NoError(t.T(), err)

	_, err = optimism.PutInfo(t.etcdTestCli, i21)
	require.NoError(t.T(), err)
	_, err = optimism.PutInfo(t.etcdTestCli, i11)
	require.NoError(t.T(), err)

	stm, _, err := optimism.GetAllSourceTables(t.etcdTestCli)
	require.NoError(t.T(), err)
	o.tk.Init(stm)
}

func (t *testOptimistSuite) TestBuildLockWithInitSchema() {
	var (
		logger     = log.L()
		o          = NewOptimist(&logger, getDownstreamMeta)
		task       = "task-lock-with-init-schema"
		source1    = "mysql-replica-1"
		source2    = "mysql-replica-2"
		downSchema = "db"
		downTable  = "tbl"
		st1        = optimism.NewSourceTables(task, source1)
		st2        = optimism.NewSourceTables(task, source2)
		p          = parser.New()
		se         = mock.NewContext()
		tblID      = int64(111)

		ti0 = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (a INT PRIMARY KEY, b INT, c INT)`)
		ti1 = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (a INT PRIMARY KEY, b INT)`)
		ti2 = createTableInfo(t.T(), p, se, tblID, `CREATE TABLE bar (a INT PRIMARY KEY)`)

		ddlDropB  = "ALTER TABLE bar DROP COLUMN b"
		ddlDropC  = "ALTER TABLE bar DROP COLUMN c"
		infoDropB = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, []string{ddlDropC}, ti0, []*model.TableInfo{ti1})
		infoDropC = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, []string{ddlDropB}, ti1, []*model.TableInfo{ti2})
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		o.Close()
	}()

	st1.AddTable("foo", "bar-1", downSchema, downTable)
	st2.AddTable("foo", "bar-1", downSchema, downTable)

	require.NoError(t.T(), o.Start(ctx, t.etcdTestCli))
	_, err := optimism.PutSourceTables(t.etcdTestCli, st1)
	require.NoError(t.T(), err)
	_, err = optimism.PutSourceTables(t.etcdTestCli, st2)
	require.NoError(t.T(), err)

	_, err = optimism.PutInfo(t.etcdTestCli, infoDropB)
	require.NoError(t.T(), err)
	_, err = optimism.PutInfo(t.etcdTestCli, infoDropC)
	require.NoError(t.T(), err)

	stm, _, err := optimism.GetAllSourceTables(t.etcdTestCli)
	require.NoError(t.T(), err)
	o.tk.Init(stm)
}

func getDownstreamMeta(string) (*config.DBConfig, string) {
	return nil, ""
}
