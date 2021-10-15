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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/schemacmp"
	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/shardddl/optimism"
	"github.com/pingcap/dm/pkg/utils"
)

type testOptimist struct{}

var _ = SerialSuites(&testOptimist{})

// clear keys in etcd test cluster.
func clearOptimistTestSourceInfoOperation(c *C) {
	c.Assert(optimism.ClearTestInfoOperationSchema(etcdTestCli), IsNil)
}

func createTableInfo(c *C, p *parser.Parser, se sessionctx.Context, tableID int64, sql string) *model.TableInfo {
	node, err := p.ParseOneStmt(sql, "utf8mb4", "utf8mb4_bin")
	if err != nil {
		c.Fatalf("fail to parse stmt, %v", err)
	}
	createStmtNode, ok := node.(*ast.CreateTableStmt)
	if !ok {
		c.Fatalf("%s is not a CREATE TABLE statement", sql)
	}
	info, err := tiddl.MockTableInfo(se, createStmtNode, tableID)
	if err != nil {
		c.Fatalf("fail to create table info, %v", err)
	}
	return info
}

func (t *testOptimist) TestOptimistSourceTables(c *C) {
	defer clearOptimistTestSourceInfoOperation(c)

	var (
		logger     = log.L()
		o          = NewOptimist(&logger)
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
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	c.Assert(o.tk.FindTables(task, downSchema, downTable), IsNil)
	o.Close()
	o.Close() // close multiple times.

	// CASE 2: start again without any previous kv.
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	c.Assert(o.tk.FindTables(task, downSchema, downTable), IsNil)

	// PUT st1, should find tables.
	_, err := optimism.PutSourceTables(etcdTestCli, st1)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		tts := o.tk.FindTables(task, downSchema, downTable)
		return len(tts) == 1
	}), IsTrue)
	tts := o.tk.FindTables(task, downSchema, downTable)
	c.Assert(tts, HasLen, 1)
	c.Assert(tts[0], DeepEquals, st1.TargetTable(downSchema, downTable))
	o.Close()

	// CASE 3: start again with previous source tables.
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	tts = o.tk.FindTables(task, downSchema, downTable)
	c.Assert(tts, HasLen, 1)
	c.Assert(tts[0], DeepEquals, st1.TargetTable(downSchema, downTable))

	// PUT st2, should find more tables.
	_, err = optimism.PutSourceTables(etcdTestCli, st2)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		tts = o.tk.FindTables(task, downSchema, downTable)
		return len(tts) == 2
	}), IsTrue)
	tts = o.tk.FindTables(task, downSchema, downTable)
	c.Assert(tts, HasLen, 2)
	c.Assert(tts[0], DeepEquals, st1.TargetTable(downSchema, downTable))
	c.Assert(tts[1], DeepEquals, st2.TargetTable(downSchema, downTable))
	o.Close()

	// CASE 4: create (not re-start) a new optimist with previous source tables.
	o = NewOptimist(&logger)
	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	tts = o.tk.FindTables(task, downSchema, downTable)
	c.Assert(tts, HasLen, 2)
	c.Assert(tts[0], DeepEquals, st1.TargetTable(downSchema, downTable))
	c.Assert(tts[1], DeepEquals, st2.TargetTable(downSchema, downTable))

	// DELETE st1, should find less tables.
	_, err = optimism.DeleteSourceTables(etcdTestCli, st1)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		tts = o.tk.FindTables(task, downSchema, downTable)
		return len(tts) == 1
	}), IsTrue)
	tts = o.tk.FindTables(task, downSchema, downTable)
	c.Assert(tts, HasLen, 1)
	c.Assert(tts[0], DeepEquals, st2.TargetTable(downSchema, downTable))
	o.Close()
}

func (t *testOptimist) TestOptimist(c *C) {
	cluster := integration.NewClusterV3(tt, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(tt)

	cli := cluster.RandClient()
	t.testOptimist(c, cli, noRestart)
	t.testOptimist(c, cli, restartOnly)
	t.testOptimist(c, cli, restartNewInstance)
	t.testSortInfos(c, cli)
}

func (t *testOptimist) testOptimist(c *C, cli *clientv3.Client, restart int) {
	defer func() {
		c.Assert(optimism.ClearTestInfoOperationSchema(cli), IsNil)
	}()

	var (
		backOff      = 30
		waitTime     = 100 * time.Millisecond
		watchTimeout = 500 * time.Millisecond
		logger       = log.L()
		o            = NewOptimist(&logger)

		rebuildOptimist = func(ctx context.Context) {
			switch restart {
			case restartOnly:
				o.Close()
				c.Assert(o.Start(ctx, cli), IsNil)
			case restartNewInstance:
				o.Close()
				o = NewOptimist(&logger)
				c.Assert(o.Start(ctx, cli), IsNil)
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		ti3              = ti1
		i11              = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i12              = optimism.NewInfo(task, source1, "foo", "bar-2", downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i21              = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2})
		i23              = optimism.NewInfo(task, source2, "foo-2", "bar-3", downSchema, downTable,
			[]string{`CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`}, ti2, []*model.TableInfo{ti2})
		i31 = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, DDLs3, ti2, []*model.TableInfo{ti3})
		i33 = optimism.NewInfo(task, source2, "foo-2", "bar-3", downSchema, downTable, DDLs3, ti2, []*model.TableInfo{ti3})
	)

	st1.AddTable("foo", "bar-1", downSchema, downTable)
	st1.AddTable("foo", "bar-2", downSchema, downTable)
	st31.AddTable("foo", "bar-1", downSchema, downTable)
	st32.AddTable("foo-2", "bar-3", downSchema, downTable)

	// put source tables first.
	_, err := optimism.PutSourceTables(cli, st1)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// CASE 1: start without any previous shard DDL info.
	c.Assert(o.Start(ctx, cli), IsNil)
	c.Assert(o.Locks(), HasLen, 0)
	o.Close()
	o.Close() // close multiple times.

	// CASE 2: start again without any previous shard DDL info.
	c.Assert(o.Start(ctx, cli), IsNil)
	c.Assert(o.Locks(), HasLen, 0)

	// PUT i11, will create a lock but not synced.
	rev1, err := optimism.PutInfo(cli, i11)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		return len(o.Locks()) == 1
	}), IsTrue)
	c.Assert(o.Locks(), HasKey, lockID)
	synced, remain := o.Locks()[lockID].IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

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
	c.Assert(o.ShowLocks("", []string{}), DeepEquals, expectedLock)

	// wait operation for i11 become available.
	opCh := make(chan optimism.Operation, 10)
	errCh := make(chan error, 10)
	ctx2, cancel2 := context.WithCancel(ctx)
	go optimism.WatchOperationPut(ctx2, cli, i11.Task, i11.Source, i11.UpSchema, i11.UpTable, rev1, opCh, errCh)
	utils.WaitSomething(10, watchTimeout, func() bool {
		return len(opCh) != 0
	})
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op11 := <-opCh
	c.Assert(op11.DDLs, DeepEquals, DDLs1)
	c.Assert(op11.ConflictStage, Equals, optimism.ConflictNone)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(o.ShowLocks("", []string{}), DeepEquals, expectedLock)

	// mark op11 as done.
	op11c := op11
	op11c.Done = true
	_, putted, err := optimism.PutOperation(cli, false, op11c, 0)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		lock := o.Locks()[lockID]
		if lock == nil {
			return false
		}
		return lock.IsDone(op11.Source, op11.UpSchema, op11.UpTable)
	}), IsTrue)
	c.Assert(o.Locks()[lockID].IsDone(i12.Source, i12.UpSchema, i12.UpTable), IsFalse)
	c.Assert(o.ShowLocks("", []string{}), DeepEquals, expectedLock)

	// PUT i12, the lock will be synced.
	rev2, err := optimism.PutInfo(cli, i12)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		synced, _ = o.Locks()[lockID].IsSynced()
		return synced
	}), IsTrue)

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
	c.Assert(o.ShowLocks("", []string{}), DeepEquals, expectedLock)

	// wait operation for i12 become available.
	opCh = make(chan optimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithCancel(ctx)
	go optimism.WatchOperationPut(ctx2, cli, i12.Task, i12.Source, i12.UpSchema, i12.UpTable, rev2, opCh, errCh)
	utils.WaitSomething(10, watchTimeout, func() bool {
		return len(opCh) != 0
	})
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op12 := <-opCh
	c.Assert(op12.DDLs, DeepEquals, DDLs1)
	c.Assert(op12.ConflictStage, Equals, optimism.ConflictNone)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(o.ShowLocks("", []string{}), DeepEquals, expectedLock)

	// mark op12 as done, the lock should be resolved.
	op12c := op12
	op12c.Done = true
	_, putted, err = optimism.PutOperation(cli, false, op12c, 0)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		_, ok := o.Locks()[lockID]
		return !ok
	}), IsTrue)
	c.Assert(o.Locks(), HasLen, 0)
	c.Assert(o.ShowLocks("", nil), HasLen, 0)

	// no shard DDL info or lock operation exists.
	ifm, _, err := optimism.GetAllInfo(cli)
	c.Assert(err, IsNil)
	c.Assert(ifm, HasLen, 0)
	opm, _, err := optimism.GetAllOperations(cli)
	c.Assert(err, IsNil)
	c.Assert(opm, HasLen, 0)

	// put another table info.
	rev1, err = optimism.PutInfo(cli, i21)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		return len(o.Locks()) == 1
	}), IsTrue)
	c.Assert(o.Locks(), HasKey, lockID)
	synced, remain = o.Locks()[lockID].IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// wait operation for i21 become available.
	opCh = make(chan optimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithCancel(ctx)
	go optimism.WatchOperationPut(ctx2, cli, i21.Task, i21.Source, i21.UpSchema, i21.UpTable, rev1, opCh, errCh)
	utils.WaitSomething(10, watchTimeout, func() bool {
		return len(opCh) != 0
	})
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op21 := <-opCh
	c.Assert(op21.DDLs, DeepEquals, i21.DDLs)
	c.Assert(op12.ConflictStage, Equals, optimism.ConflictNone)
	c.Assert(len(errCh), Equals, 0)

	// CASE 3: start again with some previous shard DDL info and the lock is un-synced.
	rebuildOptimist(ctx)
	c.Assert(o.Locks(), HasLen, 1)
	c.Assert(o.Locks(), HasKey, lockID)
	synced, remain = o.Locks()[lockID].IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// put table info for a new table (to simulate `CREATE TABLE`).
	rev3, err := optimism.PutSourceTablesInfo(cli, st32, i23)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		ready := o.Locks()[lockID].Ready()
		return ready[source2][i23.UpSchema][i23.UpTable]
	}), IsTrue)
	synced, remain = o.Locks()[lockID].IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)
	tts := o.tk.FindTables(task, downSchema, downTable)
	c.Assert(tts, HasLen, 2)
	c.Assert(tts[1].Source, Equals, source2)
	c.Assert(tts[1].UpTables, HasKey, i23.UpSchema)
	c.Assert(tts[1].UpTables[i23.UpSchema], HasKey, i23.UpTable)

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
	c.Assert(o.ShowLocks("", []string{}), DeepEquals, expectedLock)
	c.Assert(o.ShowLocks(task, []string{}), DeepEquals, expectedLock)
	c.Assert(o.ShowLocks("", []string{source1}), DeepEquals, expectedLock)
	c.Assert(o.ShowLocks("", []string{source2}), DeepEquals, expectedLock)
	c.Assert(o.ShowLocks("", []string{source1, source2}), DeepEquals, expectedLock)
	c.Assert(o.ShowLocks(task, []string{source1, source2}), DeepEquals, expectedLock)
	c.Assert(o.ShowLocks("not-exist", []string{}), HasLen, 0)
	c.Assert(o.ShowLocks("", []string{"not-exist"}), HasLen, 0)

	// wait operation for i23 become available.
	opCh = make(chan optimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithCancel(ctx)
	go optimism.WatchOperationPut(ctx2, cli, i23.Task, i23.Source, i23.UpSchema, i23.UpTable, rev3, opCh, errCh)
	utils.WaitSomething(10, watchTimeout, func() bool {
		return len(opCh) != 0
	})
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op23 := <-opCh
	c.Assert(op23.DDLs, DeepEquals, i23.DDLs)
	c.Assert(op23.ConflictStage, Equals, optimism.ConflictNone)
	c.Assert(len(errCh), Equals, 0)

	// delete i12 for a table (to simulate `DROP TABLE`), the lock should become synced again.
	rev2, err = optimism.PutInfo(cli, i12) // put i12 first to trigger DELETE for i12.
	c.Assert(err, IsNil)
	// wait until operation for i12 ready.
	opCh = make(chan optimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithCancel(ctx)
	go optimism.WatchOperationPut(ctx2, cli, i12.Task, i12.Source, i12.UpSchema, i12.UpTable, rev2, opCh, errCh)
	utils.WaitSomething(10, watchTimeout, func() bool {
		return len(opCh) != 0
	})
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	c.Assert(len(errCh), Equals, 0)

	_, err = optimism.PutSourceTablesDeleteInfo(cli, st31, i12)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		synced, _ = o.Locks()[lockID].IsSynced()
		return synced
	}), IsTrue)
	tts = o.tk.FindTables(task, downSchema, downTable)
	c.Assert(tts, HasLen, 2)
	c.Assert(tts[0].Source, Equals, source1)
	c.Assert(tts[0].UpTables, HasLen, 1)
	c.Assert(tts[0].UpTables[i21.UpSchema], HasKey, i21.UpTable)
	c.Assert(tts[1].Source, Equals, source2)
	c.Assert(tts[1].UpTables, HasLen, 1)
	c.Assert(tts[1].UpTables[i23.UpSchema], HasKey, i23.UpTable)
	c.Assert(o.Locks()[lockID].IsResolved(), IsFalse)
	c.Assert(o.Locks()[lockID].IsDone(i21.Source, i21.UpSchema, i21.UpTable), IsFalse)
	c.Assert(o.Locks()[lockID].IsDone(i23.Source, i23.UpSchema, i23.UpTable), IsFalse)

	// CASE 4: start again with some previous shard DDL info and non-`done` operation.
	rebuildOptimist(ctx)
	c.Assert(o.Locks(), HasLen, 1)
	c.Assert(o.Locks(), HasKey, lockID)
	synced, remain = o.Locks()[lockID].IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)
	c.Assert(o.Locks()[lockID].IsDone(i21.Source, i21.UpSchema, i21.UpTable), IsFalse)
	c.Assert(o.Locks()[lockID].IsDone(i23.Source, i23.UpSchema, i23.UpTable), IsFalse)

	// mark op21 as done.
	op21c := op21
	op21c.Done = true
	_, putted, err = optimism.PutOperation(cli, false, op21c, 0)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		return o.Locks()[lockID].IsDone(i21.Source, i21.UpSchema, i21.UpTable)
	}), IsTrue)

	// CASE 5: start again with some previous shard DDL info and `done` operation.
	rebuildOptimist(ctx)
	c.Assert(o.Locks(), HasLen, 1)
	c.Assert(o.Locks(), HasKey, lockID)
	synced, remain = o.Locks()[lockID].IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)
	c.Assert(o.Locks()[lockID].IsDone(i21.Source, i21.UpSchema, i21.UpTable), IsTrue)
	c.Assert(o.Locks()[lockID].IsDone(i23.Source, i23.UpSchema, i23.UpTable), IsFalse)

	// mark op23 as done.
	op23c := op23
	op23c.Done = true
	_, putted, err = optimism.PutOperation(cli, false, op23c, 0)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		_, ok := o.Locks()[lockID]
		return !ok
	}), IsTrue)
	c.Assert(o.Locks(), HasLen, 0)

	// PUT i31, will create a lock but not synced (to test `DROP COLUMN`)
	rev1, err = optimism.PutInfo(cli, i31)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		return len(o.Locks()) == 1
	}), IsTrue)
	c.Assert(o.Locks(), HasKey, lockID)
	synced, remain = o.Locks()[lockID].IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

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
	c.Assert(o.ShowLocks("", []string{}), DeepEquals, expectedLock)

	// wait operation for i31 become available.
	opCh = make(chan optimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithCancel(ctx)
	go optimism.WatchOperationPut(ctx2, cli, i31.Task, i31.Source, i31.UpSchema, i31.UpTable, rev1, opCh, errCh)
	utils.WaitSomething(10, watchTimeout, func() bool {
		return len(opCh) != 0
	})
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op31 := <-opCh
	c.Assert(op31.DDLs, DeepEquals, []string{})
	c.Assert(op31.ConflictStage, Equals, optimism.ConflictNone)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(o.ShowLocks("", []string{}), DeepEquals, expectedLock)

	// mark op31 as done.
	op31c := op31
	op31c.Done = true
	_, putted, err = optimism.PutOperation(cli, false, op31c, 0)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		return o.Locks()[lockID].IsDone(op31c.Source, op31c.UpSchema, op31c.UpTable)
	}), IsTrue)
	c.Assert(o.ShowLocks("", []string{}), DeepEquals, expectedLock)

	// PUT i33, the lock will be synced.
	rev3, err = optimism.PutInfo(cli, i33)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		synced, _ = o.Locks()[lockID].IsSynced()
		return synced
	}), IsTrue)

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
	c.Assert(o.ShowLocks("", []string{}), DeepEquals, expectedLock)

	// wait operation for i33 become available.
	opCh = make(chan optimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithCancel(ctx)
	go optimism.WatchOperationPut(ctx2, cli, i33.Task, i33.Source, i33.UpSchema, i33.UpTable, rev3, opCh, errCh)
	utils.WaitSomething(10, watchTimeout, func() bool {
		return len(opCh) != 0
	})
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op33 := <-opCh
	c.Assert(op33.DDLs, DeepEquals, DDLs3)
	c.Assert(op33.ConflictStage, Equals, optimism.ConflictNone)
	c.Assert(len(errCh), Equals, 0)
	c.Assert(o.ShowLocks("", []string{}), DeepEquals, expectedLock)

	// mark op33 as done, the lock should be resolved.
	op33c := op33
	op33c.Done = true
	_, putted, err = optimism.PutOperation(cli, false, op33c, 0)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		_, ok := o.Locks()[lockID]
		return !ok
	}), IsTrue)
	c.Assert(o.Locks(), HasLen, 0)
	c.Assert(o.ShowLocks("", nil), HasLen, 0)

	// CASE 6: start again after all shard DDL locks have been resolved.
	rebuildOptimist(ctx)
	c.Assert(o.Locks(), HasLen, 0)
	o.Close()
}

func (t *testOptimist) TestOptimistLockConflict(c *C) {
	defer clearOptimistTestSourceInfoOperation(c)

	var (
		watchTimeout       = 2 * time.Second
		logger             = log.L()
		o                  = NewOptimist(&logger)
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
		ti0                = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1                = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2                = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 DATETIME)`)
		ti3                = ti0
		i1                 = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i2                 = optimism.NewInfo(task, source1, "foo", "bar-2", downSchema, downTable, DDLs2, ti0, []*model.TableInfo{ti2})
		i3                 = optimism.NewInfo(task, source1, "foo", "bar-2", downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti3})
	)

	st1.AddTable("foo", "bar-1", downSchema, downTable)
	st1.AddTable("foo", "bar-2", downSchema, downTable)

	// put source tables first.
	_, err := optimism.PutSourceTables(etcdTestCli, st1)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	c.Assert(o.Locks(), HasLen, 0)

	// PUT i1, will create a lock but not synced.
	rev1, err := optimism.PutInfo(etcdTestCli, i1)
	c.Assert(err, IsNil)
	// wait operation for i1 become available.
	opCh := make(chan optimism.Operation, 10)
	errCh := make(chan error, 10)
	ctx2, cancel2 := context.WithTimeout(ctx, watchTimeout)
	optimism.WatchOperationPut(ctx2, etcdTestCli, i1.Task, i1.Source, i1.UpSchema, i1.UpTable, rev1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op1 := <-opCh
	c.Assert(op1.DDLs, DeepEquals, DDLs1)
	c.Assert(op1.ConflictStage, Equals, optimism.ConflictNone)
	c.Assert(len(errCh), Equals, 0)

	// PUT i2, conflict will be detected.
	rev2, err := optimism.PutInfo(etcdTestCli, i2)
	c.Assert(err, IsNil)
	// wait operation for i2 become available.
	opCh = make(chan optimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	optimism.WatchOperationPut(ctx2, etcdTestCli, i2.Task, i2.Source, i2.UpSchema, i2.UpTable, rev2, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op2 := <-opCh
	c.Assert(op2.DDLs, DeepEquals, []string{})
	c.Assert(op2.ConflictStage, Equals, optimism.ConflictDetected)
	c.Assert(len(errCh), Equals, 0)

	// PUT i3, no conflict now.
	// case for handle-error replace
	rev3, err := optimism.PutInfo(etcdTestCli, i3)
	c.Assert(err, IsNil)
	// wait operation for i3 become available.
	opCh = make(chan optimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	optimism.WatchOperationPut(ctx2, etcdTestCli, i3.Task, i3.Source, i3.UpSchema, i3.UpTable, rev3, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op3 := <-opCh
	c.Assert(op3.DDLs, DeepEquals, []string{})
	c.Assert(op3.ConflictStage, Equals, optimism.ConflictNone)
	c.Assert(len(errCh), Equals, 0)
}

func (t *testOptimist) TestOptimistLockMultipleTarget(c *C) {
	defer clearOptimistTestSourceInfoOperation(c)

	var (
		backOff            = 30
		waitTime           = 100 * time.Millisecond
		watchTimeout       = 2 * time.Second
		logger             = log.L()
		o                  = NewOptimist(&logger)
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
		ti0                = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1                = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
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
	_, err := optimism.PutSourceTables(etcdTestCli, sts)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	c.Assert(o.Locks(), HasLen, 0)

	// PUT i11 and i21, will create two locks but no synced.
	_, err = optimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	_, err = optimism.PutInfo(etcdTestCli, i21)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		return len(o.Locks()) == 2
	}), IsTrue)
	c.Assert(o.Locks(), HasKey, lockID1)
	c.Assert(o.Locks(), HasKey, lockID2)

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
	locks := o.ShowLocks("", []string{})
	c.Assert(locks, HasLen, 2)
	c.Assert(locks[0], DeepEquals, expectedLock[locks[0].ID])
	c.Assert(locks[1], DeepEquals, expectedLock[locks[1].ID])

	// put i12 and i22, both of locks will be synced.
	rev1, err := optimism.PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)
	rev2, err := optimism.PutInfo(etcdTestCli, i22)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		synced1, _ := o.Locks()[lockID1].IsSynced()
		synced2, _ := o.Locks()[lockID2].IsSynced()
		return synced1 && synced2
	}), IsTrue)

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
	locks = o.ShowLocks("", []string{})
	c.Assert(locks, HasLen, 2)
	c.Assert(locks[0], DeepEquals, expectedLock[locks[0].ID])
	c.Assert(locks[1], DeepEquals, expectedLock[locks[1].ID])

	// wait operation for i12 become available.
	opCh := make(chan optimism.Operation, 10)
	errCh := make(chan error, 10)
	ctx2, cancel2 := context.WithTimeout(ctx, watchTimeout)
	optimism.WatchOperationPut(ctx2, etcdTestCli, i12.Task, i12.Source, i12.UpSchema, i12.UpTable, rev1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op12 := <-opCh
	c.Assert(op12.DDLs, DeepEquals, DDLs)
	c.Assert(op12.ConflictStage, Equals, optimism.ConflictNone)
	c.Assert(len(errCh), Equals, 0)

	// mark op11 and op12 as done, the lock should be resolved.
	op11c := op12
	op11c.Done = true
	op11c.UpTable = i11.UpTable // overwrite `UpTable`.
	_, putted, err := optimism.PutOperation(etcdTestCli, false, op11c, 0)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	op12c := op12
	op12c.Done = true
	_, putted, err = optimism.PutOperation(etcdTestCli, false, op12c, 0)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		_, ok := o.Locks()[lockID1]
		return !ok
	}), IsTrue)
	c.Assert(o.Locks(), HasLen, 1)
	c.Assert(o.ShowLocks("", nil), HasLen, 1)
	c.Assert(o.ShowLocks("", nil)[0], DeepEquals, expectedLock[lockID2])

	// wait operation for i22 become available.
	opCh = make(chan optimism.Operation, 10)
	errCh = make(chan error, 10)
	ctx2, cancel2 = context.WithTimeout(ctx, watchTimeout)
	optimism.WatchOperationPut(ctx2, etcdTestCli, i22.Task, i22.Source, i22.UpSchema, i22.UpTable, rev2, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op22 := <-opCh
	c.Assert(op22.DDLs, DeepEquals, DDLs)
	c.Assert(op22.ConflictStage, Equals, optimism.ConflictNone)
	c.Assert(len(errCh), Equals, 0)

	// mark op21 and op22 as done, the lock should be resolved.
	op21c := op22
	op21c.Done = true
	op21c.UpTable = i21.UpTable // overwrite `UpTable`.
	_, putted, err = optimism.PutOperation(etcdTestCli, false, op21c, 0)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	op22c := op22
	op22c.Done = true
	_, putted, err = optimism.PutOperation(etcdTestCli, false, op22c, 0)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		_, ok := o.Locks()[lockID2]
		return !ok
	}), IsTrue)
	c.Assert(o.Locks(), HasLen, 0)
	c.Assert(o.ShowLocks("", nil), HasLen, 0)
}

func (t *testOptimist) TestOptimistInitSchema(c *C) {
	defer clearOptimistTestSourceInfoOperation(c)

	var (
		backOff      = 30
		waitTime     = 100 * time.Millisecond
		watchTimeout = 2 * time.Second
		logger       = log.L()
		o            = NewOptimist(&logger)
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
		ti0         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 INT)`)
		i11         = optimism.NewInfo(task, source, upSchema, upTables[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i12         = optimism.NewInfo(task, source, upSchema, upTables[1], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i21         = optimism.NewInfo(task, source, upSchema, upTables[0], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2})
	)

	st.AddTable(upSchema, upTables[0], downSchema, downTable)
	st.AddTable(upSchema, upTables[1], downSchema, downTable)

	// put source tables first.
	_, err := optimism.PutSourceTables(etcdTestCli, st)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	c.Assert(o.Locks(), HasLen, 0)

	// no init schema exist now.
	is, _, err := optimism.GetInitSchema(etcdTestCli, task, downSchema, downTable)
	c.Assert(err, IsNil)
	c.Assert(is.IsEmpty(), IsTrue)

	// PUT i11, will creat a lock.
	_, err = optimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		return len(o.Locks()) == 1
	}), IsTrue)
	time.Sleep(waitTime) // sleep one more time to wait for update of init schema.

	// the init schema exist now.
	is, _, err = optimism.GetInitSchema(etcdTestCli, task, downSchema, downTable)
	c.Assert(err, IsNil)
	c.Assert(is.TableInfo, DeepEquals, ti0) // the init schema.

	// PUT i12, the lock will be synced.
	rev1, err := optimism.PutInfo(etcdTestCli, i12)
	c.Assert(err, IsNil)

	// wait operation for i12 become available.
	opCh := make(chan optimism.Operation, 10)
	errCh := make(chan error, 10)
	ctx2, cancel2 := context.WithTimeout(ctx, watchTimeout)
	optimism.WatchOperationPut(ctx2, etcdTestCli, i12.Task, i12.Source, i12.UpSchema, i12.UpTable, rev1, opCh, errCh)
	cancel2()
	close(opCh)
	close(errCh)
	c.Assert(len(opCh), Equals, 1)
	op12 := <-opCh
	c.Assert(op12.DDLs, DeepEquals, DDLs1)
	c.Assert(op12.ConflictStage, Equals, optimism.ConflictNone)
	c.Assert(len(errCh), Equals, 0)

	// mark op11 and op12 as done, the lock should be resolved.
	op11c := op12
	op11c.Done = true
	op11c.UpTable = i11.UpTable // overwrite `UpTable`.
	_, putted, err := optimism.PutOperation(etcdTestCli, false, op11c, 0)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	op12c := op12
	op12c.Done = true
	_, putted, err = optimism.PutOperation(etcdTestCli, false, op12c, 0)
	c.Assert(err, IsNil)
	c.Assert(putted, IsTrue)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		return len(o.Locks()) == 0
	}), IsTrue)

	// the init schema should also be deleted.
	is, _, err = optimism.GetInitSchema(etcdTestCli, task, downSchema, downTable)
	c.Assert(err, IsNil)
	c.Assert(is.IsEmpty(), IsTrue)

	// PUT i21 to create the lock again.
	_, err = optimism.PutInfo(etcdTestCli, i21)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(backOff, waitTime, func() bool {
		return len(o.Locks()) == 1
	}), IsTrue)
	time.Sleep(waitTime) // sleep one more time to wait for update of init schema.

	// the init schema exist now.
	is, _, err = optimism.GetInitSchema(etcdTestCli, task, downSchema, downTable)
	c.Assert(err, IsNil)
	c.Assert(is.TableInfo, DeepEquals, ti1) // the init schema is ti1 now.
}

func (t *testOptimist) testSortInfos(c *C, cli *clientv3.Client) {
	defer func() {
		c.Assert(optimism.ClearTestInfoOperationSchema(cli), IsNil)
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
		ti0         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT)`)
		ti2         = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 TEXT, c2 INT)`)
		i11         = optimism.NewInfo(task, sources[0], upSchema, upTables[0], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i12         = optimism.NewInfo(task, sources[0], upSchema, upTables[1], downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i21         = optimism.NewInfo(task, sources[1], upSchema, upTables[1], downSchema, downTable, DDLs2, ti1, []*model.TableInfo{ti2})
	)

	rev1, err := optimism.PutInfo(cli, i11)
	c.Assert(err, IsNil)
	ifm, _, err := optimism.GetAllInfo(cli)
	c.Assert(err, IsNil)
	infos := sortInfos(ifm)
	c.Assert(len(infos), Equals, 1)
	i11.Version = 1
	i11.Revision = rev1
	c.Assert(infos[0], DeepEquals, i11)

	rev2, err := optimism.PutInfo(cli, i12)
	c.Assert(err, IsNil)
	ifm, _, err = optimism.GetAllInfo(cli)
	c.Assert(err, IsNil)
	infos = sortInfos(ifm)
	c.Assert(len(infos), Equals, 2)
	i11.Version = 1
	i11.Revision = rev1
	i12.Version = 1
	i12.Revision = rev2
	c.Assert(infos[0], DeepEquals, i11)
	c.Assert(infos[1], DeepEquals, i12)

	rev3, err := optimism.PutInfo(cli, i21)
	c.Assert(err, IsNil)
	rev4, err := optimism.PutInfo(cli, i11)
	c.Assert(err, IsNil)
	ifm, _, err = optimism.GetAllInfo(cli)
	c.Assert(err, IsNil)
	infos = sortInfos(ifm)
	c.Assert(len(infos), Equals, 3)

	i11.Version = 2
	i11.Revision = rev4
	i12.Version = 1
	i12.Revision = rev2
	i21.Version = 1
	i21.Revision = rev3
	c.Assert(infos[0], DeepEquals, i12)
	c.Assert(infos[1], DeepEquals, i21)
	c.Assert(infos[2], DeepEquals, i11)
}

func (t *testOptimist) TestBuildLockJoinedAndTable(c *C) {
	defer clearOptimistTestSourceInfoOperation(c)

	var (
		logger           = log.L()
		o                = NewOptimist(&logger)
		task             = "task"
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
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		ti3              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c2 INT)`)

		i11 = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i21 = optimism.NewInfo(task, source2, "foo", "bar-1", downSchema, downTable, DDLs2, ti2, []*model.TableInfo{ti3})
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st1.AddTable("foo", "bar-1", downSchema, downTable)
	st2.AddTable("foo", "bar-1", downSchema, downTable)

	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	_, err := optimism.PutSourceTables(etcdTestCli, st1)
	c.Assert(err, IsNil)
	_, err = optimism.PutSourceTables(etcdTestCli, st2)
	c.Assert(err, IsNil)

	_, err = optimism.PutInfo(etcdTestCli, i21)
	c.Assert(err, IsNil)
	_, err = optimism.PutInfo(etcdTestCli, i11)
	c.Assert(err, IsNil)

	stm, _, err := optimism.GetAllSourceTables(etcdTestCli)
	c.Assert(err, IsNil)
	o.tk.Init(stm)

	ifm, _, err := optimism.GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)

	lockJoined, lockTTS, missTable := o.buildLockJoinedAndTTS(ifm, nil)
	c.Assert(len(lockJoined), Equals, 1)
	c.Assert(len(lockTTS), Equals, 1)
	c.Assert(len(missTable), Equals, 0)
	joined, ok := lockJoined[utils.GenDDLLockID(task, downSchema, downTable)]
	c.Assert(ok, IsTrue)
	cmp, err := joined.Compare(schemacmp.Encode(ti2))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
}

func (t *testOptimist) TestBuildLockWithInitSchema(c *C) {
	defer clearOptimistTestSourceInfoOperation(c)

	var (
		logger     = log.L()
		o          = NewOptimist(&logger)
		task       = "task"
		source1    = "mysql-replica-1"
		source2    = "mysql-replica-2"
		downSchema = "db"
		downTable  = "tbl"
		st1        = optimism.NewSourceTables(task, source1)
		st2        = optimism.NewSourceTables(task, source2)
		p          = parser.New()
		se         = mock.NewContext()
		tblID      = int64(111)

		ti0 = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (a INT PRIMARY KEY, b INT, c INT)`)
		ti1 = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (a INT PRIMARY KEY, b INT)`)
		ti2 = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (a INT PRIMARY KEY)`)

		ddlDropB   = "ALTER TABLE bar DROP COLUMN b"
		ddlDropC   = "ALTER TABLE bar DROP COLUMN c"
		infoDropB  = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, []string{ddlDropC}, ti0, []*model.TableInfo{ti1})
		infoDropC  = optimism.NewInfo(task, source1, "foo", "bar-1", downSchema, downTable, []string{ddlDropB}, ti1, []*model.TableInfo{ti2})
		initSchema = optimism.NewInitSchema(task, downSchema, downTable, ti0)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st1.AddTable("foo", "bar-1", downSchema, downTable)
	st2.AddTable("foo", "bar-1", downSchema, downTable)

	c.Assert(o.Start(ctx, etcdTestCli), IsNil)
	_, err := optimism.PutSourceTables(etcdTestCli, st1)
	c.Assert(err, IsNil)
	_, err = optimism.PutSourceTables(etcdTestCli, st2)
	c.Assert(err, IsNil)

	_, err = optimism.PutInfo(etcdTestCli, infoDropB)
	c.Assert(err, IsNil)
	_, err = optimism.PutInfo(etcdTestCli, infoDropC)
	c.Assert(err, IsNil)

	stm, _, err := optimism.GetAllSourceTables(etcdTestCli)
	c.Assert(err, IsNil)
	o.tk.Init(stm)

	ifm, _, err := optimism.GetAllInfo(etcdTestCli)
	c.Assert(err, IsNil)

	initSchemas := map[string]map[string]map[string]optimism.InitSchema{task: {downSchema: {downTable: initSchema}}}
	lockJoined, lockTTS, missTable := o.buildLockJoinedAndTTS(ifm, initSchemas)
	c.Assert(len(lockJoined), Equals, 1)
	c.Assert(len(lockTTS), Equals, 1)
	c.Assert(len(missTable), Equals, 1)
	cmp, err := missTable[utils.GenDDLLockID(task, downSchema, downTable)][source2]["foo"]["bar-1"].Compare(schemacmp.Encode(initSchema.TableInfo))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	joined, ok := lockJoined[utils.GenDDLLockID(task, downSchema, downTable)]
	c.Assert(ok, IsTrue)
	cmp, err = joined.Compare(schemacmp.Encode(ti0))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
}
