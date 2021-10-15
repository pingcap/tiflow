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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/schemacmp"
	"github.com/pingcap/tidb/util/mock"
	"go.etcd.io/etcd/integration"

	"github.com/pingcap/dm/pkg/utils"
)

type testKeeper struct{}

var _ = Suite(&testKeeper{})

func TestKeeper(t *testing.T) {
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	etcdTestCli = mockCluster.RandClient()

	TestingT(t)
}

func (t *testKeeper) TestLockKeeper(c *C) {
	var (
		lk         = NewLockKeeper()
		upSchema   = "foo_1"
		upTable    = "bar_1"
		downSchema = "foo"
		downTable  = "bar"
		DDLs       = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		task1      = "task1"
		task2      = "task2"
		source1    = "mysql-replica-1"
		source2    = "mysql-replica-2"

		p              = parser.New()
		se             = mock.NewContext()
		tblID    int64 = 111
		tiBefore       = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tiAfter        = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)

		i11 = NewInfo(task1, source1, upSchema, upTable, downSchema, downTable, DDLs, tiBefore, []*model.TableInfo{tiAfter})
		i12 = NewInfo(task1, source2, upSchema, upTable, downSchema, downTable, DDLs, tiBefore, []*model.TableInfo{tiAfter})
		i21 = NewInfo(task2, source1, upSchema, upTable, downSchema, downTable, DDLs, tiBefore, []*model.TableInfo{tiAfter})

		tts1 = []TargetTable{
			newTargetTable(task1, source1, downSchema, downTable, map[string]map[string]struct{}{upSchema: {upTable: struct{}{}}}),
			newTargetTable(task1, source2, downSchema, downTable, map[string]map[string]struct{}{upSchema: {upTable: struct{}{}}}),
		}
		tts2 = []TargetTable{
			newTargetTable(task2, source1, downSchema, downTable, map[string]map[string]struct{}{upSchema: {upTable: struct{}{}}}),
		}
	)

	// lock with 2 sources.
	lockID1, newDDLs, cols, err := lk.TrySync(etcdTestCli, i11, tts1)
	c.Assert(err, IsNil)
	c.Assert(lockID1, Equals, "task1-`foo`.`bar`")
	c.Assert(newDDLs, DeepEquals, DDLs)
	c.Assert(cols, DeepEquals, []string{})
	lock1 := lk.FindLock(lockID1)
	c.Assert(lock1, NotNil)
	c.Assert(lock1.ID, Equals, lockID1)
	c.Assert(lk.FindLockByInfo(i11).ID, Equals, lockID1)
	synced, remain := lock1.IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	lockID1, newDDLs, cols, err = lk.TrySync(etcdTestCli, i12, tts1)
	c.Assert(err, IsNil)
	c.Assert(lockID1, Equals, "task1-`foo`.`bar`")
	c.Assert(newDDLs, DeepEquals, DDLs)
	c.Assert(cols, DeepEquals, []string{})
	lock1 = lk.FindLock(lockID1)
	c.Assert(lock1, NotNil)
	c.Assert(lock1.ID, Equals, lockID1)
	synced, remain = lock1.IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)

	// lock with only 1 source.
	lockID2, newDDLs, cols, err := lk.TrySync(etcdTestCli, i21, tts2)
	c.Assert(err, IsNil)
	c.Assert(lockID2, Equals, "task2-`foo`.`bar`")
	c.Assert(newDDLs, DeepEquals, DDLs)
	c.Assert(cols, DeepEquals, []string{})
	lock2 := lk.FindLock(lockID2)
	c.Assert(lock2, NotNil)
	c.Assert(lock2.ID, Equals, lockID2)
	synced, remain = lock2.IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)

	// try to find not-exists lock.
	lockIDNotExists := "lock-not-exists"
	c.Assert(lk.FindLock(lockIDNotExists), IsNil)

	// all locks.
	locks := lk.Locks()
	c.Assert(locks, HasLen, 2)
	c.Assert(locks[lockID1], Equals, lock1) // compare pointer
	c.Assert(locks[lockID2], Equals, lock2)

	// remove lock.
	c.Assert(lk.RemoveLock(lockID1), IsTrue)
	c.Assert(lk.RemoveLock(lockIDNotExists), IsFalse)
	c.Assert(lk.Locks(), HasLen, 1)

	// clear locks.
	lk.Clear()

	// no locks exist.
	c.Assert(lk.Locks(), HasLen, 0)
}

func (t *testKeeper) TestLockKeeperMultipleTarget(c *C) {
	var (
		lk         = NewLockKeeper()
		task       = "test-lock-keeper-multiple-target"
		source     = "mysql-replica-1"
		upSchema   = "foo"
		upTables   = []string{"bar-1", "bar-2"}
		downSchema = "foo"
		downTable1 = "bar"
		downTable2 = "rab"
		DDLs       = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}

		p              = parser.New()
		se             = mock.NewContext()
		tblID    int64 = 111
		tiBefore       = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tiAfter        = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)

		i11 = NewInfo(task, source, upSchema, upTables[0], downSchema, downTable1, DDLs, tiBefore, []*model.TableInfo{tiAfter})
		i12 = NewInfo(task, source, upSchema, upTables[1], downSchema, downTable1, DDLs, tiBefore, []*model.TableInfo{tiAfter})
		i21 = NewInfo(task, source, upSchema, upTables[0], downSchema, downTable2, DDLs, tiBefore, []*model.TableInfo{tiAfter})
		i22 = NewInfo(task, source, upSchema, upTables[1], downSchema, downTable2, DDLs, tiBefore, []*model.TableInfo{tiAfter})

		tts1 = []TargetTable{
			newTargetTable(task, source, downSchema, downTable1, map[string]map[string]struct{}{
				upSchema: {upTables[0]: struct{}{}, upTables[1]: struct{}{}},
			}),
		}
		tts2 = []TargetTable{
			newTargetTable(task, source, downSchema, downTable2, map[string]map[string]struct{}{
				upSchema: {upTables[0]: struct{}{}, upTables[1]: struct{}{}},
			}),
		}
	)

	// lock for target1.
	lockID1, newDDLs, cols, err := lk.TrySync(etcdTestCli, i11, tts1)
	c.Assert(err, IsNil)
	c.Assert(lockID1, DeepEquals, "test-lock-keeper-multiple-target-`foo`.`bar`")
	c.Assert(newDDLs, DeepEquals, DDLs)
	c.Assert(cols, DeepEquals, []string{})

	// lock for target2.
	lockID2, newDDLs, cols, err := lk.TrySync(etcdTestCli, i21, tts2)
	c.Assert(err, IsNil)
	c.Assert(lockID2, DeepEquals, "test-lock-keeper-multiple-target-`foo`.`rab`")
	c.Assert(newDDLs, DeepEquals, DDLs)
	c.Assert(cols, DeepEquals, []string{})

	// check two locks exist.
	lock1 := lk.FindLock(lockID1)
	c.Assert(lock1, NotNil)
	c.Assert(lock1.ID, Equals, lockID1)
	c.Assert(lk.FindLockByInfo(i11).ID, Equals, lockID1)
	synced, remain := lock1.IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)
	lock2 := lk.FindLock(lockID2)
	c.Assert(lock2, NotNil)
	c.Assert(lock2.ID, Equals, lockID2)
	c.Assert(lk.FindLockByInfo(i21).ID, Equals, lockID2)
	synced, remain = lock2.IsSynced()
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 1)

	// sync for two locks.
	lockID1, newDDLs, cols, err = lk.TrySync(etcdTestCli, i12, tts1)
	c.Assert(err, IsNil)
	c.Assert(lockID1, DeepEquals, "test-lock-keeper-multiple-target-`foo`.`bar`")
	c.Assert(newDDLs, DeepEquals, DDLs)
	c.Assert(cols, DeepEquals, []string{})
	lockID2, newDDLs, cols, err = lk.TrySync(etcdTestCli, i22, tts2)
	c.Assert(err, IsNil)
	c.Assert(lockID2, DeepEquals, "test-lock-keeper-multiple-target-`foo`.`rab`")
	c.Assert(newDDLs, DeepEquals, DDLs)
	c.Assert(cols, DeepEquals, []string{})

	lock1 = lk.FindLock(lockID1)
	c.Assert(lock1, NotNil)
	c.Assert(lock1.ID, Equals, lockID1)
	synced, remain = lock1.IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)
	lock2 = lk.FindLock(lockID2)
	c.Assert(lock2, NotNil)
	c.Assert(lock2.ID, Equals, lockID2)
	synced, remain = lock2.IsSynced()
	c.Assert(synced, IsTrue)
	c.Assert(remain, Equals, 0)
}

func (t *testKeeper) TestTableKeeper(c *C) {
	var (
		tk         = NewTableKeeper()
		task1      = "task-1"
		task2      = "task-2"
		source1    = "mysql-replica-1"
		source2    = "mysql-replica-2"
		downSchema = "db"
		downTable  = "tbl"

		tt11 = newTargetTable(task1, source1, downSchema, downTable, map[string]map[string]struct{}{
			"db": {"tbl-1": struct{}{}, "tbl-2": struct{}{}},
		})
		tt12 = newTargetTable(task1, source2, downSchema, downTable, map[string]map[string]struct{}{
			"db": {"tbl-1": struct{}{}, "tbl-2": struct{}{}},
		})
		tt21 = newTargetTable(task2, source2, downSchema, downTable, map[string]map[string]struct{}{
			"db": {"tbl-3": struct{}{}},
		})
		tt22 = newTargetTable(task2, source2, downSchema, downTable, map[string]map[string]struct{}{
			"db": {"tbl-3": struct{}{}, "tbl-4": struct{}{}},
		})

		st11 = NewSourceTables(task1, source1)
		st12 = NewSourceTables(task1, source2)
		st21 = NewSourceTables(task2, source2)
		st22 = NewSourceTables(task2, source2)
		stm  = map[string]map[string]SourceTables{
			task1: {source2: st12, source1: st11},
		}
	)
	for schema, tables := range tt11.UpTables {
		for table := range tables {
			st11.AddTable(schema, table, tt11.DownSchema, tt11.DownTable)
		}
	}
	for schema, tables := range tt12.UpTables {
		for table := range tables {
			st12.AddTable(schema, table, tt12.DownSchema, tt12.DownTable)
		}
	}
	for schema, tables := range tt21.UpTables {
		for table := range tables {
			st21.AddTable(schema, table, tt21.DownSchema, tt21.DownTable)
		}
	}
	for schema, tables := range tt22.UpTables {
		for table := range tables {
			st22.AddTable(schema, table, tt22.DownSchema, tt22.DownTable)
		}
	}

	// no tables exist before Init/Update.
	c.Assert(tk.FindTables(task1, downSchema, downTable), IsNil)

	// Init with `nil` is fine.
	tk.Init(nil)
	c.Assert(tk.FindTables(task1, downSchema, downTable), IsNil)

	// tables for task1 exit after Init.
	tk.Init(stm)
	tts := tk.FindTables(task1, downSchema, downTable)
	c.Assert(tts, HasLen, 2)
	c.Assert(tts[0], DeepEquals, tt11)
	c.Assert(tts[1], DeepEquals, tt12)

	// adds new tables.
	c.Assert(tk.Update(st21), IsTrue)
	tts = tk.FindTables(task2, downSchema, downTable)
	c.Assert(tts, HasLen, 1)
	c.Assert(tts[0], DeepEquals, tt21)

	// updates/appends new tables.
	c.Assert(tk.Update(st22), IsTrue)
	tts = tk.FindTables(task2, downSchema, downTable)
	c.Assert(tts, HasLen, 1)
	c.Assert(tts[0], DeepEquals, tt22)

	// deletes tables.
	st22.IsDeleted = true
	c.Assert(tk.Update(st22), IsTrue)
	c.Assert(tk.FindTables(task2, downSchema, downTable), IsNil)

	// try to delete, but not exist.
	c.Assert(tk.Update(st22), IsFalse)
	st22.Task = "not-exist"
	c.Assert(tk.Update(st22), IsFalse)

	// tables for task1 not affected.
	tts = tk.FindTables(task1, downSchema, downTable)
	c.Assert(tts, HasLen, 2)
	c.Assert(tts[0], DeepEquals, tt11)
	c.Assert(tts[1], DeepEquals, tt12)

	// add a table for st11.
	c.Assert(tk.AddTable(task1, st11.Source, "db-2", "tbl-3", downSchema, downTable), IsTrue)
	c.Assert(tk.AddTable(task1, st11.Source, "db-2", "tbl-3", downSchema, downTable), IsFalse)
	tts = tk.FindTables(task1, downSchema, downTable)
	st11n := tts[0]
	c.Assert(st11n.UpTables, HasKey, "db-2")
	c.Assert(st11n.UpTables["db-2"], HasKey, "tbl-3")

	// removed the added table in st11.
	c.Assert(tk.RemoveTable(task1, st11.Source, "db-2", "tbl-3", downSchema, downTable), IsTrue)
	c.Assert(tk.RemoveTable(task1, st11.Source, "db-2", "tbl-3", downSchema, downTable), IsFalse)
	tts = tk.FindTables(task1, downSchema, downTable)
	st11n = tts[0]
	c.Assert(st11n.UpTables["db-2"], IsNil)

	// adds for not existing task takes no effect.
	c.Assert(tk.AddTable("not-exist", st11.Source, "db-2", "tbl-3", downSchema, downTable), IsFalse)
	// adds for not existing source takes effect.
	c.Assert(tk.AddTable(task1, "new-source", "db-2", "tbl-3", downSchema, downTable), IsTrue)
	tts = tk.FindTables(task1, downSchema, downTable)
	c.Assert(tts, HasLen, 3)
	c.Assert(tts[2].Source, Equals, "new-source")
	c.Assert(tts[2].UpTables["db-2"], HasKey, "tbl-3")

	// removes for not existing task/source takes no effect.
	c.Assert(tk.RemoveTable("not-exit", st12.Source, "db", "tbl-1", downSchema, downTable), IsFalse)
	c.Assert(tk.RemoveTable(task1, "not-exit", "db", "tbl-1", downSchema, downTable), IsFalse)
	tts = tk.FindTables(task1, downSchema, downTable)
	c.Assert(tts[1], DeepEquals, tt12)
}

func (t *testKeeper) TestTargetTablesForTask(c *C) {
	var (
		tk         = NewTableKeeper()
		task1      = "task1"
		task2      = "task2"
		source1    = "mysql-replica-1"
		source2    = "mysql-replica-2"
		downSchema = "foo"
		downTable1 = "bar"
		downTable2 = "rab"
		stm        = map[string]map[string]SourceTables{
			task1: {source1: NewSourceTables(task1, source1), source2: NewSourceTables(task1, source2)},
			task2: {source1: NewSourceTables(task2, source1), source2: NewSourceTables(task2, source2)},
		}
	)

	// not exist task.
	c.Assert(TargetTablesForTask("not-exist", downSchema, downTable1, stm), IsNil)

	// no tables exist.
	tts := TargetTablesForTask(task1, downSchema, downTable1, stm)
	c.Assert(tts, DeepEquals, []TargetTable{})

	// add some tables.
	tt11 := stm[task1][source1]
	tt11.AddTable("foo-1", "bar-1", downSchema, downTable1)
	tt11.AddTable("foo-1", "bar-2", downSchema, downTable1)
	tt12 := stm[task1][source2]
	tt12.AddTable("foo-2", "bar-3", downSchema, downTable1)
	tt21 := stm[task2][source1]
	tt21.AddTable("foo-3", "bar-1", downSchema, downTable1)
	tt22 := stm[task2][source2]
	tt22.AddTable("foo-4", "bar-2", downSchema, downTable1)
	tt22.AddTable("foo-4", "bar-3", downSchema, downTable1)

	// get tables back.
	tts = TargetTablesForTask(task1, downSchema, downTable1, stm)
	c.Assert(tts, DeepEquals, []TargetTable{
		tt11.TargetTable(downSchema, downTable1),
		tt12.TargetTable(downSchema, downTable1),
	})
	tts = TargetTablesForTask(task2, downSchema, downTable1, stm)
	c.Assert(tts, DeepEquals, []TargetTable{
		tt21.TargetTable(downSchema, downTable1),
		tt22.TargetTable(downSchema, downTable1),
	})

	tk.Init(stm)
	tts = tk.FindTables(task1, downSchema, downTable1)
	c.Assert(tts, DeepEquals, []TargetTable{
		tt11.TargetTable(downSchema, downTable1),
		tt12.TargetTable(downSchema, downTable1),
	})

	// add some tables for another target table.
	c.Assert(tk.AddTable(task1, source1, "foo-1", "bar-3", downSchema, downTable2), IsTrue)
	c.Assert(tk.AddTable(task1, source1, "foo-1", "bar-4", downSchema, downTable2), IsTrue)
	tts = tk.FindTables(task1, downSchema, downTable2)
	c.Assert(tts, DeepEquals, []TargetTable{
		newTargetTable(task1, source1, downSchema, downTable2,
			map[string]map[string]struct{}{
				"foo-1": {"bar-3": struct{}{}, "bar-4": struct{}{}},
			}),
	})
}

func (t *testKeeper) TestRebuildLocksAndTables(c *C) {
	defer clearTestInfoOperation(c)
	var (
		lk               = NewLockKeeper()
		task             = "task"
		source1          = "mysql-replica-1"
		source2          = "mysql-replica-2"
		upSchema         = "foo"
		upTable          = "bar"
		downSchema       = "db"
		downTable        = "tbl"
		DDLs1            = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		DDLs2            = []string{"ALTER TABLE bar DROP COLUMN c1"}
		p                = parser.New()
		se               = mock.NewContext()
		tblID      int64 = 111
		ti0              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		ti1              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)
		ti2              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT, c2 INT)`)
		ti3              = createTableInfo(c, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c2 INT)`)

		i11 = NewInfo(task, source1, upSchema, upTable, downSchema, downTable, DDLs1, ti0, []*model.TableInfo{ti1})
		i21 = NewInfo(task, source2, upSchema, upTable, downSchema, downTable, DDLs2, ti2, []*model.TableInfo{ti3})

		tts = []TargetTable{
			newTargetTable(task, source1, downSchema, downTable, map[string]map[string]struct{}{upSchema: {upTable: struct{}{}}}),
			newTargetTable(task, source2, downSchema, downTable, map[string]map[string]struct{}{upSchema: {upTable: struct{}{}}}),
		}

		lockID = utils.GenDDLLockID(task, downSchema, downTable)

		ifm = map[string]map[string]map[string]map[string]Info{
			task: {
				source1: {upSchema: {upTable: i11}},
				source2: {upSchema: {upTable: i21}},
			},
		}
		colm = map[string]map[string]map[string]map[string]map[string]DropColumnStage{
			lockID: {
				"c3": {
					source1: {upSchema: {upTable: DropNotDone}},
					source2: {upSchema: {upTable: DropNotDone}},
				},
			},
		}
		lockJoined = map[string]schemacmp.Table{
			lockID: schemacmp.Encode(ti2),
		}
		lockTTS = map[string][]TargetTable{
			lockID: tts,
		}
	)

	lk.RebuildLocksAndTables(etcdTestCli, ifm, colm, lockJoined, lockTTS, nil)
	locks := lk.Locks()
	c.Assert(len(locks), Equals, 1)
	lock, ok := locks[lockID]
	c.Assert(ok, IsTrue)
	cmp, err := lock.Joined().Compare(schemacmp.Encode(ti2))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = lock.tables[source1][upSchema][upTable].Compare(schemacmp.Encode(ti0))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	cmp, err = lock.tables[source2][upSchema][upTable].Compare(schemacmp.Encode(ti2))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)
	c.Assert(lock.columns, DeepEquals, colm[lockID])
}
