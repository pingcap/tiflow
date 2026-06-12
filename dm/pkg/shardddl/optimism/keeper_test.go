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

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestLockKeeper(t *testing.T) {
	var (
		lk         = NewLockKeeper(getDownstreamMeta)
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
		tiBefore       = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tiAfter        = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)

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
	require.NoError(t, err)
	require.Equal(t, "task1-`foo`.`bar`", lockID1)
	require.Equal(t, DDLs, newDDLs)
	require.Equal(t, []string{}, cols)
	lock1 := lk.FindLock(lockID1)
	require.NotNil(t, lock1)
	require.Equal(t, lockID1, lock1.ID)
	require.Equal(t, lockID1, lk.FindLockByInfo(i11).ID)

	lks := lk.FindLocksByTask("hahaha")
	require.Equal(t, 0, len(lks))
	lks = lk.FindLocksByTask(task1)
	require.Equal(t, 1, len(lks))
	require.Equal(t, lockID1, lks[0].ID)

	synced, remain := lock1.IsSynced()
	require.False(t, synced)
	require.Equal(t, 1, remain)

	lockID1, newDDLs, cols, err = lk.TrySync(etcdTestCli, i12, tts1)
	require.NoError(t, err)
	require.Equal(t, "task1-`foo`.`bar`", lockID1)
	require.Equal(t, DDLs, newDDLs)
	require.Equal(t, []string{}, cols)
	lock1 = lk.FindLock(lockID1)
	require.NotNil(t, lock1)
	require.Equal(t, lockID1, lock1.ID)
	synced, remain = lock1.IsSynced()
	require.True(t, synced)
	require.Equal(t, 0, remain)

	// lock with only 1 source.
	lockID2, newDDLs, cols, err := lk.TrySync(etcdTestCli, i21, tts2)
	require.NoError(t, err)
	require.Equal(t, "task2-`foo`.`bar`", lockID2)
	require.Equal(t, DDLs, newDDLs)
	require.Equal(t, []string{}, cols)
	lock2 := lk.FindLock(lockID2)
	require.NotNil(t, lock2)
	require.Equal(t, lockID2, lock2.ID)
	synced, remain = lock2.IsSynced()
	require.True(t, synced)
	require.Equal(t, 0, remain)

	lks = lk.FindLocksByTask(task1)
	require.Equal(t, 1, len(lks))
	require.Equal(t, lockID1, lks[0].ID)
	lks = lk.FindLocksByTask(task2)
	require.Equal(t, 1, len(lks))
	require.Equal(t, lockID2, lks[0].ID)

	// try to find not-exists lock.
	lockIDNotExists := "lock-not-exists"
	require.Nil(t, lk.FindLock(lockIDNotExists))

	// all locks.
	locks := lk.Locks()
	require.Len(t, locks, 2)
	require.Equal(t, lock1, locks[lockID1]) // compare pointer
	require.Equal(t, lock2, locks[lockID2])

	// remove lock.
	require.True(t, lk.RemoveLock(lockID1))
	require.False(t, lk.RemoveLock(lockIDNotExists))
	require.Len(t, lk.Locks(), 1)

	// clear locks.
	lk.Clear()

	// no locks exist.
	require.Len(t, lk.Locks(), 0)
}

func TestLockKeeperMultipleTarget(t *testing.T) {
	var (
		lk         = NewLockKeeper(getDownstreamMeta)
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
		tiBefore       = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY)`)
		tiAfter        = createTableInfo(t, p, se, tblID, `CREATE TABLE bar (id INT PRIMARY KEY, c1 INT)`)

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
	require.NoError(t, err)
	require.Equal(t, "test-lock-keeper-multiple-target-`foo`.`bar`", lockID1)
	require.Equal(t, DDLs, newDDLs)
	require.Equal(t, []string{}, cols)

	// lock for target2.
	lockID2, newDDLs, cols, err := lk.TrySync(etcdTestCli, i21, tts2)
	require.NoError(t, err)
	require.Equal(t, "test-lock-keeper-multiple-target-`foo`.`rab`", lockID2)
	require.Equal(t, DDLs, newDDLs)
	require.Equal(t, []string{}, cols)

	// check two locks exist.
	lock1 := lk.FindLock(lockID1)
	require.NotNil(t, lock1)
	require.Equal(t, lockID1, lock1.ID)
	require.Equal(t, lockID1, lk.FindLockByInfo(i11).ID)
	synced, remain := lock1.IsSynced()
	require.False(t, synced)
	require.Equal(t, 1, remain)
	lock2 := lk.FindLock(lockID2)
	require.NotNil(t, lock2)
	require.Equal(t, lockID2, lock2.ID)
	require.Equal(t, lockID2, lk.FindLockByInfo(i21).ID)
	synced, remain = lock2.IsSynced()
	require.False(t, synced)
	require.Equal(t, 1, remain)

	// sync for two locks.
	lockID1, newDDLs, cols, err = lk.TrySync(etcdTestCli, i12, tts1)
	require.NoError(t, err)
	require.Equal(t, "test-lock-keeper-multiple-target-`foo`.`bar`", lockID1)
	require.Equal(t, DDLs, newDDLs)
	require.Equal(t, []string{}, cols)
	lockID2, newDDLs, cols, err = lk.TrySync(etcdTestCli, i22, tts2)
	require.NoError(t, err)
	require.Equal(t, "test-lock-keeper-multiple-target-`foo`.`rab`", lockID2)
	require.Equal(t, DDLs, newDDLs)
	require.Equal(t, []string{}, cols)

	lock1 = lk.FindLock(lockID1)
	require.NotNil(t, lock1)
	require.Equal(t, lockID1, lock1.ID)
	synced, remain = lock1.IsSynced()
	require.True(t, synced)
	require.Equal(t, 0, remain)
	lock2 = lk.FindLock(lockID2)
	require.NotNil(t, lock2)
	require.Equal(t, lockID2, lock2.ID)
	synced, remain = lock2.IsSynced()
	require.True(t, synced)
	require.Equal(t, 0, remain)
}

func TestTableKeeper(t *testing.T) {
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
	require.Nil(t, tk.FindTables(task1, downSchema, downTable))
	for schema, tables := range tt11.UpTables {
		for table := range tables {
			require.False(t, tk.SourceTableExist(tt11.Task, tt11.Source, schema, table, downSchema, downTable))
		}
	}

	// Init with `nil` is fine.
	tk.Init(nil)
	require.Nil(t, tk.FindTables(task1, downSchema, downTable))

	// tables for task1 exit after Init.
	tk.Init(stm)
	tts := tk.FindTables(task1, downSchema, downTable)
	require.Len(t, tts, 2)
	require.Equal(t, tt11, tts[0])
	require.Equal(t, tt12, tts[1])
	for schema, tables := range tt11.UpTables {
		for table := range tables {
			require.True(t, tk.SourceTableExist(tt11.Task, tt11.Source, schema, table, downSchema, downTable))
		}
	}

	// adds new tables.
	addTables, dropTables := tk.Update(st21)
	require.Len(t, addTables, 1)
	require.Len(t, dropTables, 0)
	tts = tk.FindTables(task2, downSchema, downTable)
	require.Len(t, tts, 1)
	require.Equal(t, tt21, tts[0])

	// updates/appends new tables.
	addTables, dropTables = tk.Update(st22)
	require.Len(t, addTables, 1)
	require.Len(t, dropTables, 0)
	tts = tk.FindTables(task2, downSchema, downTable)
	require.Len(t, tts, 1)
	require.Equal(t, tt22, tts[0])
	for schema, tables := range tt22.UpTables {
		for table := range tables {
			require.True(t, tk.SourceTableExist(tt22.Task, tt22.Source, schema, table, downSchema, downTable))
		}
	}

	// deletes tables.
	st22.IsDeleted = true
	addTables, dropTables = tk.Update(st22)
	require.Len(t, addTables, 0)
	require.Len(t, dropTables, 2)
	require.Nil(t, tk.FindTables(task2, downSchema, downTable))
	for schema, tables := range tt22.UpTables {
		for table := range tables {
			require.False(t, tk.SourceTableExist(tt22.Task, tt22.Source, schema, table, downSchema, downTable))
		}
	}

	// try to delete, but not exist.
	addTables, dropTables = tk.Update(st22)
	require.Len(t, addTables, 0)
	require.Len(t, dropTables, 0)

	st22.Task = "not-exist"
	addTables, dropTables = tk.Update(st22)
	require.Len(t, addTables, 0)
	require.Len(t, dropTables, 0)

	// tables for task1 not affected.
	tts = tk.FindTables(task1, downSchema, downTable)
	require.Len(t, tts, 2)
	require.Equal(t, tt11, tts[0])
	require.Equal(t, tt12, tts[1])
	for schema, tables := range tt11.UpTables {
		for table := range tables {
			require.True(t, tk.SourceTableExist(tt11.Task, tt11.Source, schema, table, downSchema, downTable))
		}
	}

	// add a table for st11.
	require.True(t, tk.AddTable(task1, st11.Source, "db-2", "tbl-3", downSchema, downTable))
	require.False(t, tk.AddTable(task1, st11.Source, "db-2", "tbl-3", downSchema, downTable))
	tts = tk.FindTables(task1, downSchema, downTable)
	st11n := tts[0]
	require.Contains(t, st11n.UpTables, "db-2")
	require.Contains(t, st11n.UpTables["db-2"], "tbl-3")

	// removed the added table in st11.
	require.True(t, tk.RemoveTable(task1, st11.Source, "db-2", "tbl-3", downSchema, downTable))
	require.False(t, tk.RemoveTable(task1, st11.Source, "db-2", "tbl-3", downSchema, downTable))
	tts = tk.FindTables(task1, downSchema, downTable)
	st11n = tts[0]
	require.Nil(t, st11n.UpTables["db-2"])

	// adds for not existing task takes no effect.
	require.False(t, tk.AddTable("not-exist", st11.Source, "db-2", "tbl-3", downSchema, downTable))
	// adds for not existing source takes effect.
	require.True(t, tk.AddTable(task1, "new-source", "db-2", "tbl-3", downSchema, downTable))
	tts = tk.FindTables(task1, downSchema, downTable)
	require.Len(t, tts, 3)
	require.Equal(t, "new-source", tts[2].Source)
	require.Contains(t, tts[2].UpTables["db-2"], "tbl-3")

	// removes for not existing task/source takes no effect.
	require.False(t, tk.RemoveTable("not-exit", st12.Source, "db", "tbl-1", downSchema, downTable))
	require.False(t, tk.RemoveTable(task1, "not-exit", "db", "tbl-1", downSchema, downTable))
	tts = tk.FindTables(task1, downSchema, downTable)
	require.Equal(t, tt12, tts[1])

	require.False(t, tk.RemoveTableByTask("hahaha"))
	tk.RemoveTableByTaskAndSources("hahaha", nil)
	tts = tk.FindTables(task1, downSchema, downTable)
	require.Len(t, tts, 3)
	tk.RemoveTableByTaskAndSources(task1, []string{"hahaha"})
	tts = tk.FindTables(task1, downSchema, downTable)
	require.Len(t, tts, 3)
	tk.RemoveTableByTaskAndSources(task1, []string{source1, source2})
	tts = tk.FindTables(task1, downSchema, downTable)
	require.Len(t, tts, 1)
	require.Equal(t, "new-source", tts[0].Source)
	require.Contains(t, tts[0].UpTables["db-2"], "tbl-3")
}

func TestTargetTablesForTask(t *testing.T) {
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
	require.Nil(t, TargetTablesForTask("not-exist", downSchema, downTable1, stm))

	// no tables exist.
	tts := TargetTablesForTask(task1, downSchema, downTable1, stm)
	require.Equal(t, []TargetTable{}, tts)

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
	require.Equal(t, []TargetTable{
		tt11.TargetTable(downSchema, downTable1),
		tt12.TargetTable(downSchema, downTable1),
	}, tts)
	tts = TargetTablesForTask(task2, downSchema, downTable1, stm)
	require.Equal(t, []TargetTable{
		tt21.TargetTable(downSchema, downTable1),
		tt22.TargetTable(downSchema, downTable1),
	}, tts)

	tk.Init(stm)
	tts = tk.FindTables(task1, downSchema, downTable1)
	require.Equal(t, []TargetTable{
		tt11.TargetTable(downSchema, downTable1),
		tt12.TargetTable(downSchema, downTable1),
	}, tts)

	// add some tables for another target table.
	require.True(t, tk.AddTable(task1, source1, "foo-1", "bar-3", downSchema, downTable2))
	require.True(t, tk.AddTable(task1, source1, "foo-1", "bar-4", downSchema, downTable2))
	tts = tk.FindTables(task1, downSchema, downTable2)
	require.Equal(t, []TargetTable{
		newTargetTable(task1, source1, downSchema, downTable2,
			map[string]map[string]struct{}{
				"foo-1": {"bar-3": struct{}{}, "bar-4": struct{}{}},
			}),
	}, tts)
}

func getDownstreamMeta(string) (*dbconfig.DBConfig, string) {
	return nil, ""
}

func TestGetDownstreamMeta(t *testing.T) {
	var (
		task1 = "hahaha"
		task2 = "hihihi"
		task3 = "hehehe"
	)
	getDownstreamMetaFunc := func(task string) (*dbconfig.DBConfig, string) {
		switch task {
		case task1, task2:
			return &dbconfig.DBConfig{}, "meta"
		default:
			return nil, ""
		}
	}

	conn.InitMockDB(t)
	lk := NewLockKeeper(getDownstreamMetaFunc)
	require.Len(t, lk.downstreamMetaMap, 0)

	downstreamMeta, err := lk.getDownstreamMeta(task3)
	require.Nil(t, downstreamMeta)
	require.True(t, terror.ErrMasterOptimisticDownstreamMetaNotFound.Equal(err))

	downstreamMeta, err = lk.getDownstreamMeta(task1)
	require.NoError(t, err)
	require.Len(t, lk.downstreamMetaMap, 1)
	require.Equal(t, lk.downstreamMetaMap[task1], downstreamMeta)
	downstreamMeta2, err := lk.getDownstreamMeta(task1)
	require.NoError(t, err)
	require.Len(t, lk.downstreamMetaMap, 1)
	require.Equal(t, downstreamMeta2, downstreamMeta)

	downstreamMeta3, err := lk.getDownstreamMeta(task2)
	require.NoError(t, err)
	require.Len(t, lk.downstreamMetaMap, 2)
	require.Contains(t, lk.downstreamMetaMap, task1)
	require.Contains(t, lk.downstreamMetaMap, task2)
	require.Equal(t, lk.downstreamMetaMap[task2], downstreamMeta3)

	lk.RemoveDownstreamMeta(task3)
	require.Len(t, lk.downstreamMetaMap, 2)
	require.Contains(t, lk.downstreamMetaMap, task1)
	require.Contains(t, lk.downstreamMetaMap, task2)

	lk.RemoveDownstreamMeta(task1)
	require.Len(t, lk.downstreamMetaMap, 1)
	require.Contains(t, lk.downstreamMetaMap, task2)
	require.Equal(t, lk.downstreamMetaMap[task2], downstreamMeta3)

	downstreamMeta, err = lk.getDownstreamMeta(task1)
	require.NoError(t, err)
	require.Len(t, lk.downstreamMetaMap, 2)
	require.Equal(t, lk.downstreamMetaMap[task1], downstreamMeta)
	require.Equal(t, lk.downstreamMetaMap[task2], downstreamMeta3)

	lk.Clear()
	require.Len(t, lk.downstreamMetaMap, 0)
}

func TestUpdateSourceTables(t *testing.T) {
	var (
		tk         = NewTableKeeper()
		task1      = "task-1"
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

		st11 = NewSourceTables(task1, source1)
		st12 = NewSourceTables(task1, source2)
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

	// put st11
	addTables, dropTables := tk.Update(st11)
	require.Len(t, addTables, 2)
	require.Len(t, dropTables, 0)

	// put st11 again
	addTables, dropTables = tk.Update(st11)
	require.Len(t, addTables, 0)
	require.Len(t, dropTables, 0)

	// put st12
	addTables, dropTables = tk.Update(st12)
	require.Len(t, addTables, 2)
	require.Len(t, dropTables, 0)

	// update and put st12
	newST := NewSourceTables(task1, source2)
	for schema, tables := range tt12.UpTables {
		for table := range tables {
			newST.AddTable(schema, table, tt12.DownSchema, tt12.DownTable)
		}
	}
	newST.RemoveTable("db", "tbl-1", downSchema, downTable)
	newST.AddTable("db", "tbl-3", downSchema, downTable)
	addTables, dropTables = tk.Update(newST)
	require.Len(t, addTables, 1)
	require.Len(t, dropTables, 1)
	// put st12 again
	addTables, dropTables = tk.Update(newST)
	require.Len(t, addTables, 0)
	require.Len(t, dropTables, 0)

	// delete source table
	newST.IsDeleted = true
	addTables, dropTables = tk.Update(newST)
	require.Len(t, addTables, 0)
	require.Len(t, dropTables, 2)
}
