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

func TestSourceTablesJSON(t *testing.T) {
	st1 := NewSourceTables("test", "mysql-replica-1")
	st1.AddTable("db1", "tbl1", "db", "tbl")
	j, err := st1.toJSON()
	require.NoError(t, err)
	require.Equal(t, `{"task":"test","source":"mysql-replica-1","tables":{"db":{"tbl":{"db1":{"tbl1":{}}}}}}`, j)
	require.Equal(t, st1.String(), j)

	st2, err := sourceTablesFromJSON(j)
	require.NoError(t, err)
	require.Equal(t, st1, st2)
}

func TestSourceTablesAddRemove(t *testing.T) {
	var (
		task       = "task"
		source     = "mysql-replica-1"
		downSchema = "foo"
		downTable1 = "bar1"
		downTable2 = "bar2"
		st         = NewSourceTables(task, source)
	)

	// no target table exist.
	require.True(t, st.TargetTable(downSchema, downTable1).IsEmpty())
	require.True(t, st.TargetTable(downSchema, downTable2).IsEmpty())

	// add a table for downTable1.
	require.True(t, st.AddTable("foo1", "bar1", downSchema, downTable1))
	require.False(t, st.AddTable("foo1", "bar1", downSchema, downTable1))
	require.Equal(t, newTargetTable(task, source, downSchema, downTable1, map[string]map[string]struct{}{
		"foo1": {"bar1": struct{}{}},
	}), st.TargetTable(downSchema, downTable1))
	require.True(t, st.TargetTable(downSchema, downTable2).IsEmpty())

	// add a table for downTable2.
	require.True(t, st.AddTable("foo2", "bar2", downSchema, downTable2))
	require.False(t, st.AddTable("foo2", "bar2", downSchema, downTable2))
	require.Equal(t, newTargetTable(task, source, downSchema, downTable2, map[string]map[string]struct{}{
		"foo2": {"bar2": struct{}{}},
	}), st.TargetTable(downSchema, downTable2))

	// remove a table for downTable1.
	require.True(t, st.RemoveTable("foo1", "bar1", downSchema, downTable1))
	require.False(t, st.RemoveTable("foo1", "bar1", downSchema, downTable1))
	require.True(t, st.TargetTable(downSchema, downTable1).IsEmpty())

	// remove a table for downTable2.
	require.True(t, st.RemoveTable("foo2", "bar2", downSchema, downTable2))
	require.False(t, st.RemoveTable("foo2", "bar2", downSchema, downTable2))
	require.True(t, st.TargetTable(downSchema, downTable2).IsEmpty())
}

func TestSourceTablesEtcd(t *testing.T) {
	defer clearTestInfoOperation(t)

	var (
		watchTimeout = 2 * time.Second
		task         = "task"
		source1      = "mysql-replica-1"
		source2      = "mysql-replica-2"
		downSchema   = "db"
		downTable    = "tbl"
		st1          = NewSourceTables(task, source1)
		st2          = NewSourceTables(task, source2)
	)

	st1.AddTable("db", "tbl-1", downSchema, downTable)
	st1.AddTable("db", "tbl-2", downSchema, downTable)
	st2.AddTable("db", "tbl-1", downSchema, downTable)
	st2.AddTable("db", "tbl-2", downSchema, downTable)

	// put two SourceTables.
	rev1, err := PutSourceTables(etcdTestCli, st1)
	require.NoError(t, err)
	rev2, err := PutSourceTables(etcdTestCli, st2)
	require.NoError(t, err)
	require.Greater(t, rev2, rev1)

	// get with two SourceTables.
	stm, rev3, err := GetAllSourceTables(etcdTestCli)
	require.NoError(t, err)
	require.Equal(t, rev2, rev3)
	require.Len(t, stm, 1)
	require.Len(t, stm[task], 2)
	require.Equal(t, st1, stm[task][source1])
	require.Equal(t, st2, stm[task][source2])

	// watch with an older revision for all SourceTables.
	wch := make(chan SourceTables, 10)
	ech := make(chan error, 10)
	ctx, cancel := context.WithTimeout(context.Background(), watchTimeout)
	WatchSourceTables(ctx, etcdTestCli, rev1, wch, ech)
	cancel()
	close(wch)
	close(ech)

	// get two source tables.
	require.Equal(t, 2, len(wch))
	require.Equal(t, st1, <-wch)
	require.Equal(t, st2, <-wch)
	require.Equal(t, 0, len(ech))

	// delete tow sources tables.
	_, err = DeleteSourceTables(etcdTestCli, st1)
	require.NoError(t, err)
	rev4, err := DeleteSourceTables(etcdTestCli, st2)
	require.NoError(t, err)

	// get without SourceTables.
	stm, rev5, err := GetAllSourceTables(etcdTestCli)
	require.NoError(t, err)
	require.Equal(t, rev4, rev5)
	require.Len(t, stm, 0)

	// watch the deletion for SourceTables.
	wch = make(chan SourceTables, 10)
	ech = make(chan error, 10)
	ctx, cancel = context.WithTimeout(context.Background(), watchTimeout)
	WatchSourceTables(ctx, etcdTestCli, rev4, wch, ech)
	cancel()
	close(wch)
	close(ech)
	require.Equal(t, 1, len(wch))
	std := <-wch
	require.True(t, std.IsDeleted)
	require.Equal(t, st2.Task, std.Task)
	require.Equal(t, st2.Source, std.Source)
	require.Equal(t, 0, len(ech))
}

func TestToRouteTable(t *testing.T) {
	var (
		task1      = "task-1"
		source1    = "mysql-replica-1"
		downSchema = "db"
		downTable  = "tbl"
		upSchema   = "db"
		upTable1   = "tbl-1"
		upTable2   = "tbl-2"

		tt11 = newTargetTable(task1, source1, downSchema, downTable, map[string]map[string]struct{}{
			upSchema: {upTable1: struct{}{}, upTable2: struct{}{}},
		})

		result = map[RouteTable]struct{}{
			{
				UpSchema:   upSchema,
				UpTable:    upTable1,
				DownSchema: downSchema,
				DownTable:  downTable,
			}: {},
			{
				UpSchema:   upSchema,
				UpTable:    upTable2,
				DownSchema: downSchema,
				DownTable:  downTable,
			}: {},
		}

		st11 = NewSourceTables(task1, source1)
	)

	rt := st11.toRouteTable()
	require.Len(t, rt, 0)

	for schema, tables := range tt11.UpTables {
		for table := range tables {
			st11.AddTable(schema, table, tt11.DownSchema, tt11.DownTable)
		}
	}

	rt = st11.toRouteTable()
	require.Len(t, rt, 2)
	require.Equal(t, result, rt)
}

func TestDiffSourceTable(t *testing.T) {
	var (
		task1      = "task-1"
		source1    = "mysql-replica-1"
		downSchema = "db"
		downTable  = "tbl"
		upSchema   = "db"
		upTable1   = "tbl-1"
		upTable2   = "tbl-2"

		tt11 = newTargetTable(task1, source1, downSchema, downTable, map[string]map[string]struct{}{
			upSchema: {upTable1: struct{}{}, upTable2: struct{}{}},
		})

		result1 = map[RouteTable]struct{}{
			{
				UpSchema:   upSchema,
				UpTable:    upTable1,
				DownSchema: downSchema,
				DownTable:  downTable,
			}: {},
		}
		result2 = map[RouteTable]struct{}{
			{
				UpSchema:   upSchema,
				UpTable:    upTable2,
				DownSchema: downSchema,
				DownTable:  downTable,
			}: {},
		}
		st11 SourceTables
		st12 SourceTables
	)

	addTables, dropTables := DiffSourceTables(st11, st12)
	require.Len(t, addTables, 0)
	require.Len(t, dropTables, 0)

	st11 = NewSourceTables(task1, source1)
	st12 = NewSourceTables(task1, source1)
	addTables, dropTables = DiffSourceTables(st11, st12)
	require.Len(t, addTables, 0)
	require.Len(t, dropTables, 0)

	st11.AddTable(upSchema, upTable1, tt11.DownSchema, tt11.DownTable)

	addTables, dropTables = DiffSourceTables(st11, st12)
	require.Len(t, addTables, 0)
	require.Len(t, dropTables, 1)
	require.Equal(t, dropTables, result1)
	addTables, dropTables = DiffSourceTables(st12, st11)
	require.Len(t, addTables, 1)
	require.Len(t, dropTables, 0)
	require.Equal(t, addTables, result1)

	st12.AddTable(upSchema, upTable2, tt11.DownSchema, tt11.DownTable)
	addTables, dropTables = DiffSourceTables(st11, st12)
	require.Len(t, addTables, 1)
	require.Len(t, dropTables, 1)
	require.Equal(t, addTables, result2)
	require.Equal(t, dropTables, result1)
}
