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

	"github.com/stretchr/testify/require"
)

func TestDeleteInfosOperationsSchema(t *testing.T) {
	defer clearTestInfoOperation(t)

	var (
		task       = "test"
		source     = "mysql-replica-1"
		upSchema   = "foo-1"
		upTable    = "bar-1"
		downSchema = "foo"
		downTable  = "bar"
		DDLs       = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		info       = NewInfo(task, source, upSchema, upTable, downSchema, downTable, DDLs, nil, nil)
		op         = NewOperation("test-ID", task, source, upSchema, upTable, DDLs, ConflictResolved, "", false, []string{})
	)

	// put info.
	rev, err := PutInfo(etcdTestCli, info)
	require.NoError(t, err)
	ifm, _, err := GetAllInfo(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, ifm, 1)
	infoWithVer := info
	infoWithVer.Version = 1
	infoWithVer.Revision = rev
	require.Equal(t, infoWithVer, ifm[task][source][upSchema][upTable])

	// put operation.
	rev, _, err = PutOperation(etcdTestCli, false, op, 0)
	require.NoError(t, err)
	opm, _, err := GetAllOperations(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, opm, 1)
	op.Revision = rev
	require.Equal(t, op, opm[task][source][upSchema][upTable])

	// DELETE info and operation with version 0
	_, deleted, err := DeleteInfosOperationsColumns(etcdTestCli, []Info{info}, []Operation{op}, genDDLLockID(info))
	require.NoError(t, err)
	require.False(t, deleted)

	// data still exist
	ifm, _, err = GetAllInfo(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, ifm, 1)
	opm, _, err = GetAllOperations(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, opm, 1)

	// DELETE info and operation with version 1
	_, deleted, err = DeleteInfosOperationsColumns(etcdTestCli, []Info{infoWithVer}, []Operation{op}, genDDLLockID(infoWithVer))
	require.NoError(t, err)
	require.True(t, deleted)

	// verify no info & operation exist.
	ifm, _, err = GetAllInfo(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, ifm, 0)
	opm, _, err = GetAllOperations(etcdTestCli)
	require.NoError(t, err)
	require.Len(t, opm, 0)
}

func TestSourceTablesInfo(t *testing.T) {
	defer clearTestInfoOperation(t)

	var (
		task       = "task"
		source     = "mysql-replica-1"
		downSchema = "foo"
		downTable  = "bar"
		st1        = NewSourceTables(task, source)
		st2        = NewSourceTables(task, source)
	)

	st1.AddTable("db", "tbl-1", downSchema, downTable)
	st1.AddTable("db", "tbl-2", downSchema, downTable)
	st2.AddTable("db", "tbl-2", downSchema, downTable)
	st2.AddTable("db", "tbl-3", downSchema, downTable)

	// put source tables
	rev1, err := PutSourceTables(etcdTestCli, st1)
	require.NoError(t, err)
	require.Greater(t, rev1, int64(0))

	stm, rev2, err := GetAllSourceTables(etcdTestCli)
	require.NoError(t, err)
	require.Equal(t, rev1, rev2)
	require.Len(t, stm, 1)
	require.Len(t, stm[task], 1)
	require.Equal(t, st1, stm[task][source])

	// put/update source tables
	rev4, err := PutSourceTables(etcdTestCli, st2)
	require.NoError(t, err)
	require.Greater(t, rev4, rev1)

	stm, rev5, err := GetAllSourceTables(etcdTestCli)
	require.NoError(t, err)
	require.Equal(t, rev4, rev5)
	require.Len(t, stm, 1)
	require.Len(t, stm[task], 1)
	require.Equal(t, st2, stm[task][source])
}
