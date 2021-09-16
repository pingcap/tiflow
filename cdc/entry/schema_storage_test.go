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

package entry

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	ticonfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	tidbkv "github.com/pingcap/tidb/kv"
	timeta "github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestSchema(t *testing.T) {
	dbName := timodel.NewCIStr("Test")
	// db and ignoreDB info
	dbInfo := &timodel.DBInfo{
		ID:    1,
		Name:  dbName,
		State: timodel.StatePublic,
	}
	// `createSchema` job1
	job := &timodel.Job{
		ID:         3,
		State:      timodel.JobStateSynced,
		SchemaID:   1,
		Type:       timodel.ActionCreateSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, FinishedTS: 123},
		Query:      "create database test",
	}
	// reconstruct the local schema
	snap := newEmptySchemaSnapshot(false)
	err := snap.handleDDL(job)
	require.Nil(t, err)
	_, exist := snap.SchemaByID(job.SchemaID)
	require.True(t, exist)

	// test drop schema
	job = &timodel.Job{
		ID:         6,
		State:      timodel.JobStateSynced,
		SchemaID:   1,
		Type:       timodel.ActionDropSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 3, DBInfo: dbInfo, FinishedTS: 124},
		Query:      "drop database test",
	}
	err = snap.handleDDL(job)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(job.SchemaID)
	require.False(t, exist)

	job = &timodel.Job{
		ID:         3,
		State:      timodel.JobStateSynced,
		SchemaID:   1,
		Type:       timodel.ActionCreateSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 2, DBInfo: dbInfo, FinishedTS: 124},
		Query:      "create database test",
	}

	err = snap.handleDDL(job)
	require.Nil(t, err)
	err = snap.handleDDL(job)
	require.True(t, errors.IsAlreadyExists(err))

	// test schema drop schema error
	job = &timodel.Job{
		ID:         9,
		State:      timodel.JobStateSynced,
		SchemaID:   1,
		Type:       timodel.ActionDropSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, FinishedTS: 123},
		Query:      "drop database test",
	}
	err = snap.handleDDL(job)
	require.Nil(t, err)
	err = snap.handleDDL(job)
	require.True(t, errors.IsNotFound(err))
}

func TestTable(t *testing.T) {
	var jobs []*timodel.Job
	dbName := timodel.NewCIStr("Test")
	tbName := timodel.NewCIStr("T")
	colName := timodel.NewCIStr("A")
	idxName := timodel.NewCIStr("idx")
	// column info
	colInfo := &timodel.ColumnInfo{
		ID:        1,
		Name:      colName,
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     timodel.StatePublic,
	}
	// index info
	idxInfo := &timodel.IndexInfo{
		Name:  idxName,
		Table: tbName,
		Columns: []*timodel.IndexColumn{
			{
				Name:   colName,
				Offset: 0,
				Length: 10,
			},
		},
		Unique:  true,
		Primary: true,
		State:   timodel.StatePublic,
	}
	// table info
	tblInfo := &timodel.TableInfo{
		ID:    2,
		Name:  tbName,
		State: timodel.StatePublic,
	}
	// db info
	dbInfo := &timodel.DBInfo{
		ID:    3,
		Name:  dbName,
		State: timodel.StatePublic,
	}

	// `createSchema` job
	job := &timodel.Job{
		ID:         5,
		State:      timodel.JobStateSynced,
		SchemaID:   3,
		Type:       timodel.ActionCreateSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, FinishedTS: 123},
		Query:      "create database " + dbName.O,
	}
	jobs = append(jobs, job)

	// `createTable` job
	job = &timodel.Job{
		ID:         6,
		State:      timodel.JobStateSynced,
		SchemaID:   3,
		TableID:    2,
		Type:       timodel.ActionCreateTable,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 2, TableInfo: tblInfo, FinishedTS: 124},
		Query:      "create table " + tbName.O,
	}
	jobs = append(jobs, job)

	// `addColumn` job
	tblInfo.Columns = []*timodel.ColumnInfo{colInfo}
	job = &timodel.Job{
		ID:         7,
		State:      timodel.JobStateSynced,
		SchemaID:   3,
		TableID:    2,
		Type:       timodel.ActionAddColumn,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 3, TableInfo: tblInfo, FinishedTS: 125},
		Query:      "alter table " + tbName.O + " add column " + colName.O,
	}
	jobs = append(jobs, job)

	// construct a historical `addIndex` job
	tblInfo = tblInfo.Clone()
	tblInfo.Indices = []*timodel.IndexInfo{idxInfo}
	job = &timodel.Job{
		ID:         8,
		State:      timodel.JobStateSynced,
		SchemaID:   3,
		TableID:    2,
		Type:       timodel.ActionAddIndex,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 4, TableInfo: tblInfo, FinishedTS: 126},
		Query:      fmt.Sprintf("alter table %s add index %s(%s)", tbName, idxName, colName),
	}
	jobs = append(jobs, job)

	// reconstruct the local schema
	snap := newEmptySchemaSnapshot(false)
	for _, job := range jobs {
		err := snap.handleDDL(job)
		require.Nil(t, err)
	}

	// check the historical db that constructed above whether in the schema list of local schema
	_, ok := snap.SchemaByID(dbInfo.ID)
	require.True(t, ok)
	// check the historical table that constructed above whether in the table list of local schema
	table, ok := snap.TableByID(tblInfo.ID)
	require.True(t, ok)
	require.Len(t, table.Columns, 1)
	require.Len(t, table.Indices, 1)

	// test ineligible tables
	require.True(t, snap.IsIneligibleTableID(2))

	// check truncate table
	tblInfo1 := &timodel.TableInfo{
		ID:    9,
		Name:  tbName,
		State: timodel.StatePublic,
	}
	job = &timodel.Job{
		ID:         9,
		State:      timodel.JobStateSynced,
		SchemaID:   3,
		TableID:    2,
		Type:       timodel.ActionTruncateTable,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 5, TableInfo: tblInfo1, FinishedTS: 127},
		Query:      "truncate table " + tbName.O,
	}
	preTableInfo, err := snap.PreTableInfo(job)
	require.Nil(t, err)
	require.Equal(t, preTableInfo.TableName, model.TableName{Schema: "Test", Table: "T"})
	require.Equal(t, preTableInfo.ID, int64(2))

	err = snap.handleDDL(job)
	require.Nil(t, err)

	_, ok = snap.TableByID(tblInfo1.ID)
	require.True(t, ok)

	_, ok = snap.TableByID(2)
	require.False(t, ok)

	// test ineligible tables
	require.True(t, snap.IsIneligibleTableID(9))
	require.False(t, snap.IsIneligibleTableID(2))
	// check drop table
	job = &timodel.Job{
		ID:         9,
		State:      timodel.JobStateSynced,
		SchemaID:   3,
		TableID:    9,
		Type:       timodel.ActionDropTable,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 6, FinishedTS: 128},
		Query:      "drop table " + tbName.O,
	}
	preTableInfo, err = snap.PreTableInfo(job)
	require.Nil(t, err)
	require.Equal(t, preTableInfo.TableName, model.TableName{Schema: "Test", Table: "T"})
	require.Equal(t, preTableInfo.ID, int64(9))

	err = snap.handleDDL(job)
	require.Nil(t, err)

	_, ok = snap.TableByID(tblInfo.ID)
	require.False(t, ok)

	// test ineligible tables
	require.False(t, snap.IsIneligibleTableID(9))

	// drop schema
	err = snap.dropSchema(3)
	require.Nil(t, err)
}

func TestHandleDDL(t *testing.T) {
	snap := newEmptySchemaSnapshot(false)
	dbName := timodel.NewCIStr("Test")
	colName := timodel.NewCIStr("A")
	tbName := timodel.NewCIStr("T")
	newTbName := timodel.NewCIStr("RT")

	// db info
	dbInfo := &timodel.DBInfo{
		ID:    2,
		Name:  dbName,
		State: timodel.StatePublic,
	}
	// table Info
	tblInfo := &timodel.TableInfo{
		ID:    6,
		Name:  tbName,
		State: timodel.StatePublic,
	}
	// column info
	colInfo := &timodel.ColumnInfo{
		ID:        8,
		Name:      colName,
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     timodel.StatePublic,
	}
	tblInfo.Columns = []*timodel.ColumnInfo{colInfo}

	testCases := []struct {
		name        string
		jobID       int64
		schemaID    int64
		tableID     int64
		jobType     timodel.ActionType
		binlogInfo  *timodel.HistoryInfo
		query       string
		resultQuery string
		schemaName  string
		tableName   string
	}{
		{name: "createSchema", jobID: 3, schemaID: 2, tableID: 0, jobType: timodel.ActionCreateSchema, binlogInfo: &timodel.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, TableInfo: nil, FinishedTS: 123}, query: "create database Test", resultQuery: "create database Test", schemaName: dbInfo.Name.O, tableName: ""},
		{name: "updateSchema", jobID: 4, schemaID: 2, tableID: 0, jobType: timodel.ActionModifySchemaCharsetAndCollate, binlogInfo: &timodel.HistoryInfo{SchemaVersion: 8, DBInfo: dbInfo, TableInfo: nil, FinishedTS: 123}, query: "ALTER DATABASE Test CHARACTER SET utf8mb4;", resultQuery: "ALTER DATABASE Test CHARACTER SET utf8mb4;", schemaName: dbInfo.Name.O},
		{name: "createTable", jobID: 7, schemaID: 2, tableID: 6, jobType: timodel.ActionCreateTable, binlogInfo: &timodel.HistoryInfo{SchemaVersion: 3, DBInfo: nil, TableInfo: tblInfo, FinishedTS: 123}, query: "create table T(id int);", resultQuery: "create table T(id int);", schemaName: dbInfo.Name.O, tableName: tblInfo.Name.O},
		{name: "addColumn", jobID: 9, schemaID: 2, tableID: 6, jobType: timodel.ActionAddColumn, binlogInfo: &timodel.HistoryInfo{SchemaVersion: 4, DBInfo: nil, TableInfo: tblInfo, FinishedTS: 123}, query: "alter table T add a varchar(45);", resultQuery: "alter table T add a varchar(45);", schemaName: dbInfo.Name.O, tableName: tblInfo.Name.O},
		{name: "truncateTable", jobID: 10, schemaID: 2, tableID: 6, jobType: timodel.ActionTruncateTable, binlogInfo: &timodel.HistoryInfo{SchemaVersion: 5, DBInfo: nil, TableInfo: tblInfo, FinishedTS: 123}, query: "truncate table T;", resultQuery: "truncate table T;", schemaName: dbInfo.Name.O, tableName: tblInfo.Name.O},
		{name: "renameTable", jobID: 11, schemaID: 2, tableID: 10, jobType: timodel.ActionRenameTable, binlogInfo: &timodel.HistoryInfo{SchemaVersion: 6, DBInfo: nil, TableInfo: tblInfo, FinishedTS: 123}, query: "rename table T to RT;", resultQuery: "rename table T to RT;", schemaName: dbInfo.Name.O, tableName: newTbName.O},
		{name: "dropTable", jobID: 12, schemaID: 2, tableID: 12, jobType: timodel.ActionDropTable, binlogInfo: &timodel.HistoryInfo{SchemaVersion: 7, DBInfo: nil, TableInfo: nil, FinishedTS: 123}, query: "drop table RT;", resultQuery: "drop table RT;", schemaName: dbInfo.Name.O, tableName: newTbName.O},
		{name: "dropSchema", jobID: 13, schemaID: 2, tableID: 0, jobType: timodel.ActionDropSchema, binlogInfo: &timodel.HistoryInfo{SchemaVersion: 8, DBInfo: dbInfo, TableInfo: nil, FinishedTS: 123}, query: "drop database test;", resultQuery: "drop database test;", schemaName: dbInfo.Name.O, tableName: ""},
	}

	for _, testCase := range testCases {
		// prepare for ddl
		switch testCase.name {
		case "addColumn":
			tblInfo.Columns = []*timodel.ColumnInfo{colInfo}
		case "truncateTable":
			tblInfo.ID = 10
		case "renameTable":
			tblInfo.ID = 12
			tblInfo.Name = newTbName
		}

		job := &timodel.Job{
			ID:         testCase.jobID,
			State:      timodel.JobStateDone,
			SchemaID:   testCase.schemaID,
			TableID:    testCase.tableID,
			Type:       testCase.jobType,
			BinlogInfo: testCase.binlogInfo,
			Query:      testCase.query,
		}
		testDoDDLAndCheck(t, snap, job, false)

		// custom check after ddl
		switch testCase.name {
		case "createSchema":
			_, ok := snap.SchemaByID(dbInfo.ID)
			require.True(t, ok)
		case "createTable":
			_, ok := snap.TableByID(tblInfo.ID)
			require.True(t, ok)
		case "renameTable":
			tb, ok := snap.TableByID(tblInfo.ID)
			require.True(t, ok)
			require.Equal(t, tblInfo.Name, tb.Name)
		case "addColumn", "truncateTable":
			tb, ok := snap.TableByID(tblInfo.ID)
			require.True(t, ok)
			require.Len(t, tb.Columns, 1)
		case "dropTable":
			_, ok := snap.TableByID(tblInfo.ID)
			require.False(t, ok)
		case "dropSchema":
			_, ok := snap.SchemaByID(job.SchemaID)
			require.False(t, ok)
		}
	}
}

func testDoDDLAndCheck(t *testing.T, snap *schemaSnapshot, job *timodel.Job, isErr bool) {
	err := snap.handleDDL(job)
	require.Equal(t, err != nil, isErr)
}

func TestPKShouldBeInTheFirstPlaceWhenPKIsNotHandle(t *testing.T) {
	tblInfo := timodel.TableInfo{
		Columns: []*timodel.ColumnInfo{
			{
				Name: timodel.CIStr{O: "name"},
				FieldType: types.FieldType{
					Flag: mysql.NotNullFlag,
				},
			},
			{Name: timodel.CIStr{O: "id"}},
		},
		Indices: []*timodel.IndexInfo{
			{
				Name: timodel.CIStr{
					O: "name",
				},
				Columns: []*timodel.IndexColumn{
					{
						Name:   timodel.CIStr{O: "name"},
						Offset: 0,
					},
				},
				Unique: true,
			},
			{
				Name: timodel.CIStr{
					O: "PRIMARY",
				},
				Columns: []*timodel.IndexColumn{
					{
						Name:   timodel.CIStr{O: "id"},
						Offset: 1,
					},
				},
				Primary: true,
			},
		},
		PKIsHandle: false,
	}
	info := model.WrapTableInfo(1, "", 0, &tblInfo)
	cols := info.GetUniqueKeys()
	require.Equal(t, cols, [][]string{
		{"id"}, {"name"},
	})
}

func TestPKShouldBeInTheFirstPlaceWhenPKIsHandle(t *testing.T) {
	tblInfo := timodel.TableInfo{
		Indices: []*timodel.IndexInfo{
			{
				Name: timodel.CIStr{
					O: "uniq_job",
				},
				Columns: []*timodel.IndexColumn{
					{Name: timodel.CIStr{O: "job"}},
				},
				Unique: true,
			},
		},
		Columns: []*timodel.ColumnInfo{
			{
				Name: timodel.CIStr{
					O: "job",
				},
				FieldType: types.FieldType{
					Flag: mysql.NotNullFlag,
				},
			},
			{
				Name: timodel.CIStr{
					O: "uid",
				},
				FieldType: types.FieldType{
					Flag: mysql.PriKeyFlag,
				},
			},
		},
		PKIsHandle: true,
	}
	info := model.WrapTableInfo(1, "", 0, &tblInfo)
	cols := info.GetUniqueKeys()
	require.Equal(t, cols, [][]string{
		{"uid"}, {"job"},
	})
}

func TestMultiVersionStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dbName := timodel.NewCIStr("Test")
	tbName := timodel.NewCIStr("T1")
	// db and ignoreDB info
	dbInfo := &timodel.DBInfo{
		ID:    1,
		Name:  dbName,
		State: timodel.StatePublic,
	}
	var jobs []*timodel.Job
	// `createSchema` job1
	job := &timodel.Job{
		ID:         3,
		State:      timodel.JobStateSynced,
		SchemaID:   1,
		Type:       timodel.ActionCreateSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, FinishedTS: 100},
		Query:      "create database test",
	}
	jobs = append(jobs, job)

	// table info
	tblInfo := &timodel.TableInfo{
		ID:    2,
		Name:  tbName,
		State: timodel.StatePublic,
	}

	// `createTable` job
	job = &timodel.Job{
		ID:         6,
		State:      timodel.JobStateSynced,
		SchemaID:   1,
		TableID:    2,
		Type:       timodel.ActionCreateTable,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 2, TableInfo: tblInfo, FinishedTS: 110},
		Query:      "create table " + tbName.O,
	}

	jobs = append(jobs, job)

	tbName = timodel.NewCIStr("T2")
	// table info
	tblInfo = &timodel.TableInfo{
		ID:    3,
		Name:  tbName,
		State: timodel.StatePublic,
	}
	// `createTable` job
	job = &timodel.Job{
		ID:         6,
		State:      timodel.JobStateSynced,
		SchemaID:   1,
		TableID:    3,
		Type:       timodel.ActionCreateTable,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 2, TableInfo: tblInfo, FinishedTS: 120},
		Query:      "create table " + tbName.O,
	}

	jobs = append(jobs, job)
	storage, err := NewSchemaStorage(nil, 0, nil, false)
	require.Nil(t, err)
	for _, job := range jobs {
		err := storage.HandleDDLJob(job)
		require.Nil(t, err)
	}

	// `dropTable` job
	job = &timodel.Job{
		ID:         6,
		State:      timodel.JobStateSynced,
		SchemaID:   1,
		TableID:    2,
		Type:       timodel.ActionDropTable,
		BinlogInfo: &timodel.HistoryInfo{FinishedTS: 130},
	}

	err = storage.HandleDDLJob(job)
	require.Nil(t, err)

	// `dropSchema` job
	job = &timodel.Job{
		ID:         6,
		State:      timodel.JobStateSynced,
		SchemaID:   1,
		Type:       timodel.ActionDropSchema,
		BinlogInfo: &timodel.HistoryInfo{FinishedTS: 140, DBInfo: dbInfo},
	}

	err = storage.HandleDDLJob(job)
	require.Nil(t, err)

	require.Equal(t, storage.(*schemaStorageImpl).resolvedTs, uint64(140))
	snap, err := storage.GetSnapshot(ctx, 100)
	require.Nil(t, err)
	_, exist := snap.SchemaByID(1)
	require.True(t, exist)
	_, exist = snap.TableByID(2)
	require.False(t, exist)
	_, exist = snap.TableByID(3)
	require.False(t, exist)

	snap, err = storage.GetSnapshot(ctx, 115)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(1)
	require.True(t, exist)
	_, exist = snap.TableByID(2)
	require.True(t, exist)
	_, exist = snap.TableByID(3)
	require.False(t, exist)

	snap, err = storage.GetSnapshot(ctx, 125)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(1)
	require.True(t, exist)
	_, exist = snap.TableByID(2)
	require.True(t, exist)
	_, exist = snap.TableByID(3)
	require.True(t, exist)

	snap, err = storage.GetSnapshot(ctx, 135)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(1)
	require.True(t, exist)
	_, exist = snap.TableByID(2)
	require.False(t, exist)
	_, exist = snap.TableByID(3)
	require.True(t, exist)

	snap, err = storage.GetSnapshot(ctx, 140)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(1)
	require.False(t, exist)
	_, exist = snap.TableByID(2)
	require.False(t, exist)
	_, exist = snap.TableByID(3)
	require.False(t, exist)

	storage.DoGC(0)
	snap, err = storage.GetSnapshot(ctx, 100)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(1)
	require.True(t, exist)
	_, exist = snap.TableByID(2)
	require.False(t, exist)
	_, exist = snap.TableByID(3)
	require.False(t, exist)
	storage.DoGC(115)
	_, err = storage.GetSnapshot(ctx, 100)
	require.NotNil(t, err)
	snap, err = storage.GetSnapshot(ctx, 115)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(1)
	require.True(t, exist)
	_, exist = snap.TableByID(2)
	require.True(t, exist)
	_, exist = snap.TableByID(3)
	require.False(t, exist)

	storage.DoGC(155)
	storage.AdvanceResolvedTs(185)

	snap, err = storage.GetSnapshot(ctx, 180)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(1)
	require.False(t, exist)
	_, exist = snap.TableByID(2)
	require.False(t, exist)
	_, exist = snap.TableByID(3)
	require.False(t, exist)
	_, err = storage.GetSnapshot(ctx, 130)
	require.NotNil(t, err)

	cancel()
	_, err = storage.GetSnapshot(ctx, 200)
	require.Equal(t, errors.Cause(err), context.Canceled)
}

func TestCreateSnapFromMeta(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.Nil(t, err)
	defer store.Close() //nolint:errcheck

	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	require.Nil(t, err)
	defer domain.Close()
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database test2")
	tk.MustExec("create table test.simple_test1 (id bigint primary key)")
	tk.MustExec("create table test.simple_test2 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test3 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test4 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test5 (a bigint)")
	ver, err := store.CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	meta, err := kv.GetSnapshotMeta(store, ver.Ver)
	require.Nil(t, err)
	snap, err := newSchemaSnapshotFromMeta(meta, ver.Ver, false)
	require.Nil(t, err)
	_, ok := snap.GetTableByName("test", "simple_test1")
	require.True(t, ok)
	tableID, ok := snap.GetTableIDByName("test2", "simple_test5")
	require.True(t, ok)
	require.True(t, snap.IsIneligibleTableID(tableID))
	dbInfo, ok := snap.SchemaByTableID(tableID)
	require.True(t, ok)
	require.Equal(t, dbInfo.Name.O, "test2")
	require.Len(t, snap.tableInSchema, 3)
}

func TestSnapshotClone(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.Nil(t, err)
	defer store.Close() //nolint:errcheck

	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	require.Nil(t, err)
	defer domain.Close()
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database test2")
	tk.MustExec("create table test.simple_test1 (id bigint primary key)")
	tk.MustExec("create table test.simple_test2 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test3 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test4 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test5 (a bigint)")
	ver, err := store.CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	meta, err := kv.GetSnapshotMeta(store, ver.Ver)
	require.Nil(t, err)
	snap, err := newSchemaSnapshotFromMeta(meta, ver.Ver, false /* explicitTables */)
	require.Nil(t, err)

	clone := snap.Clone()
	require.Equal(t, clone.tableNameToID, snap.tableNameToID)
	require.Equal(t, clone.schemaNameToID, snap.schemaNameToID)
	require.Equal(t, clone.truncateTableID, snap.truncateTableID)
	require.Equal(t, clone.ineligibleTableID, snap.ineligibleTableID)
	require.Equal(t, clone.currentTs, snap.currentTs)
	require.Equal(t, clone.explicitTables, snap.explicitTables)
	require.Equal(t, len(clone.tables), len(snap.tables))
	require.Equal(t, len(clone.schemas), len(snap.schemas))
	require.Equal(t, len(clone.partitionTable), len(snap.partitionTable))

	tableCount := len(snap.tables)
	clone.tables = make(map[int64]*model.TableInfo)
	require.Len(t, snap.tables, tableCount)
}

func TestExplicitTables(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.Nil(t, err)
	defer store.Close() //nolint:errcheck

	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	require.Nil(t, err)
	defer domain.Close()
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(t, store)
	ver1, err := store.CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	tk.MustExec("create database test2")
	tk.MustExec("create table test.simple_test1 (id bigint primary key)")
	tk.MustExec("create table test.simple_test2 (id bigint unique key)")
	tk.MustExec("create table test2.simple_test3 (a bigint)")
	tk.MustExec("create table test2.simple_test4 (a varchar(20) unique key)")
	tk.MustExec("create table test2.simple_test5 (a varchar(20))")
	ver2, err := store.CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	meta1, err := kv.GetSnapshotMeta(store, ver1.Ver)
	require.Nil(t, err)
	snap1, err := newSchemaSnapshotFromMeta(meta1, ver1.Ver, true /* explicitTables */)
	require.Nil(t, err)
	meta2, err := kv.GetSnapshotMeta(store, ver2.Ver)
	require.Nil(t, err)
	snap2, err := newSchemaSnapshotFromMeta(meta2, ver2.Ver, false /* explicitTables */)
	require.Nil(t, err)
	snap3, err := newSchemaSnapshotFromMeta(meta2, ver2.Ver, true /* explicitTables */)
	require.Nil(t, err)

	require.Equal(t, len(snap2.tables)-len(snap1.tables), 5)
	// some system tables are also ineligible
	require.GreaterOrEqual(t, len(snap2.ineligibleTableID), 4)

	require.Equal(t, len(snap3.tables)-len(snap1.tables), 5)
	require.Len(t, snap3.ineligibleTableID, 0)
}

/*
TODO: Untested Action:

ActionAddForeignKey                 ActionType = 9
ActionDropForeignKey                ActionType = 10
ActionRebaseAutoID                  ActionType = 13
ActionShardRowID                    ActionType = 16
ActionLockTable                     ActionType = 27
ActionUnlockTable                   ActionType = 28
ActionRepairTable                   ActionType = 29
ActionSetTiFlashReplica             ActionType = 30
ActionUpdateTiFlashReplicaStatus    ActionType = 31
ActionCreateSequence                ActionType = 34
ActionAlterSequence                 ActionType = 35
ActionDropSequence                  ActionType = 36
ActionModifyTableAutoIdCache        ActionType = 39
ActionRebaseAutoRandomBase          ActionType = 40
ActionExchangeTablePartition        ActionType = 42
ActionAddCheckConstraint            ActionType = 43
ActionDropCheckConstraint           ActionType = 44
ActionAlterCheckConstraint          ActionType = 45
ActionAlterTableAlterPartition      ActionType = 46

... Any Action which of value is greater than 46 ...
*/
func TestSchemaStorage(t *testing.T) {
	ctx := context.Background()
	testCases := [][]string{{
		"create database test_ddl1",                                                               // ActionCreateSchema
		"create table test_ddl1.simple_test1 (id bigint primary key)",                             // ActionCreateTable
		"create table test_ddl1.simple_test2 (id bigint)",                                         // ActionCreateTable
		"create table test_ddl1.simple_test3 (id bigint primary key)",                             // ActionCreateTable
		"create table test_ddl1.simple_test4 (id bigint primary key)",                             // ActionCreateTable
		"DROP TABLE test_ddl1.simple_test3",                                                       // ActionDropTable
		"ALTER TABLE test_ddl1.simple_test1 ADD COLUMN c1 INT NOT NULL",                           // ActionAddColumn
		"ALTER TABLE test_ddl1.simple_test1 ADD c2 INT NOT NULL AFTER id",                         // ActionAddColumn
		"ALTER TABLE test_ddl1.simple_test1 ADD c3 INT NOT NULL, ADD c4 INT NOT NULL",             // ActionAddColumns
		"ALTER TABLE test_ddl1.simple_test1 DROP c1",                                              // ActionDropColumn
		"ALTER TABLE test_ddl1.simple_test1 DROP c2, DROP c3",                                     // ActionDropColumns
		"ALTER TABLE test_ddl1.simple_test1 ADD INDEX (c4)",                                       // ActionAddIndex
		"ALTER TABLE test_ddl1.simple_test1 DROP INDEX c4",                                        // ActionDropIndex
		"TRUNCATE test_ddl1.simple_test1",                                                         // ActionTruncateTable
		"ALTER DATABASE test_ddl1 CHARACTER SET = binary COLLATE binary",                          // ActionModifySchemaCharsetAndCollate
		"ALTER TABLE test_ddl1.simple_test2 ADD c1 INT NOT NULL, ADD c2 INT NOT NULL",             // ActionAddColumns
		"ALTER TABLE test_ddl1.simple_test2 ADD INDEX (c1)",                                       // ActionAddIndex
		"ALTER TABLE test_ddl1.simple_test2 ALTER INDEX c1 INVISIBLE",                             // ActionAlterIndexVisibility
		"ALTER TABLE test_ddl1.simple_test2 RENAME INDEX c1 TO idx_c1",                            // ActionRenameIndex
		"ALTER TABLE test_ddl1.simple_test2 MODIFY c2 BIGINT",                                     // ActionModifyColumn
		"CREATE VIEW test_ddl1.view_test2 AS SELECT * FROM test_ddl1.simple_test2 WHERE id > 2",   // ActionCreateView
		"DROP VIEW test_ddl1.view_test2",                                                          // ActionDropView
		"RENAME TABLE test_ddl1.simple_test2 TO test_ddl1.simple_test5",                           // ActionRenameTable
		"DROP DATABASE test_ddl1",                                                                 // ActionDropSchema
		"create database test_ddl2",                                                               // ActionCreateSchema
		"create table test_ddl2.simple_test1 (id bigint primary key, c1 int not null unique key)", // ActionCreateTable
		`CREATE TABLE test_ddl2.employees  (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			fname VARCHAR(25) NOT NULL,
			lname VARCHAR(25) NOT NULL,
			store_id INT NOT NULL,
			department_id INT NOT NULL
		)

		PARTITION BY RANGE(id)  (
			PARTITION p0 VALUES LESS THAN (5),
			PARTITION p1 VALUES LESS THAN (10),
			PARTITION p2 VALUES LESS THAN (15),
			PARTITION p3 VALUES LESS THAN (20)
		)`, // ActionCreateTable
		"ALTER TABLE test_ddl2.employees DROP PARTITION p2",                                  // ActionDropTablePartition
		"ALTER TABLE test_ddl2.employees ADD PARTITION (PARTITION p4 VALUES LESS THAN (25))", // ActionAddTablePartition
		"ALTER TABLE test_ddl2.employees TRUNCATE PARTITION p3",                              // ActionTruncateTablePartition
		"alter table test_ddl2.employees comment='modify comment'",                           // ActionModifyTableComment
		"alter table test_ddl2.simple_test1 drop primary key",                                // ActionDropPrimaryKey
		"alter table test_ddl2.simple_test1 add primary key pk(id)",                          // ActionAddPrimaryKey
		"ALTER TABLE test_ddl2.simple_test1 ALTER id SET DEFAULT 18",                         // ActionSetDefaultValue
		"ALTER TABLE test_ddl2.simple_test1 CHARACTER SET = utf8mb4",                         // ActionModifyTableCharsetAndCollate
		// "recover table test_ddl2.employees",                                                  // ActionRecoverTable this ddl can't work on mock tikv

		"DROP TABLE test_ddl2.employees",
		`CREATE TABLE test_ddl2.employees2  (
			id INT NOT NULL,
			fname VARCHAR(25) NOT NULL,
			lname VARCHAR(25) NOT NULL,
			store_id INT NOT NULL,
			department_id INT NOT NULL
		)

		PARTITION BY RANGE(id)  (
			PARTITION p0 VALUES LESS THAN (5),
			PARTITION p1 VALUES LESS THAN (10),
			PARTITION p2 VALUES LESS THAN (15),
			PARTITION p3 VALUES LESS THAN (20)
		)`,
		"ALTER TABLE test_ddl2.employees2 CHARACTER SET = utf8mb4",
		"DROP DATABASE test_ddl2",
	}}

	testOneGroup := func(tc []string) {
		store, err := mockstore.NewMockStore()
		require.Nil(t, err)
		defer store.Close() //nolint:errcheck
		ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
			conf.AlterPrimaryKey = true
		})
		session.SetSchemaLease(0)
		session.DisableStats4Test()
		domain, err := session.BootstrapSession(store)
		require.Nil(t, err)
		defer domain.Close()
		domain.SetStatsUpdating(true)
		tk := testkit.NewTestKit(t, store)

		for _, ddlSQL := range tc {
			tk.MustExec(ddlSQL)
		}

		jobs, err := getAllHistoryDDLJob(store)
		require.Nil(t, err)
		scheamStorage, err := NewSchemaStorage(nil, 0, nil, false)
		require.Nil(t, err)
		for _, job := range jobs {
			err := scheamStorage.HandleDDLJob(job)
			require.Nil(t, err)
		}

		for _, job := range jobs {
			ts := job.BinlogInfo.FinishedTS
			meta, err := kv.GetSnapshotMeta(store, ts)
			require.Nil(t, err)
			snapFromMeta, err := newSchemaSnapshotFromMeta(meta, ts, false)
			require.Nil(t, err)
			snapFromSchemaStore, err := scheamStorage.GetSnapshot(ctx, ts)
			require.Nil(t, err)

			tidySchemaSnapshot(snapFromMeta)
			tidySchemaSnapshot(snapFromSchemaStore)
			require.Equal(t, snapFromMeta, snapFromSchemaStore)
		}
	}

	for _, tc := range testCases {
		testOneGroup(tc)
	}
}

func tidySchemaSnapshot(snap *schemaSnapshot) {
	for _, dbInfo := range snap.schemas {
		if len(dbInfo.Tables) == 0 {
			dbInfo.Tables = nil
		}
	}
	for _, tableInfo := range snap.tables {
		tableInfo.TableInfoVersion = 0
		if len(tableInfo.Columns) == 0 {
			tableInfo.Columns = nil
		}
		if len(tableInfo.Indices) == 0 {
			tableInfo.Indices = nil
		}
		if len(tableInfo.ForeignKeys) == 0 {
			tableInfo.ForeignKeys = nil
		}
	}
	// the snapshot from meta doesn't know which ineligible tables that have existed in history
	// so we delete the ineligible tables which are already not exist
	for tableID := range snap.ineligibleTableID {
		if _, ok := snap.tables[tableID]; !ok {
			delete(snap.ineligibleTableID, tableID)
		}
	}
	// the snapshot from meta doesn't know which tables are truncated, so we just ignore it
	snap.truncateTableID = nil
	for _, v := range snap.tableInSchema {
		sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	}
}

func getAllHistoryDDLJob(storage tidbkv.Storage) ([]*timodel.Job, error) {
	s, err := session.CreateSession(storage)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if s != nil {
		defer s.Close()
	}

	store := domain.GetDomain(s.(sessionctx.Context)).Store()
	txn, err := store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer txn.Rollback() //nolint:errcheck
	txnMeta := timeta.NewMeta(txn)

	jobs, err := txnMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return jobs, nil
}
