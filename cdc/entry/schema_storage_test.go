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

//go:build intest
// +build intest

package entry

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	ticonfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	tidbkv "github.com/pingcap/tidb/kv"
	timeta "github.com/pingcap/tidb/meta"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
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
		State:      timodel.JobStateDone,
		SchemaID:   1,
		Type:       timodel.ActionCreateSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, FinishedTS: 123},
		Query:      "create database test",
	}
	// reconstruct the local schema
	snap := schema.NewEmptySnapshot(false)
	err := snap.HandleDDL(job)
	require.Nil(t, err)
	_, exist := snap.SchemaByID(job.SchemaID)
	require.True(t, exist)

	// test drop schema
	job = &timodel.Job{
		ID:         6,
		State:      timodel.JobStateDone,
		SchemaID:   1,
		Type:       timodel.ActionDropSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 3, DBInfo: dbInfo, FinishedTS: 124},
		Query:      "drop database test",
	}
	err = snap.HandleDDL(job)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(job.SchemaID)
	require.False(t, exist)

	job = &timodel.Job{
		ID:         3,
		State:      timodel.JobStateDone,
		SchemaID:   1,
		Type:       timodel.ActionCreateSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 2, DBInfo: dbInfo, FinishedTS: 124},
		Query:      "create database test",
	}

	err = snap.HandleDDL(job)
	require.Nil(t, err)
	err = snap.HandleDDL(job)
	require.True(t, errors.IsAlreadyExists(err))

	// test schema drop schema error
	job = &timodel.Job{
		ID:         9,
		State:      timodel.JobStateDone,
		SchemaID:   1,
		Type:       timodel.ActionDropSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, FinishedTS: 123},
		Query:      "drop database test",
	}
	err = snap.HandleDDL(job)
	require.Nil(t, err)
	err = snap.HandleDDL(job)
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
		Unique:  false,
		Primary: false,
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
		State:      timodel.JobStateDone,
		SchemaID:   3,
		Type:       timodel.ActionCreateSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, FinishedTS: 123},
		Query:      "create database " + dbName.O,
	}
	jobs = append(jobs, job)

	// `createTable` job
	job = &timodel.Job{
		ID:         6,
		State:      timodel.JobStateDone,
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
		State:      timodel.JobStateDone,
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
		State:      timodel.JobStateDone,
		SchemaID:   3,
		TableID:    2,
		Type:       timodel.ActionAddIndex,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 4, TableInfo: tblInfo, FinishedTS: 126},
		Query:      fmt.Sprintf("alter table %s add index %s(%s)", tbName, idxName, colName),
	}
	jobs = append(jobs, job)

	// reconstruct the local schema
	snap := schema.NewEmptySnapshot(false)
	for _, job := range jobs {
		err := snap.HandleDDL(job)
		require.Nil(t, err)
	}

	// check the historical db that constructed above whether in the schema list of local schema
	_, ok := snap.SchemaByID(dbInfo.ID)
	require.True(t, ok)
	// check the historical table that constructed above whether in the table list of local schema
	table, ok := snap.PhysicalTableByID(tblInfo.ID)
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
		State:      timodel.JobStateDone,
		SchemaID:   3,
		TableID:    2,
		Type:       timodel.ActionTruncateTable,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 5, TableInfo: tblInfo1, FinishedTS: 127},
		Query:      "truncate table " + tbName.O,
	}
	preTableInfo, err := snap.PreTableInfo(job)
	require.Nil(t, err)
	require.Equal(t, preTableInfo.TableName, model.TableName{Schema: "Test", Table: "T", TableID: 2})
	require.Equal(t, preTableInfo.ID, int64(2))

	err = snap.HandleDDL(job)
	require.Nil(t, err)

	_, ok = snap.PhysicalTableByID(tblInfo1.ID)
	require.True(t, ok)

	_, ok = snap.PhysicalTableByID(2)
	require.False(t, ok)

	// test ineligible tables
	require.True(t, snap.IsIneligibleTableID(9))
	require.False(t, snap.IsIneligibleTableID(2))
	// check drop table
	job = &timodel.Job{
		ID:         9,
		State:      timodel.JobStateDone,
		SchemaID:   3,
		TableID:    9,
		Type:       timodel.ActionDropTable,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 6, FinishedTS: 128},
		Query:      "drop table " + tbName.O,
	}
	preTableInfo, err = snap.PreTableInfo(job)
	require.Nil(t, err)
	require.Equal(t, preTableInfo.TableName, model.TableName{Schema: "Test", Table: "T", TableID: 9})
	require.Equal(t, preTableInfo.ID, int64(9))

	err = snap.HandleDDL(job)
	require.Nil(t, err)

	_, ok = snap.PhysicalTableByID(tblInfo.ID)
	require.False(t, ok)

	// test ineligible tables
	require.False(t, snap.IsIneligibleTableID(9))

	// drop schema
	job = &timodel.Job{
		ID:         10,
		State:      timodel.JobStateDone,
		SchemaID:   3,
		Type:       timodel.ActionDropSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 7, FinishedTS: 129},
		Query:      "drop table " + dbName.O,
	}
	err = snap.DoHandleDDL(job)
	require.Nil(t, err)
}

func TestHandleDDL(t *testing.T) {
	snap := schema.NewEmptySnapshot(false)
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
			_, ok := snap.PhysicalTableByID(tblInfo.ID)
			require.True(t, ok)
		case "renameTable":
			tb, ok := snap.PhysicalTableByID(tblInfo.ID)
			require.True(t, ok)
			require.Equal(t, tblInfo.Name, tb.Name)
		case "addColumn", "truncateTable":
			tb, ok := snap.PhysicalTableByID(tblInfo.ID)
			require.True(t, ok)
			require.Len(t, tb.Columns, 1)
		case "dropTable":
			_, ok := snap.PhysicalTableByID(tblInfo.ID)
			require.False(t, ok)
		case "dropSchema":
			_, ok := snap.SchemaByID(job.SchemaID)
			require.False(t, ok)
		}
	}
}

func TestHandleRenameTables(t *testing.T) {
	// Initial schema: db_1.table_1 and db_2.table_2.
	snap := schema.NewEmptySnapshot(true)
	var i int64
	for i = 1; i < 3; i++ {
		dbInfo := &timodel.DBInfo{
			ID:    i,
			Name:  timodel.NewCIStr(fmt.Sprintf("db_%d", i)),
			State: timodel.StatePublic,
		}
		job := &timodel.Job{
			ID:         i,
			State:      timodel.JobStateDone,
			SchemaID:   i,
			Type:       timodel.ActionCreateSchema,
			BinlogInfo: &timodel.HistoryInfo{SchemaVersion: i, DBInfo: dbInfo, FinishedTS: uint64(i)},
			Query:      fmt.Sprintf("create database %s", dbInfo.Name.O),
		}
		err := snap.HandleDDL(job)
		require.Nil(t, err)
	}
	for i = 1; i < 3; i++ {
		tblInfo := &timodel.TableInfo{
			ID:    10 + i,
			Name:  timodel.NewCIStr(fmt.Sprintf("table_%d", i)),
			State: timodel.StatePublic,
		}
		job := &timodel.Job{
			ID:         i,
			State:      timodel.JobStateDone,
			SchemaID:   i,
			TableID:    10 + i,
			Type:       timodel.ActionCreateTable,
			BinlogInfo: &timodel.HistoryInfo{SchemaVersion: i, TableInfo: tblInfo, FinishedTS: uint64(10 + i)},
			Query:      "create table " + tblInfo.Name.O,
		}
		err := snap.HandleDDL(job)
		require.Nil(t, err)
	}

	// rename table db1.table_1 to db2.x, db2.table_2 to db1.y
	oldSchemaIDs := []int64{1, 2}
	newSchemaIDs := []int64{2, 1}
	oldTableIDs := []int64{11, 12}
	newTableNames := []timodel.CIStr{timodel.NewCIStr("x"), timodel.NewCIStr("y")}
	oldSchemaNames := []timodel.CIStr{timodel.NewCIStr("db_1"), timodel.NewCIStr("db_2")}
	args := []interface{}{oldSchemaIDs, newSchemaIDs, newTableNames, oldTableIDs, oldSchemaNames}
	rawArgs, err := json.Marshal(args)
	require.Nil(t, err)
	var job *timodel.Job = &timodel.Job{
		Type:    timodel.ActionRenameTables,
		RawArgs: rawArgs,
		BinlogInfo: &timodel.HistoryInfo{
			FinishedTS: 11112222,
		},
	}
	job.BinlogInfo.MultipleTableInfos = append(job.BinlogInfo.MultipleTableInfos,
		&timodel.TableInfo{
			ID:    13,
			Name:  timodel.NewCIStr("x"),
			State: timodel.StatePublic,
		})
	job.BinlogInfo.MultipleTableInfos = append(job.BinlogInfo.MultipleTableInfos,
		&timodel.TableInfo{
			ID:    14,
			Name:  timodel.NewCIStr("y"),
			State: timodel.StatePublic,
		})
	testDoDDLAndCheck(t, snap, job, false)

	var ok bool
	_, ok = snap.PhysicalTableByID(13)
	require.True(t, ok)
	_, ok = snap.PhysicalTableByID(14)
	require.True(t, ok)
	_, ok = snap.PhysicalTableByID(11)
	require.False(t, ok)
	_, ok = snap.PhysicalTableByID(12)
	require.False(t, ok)

	n1, _ := snap.TableIDByName("db_2", "x")
	require.Equal(t, n1, int64(13))
	n2, _ := snap.TableIDByName("db_1", "y")
	require.Equal(t, n2, int64(14))
	require.Equal(t, uint64(11112222), snap.CurrentTs())
}

func testDoDDLAndCheck(t *testing.T, snap *schema.Snapshot, job *timodel.Job, isErr bool) {
	err := snap.HandleDDL(job)
	require.Equal(t, err != nil, isErr)
}

func TestMultiVersionStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dbName := timodel.NewCIStr("Test")
	tbName := timodel.NewCIStr("T1")
	// db and ignoreDB info
	dbInfo := &timodel.DBInfo{
		ID:    11,
		Name:  dbName,
		State: timodel.StatePublic,
	}
	var jobs []*timodel.Job
	// `createSchema` job1
	job := &timodel.Job{
		ID:         13,
		State:      timodel.JobStateDone,
		SchemaID:   11,
		Type:       timodel.ActionCreateSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, FinishedTS: 100},
		Query:      "create database test",
	}
	jobs = append(jobs, job)

	// table info
	tblInfo := &timodel.TableInfo{
		ID:    12,
		Name:  tbName,
		State: timodel.StatePublic,
	}

	// `createTable` job
	job = &timodel.Job{
		ID:         16,
		State:      timodel.JobStateDone,
		SchemaID:   11,
		TableID:    12,
		Type:       timodel.ActionCreateTable,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 2, TableInfo: tblInfo, FinishedTS: 110},
		Query:      "create table " + tbName.O,
	}

	jobs = append(jobs, job)

	tbName = timodel.NewCIStr("T2")
	// table info
	tblInfo = &timodel.TableInfo{
		ID:    13,
		Name:  tbName,
		State: timodel.StatePublic,
	}
	// `createTable` job
	job = &timodel.Job{
		ID:         16,
		State:      timodel.JobStateDone,
		SchemaID:   11,
		TableID:    13,
		Type:       timodel.ActionCreateTable,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 3, TableInfo: tblInfo, FinishedTS: 120},
		Query:      "create table " + tbName.O,
	}

	jobs = append(jobs, job)
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	storage, err := NewSchemaStorage(nil, 0, false, model.DefaultChangeFeedID("dummy"), util.RoleTester, f)
	require.Nil(t, err)
	for _, job := range jobs {
		err := storage.HandleDDLJob(job)
		require.Nil(t, err)
	}

	// `dropTable` job
	job = &timodel.Job{
		ID:         16,
		State:      timodel.JobStateDone,
		SchemaID:   11,
		TableID:    12,
		Type:       timodel.ActionDropTable,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 4, FinishedTS: 130},
	}

	err = storage.HandleDDLJob(job)
	require.Nil(t, err)

	// `dropSchema` job
	job = &timodel.Job{
		ID:         16,
		State:      timodel.JobStateDone,
		SchemaID:   11,
		Type:       timodel.ActionDropSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 5, FinishedTS: 140, DBInfo: dbInfo},
	}

	err = storage.HandleDDLJob(job)
	require.Nil(t, err)

	require.Equal(t, storage.(*schemaStorageImpl).resolvedTs, uint64(140))
	snap, err := storage.GetSnapshot(ctx, 100)
	require.Nil(t, err)
	_, exist := snap.SchemaByID(11)
	require.True(t, exist)
	_, exist = snap.PhysicalTableByID(12)
	require.False(t, exist)
	_, exist = snap.PhysicalTableByID(13)
	require.False(t, exist)

	snap, err = storage.GetSnapshot(ctx, 115)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(11)
	require.True(t, exist)
	_, exist = snap.PhysicalTableByID(12)
	require.True(t, exist)
	_, exist = snap.PhysicalTableByID(13)
	require.False(t, exist)

	snap, err = storage.GetSnapshot(ctx, 125)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(11)
	require.True(t, exist)
	_, exist = snap.PhysicalTableByID(12)
	require.True(t, exist)
	_, exist = snap.PhysicalTableByID(13)
	require.True(t, exist)

	snap, err = storage.GetSnapshot(ctx, 135)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(11)
	require.True(t, exist)
	_, exist = snap.PhysicalTableByID(12)
	require.False(t, exist)
	_, exist = snap.PhysicalTableByID(13)
	require.True(t, exist)

	snap, err = storage.GetSnapshot(ctx, 140)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(11)
	require.False(t, exist)
	_, exist = snap.PhysicalTableByID(12)
	require.False(t, exist)
	_, exist = snap.PhysicalTableByID(13)
	require.False(t, exist)

	lastSchemaTs := storage.DoGC(0)
	require.Equal(t, uint64(0), lastSchemaTs)

	snap, err = storage.GetSnapshot(ctx, 100)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(11)
	require.True(t, exist)
	_, exist = snap.PhysicalTableByID(12)
	require.False(t, exist)
	_, exist = snap.PhysicalTableByID(13)
	require.False(t, exist)
	storage.DoGC(115)
	_, err = storage.GetSnapshot(ctx, 100)
	require.NotNil(t, err)
	snap, err = storage.GetSnapshot(ctx, 115)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(11)
	require.True(t, exist)
	_, exist = snap.PhysicalTableByID(12)
	require.True(t, exist)
	_, exist = snap.PhysicalTableByID(13)
	require.False(t, exist)

	lastSchemaTs = storage.DoGC(155)
	require.Equal(t, uint64(140), lastSchemaTs)

	storage.AdvanceResolvedTs(185)

	snap, err = storage.GetSnapshot(ctx, 180)
	require.Nil(t, err)
	_, exist = snap.SchemaByID(11)
	require.False(t, exist)
	_, exist = snap.PhysicalTableByID(12)
	require.False(t, exist)
	_, exist = snap.PhysicalTableByID(13)
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
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	snap, err := schema.NewSnapshotFromMeta(meta, ver.Ver, false, f)
	require.Nil(t, err)
	_, ok := snap.TableByName("test", "simple_test1")
	require.True(t, ok)
	tableID, ok := snap.TableIDByName("test2", "simple_test5")
	require.True(t, ok)
	require.True(t, snap.IsIneligibleTableID(tableID))
	dbInfo, ok := snap.SchemaByTableID(tableID)
	require.True(t, ok)
	require.Equal(t, dbInfo.Name.O, "test2")
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
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	snap1, err := schema.NewSnapshotFromMeta(meta1, ver1.Ver, true /* forceReplicate */, f)
	require.Nil(t, err)
	meta2, err := kv.GetSnapshotMeta(store, ver2.Ver)
	require.Nil(t, err)
	snap2, err := schema.NewSnapshotFromMeta(meta2, ver2.Ver, false /* forceReplicate */, f)
	require.Nil(t, err)
	snap3, err := schema.NewSnapshotFromMeta(meta2, ver2.Ver, true /* forceReplicate */, f)
	require.Nil(t, err)

	// we don't need to count system tables since TiCDC
	// don't replicate them and TiDB change them frequently,
	// so we don't need to consider them in the table count
	systemTablesFilter := func(dbName, tableName string) bool {
		return dbName != "mysql" && dbName != "information_schema"
	}
	require.Equal(t, 5, snap2.TableCount(true,
		systemTablesFilter)-snap1.TableCount(true, systemTablesFilter))
	// only test simple_test1 included
	require.Equal(t, 1, snap2.TableCount(false, systemTablesFilter))

	require.Equal(t, 5, snap3.TableCount(true,
		systemTablesFilter)-snap1.TableCount(true, systemTablesFilter))
	// since we create a snapshot from meta2 and forceReplicate is true, so all tables are included
	require.Equal(t, 5, snap3.TableCount(false, systemTablesFilter))
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
		"create database test_ddl1",                                                                            // ActionCreateSchema
		"create table test_ddl1.simple_test1 (id bigint primary key)",                                          // ActionCreateTable
		"create table test_ddl1.simple_test2 (id bigint)",                                                      // ActionCreateTable
		"create table test_ddl1.simple_test3 (id bigint primary key)",                                          // ActionCreateTable
		"create table test_ddl1.simple_test4 (id bigint primary key)",                                          // ActionCreateTable
		"DROP TABLE test_ddl1.simple_test3",                                                                    // ActionDropTable
		"ALTER TABLE test_ddl1.simple_test1 ADD COLUMN c1 INT NOT NULL",                                        // ActionAddColumn
		"ALTER TABLE test_ddl1.simple_test1 ADD c2 INT NOT NULL AFTER id",                                      // ActionAddColumn
		"ALTER TABLE test_ddl1.simple_test1 ADD c3 INT NOT NULL, ADD c4 INT NOT NULL",                          // ActionAddColumns
		"ALTER TABLE test_ddl1.simple_test1 DROP c1",                                                           // ActionDropColumn
		"ALTER TABLE test_ddl1.simple_test1 DROP c2, DROP c3",                                                  // ActionDropColumns
		"ALTER TABLE test_ddl1.simple_test1 ADD INDEX (c4)",                                                    // ActionAddIndex
		"ALTER TABLE test_ddl1.simple_test1 DROP INDEX c4",                                                     // ActionDropIndex
		"TRUNCATE test_ddl1.simple_test1",                                                                      // ActionTruncateTable
		"ALTER DATABASE test_ddl1 CHARACTER SET = binary COLLATE binary",                                       // ActionModifySchemaCharsetAndCollate
		"ALTER TABLE test_ddl1.simple_test2 ADD c1 INT NOT NULL, ADD c2 INT NOT NULL",                          // ActionAddColumns
		"ALTER TABLE test_ddl1.simple_test2 ADD INDEX (c1)",                                                    // ActionAddIndex
		"ALTER TABLE test_ddl1.simple_test2 ALTER INDEX c1 INVISIBLE",                                          // ActionAlterIndexVisibility
		"ALTER TABLE test_ddl1.simple_test2 RENAME INDEX c1 TO idx_c1",                                         // ActionRenameIndex
		"ALTER TABLE test_ddl1.simple_test2 MODIFY c2 BIGINT",                                                  // ActionModifyColumn
		"CREATE VIEW test_ddl1.view_test2 AS SELECT * FROM test_ddl1.simple_test2 WHERE id > 2",                // ActionCreateView
		"DROP VIEW test_ddl1.view_test2",                                                                       // ActionDropView
		"RENAME TABLE test_ddl1.simple_test2 TO test_ddl1.simple_test5",                                        // ActionRenameTable
		"DROP DATABASE test_ddl1",                                                                              // ActionDropSchema
		"create database test_ddl2",                                                                            // ActionCreateSchema
		"create table test_ddl2.simple_test1 (id bigint primary key nonclustered, c1 int not null unique key)", // ActionCreateTable
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

		"ALTER TABLE test_ddl2.employees REORGANIZE PARTITION p3 INTO (PARTITION p2 VALUES LESS THAN (15), PARTITION p3 VALUES LESS THAN (20))", // ActionReorganizePartition
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
		tk.MustExec("set global tidb_enable_clustered_index = 'int_only';")
		for _, ddlSQL := range tc {
			tk.MustExec(ddlSQL)
		}

		f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
		require.Nil(t, err)

		jobs, err := getAllHistoryDDLJob(store, f)
		require.Nil(t, err)

		schemaStorage, err := NewSchemaStorage(nil, 0, false, model.DefaultChangeFeedID("dummy"), util.RoleTester, f)
		require.Nil(t, err)
		for _, job := range jobs {
			err := schemaStorage.HandleDDLJob(job)
			require.Nil(t, err)
		}

		for _, job := range jobs {
			ts := job.BinlogInfo.FinishedTS
			meta, err := kv.GetSnapshotMeta(store, ts)
			require.Nil(t, err)
			snapFromMeta, err := schema.NewSnapshotFromMeta(meta, ts, false, f)
			require.Nil(t, err)
			snapFromSchemaStore, err := schemaStorage.GetSnapshot(ctx, ts)
			require.Nil(t, err)

			s1 := snapFromMeta.DumpToString()
			s2 := snapFromSchemaStore.DumpToString()
			require.Equal(t, s1, s2)
		}
	}

	for _, tc := range testCases {
		testOneGroup(tc)
	}
}

func getAllHistoryDDLJob(storage tidbkv.Storage, f filter.Filter) ([]*timodel.Job, error) {
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

	jobs, err := ddl.GetAllHistoryDDLJobs(txnMeta)
	res := make([]*timodel.Job, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, job := range jobs {
		ignoreSchema := f.ShouldIgnoreSchema(job.SchemaName)
		ignoreTable := f.ShouldIgnoreTable(job.SchemaName, job.TableName)
		if ignoreSchema || ignoreTable {
			log.Info("Ignore ddl job", zap.Stringer("job", job))
			continue
		}
		// Set State from Synced to Done.
		// Because jobs are put to history queue after TiDB alter its state from
		// Done to Synced.
		jobs[i].State = timodel.JobStateDone
		res = append(res, job)
	}
	return jobs, nil
}

// This test is used to show how the schemaStorage choose a handleKey of a table.
// The handleKey is chosen by the following rules:
// 1. If the table has a primary key, the handleKey is the first column of the primary key.
// 2. If the table has not null unique key, the handleKey is the first column of the unique key.
// 3. If the table has no primary key and no not null unique key, it has no handleKey.
func TestHandleKey(t *testing.T) {
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
	tk.MustExec("create table test.simple_test2 (id bigint, age int NOT NULL, " +
		"name char NOT NULL, UNIQUE KEY(age, name))")
	tk.MustExec("create table test.simple_test3 (id bigint, age int)")
	ver, err := store.CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	meta, err := kv.GetSnapshotMeta(store, ver.Ver)
	require.Nil(t, err)
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	snap, err := schema.NewSnapshotFromMeta(meta, ver.Ver, false, f)
	require.Nil(t, err)
	tb1, ok := snap.TableByName("test", "simple_test1")
	require.True(t, ok)
	require.Equal(t, int64(-1), tb1.HandleIndexID) // pk is handleKey
	columnID := int64(1)
	flag := tb1.ColumnsFlag[columnID]
	require.True(t, flag.IsHandleKey())
	for _, column := range tb1.Columns {
		if column.ID == columnID {
			require.True(t, column.Name.O == "id")
		}
	}

	// unique key is handleKey
	tb2, ok := snap.TableByName("test", "simple_test2")
	require.True(t, ok)
	require.Equal(t, int64(1), tb2.HandleIndexID)
	columnID = int64(2)
	flag = tb2.ColumnsFlag[columnID]
	require.True(t, flag.IsHandleKey())
	for _, column := range tb2.Columns {
		if column.ID == columnID {
			require.True(t, column.Name.O == "age")
		}
	}

	// has no handleKey
	tb3, ok := snap.TableByName("test", "simple_test3")
	require.True(t, ok)
	require.Equal(t, int64(-2), tb3.HandleIndexID)
}

func TestGetPrimaryKey(t *testing.T) {
	t.Parallel()

	helper := NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b int)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 0, job.BinlogInfo.TableInfo)

	names := tableInfo.GetPrimaryKeyColumnNames()
	require.Len(t, names, 1)
	require.Containsf(t, names, "a", "names: %v", names)

	sql = `create table test.t2(a int, b int, c int, primary key(a, b))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 0, job.BinlogInfo.TableInfo)

	names = tableInfo.GetPrimaryKeyColumnNames()
	require.Len(t, names, 2)
	require.Containsf(t, names, "a", "names: %v", names)
	require.Containsf(t, names, "b", "names: %v", names)
}
