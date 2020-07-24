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

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	parser_types "github.com/pingcap/parser/types"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
)

type schemaSuite struct{}

var _ = Suite(&schemaSuite{})

func (t *schemaSuite) TestSchema(c *C) {
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
	snap := newEmptySchemaSnapshot()
	err := snap.handleDDL(job)
	c.Assert(err, IsNil)
	_, exist := snap.SchemaByID(job.SchemaID)
	c.Assert(exist, IsTrue)

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
	c.Assert(err, IsNil)
	_, exist = snap.SchemaByID(job.SchemaID)
	c.Assert(exist, IsFalse)

	job = &timodel.Job{
		ID:         3,
		State:      timodel.JobStateSynced,
		SchemaID:   1,
		Type:       timodel.ActionCreateSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 2, DBInfo: dbInfo, FinishedTS: 124},
		Query:      "create database test",
	}

	err = snap.handleDDL(job)
	c.Assert(err, IsNil)
	err = snap.handleDDL(job)
	c.Log(err)
	c.Assert(errors.IsAlreadyExists(err), IsTrue)

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
	c.Assert(err, IsNil)
	err = snap.handleDDL(job)
	c.Assert(errors.IsNotFound(err), IsTrue)
}

func (*schemaSuite) TestTable(c *C) {
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
	snap := newEmptySchemaSnapshot()
	for _, job := range jobs {
		err := snap.handleDDL(job)
		c.Assert(err, IsNil)
	}

	// check the historical db that constructed above whether in the schema list of local schema
	_, ok := snap.SchemaByID(dbInfo.ID)
	c.Assert(ok, IsTrue)
	// check the historical table that constructed above whether in the table list of local schema
	table, ok := snap.TableByID(tblInfo.ID)
	c.Assert(ok, IsTrue)
	c.Assert(table.Columns, HasLen, 1)
	c.Assert(table.Indices, HasLen, 1)

	// test ineligible tables
	c.Assert(snap.IsIneligibleTableID(2), IsTrue)

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

	err := snap.handleDDL(job)
	c.Assert(err, IsNil)

	_, ok = snap.TableByID(tblInfo1.ID)
	c.Assert(ok, IsTrue)

	_, ok = snap.TableByID(2)
	c.Assert(ok, IsFalse)

	// test ineligible tables
	c.Assert(snap.IsIneligibleTableID(9), IsTrue)
	c.Assert(snap.IsIneligibleTableID(2), IsFalse)
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
	err = snap.handleDDL(job)
	c.Assert(err, IsNil)

	_, ok = snap.TableByID(tblInfo.ID)
	c.Assert(ok, IsFalse)

	// test ineligible tables
	c.Assert(snap.IsIneligibleTableID(9), IsFalse)

	// drop schema
	err = snap.dropSchema(3)
	c.Assert(err, IsNil)

}

func (t *schemaSuite) TestHandleDDL(c *C) {

	snap := newEmptySchemaSnapshot()
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
		testDoDDLAndCheck(c, snap, job, false)

		// custom check after ddl
		switch testCase.name {
		case "createSchema":
			_, ok := snap.SchemaByID(dbInfo.ID)
			c.Assert(ok, IsTrue)
		case "createTable":
			_, ok := snap.TableByID(tblInfo.ID)
			c.Assert(ok, IsTrue)
		case "renameTable":
			tb, ok := snap.TableByID(tblInfo.ID)
			c.Assert(ok, IsTrue)
			c.Assert(tblInfo.Name, Equals, tb.Name)
		case "addColumn", "truncateTable":
			tb, ok := snap.TableByID(tblInfo.ID)
			c.Assert(ok, IsTrue)
			c.Assert(tb.Columns, HasLen, 1)
		case "dropTable":
			_, ok := snap.TableByID(tblInfo.ID)
			c.Assert(ok, IsFalse)
		case "dropSchema":
			_, ok := snap.SchemaByID(job.SchemaID)
			c.Assert(ok, IsFalse)
		}
	}
}

func testDoDDLAndCheck(c *C, snap *schemaSnapshot, job *timodel.Job, isErr bool) {
	err := snap.handleDDL(job)
	c.Assert(err != nil, Equals, isErr)
}

type getUniqueKeysSuite struct{}

var _ = Suite(&getUniqueKeysSuite{})

func (s *getUniqueKeysSuite) TestPKShouldBeInTheFirstPlaceWhenPKIsNotHandle(c *C) {
	t := timodel.TableInfo{
		Columns: []*timodel.ColumnInfo{
			{Name: timodel.CIStr{O: "name"},
				FieldType: parser_types.FieldType{
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
					{Name: timodel.CIStr{O: "name"},
						Offset: 0},
				},
				Unique: true,
			},
			{
				Name: timodel.CIStr{
					O: "PRIMARY",
				},
				Columns: []*timodel.IndexColumn{
					{Name: timodel.CIStr{O: "id"},
						Offset: 1},
				},
				Primary: true,
			},
		},
		PKIsHandle: false,
	}
	info := WrapTableInfo(1, "", 0, &t)
	cols := info.GetUniqueKeys()
	c.Assert(cols, DeepEquals, [][]string{
		{"id"}, {"name"},
	})
}

func (s *getUniqueKeysSuite) TestPKShouldBeInTheFirstPlaceWhenPKIsHandle(c *C) {
	t := timodel.TableInfo{
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
				FieldType: parser_types.FieldType{
					Flag: mysql.NotNullFlag,
				},
			},
			{
				Name: timodel.CIStr{
					O: "uid",
				},
				FieldType: parser_types.FieldType{
					Flag: mysql.PriKeyFlag,
				},
			},
		},
		PKIsHandle: true,
	}
	info := WrapTableInfo(1, "", 0, &t)
	cols := info.GetUniqueKeys()
	c.Assert(cols, DeepEquals, [][]string{
		{"uid"}, {"job"},
	})
}

func (t *schemaSuite) TestMultiVersionStorage(c *C) {
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
	storage, err := NewSchemaStorage(nil, 0, nil)
	c.Assert(err, IsNil)
	for _, job := range jobs {
		err := storage.HandleDDLJob(job)
		c.Assert(err, IsNil)
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
	c.Assert(err, IsNil)

	// `dropSchema` job
	job = &timodel.Job{
		ID:         6,
		State:      timodel.JobStateSynced,
		SchemaID:   1,
		Type:       timodel.ActionDropSchema,
		BinlogInfo: &timodel.HistoryInfo{FinishedTS: 140, DBInfo: dbInfo},
	}

	err = storage.HandleDDLJob(job)
	c.Assert(err, IsNil)

	c.Assert(storage.resolvedTs, Equals, uint64(140))
	snap, err := storage.GetSnapshot(ctx, 100)
	c.Assert(err, IsNil)
	_, exist := snap.SchemaByID(1)
	c.Assert(exist, IsTrue)
	_, exist = snap.TableByID(2)
	c.Assert(exist, IsFalse)
	_, exist = snap.TableByID(3)
	c.Assert(exist, IsFalse)

	snap, err = storage.GetSnapshot(ctx, 115)
	c.Assert(err, IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, IsTrue)
	_, exist = snap.TableByID(2)
	c.Assert(exist, IsTrue)
	_, exist = snap.TableByID(3)
	c.Assert(exist, IsFalse)

	snap, err = storage.GetSnapshot(ctx, 125)
	c.Assert(err, IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, IsTrue)
	_, exist = snap.TableByID(2)
	c.Assert(exist, IsTrue)
	_, exist = snap.TableByID(3)
	c.Assert(exist, IsTrue)

	snap, err = storage.GetSnapshot(ctx, 135)
	c.Assert(err, IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, IsTrue)
	_, exist = snap.TableByID(2)
	c.Assert(exist, IsFalse)
	_, exist = snap.TableByID(3)
	c.Assert(exist, IsTrue)

	snap, err = storage.GetSnapshot(ctx, 140)
	c.Assert(err, IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, IsFalse)
	_, exist = snap.TableByID(2)
	c.Assert(exist, IsFalse)
	_, exist = snap.TableByID(3)
	c.Assert(exist, IsFalse)

	storage.DoGC(0)
	snap, err = storage.GetSnapshot(ctx, 100)
	c.Assert(err, IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, IsTrue)
	_, exist = snap.TableByID(2)
	c.Assert(exist, IsFalse)
	_, exist = snap.TableByID(3)
	c.Assert(exist, IsFalse)
	storage.DoGC(115)
	_, err = storage.GetSnapshot(ctx, 100)
	c.Assert(err, NotNil)
	snap, err = storage.GetSnapshot(ctx, 115)
	c.Assert(err, IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, IsTrue)
	_, exist = snap.TableByID(2)
	c.Assert(exist, IsTrue)
	_, exist = snap.TableByID(3)
	c.Assert(exist, IsFalse)

	storage.DoGC(155)
	storage.AdvanceResolvedTs(185)

	snap, err = storage.GetSnapshot(ctx, 180)
	c.Assert(err, IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, IsFalse)
	_, exist = snap.TableByID(2)
	c.Assert(exist, IsFalse)
	_, exist = snap.TableByID(3)
	c.Assert(exist, IsFalse)
	_, err = storage.GetSnapshot(ctx, 130)
	c.Assert(err, NotNil)

	cancel()
	_, err = storage.GetSnapshot(ctx, 200)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
}

func (t *schemaSuite) TestCreateSnapFromMeta(c *C) {
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
		}),
	)
	c.Assert(err, IsNil)

	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("create database test2")
	tk.MustExec("create table test.simple_test1 (id bigint primary key)")
	tk.MustExec("create table test.simple_test2 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test3 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test4 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test5 (a bigint)")
	ver, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	meta, err := kv.GetSnapshotMeta(store, ver.Ver)
	c.Assert(err, IsNil)
	snap, err := newSchemaSnapshotFromMeta(meta, ver.Ver)
	c.Assert(err, IsNil)
	_, ok := snap.GetTableByName("test", "simple_test1")
	c.Assert(ok, IsTrue)
	tableID, ok := snap.GetTableIDByName("test2", "simple_test5")
	c.Assert(ok, IsTrue)
	c.Assert(snap.IsIneligibleTableID(tableID), IsTrue)
	dbInfo, ok := snap.SchemaByTableID(tableID)
	c.Assert(ok, IsTrue)
	c.Assert(dbInfo.Name.O, Equals, "test2")
	c.Assert(len(dbInfo.Tables), Equals, 3)
}
