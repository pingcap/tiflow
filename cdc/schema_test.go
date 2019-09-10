// Copyright 2019 PingCAP, Inc.
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

package cdc

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

type schemaSuite struct{}

var _ = Suite(&schemaSuite{})

func (t *schemaSuite) TestSchema(c *C) {
	var jobs []*model.Job
	dbName := model.NewCIStr("Test")
	// db and ignoreDB info
	dbInfo := &model.DBInfo{
		ID:    1,
		Name:  dbName,
		State: model.StatePublic,
	}
	// `createSchema` job
	job := &model.Job{
		ID:         3,
		State:      model.JobStateSynced,
		SchemaID:   1,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, FinishedTS: 123},
		Query:      "create database test",
	}
	jobDup := &model.Job{
		ID:         3,
		State:      model.JobStateSynced,
		SchemaID:   1,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{SchemaVersion: 2, DBInfo: dbInfo, FinishedTS: 123},
		Query:      "create database test",
	}
	jobs = append(jobs, job)

	// construct a rollbackdone job
	jobs = append(jobs, &model.Job{ID: 5, State: model.JobStateRollbackDone})

	// reconstruct the local schema
	schema, err := NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(2)
	c.Assert(err, IsNil)

	// test drop schema
	jobs = append(
		jobs,
		&model.Job{
			ID:         6,
			State:      model.JobStateSynced,
			SchemaID:   1,
			Type:       model.ActionDropSchema,
			BinlogInfo: &model.HistoryInfo{SchemaVersion: 3, FinishedTS: 123},
			Query:      "drop database test",
		},
	)
	schema, err = NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(3)
	c.Assert(err, IsNil)

	// test create schema already exist error
	jobs = jobs[:0]
	jobs = append(jobs, job)
	jobs = append(jobs, jobDup)
	schema, err = NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(2)
	c.Log(err)
	c.Assert(errors.IsAlreadyExists(err), IsTrue)

	// test schema drop schema error
	jobs = jobs[:0]
	jobs = append(
		jobs,
		&model.Job{
			ID:         9,
			State:      model.JobStateSynced,
			SchemaID:   1,
			Type:       model.ActionDropSchema,
			BinlogInfo: &model.HistoryInfo{SchemaVersion: 1, FinishedTS: 123},
			Query:      "drop database test",
		},
	)
	schema, err = NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(1)
	c.Assert(errors.IsNotFound(err), IsTrue)
}

func (*schemaSuite) TestTable(c *C) {
	var jobs []*model.Job
	dbName := model.NewCIStr("Test")
	tbName := model.NewCIStr("T")
	colName := model.NewCIStr("A")
	idxName := model.NewCIStr("idx")
	// column info
	colInfo := &model.ColumnInfo{
		ID:        1,
		Name:      colName,
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}
	// index info
	idxInfo := &model.IndexInfo{
		Name:  idxName,
		Table: tbName,
		Columns: []*model.IndexColumn{
			{
				Name:   colName,
				Offset: 0,
				Length: 10,
			},
		},
		Unique:  true,
		Primary: true,
		State:   model.StatePublic,
	}
	// table info
	tblInfo := &model.TableInfo{
		ID:    2,
		Name:  tbName,
		State: model.StatePublic,
	}
	// db info
	dbInfo := &model.DBInfo{
		ID:    3,
		Name:  dbName,
		State: model.StatePublic,
	}

	// `createSchema` job
	job := &model.Job{
		ID:         5,
		State:      model.JobStateSynced,
		SchemaID:   3,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, FinishedTS: 123},
		Query:      "create database " + dbName.O,
	}
	jobs = append(jobs, job)

	// `createTable` job
	job = &model.Job{
		ID:         6,
		State:      model.JobStateSynced,
		SchemaID:   3,
		TableID:    2,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{SchemaVersion: 2, TableInfo: tblInfo, FinishedTS: 123},
		Query:      "create table " + tbName.O,
	}
	jobs = append(jobs, job)

	// `addColumn` job
	tblInfo.Columns = []*model.ColumnInfo{colInfo}
	job = &model.Job{
		ID:         7,
		State:      model.JobStateSynced,
		SchemaID:   3,
		TableID:    2,
		Type:       model.ActionAddColumn,
		BinlogInfo: &model.HistoryInfo{SchemaVersion: 3, TableInfo: tblInfo, FinishedTS: 123},
		Query:      "alter table " + tbName.O + " add column " + colName.O,
	}
	jobs = append(jobs, job)

	// construct a historical `addIndex` job
	tblInfo.Indices = []*model.IndexInfo{idxInfo}
	job = &model.Job{
		ID:         8,
		State:      model.JobStateSynced,
		SchemaID:   3,
		TableID:    2,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{SchemaVersion: 4, TableInfo: tblInfo, FinishedTS: 123},
		Query:      fmt.Sprintf("alter table %s add index %s(%s)", tbName, idxName, colName),
	}
	jobs = append(jobs, job)

	// reconstruct the local schema
	schema, err := NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema.handlePreviousDDLJobIfNeed(4)
	c.Assert(err, IsNil)

	// check the historical db that constructed above whether in the schema list of local schema
	_, ok := schema.SchemaByID(dbInfo.ID)
	c.Assert(ok, IsTrue)
	// check the historical table that constructed above whether in the table list of local schema
	table, ok := schema.TableByID(tblInfo.ID)
	c.Assert(ok, IsTrue)
	c.Assert(table.Columns, HasLen, 1)
	c.Assert(table.Indices, HasLen, 1)
	// check truncate table
	tblInfo1 := &model.TableInfo{
		ID:    9,
		Name:  tbName,
		State: model.StatePublic,
	}
	jobs = append(
		jobs,
		&model.Job{
			ID:         9,
			State:      model.JobStateSynced,
			SchemaID:   3,
			TableID:    2,
			Type:       model.ActionTruncateTable,
			BinlogInfo: &model.HistoryInfo{SchemaVersion: 5, TableInfo: tblInfo1, FinishedTS: 123},
			Query:      "truncate table " + tbName.O,
		},
	)
	schema1, err := NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema1.handlePreviousDDLJobIfNeed(5)
	c.Assert(err, IsNil)
	_, ok = schema1.TableByID(tblInfo1.ID)
	c.Assert(ok, IsTrue)

	_, ok = schema1.TableByID(2)
	c.Assert(ok, IsFalse)
	// check drop table
	jobs = append(
		jobs,
		&model.Job{
			ID:         9,
			State:      model.JobStateSynced,
			SchemaID:   3,
			TableID:    9,
			Type:       model.ActionDropTable,
			BinlogInfo: &model.HistoryInfo{SchemaVersion: 6, FinishedTS: 123},
			Query:      "drop table " + tbName.O,
		},
	)
	schema2, err := NewSchema(jobs, false)
	c.Assert(err, IsNil)
	err = schema2.handlePreviousDDLJobIfNeed(6)
	c.Assert(err, IsNil)

	_, ok = schema2.TableByID(tblInfo.ID)
	c.Assert(ok, IsFalse)
	// test schemaAndTableName
	_, _, ok = schema1.SchemaAndTableName(9)
	c.Assert(ok, IsTrue)
	// drop schema
	_, err = schema1.DropSchema(3)
	c.Assert(err, IsNil)
	// test schema version
	c.Assert(schema.SchemaMetaVersion(), Equals, int64(0))
}

func (t *schemaSuite) TestHandleDDL(c *C) {
	schema, err := NewSchema(nil, false)
	c.Assert(err, IsNil)
	dbName := model.NewCIStr("Test")
	colName := model.NewCIStr("A")
	tbName := model.NewCIStr("T")
	newTbName := model.NewCIStr("RT")

	// check rollback done job
	job := &model.Job{ID: 1, State: model.JobStateRollbackDone}
	_, _, sql, err := schema.handleDDL(job)
	c.Assert(err, IsNil)
	c.Assert(sql, Equals, "")

	// check job.Query is empty
	job = &model.Job{ID: 1, State: model.JobStateDone}
	_, _, sql, err = schema.handleDDL(job)
	c.Assert(sql, Equals, "")
	c.Assert(err, NotNil, Commentf("should return not found job.Query"))

	// db info
	dbInfo := &model.DBInfo{
		ID:    2,
		Name:  dbName,
		State: model.StatePublic,
	}
	// table Info
	tblInfo := &model.TableInfo{
		ID:    6,
		Name:  tbName,
		State: model.StatePublic,
	}
	// column info
	colInfo := &model.ColumnInfo{
		ID:        8,
		Name:      colName,
		Offset:    0,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
		State:     model.StatePublic,
	}
	tblInfo.Columns = []*model.ColumnInfo{colInfo}

	testCases := []struct {
		name        string
		jobID       int64
		schemaID    int64
		tableID     int64
		jobType     model.ActionType
		binlogInfo  *model.HistoryInfo
		query       string
		resultQuery string
		schemaName  string
		tableName   string
	}{
		{name: "createSchema", jobID: 3, schemaID: 2, tableID: 0, jobType: model.ActionCreateSchema, binlogInfo: &model.HistoryInfo{SchemaVersion: 1, DBInfo: dbInfo, TableInfo: nil, FinishedTS: 123}, query: "create database Test", resultQuery: "create database Test", schemaName: dbInfo.Name.O, tableName: ""},
		{name: "createTable", jobID: 7, schemaID: 2, tableID: 6, jobType: model.ActionCreateTable, binlogInfo: &model.HistoryInfo{SchemaVersion: 3, DBInfo: nil, TableInfo: tblInfo, FinishedTS: 123}, query: "create table T(id int);", resultQuery: "create table T(id int);", schemaName: dbInfo.Name.O, tableName: tblInfo.Name.O},
		{name: "addColumn", jobID: 9, schemaID: 2, tableID: 6, jobType: model.ActionAddColumn, binlogInfo: &model.HistoryInfo{SchemaVersion: 4, DBInfo: nil, TableInfo: tblInfo, FinishedTS: 123}, query: "alter table T add a varchar(45);", resultQuery: "alter table T add a varchar(45);", schemaName: dbInfo.Name.O, tableName: tblInfo.Name.O},
		{name: "truncateTable", jobID: 10, schemaID: 2, tableID: 6, jobType: model.ActionTruncateTable, binlogInfo: &model.HistoryInfo{SchemaVersion: 5, DBInfo: nil, TableInfo: tblInfo, FinishedTS: 123}, query: "truncate table T;", resultQuery: "truncate table T;", schemaName: dbInfo.Name.O, tableName: tblInfo.Name.O},
		{name: "renameTable", jobID: 11, schemaID: 2, tableID: 10, jobType: model.ActionRenameTable, binlogInfo: &model.HistoryInfo{SchemaVersion: 6, DBInfo: nil, TableInfo: tblInfo, FinishedTS: 123}, query: "rename table T to RT;", resultQuery: "rename table T to RT;", schemaName: dbInfo.Name.O, tableName: newTbName.O},
		{name: "dropTable", jobID: 12, schemaID: 2, tableID: 12, jobType: model.ActionDropTable, binlogInfo: &model.HistoryInfo{SchemaVersion: 7, DBInfo: nil, TableInfo: nil, FinishedTS: 123}, query: "drop table RT;", resultQuery: "drop table RT;", schemaName: dbInfo.Name.O, tableName: newTbName.O},
		{name: "dropSchema", jobID: 13, schemaID: 2, tableID: 0, jobType: model.ActionDropSchema, binlogInfo: &model.HistoryInfo{SchemaVersion: 8, DBInfo: nil, TableInfo: nil, FinishedTS: 123}, query: "drop database test;", resultQuery: "drop database test;", schemaName: dbInfo.Name.O, tableName: ""},
	}

	for _, testCase := range testCases {
		// prepare for ddl
		switch testCase.name {
		case "addColumn":
			tblInfo.Columns = []*model.ColumnInfo{colInfo}
		case "truncateTable":
			tblInfo.ID = 10
		case "renameTable":
			tblInfo.ID = 12
			tblInfo.Name = newTbName
		}

		job = &model.Job{
			ID:         testCase.jobID,
			State:      model.JobStateDone,
			SchemaID:   testCase.schemaID,
			TableID:    testCase.tableID,
			Type:       testCase.jobType,
			BinlogInfo: testCase.binlogInfo,
			Query:      testCase.query,
		}
		testDoDDLAndCheck(c, schema, job, false, testCase.resultQuery, testCase.schemaName, testCase.tableName)

		// custom check after ddl
		switch testCase.name {
		case "createSchema":
			_, ok := schema.SchemaByID(dbInfo.ID)
			c.Assert(ok, IsTrue)
		case "createTable":
			_, ok := schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsTrue)
		case "renameTable":
			tb, ok := schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsTrue)
			c.Assert(tblInfo.Name, Equals, tb.Name)
		case "addColumn", "truncateTable":
			tb, ok := schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsTrue)
			c.Assert(tb.Columns, HasLen, 1)
		case "dropTable":
			_, ok := schema.TableByID(tblInfo.ID)
			c.Assert(ok, IsFalse)
		case "dropSchema":
			_, ok := schema.SchemaByID(job.SchemaID)
			c.Assert(ok, IsFalse)
		}
	}
}

func (t *schemaSuite) TestAddImplicitColumn(c *C) {
	tbl := model.TableInfo{}

	addImplicitColumn(&tbl)

	c.Assert(tbl.Columns, HasLen, 1)
	c.Assert(tbl.Columns[0].ID, Equals, int64(implicitColID))
	c.Assert(tbl.Indices, HasLen, 1)
	c.Assert(tbl.Indices[0].Primary, IsTrue)
}

func testDoDDLAndCheck(c *C, schema *Schema, job *model.Job, isErr bool, sql string, expectedSchema string, expectedTable string) {
	schemaName, tableName, resSQL, err := schema.handleDDL(job)
	c.Logf("handle: %s", job.Query)
	c.Logf("result: %s, %s, %s, %v", schemaName, tableName, resSQL, err)
	c.Assert(err != nil, Equals, isErr)
	c.Assert(sql, Equals, resSQL)
	c.Assert(schemaName, Equals, expectedSchema)
	c.Assert(tableName, Equals, expectedTable)
}
