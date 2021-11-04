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

	"github.com/google/go-cmp/cmp"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	ticonfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	tidbkv "github.com/pingcap/tidb/kv"
	timeta "github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
)

type schemaSuite struct{}

var _ = check.Suite(&schemaSuite{})

func (t *schemaSuite) TestSchema(c *check.C) {
	defer testleak.AfterTest(c)()
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
	c.Assert(err, check.IsNil)
	_, exist := snap.SchemaByID(job.SchemaID)
	c.Assert(exist, check.IsTrue)

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
	c.Assert(err, check.IsNil)
	_, exist = snap.SchemaByID(job.SchemaID)
	c.Assert(exist, check.IsFalse)

	job = &timodel.Job{
		ID:         3,
		State:      timodel.JobStateSynced,
		SchemaID:   1,
		Type:       timodel.ActionCreateSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 2, DBInfo: dbInfo, FinishedTS: 124},
		Query:      "create database test",
	}

	err = snap.handleDDL(job)
	c.Assert(err, check.IsNil)
	err = snap.handleDDL(job)
	c.Log(err)
	c.Assert(errors.IsAlreadyExists(err), check.IsTrue)

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
	c.Assert(err, check.IsNil)
	err = snap.handleDDL(job)
	c.Assert(errors.IsNotFound(err), check.IsTrue)
}

func (*schemaSuite) TestTable(c *check.C) {
	defer testleak.AfterTest(c)()
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
		c.Assert(err, check.IsNil)
	}

	// check the historical db that constructed above whether in the schema list of local schema
	_, ok := snap.SchemaByID(dbInfo.ID)
	c.Assert(ok, check.IsTrue)
	// check the historical table that constructed above whether in the table list of local schema
	table, ok := snap.TableByID(tblInfo.ID)
	c.Assert(ok, check.IsTrue)
	c.Assert(table.Columns, check.HasLen, 1)
	c.Assert(table.Indices, check.HasLen, 1)

	// test ineligible tables
	c.Assert(snap.IsIneligibleTableID(2), check.IsTrue)

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
	c.Assert(err, check.IsNil)
	c.Assert(preTableInfo.TableName, check.Equals, model.TableName{Schema: "Test", Table: "T"})
	c.Assert(preTableInfo.ID, check.Equals, int64(2))

	err = snap.handleDDL(job)
	c.Assert(err, check.IsNil)

	_, ok = snap.TableByID(tblInfo1.ID)
	c.Assert(ok, check.IsTrue)

	_, ok = snap.TableByID(2)
	c.Assert(ok, check.IsFalse)

	// test ineligible tables
	c.Assert(snap.IsIneligibleTableID(9), check.IsTrue)
	c.Assert(snap.IsIneligibleTableID(2), check.IsFalse)
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
	c.Assert(err, check.IsNil)
	c.Assert(preTableInfo.TableName, check.Equals, model.TableName{Schema: "Test", Table: "T"})
	c.Assert(preTableInfo.ID, check.Equals, int64(9))

	err = snap.handleDDL(job)
	c.Assert(err, check.IsNil)

	_, ok = snap.TableByID(tblInfo.ID)
	c.Assert(ok, check.IsFalse)

	// test ineligible tables
	c.Assert(snap.IsIneligibleTableID(9), check.IsFalse)

	// drop schema
	err = snap.dropSchema(3)
	c.Assert(err, check.IsNil)
}

func (t *schemaSuite) TestHandleDDL(c *check.C) {
	defer testleak.AfterTest(c)()

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
		testDoDDLAndCheck(c, snap, job, false)

		// custom check after ddl
		switch testCase.name {
		case "createSchema":
			_, ok := snap.SchemaByID(dbInfo.ID)
			c.Assert(ok, check.IsTrue)
		case "createTable":
			_, ok := snap.TableByID(tblInfo.ID)
			c.Assert(ok, check.IsTrue)
		case "renameTable":
			tb, ok := snap.TableByID(tblInfo.ID)
			c.Assert(ok, check.IsTrue)
			c.Assert(tblInfo.Name, check.Equals, tb.Name)
		case "addColumn", "truncateTable":
			tb, ok := snap.TableByID(tblInfo.ID)
			c.Assert(ok, check.IsTrue)
			c.Assert(tb.Columns, check.HasLen, 1)
		case "dropTable":
			_, ok := snap.TableByID(tblInfo.ID)
			c.Assert(ok, check.IsFalse)
		case "dropSchema":
			_, ok := snap.SchemaByID(job.SchemaID)
			c.Assert(ok, check.IsFalse)
		}
	}
}

func testDoDDLAndCheck(c *check.C, snap *schemaSnapshot, job *timodel.Job, isErr bool) {
	err := snap.handleDDL(job)
	c.Assert(err != nil, check.Equals, isErr)
}

type getUniqueKeysSuite struct{}

var _ = check.Suite(&getUniqueKeysSuite{})

func (s *getUniqueKeysSuite) TestPKShouldBeInTheFirstPlaceWhenPKIsNotHandle(c *check.C) {
	defer testleak.AfterTest(c)()
	t := timodel.TableInfo{
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
	info := model.WrapTableInfo(1, "", 0, &t)
	cols := info.GetUniqueKeys()
	c.Assert(cols, check.DeepEquals, [][]string{
		{"id"}, {"name"},
	})
}

func (s *getUniqueKeysSuite) TestPKShouldBeInTheFirstPlaceWhenPKIsHandle(c *check.C) {
	defer testleak.AfterTest(c)()
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
	info := model.WrapTableInfo(1, "", 0, &t)
	cols := info.GetUniqueKeys()
	c.Assert(cols, check.DeepEquals, [][]string{
		{"uid"}, {"job"},
	})
}

func (t *schemaSuite) TestMultiVersionStorage(c *check.C) {
	defer testleak.AfterTest(c)()
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
	c.Assert(err, check.IsNil)
	for _, job := range jobs {
		err := storage.HandleDDLJob(job)
		c.Assert(err, check.IsNil)
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
	c.Assert(err, check.IsNil)

	// `dropSchema` job
	job = &timodel.Job{
		ID:         6,
		State:      timodel.JobStateSynced,
		SchemaID:   1,
		Type:       timodel.ActionDropSchema,
		BinlogInfo: &timodel.HistoryInfo{FinishedTS: 140, DBInfo: dbInfo},
	}

	err = storage.HandleDDLJob(job)
	c.Assert(err, check.IsNil)

	c.Assert(storage.(*schemaStorageImpl).resolvedTs, check.Equals, uint64(140))
	snap, err := storage.GetSnapshot(ctx, 100)
	c.Assert(err, check.IsNil)
	_, exist := snap.SchemaByID(1)
	c.Assert(exist, check.IsTrue)
	_, exist = snap.TableByID(2)
	c.Assert(exist, check.IsFalse)
	_, exist = snap.TableByID(3)
	c.Assert(exist, check.IsFalse)

	snap, err = storage.GetSnapshot(ctx, 115)
	c.Assert(err, check.IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, check.IsTrue)
	_, exist = snap.TableByID(2)
	c.Assert(exist, check.IsTrue)
	_, exist = snap.TableByID(3)
	c.Assert(exist, check.IsFalse)

	snap, err = storage.GetSnapshot(ctx, 125)
	c.Assert(err, check.IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, check.IsTrue)
	_, exist = snap.TableByID(2)
	c.Assert(exist, check.IsTrue)
	_, exist = snap.TableByID(3)
	c.Assert(exist, check.IsTrue)

	snap, err = storage.GetSnapshot(ctx, 135)
	c.Assert(err, check.IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, check.IsTrue)
	_, exist = snap.TableByID(2)
	c.Assert(exist, check.IsFalse)
	_, exist = snap.TableByID(3)
	c.Assert(exist, check.IsTrue)

	snap, err = storage.GetSnapshot(ctx, 140)
	c.Assert(err, check.IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, check.IsFalse)
	_, exist = snap.TableByID(2)
	c.Assert(exist, check.IsFalse)
	_, exist = snap.TableByID(3)
	c.Assert(exist, check.IsFalse)

	lastSchemaTs := storage.DoGC(0)
	require.Equal(t, uint64(0), lastSchemaTs)

	snap, err = storage.GetSnapshot(ctx, 100)
	c.Assert(err, check.IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, check.IsTrue)
	_, exist = snap.TableByID(2)
	c.Assert(exist, check.IsFalse)
	_, exist = snap.TableByID(3)
	c.Assert(exist, check.IsFalse)
	storage.DoGC(115)
	_, err = storage.GetSnapshot(ctx, 100)
	c.Assert(err, check.NotNil)
	snap, err = storage.GetSnapshot(ctx, 115)
	c.Assert(err, check.IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, check.IsTrue)
	_, exist = snap.TableByID(2)
	c.Assert(exist, check.IsTrue)
	_, exist = snap.TableByID(3)
	c.Assert(exist, check.IsFalse)

	lastSchemaTs = storage.DoGC(155)
	require.Equal(t, uint64(140), lastSchemaTs)

	storage.AdvanceResolvedTs(185)

	snap, err = storage.GetSnapshot(ctx, 180)
	c.Assert(err, check.IsNil)
	_, exist = snap.SchemaByID(1)
	c.Assert(exist, check.IsFalse)
	_, exist = snap.TableByID(2)
	c.Assert(exist, check.IsFalse)
	_, exist = snap.TableByID(3)
	c.Assert(exist, check.IsFalse)
	_, err = storage.GetSnapshot(ctx, 130)
	c.Assert(err, check.NotNil)

	cancel()
	_, err = storage.GetSnapshot(ctx, 200)
	c.Assert(errors.Cause(err), check.Equals, context.Canceled)
}

func (t *schemaSuite) TestCreateSnapFromMeta(c *check.C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockStore()
	c.Assert(err, check.IsNil)
	defer store.Close() //nolint:errcheck

	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	c.Assert(err, check.IsNil)
	defer domain.Close()
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("create database test2")
	tk.MustExec("create table test.simple_test1 (id bigint primary key)")
	tk.MustExec("create table test.simple_test2 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test3 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test4 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test5 (a bigint)")
	ver, err := store.CurrentVersion(oracle.GlobalTxnScope)
	c.Assert(err, check.IsNil)
	meta, err := kv.GetSnapshotMeta(store, ver.Ver)
	c.Assert(err, check.IsNil)
	snap, err := newSchemaSnapshotFromMeta(meta, ver.Ver, false)
	c.Assert(err, check.IsNil)
	_, ok := snap.GetTableByName("test", "simple_test1")
	c.Assert(ok, check.IsTrue)
	tableID, ok := snap.GetTableIDByName("test2", "simple_test5")
	c.Assert(ok, check.IsTrue)
	c.Assert(snap.IsIneligibleTableID(tableID), check.IsTrue)
	dbInfo, ok := snap.SchemaByTableID(tableID)
	c.Assert(ok, check.IsTrue)
	c.Assert(dbInfo.Name.O, check.Equals, "test2")
	c.Assert(len(snap.tableInSchema), check.Equals, 3)
}

func (t *schemaSuite) TestSnapshotClone(c *check.C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockStore()
	c.Assert(err, check.IsNil)
	defer store.Close() //nolint:errcheck

	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	c.Assert(err, check.IsNil)
	defer domain.Close()
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("create database test2")
	tk.MustExec("create table test.simple_test1 (id bigint primary key)")
	tk.MustExec("create table test.simple_test2 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test3 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test4 (id bigint primary key)")
	tk.MustExec("create table test2.simple_test5 (a bigint)")
	ver, err := store.CurrentVersion(oracle.GlobalTxnScope)
	c.Assert(err, check.IsNil)
	meta, err := kv.GetSnapshotMeta(store, ver.Ver)
	c.Assert(err, check.IsNil)
	snap, err := newSchemaSnapshotFromMeta(meta, ver.Ver, false /* explicitTables */)
	c.Assert(err, check.IsNil)

	clone := snap.Clone()
	c.Assert(clone.tableNameToID, check.DeepEquals, snap.tableNameToID)
	c.Assert(clone.schemaNameToID, check.DeepEquals, snap.schemaNameToID)
	c.Assert(clone.truncateTableID, check.DeepEquals, snap.truncateTableID)
	c.Assert(clone.ineligibleTableID, check.DeepEquals, snap.ineligibleTableID)
	c.Assert(clone.currentTs, check.Equals, snap.currentTs)
	c.Assert(clone.explicitTables, check.Equals, snap.explicitTables)
	c.Assert(len(clone.tables), check.Equals, len(snap.tables))
	c.Assert(len(clone.schemas), check.Equals, len(snap.schemas))
	c.Assert(len(clone.partitionTable), check.Equals, len(snap.partitionTable))

	tableCount := len(snap.tables)
	clone.tables = make(map[int64]*model.TableInfo)
	c.Assert(len(snap.tables), check.Equals, tableCount)
}

func (t *schemaSuite) TestExplicitTables(c *check.C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockStore()
	c.Assert(err, check.IsNil)
	defer store.Close() //nolint:errcheck

	session.SetSchemaLease(0)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	c.Assert(err, check.IsNil)
	defer domain.Close()
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(c, store)
	ver1, err := store.CurrentVersion(oracle.GlobalTxnScope)
	c.Assert(err, check.IsNil)
	tk.MustExec("create database test2")
	tk.MustExec("create table test.simple_test1 (id bigint primary key)")
	tk.MustExec("create table test.simple_test2 (id bigint unique key)")
	tk.MustExec("create table test2.simple_test3 (a bigint)")
	tk.MustExec("create table test2.simple_test4 (a varchar(20) unique key)")
	tk.MustExec("create table test2.simple_test5 (a varchar(20))")
	ver2, err := store.CurrentVersion(oracle.GlobalTxnScope)
	c.Assert(err, check.IsNil)
	meta1, err := kv.GetSnapshotMeta(store, ver1.Ver)
	c.Assert(err, check.IsNil)
	snap1, err := newSchemaSnapshotFromMeta(meta1, ver1.Ver, true /* explicitTables */)
	c.Assert(err, check.IsNil)
	meta2, err := kv.GetSnapshotMeta(store, ver2.Ver)
	c.Assert(err, check.IsNil)
	snap2, err := newSchemaSnapshotFromMeta(meta2, ver2.Ver, false /* explicitTables */)
	c.Assert(err, check.IsNil)
	snap3, err := newSchemaSnapshotFromMeta(meta2, ver2.Ver, true /* explicitTables */)
	c.Assert(err, check.IsNil)

	c.Assert(len(snap2.tables)-len(snap1.tables), check.Equals, 5)
	// some system tables are also ineligible
	c.Assert(len(snap2.ineligibleTableID), check.GreaterEqual, 4)

	c.Assert(len(snap3.tables)-len(snap1.tables), check.Equals, 5)
	c.Assert(snap3.ineligibleTableID, check.HasLen, 0)
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
func (t *schemaSuite) TestSchemaStorage(c *check.C) {
	defer testleak.AfterTest(c)()
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
		c.Assert(err, check.IsNil)
		defer store.Close() //nolint:errcheck
		ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
			conf.AlterPrimaryKey = true
		})
		session.SetSchemaLease(0)
		session.DisableStats4Test()
		domain, err := session.BootstrapSession(store)
		c.Assert(err, check.IsNil)
		defer domain.Close()
		domain.SetStatsUpdating(true)
		tk := testkit.NewTestKit(c, store)

		for _, ddlSQL := range tc {
			tk.MustExec(ddlSQL)
		}

		jobs, err := getAllHistoryDDLJob(store)
		c.Assert(err, check.IsNil)
		scheamStorage, err := NewSchemaStorage(nil, 0, nil, false)
		c.Assert(err, check.IsNil)
		for _, job := range jobs {
			err := scheamStorage.HandleDDLJob(job)
			c.Assert(err, check.IsNil)
		}

		for _, job := range jobs {
			ts := job.BinlogInfo.FinishedTS
			meta, err := kv.GetSnapshotMeta(store, ts)
			c.Assert(err, check.IsNil)
			snapFromMeta, err := newSchemaSnapshotFromMeta(meta, ts, false)
			c.Assert(err, check.IsNil)
			snapFromSchemaStore, err := scheamStorage.GetSnapshot(ctx, ts)
			c.Assert(err, check.IsNil)

			tidySchemaSnapshot(snapFromMeta)
			tidySchemaSnapshot(snapFromSchemaStore)
			c.Assert(snapFromMeta, check.DeepEquals, snapFromSchemaStore,
				check.Commentf("%s", cmp.Diff(snapFromMeta, snapFromSchemaStore, cmp.AllowUnexported(schemaSnapshot{}, model.TableInfo{}))))
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
