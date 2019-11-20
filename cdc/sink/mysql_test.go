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

package sink

import (
	"context"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/check"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb/infoschema"
	dbtypes "github.com/pingcap/tidb/types"
)

type EmitSuite struct{}

var _ = check.Suite(&EmitSuite{})

type dummyInspector struct {
	tableInspector
}

func (dummyInspector) Refresh(schema, table string) {
}

func (s EmitSuite) TestShouldExecDDL(c *check.C) {
	// Set up
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	c.Assert(err, check.IsNil)
	defer db.Close()

	sink := mysqlSink{
		db:           db,
		tblInspector: dummyInspector{},
	}

	t := model.Txn{
		DDL: &model.DDL{
			Database: "test",
			Table:    "user",
			Job: &timodel.Job{
				Query: "CREATE TABLE user (id INT PRIMARY KEY);",
			},
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(t.DDL.Job.Query).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Execute
	err = sink.Emit(context.Background(), t)

	// Validate
	c.Assert(err, check.IsNil)
	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
}

func (s EmitSuite) TestShouldIgnoreCertainDDLError(c *check.C) {
	// Set up
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	c.Assert(err, check.IsNil)
	defer db.Close()

	sink := mysqlSink{
		db:           db,
		tblInspector: dummyInspector{},
	}

	t := model.Txn{
		DDL: &model.DDL{
			Database: "test",
			Table:    "user",
			Job: &timodel.Job{
				Query: "CREATE TABLE user (id INT PRIMARY KEY);",
			},
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	ignorable := dmysql.MySQLError{
		Number: uint16(infoschema.ErrTableExists.Code()),
	}
	mock.ExpectExec(t.DDL.Job.Query).WillReturnError(&ignorable)

	// Execute
	err = sink.Emit(context.Background(), t)

	// Validate
	c.Assert(err, check.IsNil)
	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
}

type tableHelper struct {
	tableInspector
	TableInfoGetter
}

func (h *tableHelper) Get(schema, table string) (*tableInfo, error) {
	return &tableInfo{
		columns: []string{"id", "name"},
		uniqueKeys: []indexInfo{
			{name: "pk", columns: []string{"id"}},
		},
	}, nil
}

func (h *tableHelper) TableByID(id int64) (info *timodel.TableInfo, ok bool) {
	return &timodel.TableInfo{
		Columns: []*timodel.ColumnInfo{
			{
				Name:  timodel.CIStr{O: "id"},
				State: timodel.StatePublic,
				FieldType: types.FieldType{
					Tp:      mysql.TypeLong,
					Flen:    types.UnspecifiedLength,
					Decimal: types.UnspecifiedLength,
				},
			},
			{
				Name:  timodel.CIStr{O: "name"},
				State: timodel.StatePublic,
				FieldType: types.FieldType{
					Tp:      mysql.TypeString,
					Flen:    types.UnspecifiedLength,
					Decimal: types.UnspecifiedLength,
				},
			},
		},
	}, true
}

func (h *tableHelper) GetTableIDByName(schema, table string) (int64, bool) {
	return 42, true
}

func (s EmitSuite) TestShouldExecReplaceInto(c *check.C) {
	// Set up
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	c.Assert(err, check.IsNil)
	defer db.Close()

	helper := tableHelper{}
	sink := mysqlSink{
		db:           db,
		tblInspector: &helper,
		infoGetter:   &helper,
	}

	t := model.Txn{
		DMLs: []*model.DML{
			{
				Database: "test",
				Table:    "user",
				Tp:       model.InsertDMLType,
				Values: map[string]dbtypes.Datum{
					"id":   dbtypes.NewDatum(42),
					"name": dbtypes.NewDatum("tester1"),
				},
			},
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("REPLACE INTO `test`.`user`(`id`,`name`) VALUES (?,?);").
		WithArgs(42, "tester1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Execute
	err = sink.Emit(context.Background(), t)

	// Validate
	c.Assert(err, check.IsNil)
	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
}

func (s EmitSuite) TestShouldExecDelete(c *check.C) {
	// Set up
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	c.Assert(err, check.IsNil)
	defer db.Close()

	helper := tableHelper{}
	sink := mysqlSink{
		db:           db,
		tblInspector: &helper,
		infoGetter:   &helper,
	}

	t := model.Txn{
		DMLs: []*model.DML{
			{
				Database: "test",
				Table:    "user",
				Tp:       model.DeleteDMLType,
				Values: map[string]dbtypes.Datum{
					"id":   dbtypes.NewDatum(123),
					"name": dbtypes.NewDatum("tester1"),
				},
			},
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM `test`.`user` WHERE `id` = ? LIMIT 1;").
		WithArgs(123).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Execute
	err = sink.Emit(context.Background(), t)

	// Validate
	c.Assert(err, check.IsNil)
	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
}

type FilterSuite struct{}

var _ = check.Suite(&FilterSuite{})

func (s *FilterSuite) TestFilterDMLs(c *check.C) {
	t := model.Txn{
		DMLs: []*model.DML{
			{Database: "INFORMATIOn_SCHEmA"},
			{Database: "test"},
			{Database: "test_mysql"},
			{Database: "mysql"},
		},
		Ts: 213,
	}
	filterBySchemaAndTable(&t)
	c.Assert(t.Ts, check.Equals, uint64(213))
	c.Assert(t.DDL, check.IsNil)
	c.Assert(t.DMLs, check.HasLen, 2)
	c.Assert(t.DMLs[0].Database, check.Equals, "test")
	c.Assert(t.DMLs[1].Database, check.Equals, "test_mysql")
}

func (s *FilterSuite) TestFilterDDL(c *check.C) {
	t := model.Txn{
		DDL: &model.DDL{Database: "performance_schema"},
		Ts:  10234,
	}
	filterBySchemaAndTable(&t)
	c.Assert(t.Ts, check.Equals, uint64((10234)))
	c.Assert(t.DMLs, check.HasLen, 0)
	c.Assert(t.DDL, check.IsNil)
}
