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
	"sort"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/check"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/tidb/infoschema"
	dbtypes "github.com/pingcap/tidb/types"
)

type EmitSuite struct{}

func Test(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&EmitSuite{})

func (s EmitSuite) TestShouldExecDDL(c *check.C) {
	// Set up
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	c.Assert(err, check.IsNil)
	defer db.Close()

	sink := mysqlSink{
		db: db,
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
	err = sink.EmitDDL(context.Background(), t)

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
		db: db,
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
	err = sink.EmitDDL(context.Background(), t)

	// Validate
	c.Assert(err, check.IsNil)
	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
}

type tableHelper struct {
}

func (h *tableHelper) TableByID(id int64) (info *schema.TableInfo, ok bool) {
	return schema.WrapTableInfo(&timodel.TableInfo{
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
	}), true
}

func (h *tableHelper) GetTableByName(schema, table string) (*schema.TableInfo, bool) {
	return h.TableByID(42)
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
		db:         db,
		infoGetter: &helper,
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
	err = sink.EmitDMLs(context.Background(), t)

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
		db:         db,
		infoGetter: &helper,
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
	mock.ExpectExec("DELETE FROM `test`.`user` WHERE `id` = ? AND `name` = ? LIMIT 1;").
		WithArgs(123, "tester1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Execute
	err = sink.EmitDMLs(context.Background(), t)

	// Validate
	c.Assert(err, check.IsNil)
	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
}

func (s EmitSuite) TestConfigureSinkURI(c *check.C) {
	cases := []struct {
		input    string
		expected string
	}{{
		input:    "root@tcp(127.0.0.1:3306)/mysql",
		expected: "root@tcp(127.0.0.1:3306)/?time_zone=UTC",
	}, {
		input:    "root@tcp(127.0.0.1:3306)/",
		expected: "root@tcp(127.0.0.1:3306)/?time_zone=UTC",
	}, {
		input:    "root@tcp(127.0.0.1:3306)/?time_zone=AA",
		expected: "root@tcp(127.0.0.1:3306)/?time_zone=UTC",
	}, {
		input:    "root@tcp(127.0.0.1:3306)/?time_zone=AA&some_option=BB",
		expected: "root@tcp(127.0.0.1:3306)/?some_option=BB&time_zone=UTC",
	}}
	for _, cs := range cases {
		sink, err := configureSinkURI(cs.input)
		c.Assert(err, check.IsNil)
		c.Assert(sink, check.Equals, cs.expected)
	}
}

type splitSuite struct{}

var _ = check.Suite(&splitSuite{})

func (s *splitSuite) TestCanHandleEmptyInput(c *check.C) {
	c.Assert(splitIndependentGroups(nil), check.HasLen, 0)
}

func (s *splitSuite) TestShouldSplitByTable(c *check.C) {
	var dmls []*model.DML
	addDMLs := func(n int, db, tbl string) {
		for i := 0; i < n; i++ {
			dml := model.DML{
				Database: db,
				Table:    tbl,
			}
			dmls = append(dmls, &dml)
		}
	}
	addDMLs(3, "db", "tbl1")
	addDMLs(2, "db", "tbl2")
	addDMLs(2, "db", "tbl1")
	addDMLs(2, "db2", "tbl2")

	groups := splitIndependentGroups(dmls)

	assertAllAreFromTbl := func(dmls []*model.DML, db, tbl string) {
		for _, dml := range dmls {
			c.Assert(dml.Database, check.Equals, db)
			c.Assert(dml.Table, check.Equals, tbl)
		}
	}
	c.Assert(groups, check.HasLen, 3)
	sort.Slice(groups, func(i, j int) bool {
		tblI := groups[i][0]
		tblJ := groups[j][0]
		if tblI.Database != tblJ.Database {
			return tblI.Database < tblJ.Database
		}
		return tblI.Table < tblJ.Table
	})
	assertAllAreFromTbl(groups[0], "db", "tbl1")
	assertAllAreFromTbl(groups[1], "db", "tbl2")
	assertAllAreFromTbl(groups[2], "db2", "tbl2")
}
