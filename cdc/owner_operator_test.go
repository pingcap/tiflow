package cdc

import (
	"context"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"github.com/pingcap/tidb/infoschema"
)

type ddlHandlerSuite struct{}

var _ = check.Suite(&ddlHandlerSuite{})

func (d *ddlHandlerSuite) TestShouldExecDDL(c *check.C) {
	// Set up
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	c.Assert(err, check.IsNil)
	defer db.Close()

	h := &ddlHandler{}

	ddl := &txn.DDL{
		Database: "test",
		Table:    "user",
		Job: &model.Job{
			Query: "CREATE TABLE user (i1d INT PRIMARY KEY);",
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(ddl.Job.Query).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = h.execDDLWithMaxRetries(context.Background(), db, ddl, 5)

	// Validate
	c.Assert(err, check.IsNil)
	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
}

func (d *ddlHandlerSuite) TestShouldIgnoreCertainDDLError(c *check.C) {
	// Set up
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	c.Assert(err, check.IsNil)
	defer db.Close()

	h := &ddlHandler{}

	ddl := &txn.DDL{
		Database: "test",
		Table:    "user",
		Job: &model.Job{
			Query: "CREATE TABLE user (id INT PRIMARY KEY);",
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	ignorable := dmysql.MySQLError{
		Number: uint16(infoschema.ErrTableExists.Code()),
	}
	mock.ExpectExec(ddl.Job.Query).WillReturnError(&ignorable)

	err = h.execDDLWithMaxRetries(context.Background(), db, ddl, 5)

	// Validate
	c.Assert(err, check.IsNil)
	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
}
