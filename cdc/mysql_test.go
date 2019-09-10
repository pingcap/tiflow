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
	"context"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/check"
)

type EmitSuite struct{}

var _ = check.Suite(&EmitSuite{})

type dummyInspector struct{}

func (dummyInspector) Get(schema, table string) (*tableInfo, error) {
	return nil, nil
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

	txn := Txn{
		DDL: &DDL{
			Database: "test",
			Table:    "user",
			SQL:      "CREATE TABLE user (id INT PRIMARY KEY);",
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(txn.DDL.SQL).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Execute
	err = sink.Emit(context.Background(), txn)

	// Validate
	c.Assert(err, check.IsNil)
}
