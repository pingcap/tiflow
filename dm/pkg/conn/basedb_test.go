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

package conn

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
)

var _ = Suite(&testBaseDBSuite{})

type testBaseDBSuite struct{}

func (t *testBaseDBSuite) TestGetBaseConn(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	baseDB := NewBaseDB(db, func() {})

	tctx := tcontext.Background()

	dbConn, err := baseDB.GetBaseConn(tctx.Context())
	c.Assert(dbConn, NotNil)
	c.Assert(err, IsNil)

	mock.ExpectQuery("select 1").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("1"))
	// nolint:sqlclosecheck
	rows, err := dbConn.QuerySQL(tctx, "select 1")
	c.Assert(err, IsNil)
	ids := make([]int, 0, 1)
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		c.Assert(err, IsNil)
		ids = append(ids, id)
	}
	c.Assert(ids, HasLen, 1)
	c.Assert(ids[0], Equals, 1)

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	affected, err := dbConn.ExecuteSQL(tctx, testStmtHistogram, "test", []string{"create database test"})
	c.Assert(err, IsNil)
	c.Assert(affected, Equals, 1)
	c.Assert(baseDB.Close(), IsNil)
}

func (t *testBaseDBSuite) TestFailDBPing(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/conn/failDBPing", "return"), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/pkg/conn/failDBPing")

	cfg := config.DBConfig{User: "root", Host: "127.0.0.1", Port: 3306}
	cfg.Adjust()
	db, err := DefaultDBProvider.Apply(cfg)
	c.Assert(db, IsNil)
	c.Assert(err, NotNil)

	err = mockDB.ExpectationsWereMet()
	c.Assert(err, IsNil)
}
