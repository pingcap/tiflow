// Copyright 2021 PingCAP, Inc.
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

package binlog

import (
	"context"

	"github.com/DATA-DOG/go-sqlmock"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
)

var _ = Suite(&testStatusSuite{})

type testStatusSuite struct{}

func (t *testStatusSuite) TestGetBinaryLogs(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	ctx := context.Background()

	cases := []struct {
		rows  *sqlmock.Rows
		sizes FileSizes
	}{
		{
			sqlmock.NewRows([]string{"Log_name", "File_size"}).
				AddRow("mysql-bin.000001", 52119).
				AddRow("mysql-bin.000002", 114),
			[]BinlogSize{
				{
					"mysql-bin.000001", 52119,
				},
				{
					"mysql-bin.000002", 114,
				},
			},
		},
		{
			sqlmock.NewRows([]string{"Log_name", "File_size", "Encrypted"}).
				AddRow("mysql-bin.000001", 52119, "No").
				AddRow("mysql-bin.000002", 114, "No"),
			[]BinlogSize{
				{
					"mysql-bin.000001", 52119,
				},
				{
					"mysql-bin.000002", 114,
				},
			},
		},
	}

	for _, ca := range cases {
		mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(ca.rows)
		sizes, err2 := GetBinaryLogs(ctx, db)
		c.Assert(err2, IsNil)
		c.Assert(sizes, DeepEquals, ca.sizes)
		c.Assert(mock.ExpectationsWereMet(), IsNil)
	}

	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnError(&mysql.MySQLError{
		Number:  1227,
		Message: "Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation",
	})
	_, err2 := GetBinaryLogs(ctx, db)
	c.Assert(err2, NotNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (t *testStatusSuite) TestBinlogSizesAfter(c *C) {
	sizes := FileSizes{
		{Name: "mysql-bin.999999", Size: 1},
		{Name: "mysql-bin.1000000", Size: 2},
		{Name: "mysql-bin.1000001", Size: 4},
	}

	cases := []struct {
		position gmysql.Position
		expected int64
	}{
		{
			gmysql.Position{Name: "mysql-bin.999999", Pos: 0},
			7,
		},
		{
			gmysql.Position{Name: "mysql-bin.1000000", Pos: 1},
			5,
		},
		{
			gmysql.Position{Name: "mysql-bin.1000001", Pos: 3},
			1,
		},
	}

	for _, ca := range cases {
		c.Assert(sizes.After(ca.position), Equals, ca.expected)
	}
}
