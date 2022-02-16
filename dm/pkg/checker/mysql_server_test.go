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

package checker

import (
	"context"

	"github.com/DATA-DOG/go-sqlmock"
	tc "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

func (t *testCheckSuite) TestMysqlVersion(c *tc.C) {
	versionChecker := &MySQLVersionChecker{}

	cases := []struct {
		rawVersion string
		pass       bool
	}{
		{"5.5.0-log", false},
		{"5.6.0-log", true},
		{"5.7.0-log", true},
		{"5.8.0-log", true}, // although it does not exist
		{"8.0.1-log", false},
		{"8.0.20", false},
		{"5.5.50-MariaDB-1~wheezy", false},
		{"10.1.1-MariaDB-1~wheezy", false},
		{"10.1.2-MariaDB-1~wheezy", false},
		{"10.13.1-MariaDB-1~wheezy", false},
	}

	for _, cs := range cases {
		result := &Result{
			State: StateWarning,
		}
		versionChecker.checkVersion(cs.rawVersion, result)
		c.Assert(result.State == StateSuccess, tc.Equals, cs.pass)
	}
}

func (t *testCheckSuite) TestBinlogDb(c *tc.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, tc.IsNil)
	ctx := context.Background()

	cases := []struct {
		flavor   string
		doDb     string
		ignoreDb string
		schemas  map[string]struct{}
		state    State
	}{
		// doDb
		{
			doDb: "do",
			schemas: map[string]struct{}{
				"do": {},
			},
			state: StateSuccess,
		},
		{
			doDb: "do",
			schemas: map[string]struct{}{
				"do":  {},
				"do2": {},
			},
			state: StateFailure,
		},
		{
			doDb: "do",
			schemas: map[string]struct{}{
				"do2": {},
			},
			state: StateFailure,
		},
		{
			doDb: "do, do2",
			schemas: map[string]struct{}{
				"do2": {},
			},
			state: StateSuccess,
		},
		// ignoreDb
		{
			ignoreDb: "ignore",
			schemas: map[string]struct{}{
				"do":     {},
				"ignore": {},
			},
			state: StateFailure,
		},
		{
			ignoreDb: "ignore",
			schemas: map[string]struct{}{
				"do":  {},
				"do2": {},
			},
			state: StateSuccess,
		},
		{
			ignoreDb: "ignore, ignore2",
			schemas: map[string]struct{}{
				"do":      {},
				"ignore2": {},
			},
			state: StateFailure,
		},
		{
			ignoreDb: "ignore, ignore2",
			schemas: map[string]struct{}{
				"ignore3": {},
			},
			state: StateSuccess,
		},
	}

	for _, cs := range cases {
		binlogDbChecker := NewBinlogDbChecker(db, &dbutil.DBConfig{}, cs.schemas)
		versionRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("version", "mysql")
		masterStatusRow := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
			AddRow("", 0, cs.doDb, cs.ignoreDb, "")
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(versionRow)
		mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(masterStatusRow)

		r := binlogDbChecker.Check(ctx)
		c.Assert(mock.ExpectationsWereMet(), tc.IsNil)
		c.Assert(r.State, tc.Equals, cs.state)
	}
}
