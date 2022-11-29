// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package checker

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/stretchr/testify/require"
)

func TestBinlogDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.Nil(t, err)
	ctx := context.Background()

	cases := []struct {
		doDB          string
		ignoreDB      string
		schemas       map[string]struct{}
		state         State
		caseSensitive bool
	}{
		// doDB
		{
			doDB: "do",
			schemas: map[string]struct{}{
				"do": {},
			},
			state: StateSuccess,
		},
		{
			doDB: "do",
			schemas: map[string]struct{}{
				"do":  {},
				"do2": {},
			},
			state: StateFailure,
		},
		{
			doDB: "do",
			schemas: map[string]struct{}{
				"do2": {},
			},
			state: StateFailure,
		},
		{
			doDB: "do,do2",
			schemas: map[string]struct{}{
				"do2": {},
			},
			state: StateSuccess,
		},
		// ignoreDB
		{
			ignoreDB: "ignore",
			schemas: map[string]struct{}{
				"do":     {},
				"ignore": {},
			},
			state: StateFailure,
		},
		{
			ignoreDB: "ignore",
			schemas: map[string]struct{}{
				"do":  {},
				"do2": {},
			},
			state: StateSuccess,
		},
		{
			ignoreDB: "ignore,ignore2",
			schemas: map[string]struct{}{
				"do":      {},
				"ignore2": {},
			},
			state: StateFailure,
		},
		{
			ignoreDB: "ignore,ignore2",
			schemas: map[string]struct{}{
				"ignore3": {},
			},
			state: StateSuccess,
		},
		// case sensitive
		{
			caseSensitive: true,
			doDB:          "Do",
			schemas: map[string]struct{}{
				"do": {},
			},
			state: StateFailure,
		},
		{
			caseSensitive: false,
			doDB:          "Do",
			schemas: map[string]struct{}{
				"do": {},
			},
			state: StateSuccess,
		},
	}

	for _, cs := range cases {
		binlogDBChecker := NewBinlogDBChecker(conn.NewBaseDBForTest(db), &dbutil.DBConfig{}, cs.schemas, cs.caseSensitive)
		versionRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("version", "mysql")
		masterStatusRow := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
			AddRow("", 0, cs.doDB, cs.ignoreDB, "")
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(versionRow)
		mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(masterStatusRow)

		r := binlogDBChecker.Check(ctx)
		require.Nil(t, mock.ExpectationsWereMet())
		require.Equal(t, cs.state, r.State)
		// the error message is moved to Errors
		if r.State == StateFailure {
			require.Equal(t, 1, len(r.Errors))
		}
	}
}

func TestMySQLBinlogRowImageChecker(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.Nil(t, err)
	ctx := context.Background()

	cases := []struct {
		version   string
		state     State
		needCheck bool
		rowImage  string
	}{
		// mysql < 5.6.2 don't need check
		{
			version:   "5.6.1-log",
			state:     StateSuccess,
			needCheck: false,
			rowImage:  "",
		},
		// mysql >= 5.6.2  need check - success
		{
			version:   "5.6.2-log",
			state:     StateSuccess,
			needCheck: true,
			rowImage:  "full",
		},
		// mysql >= 5.6.2  need check - failed
		{
			version:   "5.6.2-log",
			state:     StateFailure,
			needCheck: true,
			rowImage:  "NOBLOB",
		},

		// mariadb < 10.1.6 don't need check
		{
			version:   "10.1.5-MariaDB-1~wheezy",
			state:     StateSuccess,
			needCheck: false,
			rowImage:  "",
		},

		// mariadb >= 10.1.6  need check - success
		{
			version:   "10.1.6-MariaDB-1~wheezy",
			state:     StateSuccess,
			needCheck: true,
			rowImage:  "full",
		},
		// mariadb >= 10.1.6  need check - failed
		{
			version:   "10.1.6-MariaDB-1~wheezy",
			state:     StateFailure,
			needCheck: true,
			rowImage:  "NOBLOB",
		},
	}

	for _, cs := range cases {
		binlogDBChecker := NewMySQLBinlogRowImageChecker(db, &dbutil.DBConfig{})
		versionRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("version", cs.version)
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(versionRow)
		if cs.needCheck {
			binlogRowImageRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("binlog_row_image", cs.rowImage)
			mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'").WillReturnRows(binlogRowImageRow)
		}
		r := binlogDBChecker.Check(ctx)
		require.Nil(t, mock.ExpectationsWereMet())
		require.Equal(t, cs.state, r.State)
	}
}
