// Copyright 2025 PingCAP, Inc.
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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/stretchr/testify/require"
)

func TestPrimaryKeyChecker(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	ctx := context.Background()

	// common variables query expected by GetConcurrency
	maxConnectionsRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", "2")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnectionsRow)
	sqlModeRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)

	// 1. success: table has primary key
	createTableRow := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
            "c" int(11) NOT NULL,
            PRIMARY KEY ("c")
        ) ENGINE=InnoDB`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)

	checker := NewPrimaryKeyChecker(
		map[string]*conn.BaseDB{"test-source": conn.NewBaseDBForTest(db)},
		map[string]map[filter.Table][]filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"}: {{Schema: "test-db", Name: "test-table-1"}},
		}},
		1,
	)
	res := checker.Check(ctx)
	require.Equal(t, StateSuccess, res.State)
	require.NoError(t, mock.ExpectationsWereMet())

	// 2. failure: table missing primary key
	// reset expectations on same mock
	maxConnectionsRow2 := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", "2")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnectionsRow2)
	sqlModeRow2 := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow2)

	createTableRowNoPK := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
            "c" int(11) NOT NULL
        ) ENGINE=InnoDB`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRowNoPK)

	checker = NewPrimaryKeyChecker(
		map[string]*conn.BaseDB{"test-source": conn.NewBaseDBForTest(db)},
		map[string]map[filter.Table][]filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"}: {{Schema: "test-db", Name: "test-table-1"}},
		}},
		1,
	)
	res = checker.Check(ctx)
	require.Equal(t, StateFailure, res.State)
	require.Len(t, res.Errors, 1)
	require.Contains(t, res.Errors[0].ShortErr, "table `test-db`.`test-table-1` primary key does not exist")
	require.NoError(t, mock.ExpectationsWereMet())

	// 3. table deleted (ErrNoSuchTable): should be skipped (no errors)
	maxConnectionsRow3 := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", "2")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnectionsRow3)
	sqlModeRow3 := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow3)

	// return MySQL ErrNoSuchTable
	// MySQL ER_NO_SUCH_TABLE error number is 1146
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnError(&mysql.MySQLError{Number: 1146, Message: "no such table"})

	checker = NewPrimaryKeyChecker(
		map[string]*conn.BaseDB{"test-source": conn.NewBaseDBForTest(db)},
		map[string]map[filter.Table][]filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"}: {{Schema: "test-db", Name: "test-table-1"}},
		}},
		1,
	)
	res = checker.Check(ctx)
	// table was deleted during checking, so no error should be produced
	require.Equal(t, StateSuccess, res.State)
	require.NoError(t, mock.ExpectationsWereMet())

	// 4. parse error from GetCreateTableSQL / parser: should produce a warning
	maxConnectionsRow4 := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", "2")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnectionsRow4)
	sqlModeRow4 := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow4)

	// return an invalid create table SQL to trigger parser error
	createTableRowInvalid := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", "INVALID CREATE TABLE STATEMENT")
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRowInvalid)

	checker = NewPrimaryKeyChecker(
		map[string]*conn.BaseDB{"test-source": conn.NewBaseDBForTest(db)},
		map[string]map[filter.Table][]filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"}: {{Schema: "test-db", Name: "test-table-1"}},
		}},
		1,
	)
	res = checker.Check(ctx)
	require.Equal(t, StateWarning, res.State)
	require.Len(t, res.Errors, 1)
	require.Contains(t, res.Errors[0].ShortErr, "INVALID CREATE TABLE STATEMENT")
	require.NoError(t, mock.ExpectationsWereMet())
}
