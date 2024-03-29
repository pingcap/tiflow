// Copyright 2022 PingCAP, Inc.
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

package mysql

import (
	"database/sql"

	"github.com/DATA-DOG/go-sqlmock"
)

// MockTestDB creates a mock mysql database connection.
func MockTestDB() (*sql.DB, error) {
	// mock for test db, which is used querying TiDB session variable
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, err
	}

	columns := []string{"Variable_name", "Value"}
	mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
	)
	mock.ExpectQuery("show session variables like 'tidb_txn_mode';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("tidb_txn_mode", "pessimistic"),
	)
	mock.ExpectQuery("show session variables like 'transaction_isolation';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("transaction_isolation", "REPEATED-READ"),
	)
	mock.ExpectQuery("show session variables like 'tidb_placement_mode';").
		WillReturnRows(
			sqlmock.NewRows(columns).
				AddRow("tidb_placement_mode", "IGNORE"),
		)
	mock.ExpectQuery("show session variables like 'tidb_enable_external_ts_read';").
		WillReturnRows(
			sqlmock.NewRows(columns).
				AddRow("tidb_enable_external_ts_read", "OFF"),
		)
	mock.ExpectQuery("select character_set_name from information_schema.character_sets " +
		"where character_set_name = 'gbk';").WillReturnRows(
		sqlmock.NewRows([]string{"character_set_name"}).AddRow("gbk"),
	)
	mock.ExpectQuery("select tidb_version()").
		WillReturnRows(sqlmock.NewRows([]string{"tidb_version()"}).AddRow("5.7.25-TiDB-v4.0.0-beta-191-ga1b3e3b"))
	mock.ExpectQuery("select tidb_version()").
		WillReturnRows(sqlmock.NewRows([]string{"tidb_version()"}).AddRow("5.7.25-TiDB-v4.0.0-beta-191-ga1b3e3b"))
	mock.ExpectClose()
	return db, nil
}
