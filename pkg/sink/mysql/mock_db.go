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
func MockTestDB(adjustSQLMode bool) (*sql.DB, error) {
	// mock for test db, which is used querying TiDB session variable
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, err
	}
	if adjustSQLMode {
		mock.ExpectQuery("SELECT @@SESSION.sql_mode;").
			WillReturnRows(sqlmock.NewRows([]string{"@@SESSION.sql_mode"}).
				AddRow("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE"))
	}

	mock.ExpectQuery("select @@allow_auto_random_explicit_insert;").WillReturnRows(
		sqlmock.NewRows([]string{"@@allow_auto_random_explicit_insert"}).AddRow("0"),
	)
	mock.ExpectQuery("select @@tidb_txn_mode;").WillReturnRows(
		sqlmock.NewRows([]string{"@@tidb_txn_mode"}).AddRow("pessimistic"),
	)
	mock.ExpectQuery("select @@transaction_isolation;").WillReturnRows(
		sqlmock.NewRows([]string{"@@transaction_isolation"}).AddRow("REPEATED-READ"),
	)
	mock.ExpectQuery("select @@tidb_placement_mode;").
		WillReturnRows(
			sqlmock.NewRows([]string{"@@tidb_placement_mode"}).
				AddRow("IGNORE"),
		)
	mock.ExpectQuery("select @@tidb_enable_external_ts_read;").
		WillReturnRows(
			sqlmock.NewRows([]string{"@@tidb_enable_external_ts_read"}).
				AddRow("OFF"),
		)
	mock.ExpectQuery("select character_set_name from information_schema.character_sets " +
		"where character_set_name = 'gbk';").WillReturnRows(
		sqlmock.NewRows([]string{"character_set_name"}).AddRow("gbk"),
	)

	mock.ExpectClose()
	return db, nil
}
