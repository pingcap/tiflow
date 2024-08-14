// Copyright 2023 PingCAP, Inc.
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

package observer

import (
	"context"
	"database/sql"
	"net/url"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/stretchr/testify/require"
)

func TestTiDBObserver(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)

	mockSQLForPassCreateConnCheck(mock)
	mock.ExpectQuery(queryConnIdleDurationStmt).
		WillReturnRows(
			sqlmock.NewRows(
				[]string{"time", "instance", "in_txn", "quantile", "value"}).
				AddRow("2023-01-16 17:22:16.881000", "10.2.6.127:11080", 0, 0.9, 0.309),
		)

	mock.ExpectQuery(queryConnCountStmt).
		WillReturnRows(
			sqlmock.NewRows(
				[]string{"time", "instance", "value"}).
				AddRow("2023-01-10 16:44:39.123000", "10.2.6.127:11080", 24),
		)

	mock.ExpectQuery(queryQueryDurationStmt).
		WillReturnRows(
			sqlmock.NewRows(
				[]string{"time", "instance", "sql_type", "value"}).
				AddRow("2023-01-10 16:47:08.283000", "10.2.6.127:11080",
					"Begin", 0.0018886375591793793).
				AddRow("2023-01-10 16:47:08.283000", "10.2.6.127:11080",
					"Insert", 0.014228768066070199).
				AddRow("2023-01-10 16:47:08.283000", "10.2.6.127:11080",
					"Delete", nil).
				AddRow("2023-01-10 16:47:08.283000", "10.2.6.127:11080",
					"Commit", 0.0004933262664880737),
		)

	mock.ExpectQuery(queryTxnDurationStmt).
		WillReturnRows(
			sqlmock.NewRows(
				[]string{"time", "instance", "type", "value"}).
				AddRow("2023-01-10 16:50:38.153000", "10.2.6.127:11080",
					"abort", nil).
				AddRow("2023-01-10 16:50:38.153000", "10.2.6.127:11080",
					"commit", 0.06155323076923076).
				AddRow("2023-01-10 16:50:38.153000", "10.2.6.127:11080",
					"rollback", nil),
		)
	mock.ExpectClose()

	mockGetDBConnImpl := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		return db, nil
	}
	cfg := mysql.NewConfig()
	sinkURIStr := "mysql://127.0.0.1:3306/"
	sinkURI, err := url.Parse(sinkURIStr)
	require.NoError(t, err)
	ctx := context.Background()
	connector, err := mysql.NewMySQLDBConnectorWithFactory(ctx, cfg, sinkURI, mockGetDBConnImpl)
	require.NoError(t, err)

	observer := NewTiDBObserver(connector)
	err = observer.Tick(ctx)
	require.NoError(t, err)
	err = observer.Close()
	require.NoError(t, err)
}

func mockSQLForPassCreateConnCheck(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("select character_set_name from information_schema.character_sets " +
		"where character_set_name = 'gbk';").WillReturnRows(
		sqlmock.NewRows([]string{"character_set_name"}).AddRow("gbk"),
	)
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
	mock.ExpectClose()
	// mock.ExpectQuery("select tidb_version()").
	// 	WillReturnRows(sqlmock.NewRows([]string{"tidb_version()"}).AddRow("5.7.25-TiDB-v4.0.0-beta-191-ga1b3e3b"))
	// mock.ExpectQuery("select tidb_version()").
	// 	WillReturnRows(sqlmock.NewRows([]string{"tidb_version()"}).AddRow("5.7.25-TiDB-v4.0.0-beta-191-ga1b3e3b"))
	// mock.ExpectClose()
}
