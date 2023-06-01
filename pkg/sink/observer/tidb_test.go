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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestTiDBObserver(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)

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

	ctx := context.Background()
	observer := NewTiDBObserver(db)
	err = observer.Tick(ctx)
	require.NoError(t, err)
	err = observer.Close()
	require.NoError(t, err)
}
