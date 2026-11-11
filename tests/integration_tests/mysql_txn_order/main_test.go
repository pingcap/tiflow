// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// TestTiDBVerifierChecksBeforeUnlock prevents a completed backlog from hiding an overtake.
func TestTiDBVerifierChecksBeforeUnlock(t *testing.T) {
	up, upMock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer up.Close()
	down, downMock, err := sqlmock.New()
	require.NoError(t, err)
	defer down.Close()
	upMock.ExpectExec("INSERT INTO t VALUES (3,0)").WillReturnResult(sqlmock.NewResult(0, 1))
	downMock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	upMock.ExpectExec("DELETE FROM t WHERE id=3").WillReturnResult(sqlmock.NewResult(0, 1))
	downMock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	downMock.ExpectQuery(regexp.QuoteMeta("SELECT tidb_version()")).WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("TiDB"))
	downMock.ExpectBegin()
	downMock.ExpectQuery("SELECT id FROM t WHERE id=1 FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	for value := 1; value <= hotUpdates; value++ {
		upMock.ExpectExec("UPDATE t SET v=? WHERE id=1").WithArgs(value).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	upMock.ExpectExec("UPDATE t SET v=1 WHERE id=2").WillReturnResult(sqlmock.NewResult(0, 1))
	// The cold transaction is visible while the hot row is still locked.
	downMock.ExpectQuery("SELECT MAX").WillReturnRows(sqlmock.NewRows([]string{"hot", "cold"}).AddRow(0, 1))
	downMock.ExpectRollback()
	err = verifyOrder(context.Background(), up, down)
	require.ErrorContains(t, err, "while the hot row is locked")
	require.NoError(t, upMock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())
}
