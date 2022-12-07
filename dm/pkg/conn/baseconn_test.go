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
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/errno"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestBaseConn(t *testing.T) {
	baseConn := NewBaseConnForTest(nil, nil)

	tctx := tcontext.Background()
	err := baseConn.SetRetryStrategy(nil)
	require.NoError(t, err)

	// nolint:sqlclosecheck,rowserrcheck
	_, err = baseConn.QuerySQL(tctx, "select 1")
	require.True(t, terror.ErrDBUnExpect.Equal(err))

	_, err = baseConn.ExecuteSQL(tctx, nil, "test", []string{""})
	require.True(t, terror.ErrDBUnExpect.Equal(err))

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	dbConn, err := db.Conn(tctx.Context())
	require.NoError(t, err)

	baseConn = &BaseConn{dbConn, terror.ScopeNotSet, nil}

	err = baseConn.SetRetryStrategy(&retry.FiniteRetryStrategy{})
	require.NoError(t, err)

	mock.ExpectQuery("select 1").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	// nolint:sqlclosecheck,rowserrcheck
	rows, err := baseConn.QuerySQL(tctx, "select 1")
	require.NoError(t, err)
	ids := make([]int, 0, 1)
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		require.NoError(t, err)
		ids = append(ids, id)
	}
	require.Equal(t, []int{1}, ids)

	mock.ExpectQuery("select 1").WillReturnError(errors.New("invalid connection"))
	// nolint:sqlclosecheck,rowserrcheck
	_, err = baseConn.QuerySQL(tctx, "select 1")
	require.True(t, terror.ErrDBQueryFailed.Equal(err))

	affected, err := baseConn.ExecuteSQL(tctx, nil, "test", []string{})
	require.NoError(t, err)
	require.Equal(t, 0, affected)

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	affected, err = baseConn.ExecuteSQL(tctx, nil, "test", []string{"create database test"})
	require.NoError(t, err)
	require.Equal(t, 1, affected)

	mock.ExpectBegin().WillReturnError(errors.New("begin error"))
	_, err = baseConn.ExecuteSQL(tctx, nil, "test", []string{"create database test"})
	require.True(t, terror.ErrDBExecuteFailed.Equal(err))

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnError(errors.New("invalid connection"))
	mock.ExpectRollback()
	_, err = baseConn.ExecuteSQL(tctx, nil, "test", []string{"create database test"})
	require.True(t, terror.ErrDBExecuteFailed.Equal(err))

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnError(errors.New("ignore me"))
	mock.ExpectExec("create database test").WillReturnError(errors.New("don't ignore me"))
	mock.ExpectRollback()
	ignoreF := func(err error) bool {
		return err.Error() == "ignore me"
	}
	affected, err = baseConn.ExecuteSQLWithIgnoreError(tctx, nil, "test", ignoreF, []string{"create database test", "create database test"})
	require.Contains(t, err.Error(), "don't ignore me")
	require.Equal(t, 1, affected)

	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, baseConn.forceClose())
}

func TestAutoSplit4TxnTooLarge(t *testing.T) {
	tctx := tcontext.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	dbConn, err := db.Conn(tctx.Context())
	require.NoError(t, err)

	baseConn := &BaseConn{dbConn, terror.ScopeNotSet, nil}

	errTxnTooLarge := &mysql.MySQLError{
		Number:  errno.ErrTxnTooLarge,
		Message: "Transaction is too large, size: 123456",
	}
	dml := "some DML"

	mockTxn := func(size int) {
		mock.ExpectBegin()
		for i := 0; i < size; i++ {
			mock.ExpectExec(dml).WillReturnResult(sqlmock.NewResult(1, 1))
		}
		if size == 1 {
			mock.ExpectCommit()
		} else {
			mock.ExpectCommit().WillReturnError(errTxnTooLarge)
		}
	}

	// we test a transaction of 3 statements
	mockTxn(3) // [1,2,3]
	mockTxn(1) // [1]
	mockTxn(2) // [2,3]
	mockTxn(1) // [2]
	mockTxn(1) // [3]
	txn := []string{dml, dml, dml}
	args := [][]interface{}{nil, nil, nil}

	err = baseConn.ExecuteSQLsAutoSplit(tctx, nil, "test", txn, args...)
	require.NoError(t, err)

	mockTxnAlwaysError := func(size int) {
		mock.ExpectBegin()
		for i := 0; i < size; i++ {
			mock.ExpectExec(dml).WillReturnResult(sqlmock.NewResult(1, 1))
		}
		mock.ExpectCommit().WillReturnError(errTxnTooLarge)
	}

	mockTxnAlwaysError(3)
	mockTxnAlwaysError(1)

	err = baseConn.ExecuteSQLsAutoSplit(tctx, nil, "test", txn, args...)
	require.ErrorContains(t, err, "Transaction is too large")

	require.NoError(t, mock.ExpectationsWereMet())
}
