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
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tiflow/pkg/config"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func newTestMockDB(t *testing.T) (db *sql.DB, mock sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	mock.ExpectQuery("select tidb_version()").WillReturnError(&dmysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})
	require.Nil(t, err)
	return
}

func TestNewObserver(t *testing.T) {
	t.Parallel()

	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() { dbIndex++ }()

		if dbIndex == 0 {
			// test db
			db, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}

		// normal db
		db, mock := newTestMockDB(t)
		mock.ExpectBegin()
		mock.ExpectClose()
		return db, nil
	}

	ctx := context.Background()
	sinkURI := "mysql://127.0.0.1:21347/"
	obs, err := NewObserver(ctx, sinkURI, config.GetDefaultReplicaConfig(),
		WithDBConnFactory(mockGetDBConn))
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		err = obs.Tick(ctx)
		require.NoError(t, err)
	}
	err = obs.Close()
	require.NoError(t, err)
}
