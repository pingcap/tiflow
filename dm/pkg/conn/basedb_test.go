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
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/dm/config"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestGetBaseConn(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	baseDB := NewBaseDB(db)

	tctx := tcontext.Background()

	dbConn, err := baseDB.GetBaseConn(tctx.Context())
	require.NoError(t, err)
	require.NotNil(t, dbConn)

	mock.ExpectQuery("select 1").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("1"))
	// nolint:sqlclosecheck,rowserrcheck
	rows, err := dbConn.QuerySQL(tctx, "select 1")
	require.NoError(t, err)
	ids := make([]int, 0, 1)
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		require.NoError(t, err)
		ids = append(ids, id)
	}
	require.Equal(t, []int{1}, ids)

	mock.ExpectBegin()
	mock.ExpectExec("create database test").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	affected, err := dbConn.ExecuteSQL(tctx, nil, "test", []string{"create database test"})
	require.NoError(t, err)
	require.Equal(t, 1, affected)
	require.NoError(t, baseDB.Close())
}

func TestFailDBPing(t *testing.T) {
	netTimeout = time.Second
	defer func() {
		netTimeout = utils.DefaultDBTimeout
	}()
	port := freeport.GetPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	l, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	defer l.Close()

	cfg := &config.DBConfig{User: "root", Host: "127.0.0.1", Port: port}
	cfg.Adjust()
	impl := &DefaultDBProviderImpl{}
	db, err := impl.Apply(cfg)
	require.Error(t, err)
	require.Nil(t, db)
}

func TestGetBaseConnWontBlock(t *testing.T) {
	netTimeout = time.Second
	defer func() {
		netTimeout = utils.DefaultDBTimeout
	}()
	ctx := context.Background()

	port := freeport.GetPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	l, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	defer l.Close()

	// no such MySQL listening on port, so Conn will block
	db, err := sql.Open("mysql", "root:@tcp("+addr+")/test")
	require.NoError(t, err)

	baseDB := NewBaseDB(db)

	_, err = baseDB.GetBaseConn(ctx)
	require.Error(t, err)
}
