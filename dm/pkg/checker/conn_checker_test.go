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

package checker

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/stretchr/testify/require"
)

func TestConnNumberChecker(t *testing.T) {
	var err error
	db, dbMock, err := sqlmock.New()
	require.NoError(t, err)
	stCfgs := []*config.SubTaskConfig{
		{
			SyncerConfig: config.SyncerConfig{
				WorkerCount: 10,
			},
			LoaderConfig: config.LoaderConfig{
				PoolSize: 16,
			},
		},
	}
	baseDB := conn.NewBaseDB(db, func() {})
	// test loader: fail
	dbMock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", 16))
	dbMock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT ALL PRIVILEGES ON *.* TO 'test'@'%'"))
	dbMock.ExpectQuery("SHOW PROCESSLIST").WillReturnRows(sqlmock.NewRows(
		[]string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}).
		AddRow(1, "root", "localhost", "test", "Query", 0, "init", ""),
	)
	loaderChecker := NewLoaderConnNumberChecker(baseDB, stCfgs)
	result := loaderChecker.Check(context.Background())
	require.Equal(t, 1, len(result.Errors))
	require.Equal(t, StateFailure, result.State)
	require.Regexp(t, "(.|\n)*is less than the number loader(.|\n)*", result.Errors[0].ShortErr)

	// test loader: success
	db, dbMock, err = sqlmock.New()
	require.NoError(t, err)
	baseDB = conn.NewBaseDB(db, func() {})
	dbMock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", 17))
	dbMock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT ALL PRIVILEGES ON *.* TO 'test'@'%'"))
	dbMock.ExpectQuery("SHOW PROCESSLIST").WillReturnRows(sqlmock.NewRows(
		[]string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}).
		AddRow(1, "root", "localhost", "test", "Query", 0, "init", ""),
	)
	loaderChecker = NewLoaderConnNumberChecker(baseDB, stCfgs)
	result = loaderChecker.Check(context.Background())
	require.Equal(t, 0, len(result.Errors))
	require.Equal(t, StateSuccess, result.State)

	// test loader maxConn - usedConn < neededConn: warn
	db, dbMock, err = sqlmock.New()
	require.NoError(t, err)
	baseDB = conn.NewBaseDB(db, func() {})
	dbMock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", 17))
	dbMock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT ALL PRIVILEGES ON *.* TO 'test'@'%'"))
	dbMock.ExpectQuery("SHOW PROCESSLIST").WillReturnRows(sqlmock.NewRows(
		[]string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}).
		AddRow(1, "root", "localhost", "test", "Query", 0, "init", "").
		AddRow(2, "root", "localhost", "test", "Query", 0, "init", ""),
	)
	loaderChecker = NewLoaderConnNumberChecker(baseDB, stCfgs)
	result = loaderChecker.Check(context.Background())
	require.Equal(t, 1, len(result.Errors))
	require.Equal(t, StateWarning, result.State)
	require.Regexp(t, "(.|\n)*is less than loader needs(.|\n)*", result.Errors[0].ShortErr)

	// test loader no enough privilege: warn
	db, dbMock, err = sqlmock.New()
	require.NoError(t, err)
	baseDB = conn.NewBaseDB(db, func() {})
	dbMock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", 17))
	dbMock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT INDEX ON *.* TO 'test'@'%'"))
	dbMock.ExpectQuery("SHOW PROCESSLIST").WillReturnRows(sqlmock.NewRows(
		[]string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}).
		AddRow(1, "root", "localhost", "test", "Query", 0, "init", ""),
	)
	loaderChecker = NewLoaderConnNumberChecker(baseDB, stCfgs)
	result = loaderChecker.Check(context.Background())
	require.Equal(t, 1, len(result.Errors))
	require.Equal(t, StateWarning, result.State)
	require.Regexp(t, "(.|\n)*lack of Super global(.|\n)*", result.Errors[0].ShortErr)
}
