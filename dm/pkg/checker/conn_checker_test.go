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
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/stretchr/testify/require"
)

func TestConnAmountChecker(t *testing.T) {
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
	// test syncer
	dbMock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", 14))
	syncerChecker := NewSyncerConnAmountCheker(baseDB, stCfgs)
	result := syncerChecker.Check(context.Background())
	require.Equal(t, 1, len(result.Errors))
	require.Equal(t, StateFailure, result.State)
	require.Regexp(t, "(.|\n)*is less than the amount syncer(.|\n)*", result.Errors[0].ShortErr)
	dbMock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", 15))
	result = syncerChecker.Check(context.Background())
	require.Equal(t, 0, len(result.Errors))
	require.Equal(t, StateSuccess, result.State)
	// test loader
	dbMock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", 16))
	loaderChecker := NewLoaderConnAmountChecker(baseDB, stCfgs)
	result = loaderChecker.Check(context.Background())
	require.Equal(t, 1, len(result.Errors))
	require.Equal(t, StateFailure, result.State)
	require.Regexp(t, "(.|\n)*is less than the amount loader(.|\n)*", result.Errors[0].ShortErr)
	dbMock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", 17))
	result = loaderChecker.Check(context.Background())
	require.Equal(t, 0, len(result.Errors))
	require.Equal(t, StateSuccess, result.State)
}
