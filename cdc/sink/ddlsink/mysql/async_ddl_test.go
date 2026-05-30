// Copyright 2024 PingCAP, Inc.
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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/stretchr/testify/require"
)

func TestWaitAsynExecDone(t *testing.T) {
	dbConnFactory := pmysql.NewDBConnectionFactoryForTest()
	dbConnFactory.SetStandardConnectionFactory(func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectQuery("select tidb_version()").
			WillReturnRows(sqlmock.NewRows([]string{"tidb_version()"}).AddRow("5.7.25-TiDB-v4.0.0-beta-191-ga1b3e3b"))
		mock.ExpectQuery("select tidb_version()").WillReturnError(&dmysql.MySQLError{
			Number:  1305,
			Message: "FUNCTION test.tidb_version does not exist",
		})

		// Case 1: there is a running add index job
		mock.ExpectQuery(fmt.Sprintf(checkRunningAddIndexSQL, "test", "sbtest0")).WillReturnRows(
			sqlmock.NewRows([]string{
				"JOB_ID", "DB_NAME", "TABLE_NAME", "JOB_TYPE", "SCHEMA_STATE", "SCHEMA_ID", "TABLE_ID",
				"ROW_COUNT", "CREATE_TIME", "START_TIME", "END_TIME", "STATE",
			}).AddRow(
				1, "test", "sbtest0", "add index", "write reorganization", 1, 1, 0, time.Now(), nil, time.Now(), "running",
			).AddRow(
				2, "test", "sbtest0", "add index", "write reorganization", 1, 1, 0, time.Now(), time.Now(), time.Now(), "queueing",
			),
		)
		// Case 2: there is no running add index job
		// Case 3: no permission to query ddl_jobs, TiDB will return empty result
		mock.ExpectQuery(fmt.Sprintf(checkRunningAddIndexSQL, "test", "sbtest0")).WillReturnRows(
			sqlmock.NewRows(nil),
		)
		// Case 4: query ddl_jobs failed
		mock.ExpectQuery(fmt.Sprintf(checkRunningAddIndexSQL, "test", "sbtest0")).WillReturnError(
			errors.New("mock error"),
		)

		mock.ExpectClose()
		return db, nil
	})
	GetDBConnImpl = dbConnFactory

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sinkURI, err := url.Parse("mysql://root:@127.0.0.1:4000")
	require.NoError(t, err)
	replicateCfg := config.GetDefaultReplicaConfig()
	ddlSink, err := NewDDLSink(ctx, model.DefaultChangeFeedID("test"), sinkURI, replicateCfg)
	require.NoError(t, err)

	table := model.TableName{Schema: "test", Table: "sbtest0"}
	tables := make(map[model.TableName]struct{})
	tables[table] = struct{}{}

	// Test has running async ddl job
	done := ddlSink.checkAsyncExecDDLDone(ctx, tables)
	require.False(t, done)

	// Test no running async ddl job
	done = ddlSink.checkAsyncExecDDLDone(ctx, tables)
	require.True(t, done)

	// Test ignore error
	done = ddlSink.checkAsyncExecDDLDone(ctx, tables)
	require.True(t, done)

	ddlSink.Close()
}

func TestNeedWaitAsyncExecDone(t *testing.T) {
	sink := &DDLSink{
		cfg: &pmysql.Config{
			IsTiDB: false,
		},
	}
	require.False(t, sink.needWaitAsyncExecDone(timodel.ActionTruncateTable))

	sink.cfg.IsTiDB = true
	require.True(t, sink.needWaitAsyncExecDone(timodel.ActionTruncateTable))
	require.True(t, sink.needWaitAsyncExecDone(timodel.ActionDropTable))
	require.True(t, sink.needWaitAsyncExecDone(timodel.ActionDropIndex))

	require.False(t, sink.needWaitAsyncExecDone(timodel.ActionCreateTable))
	require.False(t, sink.needWaitAsyncExecDone(timodel.ActionCreateTables))
	require.False(t, sink.needWaitAsyncExecDone(timodel.ActionCreateSchema))
}
