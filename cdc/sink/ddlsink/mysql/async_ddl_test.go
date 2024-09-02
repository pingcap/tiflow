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
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/stretchr/testify/require"
)

func TestWaitAsynExecDone(t *testing.T) {
	var dbIndex int32 = 0
	GetDBConnImpl = func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			atomic.AddInt32(&dbIndex, 1)
		}()
		if atomic.LoadInt32(&dbIndex) == 0 {
			// test db
			db, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectQuery("select tidb_version()").
			WillReturnRows(sqlmock.NewRows([]string{"tidb_version()"}).AddRow("5.7.25-TiDB-v4.0.0-beta-191-ga1b3e3b"))

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
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sinkURI, err := url.Parse("mysql://root:@127.0.0.1:4000")
	require.NoError(t, err)
	replicateCfg := config.GetDefaultReplicaConfig()
	ddlSink, err := NewDDLSink(ctx, sinkURI, replicateCfg)
	require.NoError(t, err)

	table := model.TableName{Schema: "test", Table: "sbtest0"}
	tables := make(map[model.TableName]struct{})
	tables[table] = struct{}{}

	// Test fast path, ddlSink.lastExecutedNormalDDLCache meet panic
	ddlSink.lastExecutedNormalDDLCache.Add(table, timodel.ActionAddIndex)
	require.Panics(t, func() {
		ddlSink.checkAsyncExecDDLDone(ctx, tables)
	})

	// Test fast path, ddlSink.lastExecutedNormalDDLCache is hit
	ddlSink.lastExecutedNormalDDLCache.Add(table, timodel.ActionCreateTable)
	done := ddlSink.checkAsyncExecDDLDone(ctx, tables)
	require.True(t, done)

	// Clenup the cache, always check the async running state
	ddlSink.lastExecutedNormalDDLCache.Remove(table)

	// Test has running async ddl job
	done = ddlSink.checkAsyncExecDDLDone(ctx, tables)
	require.False(t, done)

	// Test no running async ddl job
	done = ddlSink.checkAsyncExecDDLDone(ctx, tables)
	require.True(t, done)

	// Test ignore error
	done = ddlSink.checkAsyncExecDDLDone(ctx, tables)
	require.True(t, done)

	ddlSink.Close()
}

func TestAsyncExecAddIndex(t *testing.T) {
	ddlExecutionTime := time.Second * 15
	var dbIndex int32 = 0
	GetDBConnImpl = func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			atomic.AddInt32(&dbIndex, 1)
		}()
		if atomic.LoadInt32(&dbIndex) == 0 {
			// test db
			db, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectQuery("select tidb_version()").
			WillReturnRows(sqlmock.NewRows([]string{"tidb_version()"}).AddRow("5.7.25-TiDB-v4.0.0-beta-191-ga1b3e3b"))
		mock.ExpectBegin()
		mock.ExpectExec("USE `test`;").
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("Create index idx1 on test.t1(a)").
			WillDelayFor(ddlExecutionTime).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		mock.ExpectClose()
		return db, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewDDLSink(ctx, sinkURI, rc)

	require.Nil(t, err)

	ddl1 := &model.DDLEvent{
		StartTs:  1000,
		CommitTs: 1010,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "test",
				Table:  "t1",
			},
		},
		Type:  timodel.ActionAddIndex,
		Query: "Create index idx1 on test.t1(a)",
	}
	start := time.Now()
	err = sink.WriteDDLEvent(ctx, ddl1)
	require.Nil(t, err)
	require.True(t, time.Since(start) < ddlExecutionTime)
	require.True(t, time.Since(start) >= 10*time.Second)
	sink.Close()
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
