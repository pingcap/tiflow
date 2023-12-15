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

package mysql

import (
	"context"
	"database/sql"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/infoschema"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/stretchr/testify/require"
)

func TestWriteDDLEvent(t *testing.T) {
	dbIndex := 0
	GetDBConnImpl = func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
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
		mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("ALTER TABLE test.t1 ADD COLUMN a int").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		mock.ExpectBegin()
		mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("ALTER TABLE test.t1 ADD COLUMN a int").
			WillReturnError(&dmysql.MySQLError{
				Number: uint16(infoschema.ErrColumnExists.Code()),
			})
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewDDLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI, rc)

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
		Type:  timodel.ActionAddColumn,
		Query: "ALTER TABLE test.t1 ADD COLUMN a int",
	}
	err = sink.WriteDDLEvent(ctx, ddl1)
	require.Nil(t, err)
	err = sink.WriteDDLEvent(ctx, ddl1)
	require.Nil(t, err)

	sink.Close()
}

func TestNeedSwitchDB(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		ddl        *model.DDLEvent
		needSwitch bool
	}{
		{
			&model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "",
					},
				},
				Type: timodel.ActionCreateTable,
			},
			false,
		},
		{
			&model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{Schema: "golang"},
				},
				Type: timodel.ActionCreateSchema,
			},
			false,
		},
		{
			&model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{Schema: "golang"},
				},
				Type: timodel.ActionDropSchema,
			},
			false,
		},
		{
			&model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{Schema: "golang"},
				},
				Type: timodel.ActionCreateTable,
			},
			true,
		},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.needSwitch, needSwitchDB(tc.ddl))
	}
}

func TestAsyncExecAddIndex(t *testing.T) {
	ddlExecutionTime := time.Millisecond * 3000
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
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewDDLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI, rc)

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
	require.True(t, time.Since(start) >= 2*time.Second)
	sink.Close()
}
