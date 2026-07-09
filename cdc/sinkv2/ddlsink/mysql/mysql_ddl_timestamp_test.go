// Copyright 2025 PingCAP, Inc.
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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/stretchr/testify/require"
)

func ddlSessionTimestampForTest(ddl *model.DDLEvent, timezone string) (string, bool) {
	if ddl == nil {
		return "", false
	}
	ts, ok := ddlSessionTimestampFromOriginDefault(ddl, timezone)
	if !ok {
		return "", false
	}
	return formatUnixTimestamp(ts), true
}

func expectDDLExec(mock sqlmock.Sqlmock, ddl *model.DDLEvent, timezone string) {
	ddlTimestamp, ok := ddlSessionTimestampForTest(ddl, timezone)
	mock.ExpectExec("SET TIMESTAMP = DEFAULT").
		WillReturnResult(sqlmock.NewResult(1, 1))
	if ok {
		mock.ExpectExec("SET TIMESTAMP = " + ddlTimestamp).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectExec(ddl.Query).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if ok {
		mock.ExpectExec("SET TIMESTAMP = DEFAULT").
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
}

func newTestDDLSink(t *testing.T) (*DDLSink, *sql.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)

	cfg := pmysql.NewConfig()
	cfg.Timezone = "\"UTC\""

	sink := &DDLSink{
		id:  model.DefaultChangeFeedID("test"),
		db:  db,
		cfg: cfg,
	}

	return sink, db, mock
}

func ddlEventFromJob(t *testing.T, job *timodel.Job) *model.DDLEvent {
	require.NotNil(t, job)
	require.NotNil(t, job.BinlogInfo)
	schemaName := job.SchemaName
	if schemaName == "" {
		schemaName = "test"
	}
	tableInfo := model.WrapTableInfo(job.SchemaID, schemaName, job.BinlogInfo.FinishedTS, job.BinlogInfo.TableInfo)
	return &model.DDLEvent{
		StartTs:   job.StartTS,
		CommitTs:  job.BinlogInfo.FinishedTS,
		Query:     job.Query,
		TableInfo: tableInfo,
		Type:      job.Type,
	}
}

func TestExecDDL_UsesOriginDefaultTimestampForCurrentTimestampDefault(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.Tk().MustExec("set time_zone = 'UTC'")
	helper.Tk().MustExec("set @@timestamp = 1720000000.123456")
	_ = helper.DDL2Job("create table t (id int primary key)")

	job := helper.DDL2Job("alter table t add column updatetime datetime(6) default current_timestamp(6)")
	ddlEvent := ddlEventFromJob(t, job)

	sink, db, mock := newTestDDLSink(t)
	defer db.Close()

	originTs, ok := ddlSessionTimestampFromOriginDefault(ddlEvent, sink.cfg.Timezone)
	require.True(t, ok)
	ddlTimestamp, ok := ddlSessionTimestampForTest(ddlEvent, sink.cfg.Timezone)
	require.True(t, ok)
	require.Equal(t, formatUnixTimestamp(originTs), ddlTimestamp)

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	expectDDLExec(mock, ddlEvent, sink.cfg.Timezone)
	mock.ExpectCommit()

	err := sink.execDDL(context.Background(), ddlEvent)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecDDL_DoesNotSetTimestampWhenNoCurrentTimestampDefault(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	_ = helper.DDL2Job("create table t (id int primary key)")

	job := helper.DDL2Job("alter table t add column age int default 1")
	ddlEvent := ddlEventFromJob(t, job)

	sink, db, mock := newTestDDLSink(t)
	defer db.Close()

	_, ok := ddlSessionTimestampForTest(ddlEvent, sink.cfg.Timezone)
	require.False(t, ok)

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	expectDDLExec(mock, ddlEvent, sink.cfg.Timezone)
	mock.ExpectCommit()

	err := sink.execDDL(context.Background(), ddlEvent)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDDLSessionTimestampFromOriginDefaultWithNilTableInfo(t *testing.T) {
	ddlEvent := &model.DDLEvent{
		Query:     "create table t (id int primary key auto_increment, t datetime default current_timestamp)",
		TableInfo: &model.TableInfo{},
	}

	require.NotPanics(t, func() {
		ts, ok := ddlSessionTimestampFromOriginDefault(ddlEvent, "UTC")
		require.False(t, ok)
		require.Equal(t, float64(0), ts)
	})
}
