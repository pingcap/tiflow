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

package orm

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	metaMock "github.com/pingcap/tiflow/engine/pkg/meta/mock"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIsNotFoundError(t *testing.T) {
	b := IsNotFoundError(errors.ErrMetaEntryNotFound.GenWithStackByArgs("error"))
	require.True(t, b)

	b = IsNotFoundError(errors.ErrMetaEntryNotFound.GenWithStack("err:%s", "error"))
	require.True(t, b)

	b = IsNotFoundError(errors.ErrMetaEntryNotFound.Wrap(errors.New("error")))
	require.True(t, b)

	b = IsNotFoundError(errors.ErrMetaNewClientFail.Wrap(errors.New("error")))
	require.False(t, b)

	b = IsNotFoundError(errors.New("error"))
	require.False(t, b)
}

func TestInitialize(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.Nil(t, err)
	defer db.Close()
	defer mock.ExpectClose()

	// common execution for orm
	mock.ExpectQuery("SELECT VERSION()").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.35-log"))

	mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `project_infos` (`seq_id` bigint unsigned AUTO_INCREMENT," +
		"`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL," +
		"`id` varchar(128) not null,`name` varchar(128) not null,PRIMARY KEY (`seq_id`)," +
		"UNIQUE INDEX `uidx_id` (`id`))")).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `project_operations` (`seq_id` bigint unsigned AUTO_INCREMENT," +
		"`project_id` varchar(128) not null,`operation` varchar(16) not null,`job_id` varchar(128) not null," +
		"`created_at` datetime(3) NULL,PRIMARY KEY (`seq_id`),INDEX `idx_op` (`project_id`,`created_at`))")).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec(regexp.QuoteMeta(
		"CREATE TABLE `master_meta` (`seq_id` bigint unsigned AUTO_INCREMENT,"+
			"`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL,"+
			"`project_id` varchar(128) not null,`id` varchar(128) not null,"+
			"`type` smallint not null COMMENT "+
			"'JobManager(1),CvsJobMaster(2),FakeJobMaster(3),DMJobMaster(4),CDCJobMaster(5)',"+
			"`state` tinyint not null COMMENT "+
			"'Uninit(1),Init(2),Finished(3),Stopped(4),Failed(5)',"+
			"`node_id` varchar(128) not null,`address` varchar(256) not null,"+
			"`epoch` bigint not null,`config` blob,`error_message` text,"+
			"`detail` blob,`ext` JSON,`deleted` datetime(3) NULL,"+
			"PRIMARY KEY (`seq_id`)") +
		".*", // sequence of indexes are nondeterministic
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec(regexp.QuoteMeta(
		"CREATE TABLE `worker_statuses` (`seq_id` bigint unsigned AUTO_INCREMENT,"+
			"`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL,"+
			"`project_id` varchar(128) not null,`job_id` varchar(128) not null,"+
			"`id` varchar(128) not null,`type` smallint not null COMMENT "+
			"'JobManager(1),CvsJobMaster(2),FakeJobMaster(3),DMJobMaster(4),"+
			"CDCJobMaster(5),CvsTask(6),FakeTask(7),DMTask(8),CDCTask(9),"+
			"WorkerDMDump(10),WorkerDMLoad(11),WorkerDMSync(12)',"+
			"`state` tinyint not null COMMENT "+
			"'Normal(1),Created(2),Init(3),Error(4),Finished(5),Stopped(6)',"+
			"`epoch` bigint not null,`error_message` text,`extend_bytes` blob,"+
			"PRIMARY KEY (`seq_id`)") +
		".*", // sequence of indexes are nondeterministic
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec(regexp.QuoteMeta(
		"CREATE TABLE `resource_meta` (`seq_id` bigint unsigned AUTO_INCREMENT,"+
			"`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL,"+
			"`project_id` varchar(128) not null,`tenant_id` varchar(128) not null,"+
			"`id` varchar(128) not null,`job_id` varchar(128) not null,"+
			"`worker_id` varchar(128) not null,`executor_id` varchar(128) not null,"+
			"`gc_pending` BOOLEAN,`deleted` BOOLEAN,PRIMARY KEY (`seq_id`),") +
		".*", // sequence of indexes are nondeterministic
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).
		WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `logic_epoches` (`seq_id` bigint unsigned AUTO_INCREMENT,`created_at` datetime(3) NULL," +
		"`updated_at` datetime(3) NULL,`job_id` varchar(128) not null,`epoch` bigint not null default 1,PRIMARY KEY (`seq_id`),UNIQUE INDEX `uidx_jk` (`job_id`))")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT SCHEMA_NAME from Information_schema.SCHEMATA " +
			"where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1"),
	).WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec(regexp.QuoteMeta(
		"CREATE TABLE `job_ops` (`seq_id` bigint unsigned AUTO_INCREMENT,"+
			"`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL,"+
			"`op` tinyint not null COMMENT 'Canceling(1),Canceled(2)',"+
			"`job_id` varchar(128) not null,PRIMARY KEY (`seq_id`),") +
		".*", // sequence of indexes are nondeterministic
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT SCHEMA_NAME from Information_schema.SCHEMATA " +
			"where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1"),
	).WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec(regexp.QuoteMeta(
		"CREATE TABLE `executors` (`seq_id` bigint unsigned AUTO_INCREMENT," +
			"`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL," +
			"`id` varchar(256) not null,`name` varchar(256) not null," +
			"`address` varchar(256) not null,`labels` json,PRIMARY KEY (`seq_id`)," +
			"UNIQUE INDEX `uni_id` (`id`))"),
	).WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA " +
		"where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `logic_epoches` (`seq_id` bigint unsigned AUTO_INCREMENT," +
		"`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL,`job_id` varchar(128) not null,`epoch` bigint not null default 1," +
		"PRIMARY KEY (`seq_id`),UNIQUE INDEX `uidx_jk` (`job_id`))")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	conn := metaMock.NewClientConnWithDB(db)
	require.NotNil(t, conn)
	defer conn.Close()

	err = InitAllFrameworkModels(context.TODO(), conn)
	require.Nil(t, err)
}

func TestInitEpochModel(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.Nil(t, err)
	defer db.Close()
	defer mock.ExpectClose()

	ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
	defer cancel()

	err = InitEpochModel(ctx, nil)
	require.Regexp(t, regexp.QuoteMeta("input client conn is nil"), err.Error())

	mock.ExpectQuery("SELECT VERSION()").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.35-log"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA " +
		"where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `logic_epoches` (`seq_id` bigint unsigned AUTO_INCREMENT," +
		"`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL,`job_id` varchar(128) not null,`epoch` bigint not null default 1," +
		"PRIMARY KEY (`seq_id`),UNIQUE INDEX `uidx_jk` (`job_id`))")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	conn := metaMock.NewClientConnWithDB(db)
	require.NotNil(t, conn)
	defer conn.Close()

	err = InitEpochModel(ctx, conn)
	require.NoError(t, err)
}

func TestIsDuplicateEntryError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		err      error
		expected bool
	}{
		{
			nil, false,
		},
		{
			errors.New("invalid connection"), false,
		},
		{
			&mysql.MySQLError{
				Number:  tmysql.ErrDupEntry,
				Message: "Duplicate entry '123456' for key 'index'",
			}, true,
		},
		{
			errors.New("constraint failed: UNIQUE constraint failed: master_meta.id (2067)"),
			true,
		},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.expected, IsDuplicateEntryError(tc.err))
	}
}
