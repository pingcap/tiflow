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

package sql

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestORMUUIDGeneratorInitFail(t *testing.T) {
	t.Parallel()

	backendDB, db, mock := newMockDB(t)
	defer backendDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// auto migrate
	mock.ExpectQuery("SELECT SCHEMA_NAME from Information_schema.SCHEMATA " +
		"where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1").WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec("CREATE TABLE `logic_epoch` (`task_id` varchar(128),`epoch` bigint(20) unsigned NOT NULL," +
		"`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL,PRIMARY KEY (`task_id`))").
		WillReturnResult(sqlmock.NewResult(0, 0))

	// insert first record fail
	mock.ExpectExec("INSERT INTO `logic_epoch` (`task_id`,`epoch`,`created_at`,`updated_at`) VALUES " +
		"(?,?,?,?) ON DUPLICATE KEY UPDATE `task_id`=`task_id`").
		WillReturnError(&mysql.MySQLError{Number: 1062, Message: "test error"})
	gen := newORMUUIDGenerator(taskCDCChangefeedUUID, db)
	uuid, err := gen.GenChangefeedUUID(ctx)
	require.ErrorIs(t, err, errors.ErrMetaOpFailed)
	require.ErrorContains(t, err, "test error")
	require.Equal(t, uint64(0), uuid)

	mock.ExpectClose()
}

func TestORMUUIDGenerator(t *testing.T) {
	t.Parallel()

	backendDB, db, mock := newMockDB(t)
	defer backendDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	gen := newORMUUIDGenerator(taskCDCChangefeedUUID, db)

	// auto migrate
	mock.ExpectQuery("SELECT SCHEMA_NAME from Information_schema.SCHEMATA " +
		"where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1").WillReturnRows(
		sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec("CREATE TABLE `logic_epoch` (`task_id` varchar(128),`epoch` bigint(20) unsigned NOT NULL," +
		"`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL,PRIMARY KEY (`task_id`))").
		WillReturnResult(sqlmock.NewResult(0, 0))

	// insert first record
	mock.ExpectExec("INSERT INTO `logic_epoch` (`task_id`,`epoch`,`created_at`,`updated_at`) VALUES " +
		"(?,?,?,?) ON DUPLICATE KEY UPDATE `task_id`=`task_id`").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// case 1: update success
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `logic_epoch` SET `epoch`=epoch + ?,`updated_at`=? WHERE task_id = ?").
		WithArgs(1, sqlmock.AnyArg(), taskCDCChangefeedUUID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// select
	mock.ExpectQuery("SELECT `epoch` FROM `logic_epoch` WHERE task_id = ? LIMIT 1").
		WithArgs(taskCDCChangefeedUUID).
		WillReturnRows(sqlmock.NewRows([]string{"epoch"}).AddRow(11))
	mock.ExpectCommit()

	uuid, err := gen.GenChangefeedUUID(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(11), uuid)

	// case 2: update fail
	failedErr := errors.New("gen epoch error")
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `logic_epoch` SET `epoch`=epoch + ?,`updated_at`=? WHERE task_id = ?").
		WithArgs(1, sqlmock.AnyArg(), taskCDCChangefeedUUID).
		WillReturnError(failedErr)
	mock.ExpectRollback()
	uuid, err = gen.GenChangefeedUUID(ctx)
	require.ErrorContains(t, err, failedErr.Error())
	require.Equal(t, uint64(0), uuid)

	mock.ExpectClose()
}
