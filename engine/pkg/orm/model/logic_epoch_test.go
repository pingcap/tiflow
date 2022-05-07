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

package model

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gsql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func mockGetDBConn(t *testing.T, dsnStr string) (*gorm.DB, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	// common execution for orm
	mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows(
		[]string{"VERSION()"}).AddRow("5.7.35-log"))

	gdb, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      db,
		SkipInitializeWithVersion: false,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
		// TODO: logger
	})
	require.NoError(t, err)

	return gdb, mock, nil
}

func TestInitializeEpoch(t *testing.T) {
	gdb, mock, err := mockGetDBConn(t, "test")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
	defer cancel()

	mock.ExpectExec("INSERT INTO `logic_epoches` [(]`created_at`,`updated_at`,`epoch`," +
		"`seq_id`[)]").WillReturnResult(sqlmock.NewResult(1, 1))
	err = InitializeEpoch(ctx, gdb)
	require.NoError(t, err)

	mock.ExpectExec("INSERT INTO `logic_epoches` [(]`created_at`,`updated_at`,`epoch`," +
		"`seq_id`[)]").WillReturnError(&gsql.MySQLError{Number: 1062, Message: "test error"})
	err = InitializeEpoch(ctx, gdb)
	require.Error(t, err)
}

// UPDATE `logic_epoches` SET `epoch`=epoch + ?,`updated_at`=? WHERE `seq_id` = ?
// SELECT * FROM `logic_epoches` WHERE `logic_epoches`.`seq_id` = 1 ORDER BY `logic_epoches`.`seq_id` LIMIT 1
func TestGenEpoch(t *testing.T) {
	gdb, mock, err := mockGetDBConn(t, "test")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
	defer cancel()

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `logic_epoches` SET").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT [*] FROM `logic_epoches` WHERE `logic_epoches`[.]`seq_id`").WithArgs(1).WillReturnRows(
		sqlmock.NewRows([]string{"seq_id", "created_at", "updated_at", "epoch"}).AddRow(1, createdAt, updatedAt, 11))
	mock.ExpectCommit()
	epoch, err := GenEpoch(ctx, gdb)
	require.NoError(t, err)
	require.Equal(t, int64(11), epoch)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `logic_epoches` SET").WillReturnError(errors.New("gen epoch error"))
	mock.ExpectRollback()
	_, err = GenEpoch(ctx, gdb)
	require.Error(t, err)
}
