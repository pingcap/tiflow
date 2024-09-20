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
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gsql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func mockGetDBConn(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
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
	})
	require.NoError(t, err)

	return gdb, mock
}

func closeGormDB(t *testing.T, gdb *gorm.DB) {
	db, err := gdb.DB()
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

func TestNewEpochClient(t *testing.T) {
	gdb, mock := mockGetDBConn(t)
	defer closeGormDB(t, gdb)
	defer mock.ExpectClose()

	cli, err := NewEpochClient("fakeJob", gdb)
	require.NoError(t, err)
	defer cli.Close()
}

func TestGenEpoch(t *testing.T) {
	gdb, mock := mockGetDBConn(t)
	defer closeGormDB(t, gdb)
	ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
	defer cancel()

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	epochClient, err := NewEpochClient("fakeJob", gdb)
	require.NoError(t, err)

	// insert first record fail
	mock.ExpectExec(".*").
		WillReturnError(&gsql.MySQLError{Number: 1062, Message: "test error"})
	epoch, err := epochClient.GenEpoch(ctx)
	require.Error(t, err)
	require.Equal(t, int64(0), epoch)
	require.False(t, epochClient.isInitialized.Load())

	// insert first record successful
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `logic_epoches` (`created_at`,`updated_at`,`job_id`,`epoch`)" +
		" VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE `seq_id`=`seq_id`")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE `logic_epoches` SET `epoch`=epoch + ?,`updated_at`=? WHERE job_id = ?")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `logic_epoches` WHERE job_id = ? ORDER BY `logic_epoches`.`seq_id` LIMIT ?")).
		WithArgs("fakeJob", 1).
		WillReturnRows(sqlmock.NewRows([]string{"seq_id", "created_at", "updated_at", "job_id", "epoch"}).
			AddRow(1, createdAt, updatedAt, "fakeJob", 11))
	mock.ExpectCommit()

	epoch, err = epochClient.GenEpoch(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(11), epoch)
	require.True(t, epochClient.isInitialized.Load())

	// update fail
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `logic_epoches` SET").WillReturnError(errors.New("gen epoch error"))
	mock.ExpectRollback()
	_, err = epochClient.GenEpoch(ctx)
	require.Error(t, err)

	// context cancel
	ctx, cancel = context.WithTimeout(context.TODO(), 1*time.Second)
	defer cancel()

	err = failpoint.Enable("github.com/pingcap/tiflow/engine/pkg/orm/model/genEpochDelay", "sleep(2000)")
	require.NoError(t, err)
	ctx = failpoint.WithHook(ctx, func(ctx context.Context, fpname string) bool {
		return ctx.Value(fpname) != nil
	})
	ctx2 := context.WithValue(ctx, "github.com/pingcap/tiflow/engine/pkg/orm/model/genEpochDelay", struct{}{})

	_, err = epochClient.GenEpoch(ctx2)
	require.Error(t, err)
	require.Regexp(t, "context deadline exceed", err.Error())
	failpoint.Disable("github.com/pingcap/tiflow/engine/pkg/orm/model/genEpochDelay")

	mock.ExpectClose()
}
