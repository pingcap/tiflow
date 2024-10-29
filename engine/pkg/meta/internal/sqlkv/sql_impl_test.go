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

package sqlkv

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/VividCortex/mysqlerr"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	sqlkvModel "github.com/pingcap/tiflow/engine/pkg/meta/internal/sqlkv/model"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

const (
	fakeJob              = "fakeJob"
	fakeTable            = "fakeTable"
	defaultTestStoreType = metaModel.StoreTypeMySQL
)

type tCase struct {
	caseName string        // case name
	fn       string        // function name
	inputs   []interface{} // function args

	output interface{} // function output
	err    error       // function error

	mockExpectResFn func(mock sqlmock.Sqlmock) // sqlmock expectation
}

func mockGetDBConn(t *testing.T, table string) (*sql.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	require.Nil(t, err)
	// common execution for orm
	mock.ExpectQuery("SELECT VERSION()").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).
			AddRow("5.7.35-log"))
	mock.ExpectExec(regexp.QuoteMeta(fmt.Sprintf("CREATE TABLE `%s` (`seq_id` bigint unsigned AUTO_INCREMENT,"+
		"`created_at` datetime(3) NULL,`updated_at` datetime(3) NULL,`meta_key` varbinary(2048) not null,`meta_value` longblob,"+
		"`job_id` varchar(64) not null,PRIMARY KEY (`seq_id`),UNIQUE INDEX `uidx_jk` (`job_id`,`meta_key`))", table))).
		WillReturnResult(sqlmock.NewResult(1, 1))
	return db, mock
}

type anyTime struct{}

func (a anyTime) Match(v driver.Value) bool {
	_, ok := v.(time.Time)
	return ok
}

func TestNewSQLImpl(t *testing.T) {
	t.Parallel()

	sqlDB, mock := mockGetDBConn(t, fakeTable)

	defer sqlDB.Close() //nolint:staticcheck
	defer mock.ExpectClose()
	cli, err := NewSQLKVClientImpl(sqlDB, defaultTestStoreType, fakeTable, fakeJob)
	defer cli.Close() //nolint:staticcheck
	require.Nil(t, err)
	require.NotNil(t, cli)
}

func TestPut(t *testing.T) {
	t.Parallel()

	sqlDB, mock := mockGetDBConn(t, fakeTable)
	defer sqlDB.Close() //nolint:staticcheck
	defer mock.ExpectClose()
	cli, err := NewSQLKVClientImpl(sqlDB, defaultTestStoreType, fakeTable, fakeJob)
	defer cli.Close() //nolint:staticcheck
	require.Nil(t, err)
	require.NotNil(t, cli)
	var anyT anyTime

	testCases := []tCase{
		{
			fn: "Put",
			inputs: []interface{}{
				"key0",
				"value0",
			},
			output: &metaModel.PutResponse{
				Header: &metaModel.ResponseHeader{},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `fakeTable` (`created_at`,`updated_at`,"+
					"`meta_key`,`meta_value`,`job_id`) VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE `updated_at`=?,"+
					"`meta_key`=VALUES(`meta_key`),`meta_value`=VALUES(`meta_value`),`job_id`=VALUES(`job_id`)")).
					WithArgs(anyT, anyT, []byte("key0"), []byte("value0"), fakeJob, anyT).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestGet(t *testing.T) {
	t.Parallel()

	sqlDB, mock := mockGetDBConn(t, fakeTable)
	defer sqlDB.Close() //nolint:staticcheck
	defer mock.ExpectClose()
	cli, err := NewSQLKVClientImpl(sqlDB, defaultTestStoreType, fakeTable, fakeJob)
	require.Nil(t, err)
	require.NotNil(t, cli)

	testCases := []tCase{
		{
			caseName: "RecordNotFoundErrReturnEmptyResp",
			fn:       "Get",
			inputs: []interface{}{
				"key0",
			},
			output: &metaModel.GetResponse{
				Header: &metaModel.ResponseHeader{},
				Kvs:    []*metaModel.KeyValue{},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `fakeTable` WHERE job_id = ? AND "+
					"meta_key = ? ORDER BY `fakeTable`.`seq_id` LIMIT ?")).
					WithArgs(fakeJob, []byte("key0"), 1).
					WillReturnRows(sqlmock.NewRows([]string{"meta_key", "meta_value"}))
			},
		},
		{
			caseName: "NormalGet",
			fn:       "Get",
			inputs: []interface{}{
				"key0",
			},
			output: &metaModel.GetResponse{
				Header: &metaModel.ResponseHeader{},
				Kvs: []*metaModel.KeyValue{
					{
						Key:   []byte("key0"),
						Value: []byte("value0"),
					},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `fakeTable` WHERE job_id = ? AND "+
					"meta_key = ? ORDER BY `fakeTable`.`seq_id` LIMIT ?")).
					WithArgs(fakeJob, []byte("key0"), 1).
					WillReturnRows(sqlmock.NewRows([]string{"meta_key", "meta_value"}).AddRow("key0", "value0"))
			},
		},
		{
			caseName: "RangeGet",
			fn:       "Get",
			inputs: []interface{}{
				"key0",
				metaModel.WithRange("key999"),
			},
			output: &metaModel.GetResponse{
				Header: &metaModel.ResponseHeader{},
				Kvs: []*metaModel.KeyValue{
					{
						Key:   []byte("key0"),
						Value: []byte("value0"),
					},
					{
						Key:   []byte("key1"),
						Value: []byte("value1"),
					},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `fakeTable` WHERE "+
					"job_id = ? AND (meta_key >= ? AND meta_key < ?)")).
					WithArgs(fakeJob, []byte("key0"), []byte("key999")).
					WillReturnRows(sqlmock.NewRows([]string{"meta_key", "meta_value"}).
						AddRow("key0", "value0").AddRow("key1", "value1"))
			},
		},
		{
			caseName: "FromKeyGet",
			fn:       "Get",
			inputs: []interface{}{
				"key0",
				metaModel.WithFromKey(),
			},
			output: &metaModel.GetResponse{
				Header: &metaModel.ResponseHeader{},
				Kvs: []*metaModel.KeyValue{
					{
						Key:   []byte("key0"),
						Value: []byte("value0"),
					},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `fakeTable` WHERE job_id = ? AND meta_key >= ?")).
					WithArgs(fakeJob, []byte("key0")).
					WillReturnRows(sqlmock.NewRows([]string{"meta_key", "meta_value"}).
						AddRow("key0", "value0"))
			},
		},
		{
			caseName: "PrefixGet",
			fn:       "Get",
			inputs: []interface{}{
				"key0",
				metaModel.WithPrefix(),
			},
			output: &metaModel.GetResponse{
				Header: &metaModel.ResponseHeader{},
				Kvs: []*metaModel.KeyValue{
					{
						Key:   []byte("key0"),
						Value: []byte("value0"),
					},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `fakeTable` WHERE job_id = ? AND meta_key like ?")).
					WithArgs(fakeJob, []byte("key0%")).
					WillReturnRows(sqlmock.NewRows([]string{"meta_key", "meta_value"}).
						AddRow("key0", "value0"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()

	sqlDB, mock := mockGetDBConn(t, fakeTable)
	defer sqlDB.Close() //nolint:staticcheck
	defer mock.ExpectClose()
	cli, err := NewSQLKVClientImpl(sqlDB, defaultTestStoreType, fakeTable, fakeJob)
	require.Nil(t, err)
	require.NotNil(t, cli)

	testCases := []tCase{
		{
			fn: "Delete",
			inputs: []interface{}{
				"key0",
			},
			output: &metaModel.DeleteResponse{
				Header: &metaModel.ResponseHeader{},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `fakeTable` WHERE job_id = ? AND meta_key = ?")).
					WithArgs(fakeJob, []byte("key0")).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "Delete",
			inputs: []interface{}{
				"key0",
				metaModel.WithRange("key999"),
			},
			output: &metaModel.DeleteResponse{
				Header: &metaModel.ResponseHeader{},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `fakeTable` "+
					"WHERE job_id = ? AND (meta_key >= ? AND meta_key < ?)")).
					WithArgs(fakeJob, []byte("key0"), []byte("key999")).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "Delete",
			inputs: []interface{}{
				"key0",
				metaModel.WithFromKey(),
			},
			output: &metaModel.DeleteResponse{
				Header: &metaModel.ResponseHeader{},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `fakeTable` WHERE job_id = ? AND meta_key >= ?")).
					WithArgs(fakeJob, []byte("key0")).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "Delete",
			inputs: []interface{}{
				"key0",
				metaModel.WithPrefix(),
			},
			output: &metaModel.DeleteResponse{
				Header: &metaModel.ResponseHeader{},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `fakeTable` WHERE job_id = ? AND meta_key like ?")).
					WithArgs(fakeJob, []byte("key0%")).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestTxn(t *testing.T) {
	t.Parallel()

	sqlDB, mock := mockGetDBConn(t, fakeTable)
	defer sqlDB.Close() //nolint:staticcheck
	defer mock.ExpectClose()
	cli, err := NewSQLKVClientImpl(sqlDB, defaultTestStoreType, fakeTable, fakeJob)
	require.Nil(t, err)
	require.NotNil(t, cli)
	var anyT anyTime

	txn := cli.Txn(context.Background())
	txn.Do(metaModel.OpGet("key0", metaModel.WithRange("key999")))
	txn.Do(metaModel.OpPut("key1", "value1"))
	txn.Do(metaModel.OpDelete("key2", metaModel.WithPrefix()))

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `fakeTable` WHERE job_id = ? AND (meta_key >= ? AND meta_key < ?)")).
		WithArgs(fakeJob, []byte("key0"), []byte("key999")).
		WillReturnRows(sqlmock.NewRows([]string{"meta_key", "meta_value"}).AddRow("key0", "value0").AddRow("key1", "value1"))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `fakeTable` (`created_at`,`updated_at`,"+
		"`meta_key`,`meta_value`,`job_id`) VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE `updated_at`=?,"+
		"`meta_key`=VALUES(`meta_key`),`meta_value`=VALUES(`meta_value`),`job_id`=VALUES(`job_id`)")).
		WithArgs(anyT, anyT, []byte("key1"), []byte("value1"), fakeJob, anyT).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `fakeTable` WHERE job_id = ? AND meta_key like ?")).
		WithArgs(fakeJob, []byte("key2%")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	rsp, err := txn.Commit()
	require.NoError(t, err)
	require.Len(t, rsp.Responses, 3)

	rr := rsp.Responses[0].GetResponseGet()
	require.NotNil(t, rr)
	require.Len(t, rr.Kvs, 2)
	require.Equal(t, []byte("key0"), rr.Kvs[0].Key)
	require.Equal(t, []byte("value0"), rr.Kvs[0].Value)
	require.Equal(t, []byte("key1"), rr.Kvs[1].Key)
	require.Equal(t, []byte("value1"), rr.Kvs[1].Value)

	rr1 := rsp.Responses[1].GetResponsePut()
	require.NotNil(t, rr1)

	rr2 := rsp.Responses[2].GetResponseDelete()
	require.NotNil(t, rr2)
}

func testInner(t *testing.T, m sqlmock.Sqlmock, cli *sqlKVClientImpl, c tCase) {
	// set the mock expectation
	c.mockExpectResFn(m)

	var args []reflect.Value
	args = append(args, reflect.ValueOf(context.Background()))
	for _, ip := range c.inputs {
		args = append(args, reflect.ValueOf(ip))
	}
	result := reflect.ValueOf(cli).MethodByName(c.fn).Call(args)
	// only error
	if len(result) == 1 {
		if c.err == nil {
			require.Nil(t, result[0].Interface())
		} else {
			require.NotNil(t, result[0].Interface())
			require.Error(t, result[0].Interface().(error))
		}
	} else if len(result) == 2 {
		// result and error
		if c.err != nil {
			require.NotNil(t, result[1].Interface())
			require.Error(t, result[1].Interface().(error))
		} else {
			require.Equal(t, c.output, result[0].Interface())
		}
	}
	require.NoError(t, m.ExpectationsWereMet())
}

func TestSQLImplWithoutNamespace(t *testing.T) {
	t.Parallel()

	sqlDB, mock := mockGetDBConn(t, sqlkvModel.MetaKVTableName)
	defer sqlDB.Close() //nolint:staticcheck
	defer mock.ExpectClose()
	cli, err := NewSQLKVClientImpl(sqlDB, defaultTestStoreType, sqlkvModel.MetaKVTableName, "")
	require.Nil(t, err)
	require.NotNil(t, cli)
	var anyT anyTime
	ctx := context.TODO()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `meta_kvs` (`created_at`,`updated_at`,"+
		"`meta_key`,`meta_value`,`job_id`) VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE `updated_at`=?,"+
		"`meta_key`=VALUES(`meta_key`),`meta_value`=VALUES(`meta_value`),`job_id`=VALUES(`job_id`)")).
		WithArgs(anyT, anyT, []byte("key0"), []byte("value0"), "", anyT).
		WillReturnResult(sqlmock.NewResult(1, 1))
	cli.Put(ctx, "key0", "value0")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `meta_kvs` WHERE job_id = ? AND "+
		"meta_key = ? ORDER BY `meta_kvs`.`seq_id` LIMIT ?")).
		WithArgs("", []byte("key1"), 1).
		WillReturnRows(sqlmock.NewRows([]string{"key", "value"}))
	cli.Get(ctx, "key1")

	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `meta_kvs` WHERE job_id = ? AND meta_key = ?")).
		WithArgs("", []byte("key2")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	cli.Delete(ctx, "key2")

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `meta_kvs` WHERE job_id = ? AND (meta_key >= ? AND meta_key < ?)")).
		WithArgs("", []byte("key0"), []byte("key999")).
		WillReturnRows(sqlmock.NewRows([]string{"meta_key", "meta_value"}).AddRow("key0", "value0").AddRow("key1", "value1"))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `meta_kvs` (`created_at`,`updated_at`,"+
		"`meta_key`,`meta_value`,`job_id`) VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE `updated_at`=?,"+
		"`meta_key`=VALUES(`meta_key`),`meta_value`=VALUES(`meta_value`),`job_id`=VALUES(`job_id`)")).
		WithArgs(anyT, anyT, []byte("key1"), []byte("value1"), "", anyT).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `meta_kvs` WHERE job_id = ? AND meta_key like ?")).
		WithArgs("", []byte("key2%")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	txn := cli.Txn(context.Background())
	txn.Do(metaModel.OpGet("key0", metaModel.WithRange("key999")))
	txn.Do(metaModel.OpPut("key1", "value1"))
	txn.Do(metaModel.OpDelete("key2", metaModel.WithPrefix()))
	txn.Commit()
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestInitializeError(t *testing.T) {
	t.Parallel()

	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	db, mock, err := sqlmock.New()
	require.Nil(t, err)
	defer db.Close()
	defer mock.ExpectClose()

	// table exists error
	mock.ExpectQuery("SELECT VERSION()").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).
			AddRow("5.7.35-log"))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec(regexp.QuoteMeta(fmt.Sprintf("CREATE TABLE `%s`", "test"))).
		WillReturnError(&mysql.MySQLError{Number: mysqlerr.ER_TABLE_EXISTS_ERROR, Message: "table already exists"})
	cli, err := NewSQLKVClientImpl(db, defaultTestStoreType, "test", "")
	require.Nil(t, err)
	require.NotNil(t, cli)
	defer cli.Close()

	// other mysql error
	mock.ExpectQuery("SELECT VERSION()").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).
			AddRow("5.7.35-log"))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec(regexp.QuoteMeta(fmt.Sprintf("CREATE TABLE `%s`", "test"))).
		WillReturnError(&mysql.MySQLError{Number: mysqlerr.ER_WRONG_OUTER_JOIN, Message: "other mysql error"})
	_, err = NewSQLKVClientImpl(db, defaultTestStoreType, "test", "")
	require.Regexp(t, "other mysql error", err.Error())
	// other error
	mock.ExpectQuery("SELECT VERSION()").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).
			AddRow("5.7.35-log"))
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	mock.ExpectExec(regexp.QuoteMeta(fmt.Sprintf("CREATE TABLE `%s`", "test"))).
		WillReturnError(errors.New("other error"))
	_, err = NewSQLKVClientImpl(db, defaultTestStoreType, "test", "")
	require.Regexp(t, "other error", err.Error())
	require.NoError(t, mock.ExpectationsWereMet())
}
