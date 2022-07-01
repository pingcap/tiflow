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
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	"github.com/stretchr/testify/require"
)

type tCase struct {
	fn     string        // function name
	inputs []interface{} // function args

	output interface{} // function output
	err    error       // function error

	mockExpectResFn func(mock sqlmock.Sqlmock) // sqlmock expectation
}

func mockGetDBConn(t *testing.T, dsnStr string) (*sql.DB, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New()
	require.Nil(t, err)
	// common execution for orm
	mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows(
		[]string{"VERSION()"}).AddRow("5.7.35-log"))
	return db, mock, nil
}

type anyTime struct{}

func (a anyTime) Match(v driver.Value) bool {
	_, ok := v.(time.Time)
	return ok
}

func TestNewSQLImpl(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newImpl(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)
}

// nolint: deadcode
func TestInitialize(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newImpl(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	testCases := []tCase{
		{
			fn:     "Initialize",
			inputs: []interface{}{},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `key_values` (`key` varbinary(2048) not null,`value` blob,PRIMARY KEY (`key`))")).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectQuery(regexp.QuoteMeta("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1")).
					WillReturnRows(sqlmock.NewRows([]string{"SCHEMA_NAME"}))
				mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `logic_epoches` (`seq_id` bigint unsigned AUTO_INCREMENT,`created_at` datetime(3) NULL," +
					"`updated_at` datetime(3) NULL,`epoch` bigint not null default 1,PRIMARY KEY (`seq_id`))")).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `logic_epoches` (`created_at`,`updated_at`,`epoch`,`seq_id`) "+
					"VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE `seq_id`=`seq_id`")).WithArgs(anyTime{}, anyTime{}, 1, 1).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestPut(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newImpl(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	testCases := []tCase{
		{
			fn: "Put",
			inputs: []interface{}{
				"key0",
				"value0",
			},
			output: &metaclient.PutResponse{
				Header: &metaclient.ResponseHeader{},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `key_values` (`key`,`value`) VALUES (?,?) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)")).
					WithArgs([]byte("key0"), []byte("value0")).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestGet(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newImpl(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	testCases := []tCase{
		{
			fn: "Get",
			inputs: []interface{}{
				"key0",
			},
			output: &metaclient.GetResponse{
				Header: &metaclient.ResponseHeader{},
				Kvs: []*metaclient.KeyValue{
					{
						Key:   []byte("key0"),
						Value: []byte("value0"),
					},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `key_values` WHERE key = ? ORDER BY `key_values`.`key` LIMIT 1")).
					WithArgs([]byte("key0")).
					WillReturnRows(sqlmock.NewRows([]string{"key", "value"}).AddRow("key0", "value0"))
			},
		},
		{
			fn: "Get",
			inputs: []interface{}{
				"key0",
				metaclient.WithRange("key999"),
			},
			output: &metaclient.GetResponse{
				Header: &metaclient.ResponseHeader{},
				Kvs: []*metaclient.KeyValue{
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
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `key_values` WHERE key >= ? && key < ?")).
					WithArgs([]byte("key0"), []byte("key999")).
					WillReturnRows(sqlmock.NewRows([]string{"key", "value"}).AddRow("key0", "value0").AddRow("key1", "value1"))
			},
		},
		{
			fn: "Get",
			inputs: []interface{}{
				"key0",
				metaclient.WithFromKey(),
			},
			output: &metaclient.GetResponse{
				Header: &metaclient.ResponseHeader{},
				Kvs: []*metaclient.KeyValue{
					{
						Key:   []byte("key0"),
						Value: []byte("value0"),
					},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `key_values` WHERE key >= ?")).WithArgs([]byte("key0")).
					WillReturnRows(sqlmock.NewRows([]string{"key", "value"}).AddRow("key0", "value0"))
			},
		},
		{
			fn: "Get",
			inputs: []interface{}{
				"key0",
				metaclient.WithPrefix(),
			},
			output: &metaclient.GetResponse{
				Header: &metaclient.ResponseHeader{},
				Kvs: []*metaclient.KeyValue{
					{
						Key:   []byte("key0"),
						Value: []byte("value0"),
					},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `key_values` WHERE key like ?%")).WithArgs([]byte("key0")).
					WillReturnRows(sqlmock.NewRows([]string{"key", "value"}).AddRow("key0", "value0"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newImpl(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	testCases := []tCase{
		{
			fn: "Delete",
			inputs: []interface{}{
				"key0",
			},
			output: &metaclient.DeleteResponse{
				Header: &metaclient.ResponseHeader{},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `key_values` WHERE key = ?")).
					WithArgs([]byte("key0")).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "Delete",
			inputs: []interface{}{
				"key0",
				metaclient.WithRange("key999"),
			},
			output: &metaclient.DeleteResponse{
				Header: &metaclient.ResponseHeader{},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `key_values` WHERE key >= ? && key < ?")).
					WithArgs([]byte("key0"), []byte("key999")).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "Delete",
			inputs: []interface{}{
				"key0",
				metaclient.WithFromKey(),
			},
			output: &metaclient.DeleteResponse{
				Header: &metaclient.ResponseHeader{},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `key_values` WHERE key >= ?")).WithArgs([]byte("key0")).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "Delete",
			inputs: []interface{}{
				"key0",
				metaclient.WithPrefix(),
			},
			output: &metaclient.DeleteResponse{
				Header: &metaclient.ResponseHeader{},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `key_values` WHERE key like ?%")).WithArgs([]byte("key0")).
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

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := newImpl(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	txn := cli.Txn(context.Background())
	txn.Do(metaclient.OpGet("key0", metaclient.WithRange("key999")))
	txn.Do(metaclient.OpPut("key1", "value1"))
	txn.Do(metaclient.OpDelete("key2", metaclient.WithPrefix()))

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `key_values` WHERE key >= ? && key < ?")).
		WithArgs([]byte("key0"), []byte("key999")).
		WillReturnRows(sqlmock.NewRows([]string{"key", "value"}).AddRow("key0", "value0").AddRow("key1", "value1"))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `key_values` (`key`,`value`) VALUES (?,?) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)")).
		WithArgs([]byte("key1"), []byte("value1")).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `key_values` WHERE key like ?%")).WithArgs([]byte("key2")).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	rsp, err := txn.Commit()
	require.NoError(t, err)
	require.Len(t, rsp.Responses, 3)
}

func testInner(t *testing.T, m sqlmock.Sqlmock, cli *sqlImpl, c tCase) {
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
}
