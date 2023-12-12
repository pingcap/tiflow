// Copyright 2021 PingCAP, Inc.
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

package errorutil

import (
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/infoschema"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/raft/v3"
)

func newMysqlErr(number uint16, message string) *mysql.MySQLError {
	return &mysql.MySQLError{
		Number:  number,
		Message: message,
	}
}

func TestIgnoreMysqlDDLError(t *testing.T) {
	cases := []struct {
		err error
		ret bool
	}{
		{errors.New("raw error"), false},
		{newMysqlErr(tmysql.ErrDupKeyName, "Error: Duplicate key name 'some_key'"), true},
		{newMysqlErr(uint16(infoschema.ErrDatabaseExists.Code()), "Can't create database"), true},
		{newMysqlErr(uint16(infoschema.ErrAccessDenied.Code()), "Access denied for user"), false},
	}

	for _, item := range cases {
		require.Equal(t, item.ret, IsIgnorableMySQLDDLError(item.err))
	}
}

func TestIsRetryableEtcdError(t *testing.T) {
	cases := []struct {
		err error
		ret bool
	}{
		{nil, false},
		{v3rpc.ErrCorrupt, false},

		{v3rpc.ErrGRPCTimeoutDueToConnectionLost, true},
		{v3rpc.ErrTimeoutDueToLeaderFail, true},
		{v3rpc.ErrNoSpace, true},
		{raft.ErrStopped, true},
		{errors.New("rpc error: code = Unavailable desc = closing transport due to: " +
			"connection error: desc = \\\"error reading from server: EOF\\\", " +
			"received prior goaway: code: NO_ERROR\""), true},
		{errors.New("rpc error: code = Unavailable desc = error reading from server: " +
			"xxx: read: connection reset by peer"), true},
	}

	for _, item := range cases {
		require.Equal(t, item.ret, IsRetryableEtcdError(item.err))
	}
}

func TestIsRetryableDMLError(t *testing.T) {
	cases := []struct {
		err error
		ret bool
	}{
		{nil, false},
		{errors.New("raw error"), false},
		{newMysqlErr(tmysql.ErrDupKeyName, "Error: Duplicate key name 'some_key'"), false},
		{tmysql.ErrBadConn, true},
		{newMysqlErr(tmysql.ErrLockWaitTimeout, "Lock wait timeout exceeded"), false},
		{newMysqlErr(tmysql.ErrLockDeadlock, "Deadlock found when trying to get lock"), true},
	}

	for _, c := range cases {
		require.Equal(t, c.ret, IsRetryableDMLError(c.err))
	}
}

func TestIsRetryableDDLError(t *testing.T) {
	cases := []struct {
		err error
		ret bool
	}{
		{errors.New("raw error"), false},
		{newMysqlErr(tmysql.ErrNoDB, "Error: Duplicate key name 'some_key'"), false},
		{newMysqlErr(tmysql.ErrParse, "Can't create database"), false},
		{newMysqlErr(tmysql.ErrAccessDenied, "Access denied for user"), false},
		{newMysqlErr(tmysql.ErrDBaccessDenied, "Access denied for db"), false},
		{newMysqlErr(tmysql.ErrNoSuchTable, "table not exist"), false},
		{newMysqlErr(tmysql.ErrNoSuchIndex, "index not exist"), false},
		{newMysqlErr(tmysql.ErrWrongColumnName, "wrong column name'"), false},
		{newMysqlErr(tmysql.ErrDupKeyName, "Duplicate key name 'some_key'"), true},
		{newMysqlErr(tmysql.ErrPartitionMgmtOnNonpartitioned, "xx"), false},
		{mysql.ErrInvalidConn, true},
	}

	for _, c := range cases {
		require.Equal(t, c.ret, IsRetryableDDLError(c.err))
	}
}

func TestIsSyncPointIgnoreError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		err error
		ret bool
	}{
		{errors.New("raw error"), false},
		{newMysqlErr(tmysql.ErrDupKeyName, "Error: Duplicate key name 'some_key'"), false},
		{newMysqlErr(tmysql.ErrNoDB, "Error: Duplicate key name 'some_key'"), false},
		{newMysqlErr(tmysql.ErrParse, "Can't create database"), false},
		{newMysqlErr(tmysql.ErrUnknownSystemVariable, "Unknown system variable"), true},
	}
	for _, c := range cases {
		require.Equal(t, c.ret, IsSyncPointIgnoreError(c.err))
	}
}
