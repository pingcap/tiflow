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
	"github.com/pingcap/tidb/infoschema"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
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
	}

	for _, item := range cases {
		require.Equal(t, item.ret, IsRetryableEtcdError(item.err))
	}
}
