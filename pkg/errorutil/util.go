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
	"strings"

	gmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/dbutil"
	dmretry "github.com/pingcap/tiflow/dm/pkg/retry"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

// IsIgnorableMySQLDDLError is used to check what error can be ignored
// we can get error code from:
// infoschema's error definition: https://github.com/pingcap/tidb/blob/master/infoschema/infoschema.go
// DDL's error definition: https://github.com/pingcap/tidb/blob/master/ddl/ddl.go
// tidb/mysql error code definition: https://github.com/pingcap/tidb/blob/master/mysql/errcode.go
func IsIgnorableMySQLDDLError(err error) bool {
	err = errors.Cause(err)
	mysqlErr, ok := err.(*gmysql.MySQLError)
	if !ok {
		return false
	}

	errCode := errors.ErrCode(mysqlErr.Number)
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrIndexExists.Code(),
		infoschema.ErrKeyNotExists.Code(), dbterror.ErrCantDropFieldOrKey.Code(),
		infoschema.ErrColumnNotExists.Code(),
		mysql.ErrDupKeyName, mysql.ErrSameNamePartition,
		mysql.ErrDropPartitionNonExistent, mysql.ErrMultiplePriKey:
		return true
	default:
		return false
	}
}

// IsRetryableEtcdError is used to check what error can be retried.
func IsRetryableEtcdError(err error) bool {
	if err == nil {
		return false
	}
	etcdErr := errors.Cause(err)

	switch etcdErr {
	// Etcd ResourceExhausted errors, may recover after some time
	case v3rpc.ErrNoSpace, v3rpc.ErrTooManyRequests:
		return true
	// Etcd Unavailable errors, may be available after some time
	// https://github.com/etcd-io/etcd/pull/9934/files#diff-6d8785d0c9eaf96bc3e2b29c36493c04R162-R167
	// ErrStopped:
	// one of the etcd nodes stopped from failure injection
	// ErrNotCapable:
	// capability check has not been done (in the beginning)
	case v3rpc.ErrNoLeader, v3rpc.ErrLeaderChanged, v3rpc.ErrNotCapable, v3rpc.ErrStopped, v3rpc.ErrTimeout,
		v3rpc.ErrTimeoutDueToLeaderFail, v3rpc.ErrGRPCTimeoutDueToConnectionLost, v3rpc.ErrUnhealthy:
		return true
	default:
	}
	// when the PD instance was deleted from the PD cluster, it may meet different errors.
	// retry on such error make cdc robust to PD / ETCD cluster member removal.
	// we should tolerant such case to make cdc robust to PD / ETCD cluster member change.
	// see: https://github.com/etcd-io/etcd/blob/ae36a577d7be/raft/node.go#L35
	if strings.Contains(etcdErr.Error(), "raft: stopped") {
		return true
	}
	// see: https://github.com/pingcap/tiflow/issues/6720
	if strings.Contains(etcdErr.Error(), "received prior goaway: code: NO_ERROR") {
		return true
	}

	// this may happen if the PD instance shutdown by `kill -9`, no matter the instance is the leader or not.
	if strings.Contains(etcdErr.Error(), "connection reset by peer") {
		return true
	}
	return false
}

// IsRetryableDMLError check if the error is a retryable dml error.
func IsRetryableDMLError(err error) bool {
	if !cerror.IsRetryableError(err) {
		return false
	}
	// Check if the error is connection errors that can retry safely.
	if dmretry.IsConnectionError(err) {
		return true
	}
	// Check if the error is a retriable TiDB error or MySQL error.
	return dbutil.IsRetryableError(err)
}

// IsRetryableDDLError check if the error is a retryable ddl error.
func IsRetryableDDLError(err error) bool {
	if IsRetryableDMLError(err) {
		return true
	}

	err = errors.Cause(err)
	mysqlErr, ok := err.(*gmysql.MySQLError)
	if !ok {
		return false
	}

	// If the error is in the black list, return false.
	switch mysqlErr.Number {
	case mysql.ErrAccessDenied,
		mysql.ErrDBaccessDenied,
		mysql.ErrSyntax,
		mysql.ErrParse,
		mysql.ErrNoDB,
		mysql.ErrNoSuchTable,
		mysql.ErrNoSuchIndex,
		mysql.ErrKeyColumnDoesNotExits,
		mysql.ErrWrongColumnName,
		mysql.ErrPartitionMgmtOnNonpartitioned:
		return false
	}
	return true
}

// IsSyncPointIgnoreError returns whether the error is ignorable for syncpoint.
func IsSyncPointIgnoreError(err error) bool {
	err = errors.Cause(err)
	mysqlErr, ok := err.(*gmysql.MySQLError)
	if !ok {
		return false
	}
	// We should ignore the error when the downstream has no
	// such system variable for compatibility.
	return mysqlErr.Number == mysql.ErrUnknownSystemVariable
}
