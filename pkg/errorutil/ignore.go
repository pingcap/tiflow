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
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/mysql"
)

// IsIgnorableMySQLDDLError is used to check what error can be ignored
// we can get error code from:
// infoschema's error definition: https://github.com/pingcap/tidb/blob/master/infoschema/infoschema.go
// DDL's error definition: https://github.com/pingcap/tidb/blob/master/ddl/ddl.go
// tidb/mysql error code definition: https://github.com/pingcap/tidb/blob/master/mysql/errcode.go
func IsIgnorableMySQLDDLError(err error) bool {
	err = errors.Cause(err)
	mysqlErr, ok := err.(*dmysql.MySQLError)
	if !ok {
		return false
	}

	errCode := errors.ErrCode(mysqlErr.Number)
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrIndexExists.Code(),
		infoschema.ErrKeyNotExists.Code(), tddl.ErrCantDropFieldOrKey.Code(),
		mysql.ErrDupKeyName, mysql.ErrSameNamePartition,
		mysql.ErrDropPartitionNonExistent, mysql.ErrMultiplePriKey:
		return true
	default:
		return false
	}
}
