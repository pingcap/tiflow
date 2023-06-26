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

package mysql

import (
	"context"
	"database/sql"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/zap"
)

// CheckIsTiDB checks if the db connects to a TiDB.
func CheckIsTiDB(ctx context.Context, db *sql.DB) (bool, error) {
	var tidbVer string
	row := db.QueryRowContext(ctx, "select tidb_version()")
	err := row.Scan(&tidbVer)
	if err != nil {
		log.Error("check tidb version error", zap.Error(err))
		// downstream is not TiDB, do nothing
		if mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError); ok &&
			(mysqlErr.Number == tmysql.ErrNoDB ||
				mysqlErr.Number == tmysql.ErrSpDoesNotExist) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}
