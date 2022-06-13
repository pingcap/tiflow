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

package dbconn

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

// GetTableCreateSQL gets table create sql by 'show create table schema.table'.
func GetTableCreateSQL(tctx *tcontext.Context, conn *DBConn, tableID string) (sql string, err error) {
	querySQL := fmt.Sprintf("SHOW CREATE TABLE %s", tableID)
	var table, createStr string

	rows, err := conn.QuerySQL(tctx, nil, querySQL)
	if err != nil {
		return "", terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	defer rows.Close()
	if rows.Next() {
		if scanErr := rows.Scan(&table, &createStr); scanErr != nil {
			return "", terror.DBErrorAdapt(scanErr, terror.ErrDBDriverError)
		}
	} else {
		return "", terror.ErrSyncerDownstreamTableNotFound.Generate(tableID)
	}

	if err = rows.Close(); err != nil {
		return "", terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}
	return createStr, nil
}

func GetParserForConn(tctx *tcontext.Context, conn *DBConn) (*parser.Parser, error) {
	sqlMode, err := getSessionVariable(tctx, conn, "sql_mode")
	if err != nil {
		return nil, err
	}
	return utils.GetParserFromSQLModeStr(sqlMode)
}

//nolint:unparam
func getSessionVariable(tctx *tcontext.Context, conn *DBConn, variable string) (value string, err error) {
	failpoint.Inject("GetSessionVariableFailed", func(val failpoint.Value) {
		items := strings.Split(val.(string), ",")
		if len(items) != 2 {
			log.L().Fatal("failpoint GetSessionVariableFailed's value is invalid", zap.String("val", val.(string)))
		}
		variableName := items[0]
		errCode, err1 := strconv.ParseUint(items[1], 10, 16)
		if err1 != nil {
			log.L().Fatal("failpoint GetSessionVariableFailed's value is invalid", zap.String("val", val.(string)))
		}
		if variable == variableName {
			err = tmysql.NewErr(uint16(errCode))
			log.L().Warn("GetSessionVariable failed", zap.String("variable", variable), zap.String("failpoint", "GetSessionVariableFailed"), zap.Error(err))
			failpoint.Return("", terror.DBErrorAdapt(err, terror.ErrDBDriverError))
		}
	})
	template := "SHOW VARIABLES LIKE '%s'"
	query := fmt.Sprintf(template, variable)
	rows, err := conn.QuerySQL(tctx, nil, query)
	if err != nil {
		return "", terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	defer rows.Close()
	if rows.Next() {
		if err = rows.Scan(&variable, &value); err != nil {
			return "", terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}
	}
	if err = rows.Close(); err != nil {
		return "", terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}
	return value, nil
}
