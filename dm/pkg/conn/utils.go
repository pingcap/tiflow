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

package conn

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/dumpling/export"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
)

// GetGlobalVariable gets server's global variable.
func GetGlobalVariable(ctx *tcontext.Context, db *BaseDB, variable string) (value string, err error) {
	failpoint.Inject("GetGlobalVariableFailed", func(val failpoint.Value) {
		items := strings.Split(val.(string), ",")
		if len(items) != 2 {
			ctx.L().Fatal("failpoint GetGlobalVariableFailed's value is invalid", zap.String("val", val.(string)))
		}
		variableName := items[0]
		errCode, err1 := strconv.ParseUint(items[1], 10, 16)
		if err1 != nil {
			ctx.L().Fatal("failpoint GetGlobalVariableFailed's value is invalid", zap.String("val", val.(string)))
		}
		if variable == variableName {
			err = tmysql.NewErr(uint16(errCode))
			ctx.L().Warn("GetGlobalVariable failed", zap.String("variable", variable), zap.String("failpoint", "GetGlobalVariableFailed"), zap.Error(err))
			failpoint.Return("", terror.DBErrorAdapt(err, db.Scope, terror.ErrDBDriverError))
		}
	})

	conn, err := db.GetBaseConn(ctx.Context())
	if err != nil {
		return "", err
	}
	defer db.CloseConnWithoutErr(conn)
	return getVariable(ctx, conn, variable, true)
}

func getVariable(ctx *tcontext.Context, conn *BaseConn, variable string, isGlobal bool) (value string, err error) {
	var template string
	if isGlobal {
		template = "SHOW GLOBAL VARIABLES LIKE '%s'"
	} else {
		template = "SHOW VARIABLES LIKE '%s'"
	}
	query := fmt.Sprintf(template, variable)
	row, err := conn.QuerySQL(ctx, query)
	if err != nil {
		return "", err
	}
	defer row.Close()

	// Show an example.
	/*
		mysql> SHOW GLOBAL VARIABLES LIKE "binlog_format";
		+---------------+-------+
		| Variable_name | Value |
		+---------------+-------+
		| binlog_format | ROW   |
		+---------------+-------+
	*/

	if !row.Next() {
		return "", terror.WithScope(terror.ErrDBDriverError.Generatef("variable %s not found", variable), conn.Scope)
	}

	err = row.Scan(&variable, &value)
	if err != nil {
		return "", terror.DBErrorAdapt(err, conn.Scope, terror.ErrDBDriverError)
	}
	return value, nil
}

// GetMasterStatus gets status from master.
// When the returned error is nil, the gtid must be not nil.
func GetMasterStatus(ctx *tcontext.Context, db *BaseDB, flavor string) (
	string, uint64, string, string, string, error,
) {
	var (
		binlogName     string
		pos            uint64
		binlogDoDB     string
		binlogIgnoreDB string
		gtidStr        string
		err            error
	)
	// need REPLICATION SLAVE privilege
	rows, err := db.QueryContext(ctx, `SHOW MASTER STATUS`)
	if err != nil {
		err = terror.DBErrorAdapt(err, db.Scope, terror.ErrDBDriverError)
		return binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err
	}
	defer rows.Close()

	// Show an example.
	/*
		MySQL [test]> SHOW MASTER STATUS;
		+-----------+----------+--------------+------------------+--------------------------------------------+
		| File      | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set                          |
		+-----------+----------+--------------+------------------+--------------------------------------------+
		| ON.000001 |     4822 |              |                  | 85ab69d1-b21f-11e6-9c5e-64006a8978d2:1-46
		+-----------+----------+--------------+------------------+--------------------------------------------+
	*/
	/*
		For MariaDB,SHOW MASTER STATUS:
		+--------------------+----------+--------------+------------------+
		| File               | Position | Binlog_Do_DB | Binlog_Ignore_DB |
		+--------------------+----------+--------------+------------------+
		| mariadb-bin.000016 |      475 |              |                  |
		+--------------------+----------+--------------+------------------+
		SELECT @@global.gtid_binlog_pos;
		+--------------------------+
		| @@global.gtid_binlog_pos |
		+--------------------------+
		| 0-1-2                    |
		+--------------------------+
	*/

	var rowsResult [][]string
	if flavor == gmysql.MySQLFlavor {
		rowsResult, err = export.GetSpecifiedColumnValuesAndClose(rows, "File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set")
		if err != nil {
			err = terror.DBErrorAdapt(err, db.Scope, terror.ErrDBDriverError)
			return binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err
		}

		switch {
		case len(rowsResult) == 0:
			err = terror.ErrNoMasterStatus.Generate()
			return binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err
		case len(rowsResult[0]) != 5:
			ctx.L().DPanic("The number of columns that SHOW MASTER STATUS returns for MySQL is not equal to 5, will not use the retrieved information")
			err = terror.ErrIncorrectReturnColumnsNum.Generate()
			return binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err
		default:
			binlogName = rowsResult[0][0]
			var posInt uint64
			posInt, err = strconv.ParseUint(rowsResult[0][1], 10, 64)
			if err != nil {
				err = terror.DBErrorAdapt(err, db.Scope, terror.ErrDBDriverError)
				return binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err
			}
			pos = posInt
			binlogDoDB = rowsResult[0][2]
			binlogIgnoreDB = rowsResult[0][3]
			gtidStr = rowsResult[0][4]
		}
	} else {
		rowsResult, err = export.GetSpecifiedColumnValuesAndClose(rows, "File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB")
		if err != nil {
			err = terror.DBErrorAdapt(err, db.Scope, terror.ErrDBDriverError)
			return binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err
		}

		switch {
		case len(rowsResult) == 0:
			err = terror.ErrNoMasterStatus.Generate()
			return binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err
		case len(rowsResult[0]) != 4:
			ctx.L().DPanic("The number of columns that SHOW MASTER STATUS returns for MariaDB is not equal to 4, will not use the retrieved information")
			err = terror.ErrIncorrectReturnColumnsNum.Generate()
			return binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err
		default:
			binlogName = rowsResult[0][0]
			var posInt uint64
			posInt, err = strconv.ParseUint(rowsResult[0][1], 10, 64)
			if err != nil {
				err = terror.DBErrorAdapt(err, db.Scope, terror.ErrDBDriverError)
				return binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err
			}
			pos = posInt
			binlogDoDB = rowsResult[0][2]
			binlogIgnoreDB = rowsResult[0][3]
		}
	}

	if flavor == gmysql.MariaDBFlavor {
		gtidStr, err = GetGlobalVariable(ctx, db, "gtid_binlog_pos")
		if err != nil {
			return binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err
		}
	}

	if len(rowsResult) > 1 {
		ctx.L().Warn("SHOW MASTER STATUS returns more than one row, will only use first row")
	}
	if rows.Close() != nil {
		err = terror.DBErrorAdapt(rows.Err(), db.Scope, terror.ErrDBDriverError)
		return binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err
	}
	if rows.Err() != nil {
		err = terror.DBErrorAdapt(rows.Err(), db.Scope, terror.ErrDBDriverError)
		return binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err
	}

	return binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err
}

// GetPosAndGs get binlog position and gmysql.GTIDSet from `show master status`.
func GetPosAndGs(ctx *tcontext.Context, db *BaseDB, flavor string) (
	binlogPos gmysql.Position,
	gs gmysql.GTIDSet,
	err error,
) {
	binlogName, pos, _, _, gtidStr, err := GetMasterStatus(ctx, db, flavor)
	if err != nil {
		return
	}
	if pos > math.MaxUint32 {
		ctx.L().Warn("the pos returned by GetMasterStatus beyonds the range of uint32")
	}
	binlogPos = gmysql.Position{
		Name: binlogName,
		Pos:  uint32(pos),
	}

	gs, err = gtid.ParserGTID(flavor, gtidStr)
	return
}

// GetBinlogDB get binlog_do_db and binlog_ignore_db from `show master status`.
func GetBinlogDB(ctx *tcontext.Context, db *BaseDB, flavor string) (string, string, error) {
	// nolint:dogsled
	_, _, binlogDoDB, binlogIgnoreDB, _, err := GetMasterStatus(ctx, db, flavor)
	return binlogDoDB, binlogIgnoreDB, err
}

// LowerCaseTableNamesFlavor represents the type of db `lower_case_table_names` settings.
type LowerCaseTableNamesFlavor uint8

const (
	// LCTableNamesSensitive represent lower_case_table_names = 0, case sensitive.
	LCTableNamesSensitive LowerCaseTableNamesFlavor = 0
	// LCTableNamesInsensitive represent lower_case_table_names = 1, case insensitive.
	LCTableNamesInsensitive = 1
	// LCTableNamesMixed represent lower_case_table_names = 2, table names are case-sensitive, but case-insensitive in usage.
	LCTableNamesMixed = 2
)

// GetDBCaseSensitive returns the case-sensitive setting of target db.
func GetDBCaseSensitive(ctx context.Context, db *BaseDB) (bool, error) {
	conn, err := db.GetBaseConn(ctx)
	if err != nil {
		return true, terror.DBErrorAdapt(err, db.Scope, terror.ErrDBDriverError)
	}
	defer db.CloseConnWithoutErr(conn)
	lcFlavor, err := FetchLowerCaseTableNamesSetting(ctx, conn)
	if err != nil {
		return true, err
	}
	return lcFlavor == LCTableNamesSensitive, nil
}

// FetchLowerCaseTableNamesSetting return the `lower_case_table_names` setting of target db.
func FetchLowerCaseTableNamesSetting(ctx context.Context, conn *BaseConn) (LowerCaseTableNamesFlavor, error) {
	query := "SELECT @@lower_case_table_names;"
	row := conn.DBConn.QueryRowContext(ctx, query)
	if row.Err() != nil {
		return LCTableNamesSensitive, terror.ErrDBExecuteFailed.Delegate(row.Err(), query)
	}
	var res uint8
	if err := row.Scan(&res); err != nil {
		return LCTableNamesSensitive, terror.ErrDBExecuteFailed.Delegate(err, query)
	}
	if res > LCTableNamesMixed {
		return LCTableNamesSensitive, terror.ErrDBUnExpect.Generate(fmt.Sprintf("invalid `lower_case_table_names` value '%d'", res))
	}
	return LowerCaseTableNamesFlavor(res), nil
}

// FetchTableEstimatedBytes returns the estimated size (data + index) in bytes of the table.
func FetchTableEstimatedBytes(ctx context.Context, db *BaseDB, schema string, table string) (int64, error) {
	failpoint.Inject("VeryLargeTable", func(val failpoint.Value) {
		tblName := val.(string)
		if tblName == table {
			failpoint.Return(1<<62, nil)
		}
	})
	var size int64
	err := db.DB.QueryRowContext(ctx, "SELECT data_length + index_length FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", schema, table).Scan(&size)
	if err != nil {
		return 0, terror.DBErrorAdapt(err, db.Scope, terror.ErrDBDriverError)
	}
	return size, nil
}
