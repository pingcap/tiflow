// Copyright 2019 PingCAP, Inc.
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

package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/dbutil"
	"go.uber.org/zap"

	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/pkg/errorutil"
)

// the time layout for TiDB SHOW DDL statements.
const timeLayout = "2006-01-02 15:04:05"

// ignoreTrackerDDLError is also same with ignoreDDLError, but in order to keep tracker's table structure same as
// upstream's, we can't ignore "already exists" errors because already exists doesn't mean same.
func ignoreTrackerDDLError(err error) bool {
	switch {
	case infoschema.ErrDatabaseExists.Equal(err), infoschema.ErrDatabaseDropExists.Equal(err),
		infoschema.ErrTableDropExists.Equal(err),
		dbterror.ErrCantDropFieldOrKey.Equal(err):
		return true
	default:
		return false
	}
}

func isDropColumnWithIndexError(err error) bool {
	mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
	if !ok {
		return false
	}
	// different version of TiDB has different error message, try to cover most versions
	return (mysqlErr.Number == errno.ErrUnsupportedDDLOperation || mysqlErr.Number == tmysql.ErrUnknown) &&
		strings.Contains(mysqlErr.Message, "drop column") &&
		(strings.Contains(mysqlErr.Message, "with index") ||
			strings.Contains(mysqlErr.Message, "with composite index") ||
			strings.Contains(mysqlErr.Message, "with tidb_enable_change_multi_schema is disable"))
}

// getDDLStatusFromTiDB retrieves the synchronizing status of DDL from TiDB
// hence here db should be TiDB database
// createTime should be based on the timezone of downstream, and its unit is second.
func getDDLStatusFromTiDB(ctx context.Context, db *sql.DB, ddl string, createTime int64) (string, error) {
	rowNum := 10
	rowOffset := 0
	queryMap := make(map[int]string)

	for {
		// every attempt try 10 history jobs
		showJobs := fmt.Sprintf("ADMIN SHOW DDL JOBS %d", rowNum)
		jobsRows, err := db.QueryContext(ctx, showJobs)
		if err != nil {
			return "", err
		}

		var jobsResults [][]string
		jobsResults, err = export.GetSpecifiedColumnValuesAndClose(jobsRows, "JOB_ID", "CREATE_TIME", "STATE")
		if err != nil {
			return "", err
		}

		for i := rowNum - 10; i < rowNum; i++ {
			ddlCreateTimeStr := jobsResults[i][1]
			var ddlCreateTimeParse time.Time
			ddlCreateTimeParse, err = time.Parse("2006-01-02 15:04:05", ddlCreateTimeStr)
			if err != nil {
				return "", err
			}
			ddlCreateTime := ddlCreateTimeParse.Unix()

			// ddlCreateTime and createTime are both based on timezone of downstream
			if ddlCreateTime >= createTime {
				var jobID int
				jobID, err = strconv.Atoi(jobsResults[i][0])
				if err != nil {
					return "", err
				}

				for {
					ddlQuery, ok := queryMap[jobID]
					if !ok {
						// jobID does not exist, expand queryMap for deeper search
						showJobsLimitNext := fmt.Sprintf("ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET %d", rowOffset)
						var rowsLimitNext *sql.Rows
						rowsLimitNext, err = db.QueryContext(ctx, showJobsLimitNext)
						if err != nil {
							return "", err
						}

						var resultsLimitNext [][]string
						resultsLimitNext, err = export.GetSpecifiedColumnValuesAndClose(rowsLimitNext, "JOB_ID", "QUERY")
						if err != nil {
							return "", err
						}
						if len(resultsLimitNext) == 0 {
							// JOB QUERIES has been used up
							// requested DDL cannot be found
							return "", nil
						}

						// if new DDLs are written to TiDB after the last query 'ADMIN SHOW DDL JOB QUERIES LIMIT 10 OFFSET'
						// we may get duplicate rows here, but it does not affect the checking
						for k := range resultsLimitNext {
							var jobIDForLimit int
							jobIDForLimit, err = strconv.Atoi(resultsLimitNext[k][0])
							if err != nil {
								return "", err
							}
							queryMap[jobIDForLimit] = resultsLimitNext[k][1]
						}
						rowOffset += 10
					} else {
						if ddl == ddlQuery {
							return jobsResults[i][2], nil
						}
						break
					}
				}
			} else {
				// ddlCreateTime is monotonous in jobsResults
				// requested DDL cannot be found
				return "", nil
			}
		}
		rowNum += 10
	}
}

// handleSpecialDDLError handles special errors for DDL execution.
func (s *Syncer) handleSpecialDDLError(tctx *tcontext.Context, err error, ddls []string, index int, conn *dbconn.DBConn) error {
	// We use default parser because ddls are came from *Syncer.genDDLInfo, which is StringSingleQuotes, KeyWordUppercase and NameBackQuotes
	parser2 := parser.New()

	// it only ignore `invalid connection` error (timeout or other causes) for `ADD INDEX`.
	// `invalid connection` means some data already sent to the server,
	// and we assume that the whole SQL statement has already sent to the server for this error.
	// if we have other methods to judge the DDL dispatched but timeout for executing, we can update this method.
	// NOTE: we must ensure other PK/UK exists for correctness.
	// NOTE: when we are refactoring the shard DDL algorithm, we also need to consider supporting non-blocking `ADD INDEX`.
	invalidConnF := func(tctx *tcontext.Context, err error, ddls []string, index int, conn *dbconn.DBConn) error {
		// must ensure only the last statement executed failed with the `invalid connection` error
		if len(ddls) == 0 || index != len(ddls)-1 || errors.Cause(err) != mysql.ErrInvalidConn {
			return err // return the original error
		}

		ddl2 := ddls[index]
		stmt, err2 := parser2.ParseOneStmt(ddl2, "", "")
		if err2 != nil {
			return err // return the original error
		}

		handle := func() {
			tctx.L().Warn("ignore special error for DDL", zap.String("DDL", ddl2), log.ShortError(err))
			err2 := conn.ResetConn(tctx) // also reset the `invalid connection` for later use.
			if err2 != nil {
				tctx.L().Warn("reset connection failed", log.ShortError(err2))
			}
		}

		switch v := stmt.(type) {
		case *ast.AlterTableStmt:
			// ddls should be split with only one spec
			if len(v.Specs) > 1 {
				return err
			} else if v.Specs[0].Tp == ast.AlterTableAddConstraint {
				// only take effect on `ADD INDEX`, no UNIQUE KEY and FOREIGN KEY
				// UNIQUE KEY may affect correctness, FOREIGN KEY should be filtered.
				// ref https://github.com/pingcap/tidb/blob/3cdea0dfdf28197ee65545debce8c99e6d2945e3/ddl/ddl_api.go#L1929-L1948.
				switch v.Specs[0].Constraint.Tp {
				case ast.ConstraintKey, ast.ConstraintIndex:
					handle()
					return nil // ignore the error
				}
			}
		case *ast.CreateIndexStmt:
			handle()
			return nil // ignore the error
		}
		return err
	}

	// for DROP COLUMN with its single-column index, try drop index first then drop column
	// TiDB will support DROP COLUMN with index soon. After its support, executing that SQL will not have an error,
	// so this function will not trigger and cause some trouble
	dropColumnF := func(tctx *tcontext.Context, originErr error, ddls []string, index int, conn *dbconn.DBConn) error {
		if !isDropColumnWithIndexError(originErr) {
			return originErr
		}
		ddl2 := ddls[index]
		stmt, err2 := parser2.ParseOneStmt(ddl2, "", "")
		if err2 != nil {
			return originErr // return the original error
		}

		var (
			schema string
			table  string
			col    string
		)
		n, ok := stmt.(*ast.AlterTableStmt)
		switch {
		case !ok:
			return originErr
		case len(n.Specs) != 1:
			return originErr
		case n.Specs[0].Tp != ast.AlterTableDropColumn:
			return originErr
		default:
			schema = n.Table.Schema.O
			table = n.Table.Name.O
			col = n.Specs[0].OldColumnName.Name.O
		}
		tctx.L().Warn("try to fix drop column error", zap.String("DDL", ddl2), log.ShortError(originErr))

		// check if dependent index is single-column index on this column
		sql2 := "SELECT INDEX_NAME FROM information_schema.statistics WHERE TABLE_SCHEMA = ? and TABLE_NAME = ? and COLUMN_NAME = ?"
		var rows *sql.Rows
		rows, err2 = conn.QuerySQL(tctx, s.metricsProxies, sql2, schema, table, col)
		if err2 != nil {
			return originErr
		}
		var (
			idx       string
			idx2Check []string
			idx2Drop  []string
			count     int
		)
		for rows.Next() {
			if err2 = rows.Scan(&idx); err2 != nil {
				// nolint:sqlclosecheck
				rows.Close()
				return originErr
			}
			idx2Check = append(idx2Check, idx)
		}
		if rows.Err() != nil {
			return rows.Err()
		}
		// Close is idempotent, we could close in advance to reuse conn
		rows.Close()

		sql2 = "SELECT count(*) FROM information_schema.statistics WHERE TABLE_SCHEMA = ? and TABLE_NAME = ? and INDEX_NAME = ?"
		for _, idx := range idx2Check {
			rows, err2 = conn.QuerySQL(tctx, s.metricsProxies, sql2, schema, table, idx)
			if err2 != nil || !rows.Next() || rows.Scan(&count) != nil || count != 1 {
				tctx.L().Warn("can't auto drop index", zap.String("index", idx))
				if rows != nil {
					// nolint: sqlclosecheck
					rows.Close()
				}
				return originErr
			}
			if rows.Err() != nil {
				return rows.Err()
			}
			idx2Drop = append(idx2Drop, idx)
			rows.Close()
		}

		sqls := make([]string, len(idx2Drop))
		for i, idx := range idx2Drop {
			sqls[i] = fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", dbutil.TableName(schema, table), dbutil.ColumnName(idx))
		}
		if _, err2 = conn.ExecuteSQL(tctx, s.metricsProxies, sqls); err2 != nil {
			tctx.L().Warn("auto drop index failed", log.ShortError(err2))
			return originErr
		}

		tctx.L().Info("drop index success, now try to drop column", zap.Strings("index", idx2Drop))
		if _, err2 = conn.ExecuteSQLWithIgnore(tctx, s.metricsProxies, errorutil.IsIgnorableMySQLDDLError, ddls[index:]); err2 != nil {
			return err2
		}

		tctx.L().Info("execute drop column SQL success", zap.String("DDL", ddl2))
		return nil
	}
	// TODO: add support for downstream alter pk without schema

	retErr := err
	toHandle := []func(*tcontext.Context, error, []string, int, *dbconn.DBConn) error{
		dropColumnF,
		invalidConnF,
	}
	for _, f := range toHandle {
		retErr = f(tctx, retErr, ddls, index, conn)
		if retErr == nil {
			break
		}
	}
	return retErr
}

func isDuplicateServerIDError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "A slave with the same server_uuid/server_id as this slave has connected to the master")
}

func isConnectionRefusedError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "connect: connection refused")
}
