// Copyright 2025 PingCAP, Inc.
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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	timodel "github.com/pingcap/tidb/parser/model"
	tidbmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

func setSessionTimestamp(ctx context.Context, tx *sql.Tx, unixTimestamp float64) error {
	_, err := tx.ExecContext(ctx, fmt.Sprintf("SET TIMESTAMP = %s", formatUnixTimestamp(unixTimestamp)))
	return err
}

// resetSessionTimestamp clears session @@timestamp to prevent stale values from
// leaking across DDLs using the same session; it's a cheap safety net before
// and after DDL execution.
func resetSessionTimestamp(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "SET TIMESTAMP = DEFAULT")
	return err
}

func formatUnixTimestamp(unixTimestamp float64) string {
	return strconv.FormatFloat(unixTimestamp, 'f', 6, 64)
}

func ddlSessionTimestampFromOriginDefault(ddl *model.DDLEvent, timezone string) (float64, bool) {
	if ddl == nil || ddl.TableInfo == nil || ddl.TableInfo.TableInfo == nil {
		return 0, false
	}
	targetColumns, err := extractCurrentTimestampDefaultColumns(ddl.Query)
	if err != nil || len(targetColumns) == 0 {
		return 0, false
	}

	for _, col := range ddl.TableInfo.Columns {
		if col == nil {
			continue
		}
		if _, ok := targetColumns[col.Name.L]; !ok {
			continue
		}
		val := col.GetOriginDefaultValue()
		valStr, ok := val.(string)
		if !ok || valStr == "" {
			continue
		}
		ts, err := parseOriginDefaultTimestamp(valStr, col, timezone)
		if err != nil {
			log.Warn("Failed to parse OriginDefaultValue for DDL timestamp",
				zap.String("column", col.Name.O),
				zap.String("originDefault", valStr),
				zap.Error(err))
			continue
		}
		log.Info("Using OriginDefaultValue for DDL timestamp",
			zap.String("column", col.Name.O),
			zap.String("originDefault", valStr),
			zap.Float64("timestamp", ts),
			zap.String("timezone", timezone))
		return ts, true
	}

	return 0, false
}

func extractCurrentTimestampDefaultColumns(query string) (map[string]struct{}, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return nil, err
	}

	cols := make(map[string]struct{})
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		for _, col := range s.Cols {
			if hasCurrentTimestampDefault(col) {
				cols[col.Name.Name.L] = struct{}{}
			}
		}
	case *ast.AlterTableStmt:
		for _, spec := range s.Specs {
			switch spec.Tp {
			case ast.AlterTableAddColumns, ast.AlterTableModifyColumn, ast.AlterTableChangeColumn, ast.AlterTableAlterColumn:
				for _, col := range spec.NewColumns {
					if hasCurrentTimestampDefault(col) {
						cols[col.Name.Name.L] = struct{}{}
					}
				}
			}
		}
	}

	return cols, nil
}

func hasCurrentTimestampDefault(col *ast.ColumnDef) bool {
	if col == nil {
		return false
	}
	for _, opt := range col.Options {
		if opt.Tp != ast.ColumnOptionDefaultValue {
			continue
		}
		if isCurrentTimestampExpr(opt.Expr) {
			return true
		}
	}
	return false
}

func isCurrentTimestampExpr(expr ast.ExprNode) bool {
	if expr == nil {
		return false
	}
	switch v := expr.(type) {
	case *ast.FuncCallExpr:
		return isCurrentTimestampFuncName(v.FnName.L)
	case ast.ValueExpr:
		return isCurrentTimestampFuncName(strings.ToLower(v.GetString()))
	default:
		return false
	}
}

func isCurrentTimestampFuncName(name string) bool {
	switch name {
	case ast.CurrentTimestamp, ast.Now, ast.LocalTime, ast.LocalTimestamp:
		return true
	default:
		return false
	}
}

func parseOriginDefaultTimestamp(val string, col *timodel.ColumnInfo, timezone string) (float64, error) {
	loc, err := resolveOriginDefaultLocation(col, timezone)
	if err != nil {
		return 0, err
	}
	return parseTimestampInLocation(val, loc)
}

func resolveOriginDefaultLocation(col *timodel.ColumnInfo, timezone string) (*time.Location, error) {
	if col != nil && col.GetType() == tidbmysql.TypeTimestamp && col.Version >= timodel.ColumnInfoVersion1 {
		return time.UTC, nil
	}
	if timezone == "" {
		return time.UTC, nil
	}
	tz := strings.Trim(timezone, "\"")
	return time.LoadLocation(tz)
}

func parseTimestampInLocation(val string, loc *time.Location) (float64, error) {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999",
	}
	for _, f := range formats {
		t, err := time.ParseInLocation(f, val, loc)
		if err == nil {
			return float64(t.UnixNano()) / float64(time.Second), nil
		}
	}
	return 0, fmt.Errorf("failed to parse timestamp: %s", val)
}

func matchFailpointValue(val failpoint.Value, ddlQuery string) bool {
	if val == nil {
		return true
	}
	switch v := val.(type) {
	case bool:
		return v
	case string:
		if v == "" {
			return true
		}
		return strings.Contains(strings.ToLower(ddlQuery), strings.ToLower(v))
	default:
		return true
	}
}
