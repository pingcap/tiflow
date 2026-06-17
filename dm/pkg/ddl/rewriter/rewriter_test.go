// Copyright 2026 PingCAP, Inc.
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

package rewriter

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // register parser driver
	"github.com/stretchr/testify/require"
)

func TestRewriteStmtRemovesFunctionDefaultOnVarchar(t *testing.T) {
	stmt, changed := rewriteCreateTable(t, "CREATE TABLE t(t VARCHAR(100) DEFAULT current_timestamp());")
	require.True(t, changed)

	col := findColumn(stmt, "t")
	require.NotNil(t, col)
	require.False(t, hasColumnOption(col, ast.ColumnOptionDefaultValue))
}

func TestRewriteStmtKeepsTimeFunctionDefaultOnTimeColumn(t *testing.T) {
	stmt, changed := rewriteCreateTable(t, "CREATE TABLE t(ts TIMESTAMP DEFAULT current_timestamp());")
	require.False(t, changed)
	require.True(t, hasColumnOption(findColumn(stmt, "ts"), ast.ColumnOptionDefaultValue))
}

func TestRewriteStmtTimeFunctionDefaultRules(t *testing.T) {
	stmt, changed := rewriteCreateTable(t, `CREATE TABLE t(
  dec_col DECIMAL(30,6) DEFAULT CURRENT_TIMESTAMP(6),
  time_col TIME DEFAULT CURRENT_TIMESTAMP(),
  date_col DATE DEFAULT CURRENT_TIMESTAMP(),
  ok_date DATE DEFAULT CURRENT_DATE,
  ok_datetime DATETIME DEFAULT CURRENT_DATE
)`)
	require.True(t, changed)

	require.False(t, hasColumnOption(findColumn(stmt, "dec_col"), ast.ColumnOptionDefaultValue))
	require.False(t, hasColumnOption(findColumn(stmt, "time_col"), ast.ColumnOptionDefaultValue))
	require.False(t, hasColumnOption(findColumn(stmt, "date_col"), ast.ColumnOptionDefaultValue))
	require.True(t, hasColumnOption(findColumn(stmt, "ok_date"), ast.ColumnOptionDefaultValue))
	require.True(t, hasColumnOption(findColumn(stmt, "ok_datetime"), ast.ColumnOptionDefaultValue))
}

func TestRewriteStmtDefaultRules(t *testing.T) {
	input := `CREATE TABLE t (
  id INT(11),
  txt TEXT DEFAULT 'x',
  txt_null TEXT DEFAULT NULL,
  txt_expr TEXT DEFAULT(uuid()),
  v VARCHAR(800),
  j JSON DEFAULT(json_object('now', now())),
  g JSON GENERATED ALWAYS AS (JSON_EXTRACT(j, '$.a')) VIRTUAL,
  zero_ts TIMESTAMP DEFAULT '0000-00-00 00:00:00',
  CHECK (json_valid(j)),
  KEY idx_txt (txt),
  KEY idx_v (v),
  UNIQUE KEY uk_txt (txt)
) DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;`

	stmt, changed := rewriteCreateTable(t, input)
	require.True(t, changed)

	require.Equal(t, "latin1", findTableOption(stmt, ast.TableOptionCharset))
	require.Equal(t, "latin1_swedish_ci", findTableOption(stmt, ast.TableOptionCollate))
	require.Equal(t, 11, findColumn(stmt, "id").Tp.GetFlen())
	require.Equal(t, 800, findColumn(stmt, "v").Tp.GetFlen())
	require.False(t, hasColumnOption(findColumn(stmt, "txt"), ast.ColumnOptionDefaultValue))
	require.True(t, hasColumnOption(findColumn(stmt, "txt_null"), ast.ColumnOptionDefaultValue))
	require.True(t, hasColumnOption(findColumn(stmt, "txt_expr"), ast.ColumnOptionDefaultValue))
	require.True(t, hasColumnOption(findColumn(stmt, "j"), ast.ColumnOptionDefaultValue))
	require.True(t, hasColumnOption(findColumn(stmt, "g"), ast.ColumnOptionGenerated))
	require.True(t, hasColumnOption(findColumn(stmt, "zero_ts"), ast.ColumnOptionDefaultValue))
	require.True(t, hasCheckConstraint(stmt))

	idxTxt := findConstraint(stmt, "idx_txt")
	require.NotNil(t, idxTxt)
	require.Equal(t, 255, idxTxt.Keys[0].Length)
	idxV := findConstraint(stmt, "idx_v")
	require.NotNil(t, idxV)
	require.Equal(t, 768, idxV.Keys[0].Length)
	ukTxt := findConstraint(stmt, "uk_txt")
	require.NotNil(t, ukTxt)
	require.Equal(t, -1, ukTxt.Keys[0].Length)
}

func TestRewriteStmtSkipsExpressionIndexPrefix(t *testing.T) {
	rewriteCreateTable(t, "CREATE TABLE t(name VARCHAR(32), KEY idx_expr ((LOWER(name))));")
}

func TestRewriteStmtRewritesParenthesizedJSONValueGeneratedColumn(t *testing.T) {
	stmt, changed := rewriteCreateTable(t,
		"CREATE TABLE t(j JSON, g TIME GENERATED ALWAYS AS (CAST((JSON_VALUE(j, '$.a')) AS TIME)) VIRTUAL);",
	)
	require.True(t, changed)
	expr := generatedExpr(findColumn(stmt, "g"))
	require.NotNil(t, expr)
	restored := strings.ToLower(restoreNode(t, expr))
	require.Contains(t, restored, "json_unquote(json_extract")
	require.NotContains(t, restored, "json_value")
}

func rewriteCreateTable(t *testing.T, sql string) (*ast.CreateTableStmt, bool) {
	t.Helper()
	stmt := parseCreateTable(t, sql)
	changed, err := RewriteStmt(stmt, WithMariaDBCompatibility())
	require.NoError(t, err)
	return stmt, changed
}

func parseCreateTable(t *testing.T, sql string) *ast.CreateTableStmt {
	t.Helper()
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	create, ok := stmt.(*ast.CreateTableStmt)
	require.True(t, ok)
	return create
}

func findColumn(stmt *ast.CreateTableStmt, name string) *ast.ColumnDef {
	for _, col := range stmt.Cols {
		if strings.EqualFold(col.Name.Name.O, name) {
			return col
		}
	}
	return nil
}

func hasColumnOption(col *ast.ColumnDef, optionType ast.ColumnOptionType) bool {
	for _, opt := range col.Options {
		if opt.Tp == optionType {
			return true
		}
	}
	return false
}

func hasCheckConstraint(stmt *ast.CreateTableStmt) bool {
	for _, cons := range stmt.Constraints {
		if cons.Tp == ast.ConstraintCheck {
			return true
		}
	}
	return false
}

func generatedExpr(col *ast.ColumnDef) ast.ExprNode {
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionGenerated {
			return opt.Expr
		}
	}
	return nil
}

func findConstraint(stmt *ast.CreateTableStmt, name string) *ast.Constraint {
	for _, cons := range stmt.Constraints {
		if strings.EqualFold(cons.Name, name) {
			return cons
		}
	}
	return nil
}

func findTableOption(stmt *ast.CreateTableStmt, optionType ast.TableOptionType) string {
	for _, opt := range stmt.Options {
		if opt.Tp == optionType {
			return strings.ToLower(opt.StrValue)
		}
	}
	return ""
}

func restoreNode(t *testing.T, node ast.Node) string {
	t.Helper()
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	require.NoError(t, node.Restore(ctx))
	return sb.String()
}
