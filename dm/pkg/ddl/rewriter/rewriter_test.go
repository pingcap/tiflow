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
	"github.com/stretchr/testify/require"
)

func TestRewriteSQLRemovesFunctionDefaultOnVarchar(t *testing.T) {
	rewriter := newDefaultRewriterForTest()

	out, changed, err := rewriter.RewriteSQL("CREATE TABLE t(t VARCHAR(100) DEFAULT current_timestamp());")
	require.NoError(t, err)
	require.True(t, changed)
	require.NotContains(t, strings.ToLower(out), "default")

	stmt := parseCreateTable(t, out)
	col := findColumn(stmt, "t")
	require.NotNil(t, col)
	require.False(t, hasColumnOption(col, ast.ColumnOptionDefaultValue))
}

func TestRewriteSQLKeepsTimeFunctionDefaultOnTimeColumn(t *testing.T) {
	rewriter := newDefaultRewriterForTest()

	out, changed, err := rewriter.RewriteSQL("CREATE TABLE t(ts TIMESTAMP DEFAULT current_timestamp());")
	require.NoError(t, err)
	require.False(t, changed)
	require.Contains(t, strings.ToLower(out), "default current_timestamp")
}

func TestRewriteSQLDefaultRules(t *testing.T) {
	rewriter := newDefaultRewriterForTest()
	input := `CREATE TABLE t (
  id INT(11),
  txt TEXT DEFAULT 'x',
  v VARCHAR(800),
  j JSON,
  g JSON GENERATED ALWAYS AS (JSON_EXTRACT(j, '$.a')) VIRTUAL,
  zero_ts TIMESTAMP DEFAULT '0000-00-00 00:00:00',
  CHECK (json_valid(j)),
  KEY idx_txt (txt),
  KEY idx_v (v)
) DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;`

	out, changed, err := rewriter.RewriteSQL(input)
	require.NoError(t, err)
	require.True(t, changed)

	stmt := parseCreateTable(t, out)
	require.Equal(t, "utf8mb4", findTableOption(stmt, ast.TableOptionCharset))
	require.Equal(t, "utf8mb4_0900_ai_ci", findTableOption(stmt, ast.TableOptionCollate))
	require.Equal(t, -1, findColumn(stmt, "id").Tp.GetFlen())
	require.Equal(t, 768, findColumn(stmt, "v").Tp.GetFlen())
	require.False(t, hasColumnOption(findColumn(stmt, "txt"), ast.ColumnOptionDefaultValue))
	require.False(t, hasColumnOption(findColumn(stmt, "g"), ast.ColumnOptionGenerated))
	require.False(t, hasColumnOption(findColumn(stmt, "zero_ts"), ast.ColumnOptionDefaultValue))
	require.False(t, hasJSONValidCheck(stmt))

	idxTxt := findConstraint(stmt, "idx_txt")
	require.NotNil(t, idxTxt)
	require.Equal(t, 255, idxTxt.Keys[0].Length)
	idxV := findConstraint(stmt, "idx_v")
	require.NotNil(t, idxV)
	require.Equal(t, 768, idxV.Keys[0].Length)
}

func TestRewriteSQLSkipsExpressionIndexPrefix(t *testing.T) {
	rewriter := newDefaultRewriterForTest()

	_, _, err := rewriter.RewriteSQL("CREATE TABLE t(name VARCHAR(32), KEY idx_expr ((LOWER(name))));")
	require.NoError(t, err)
}

func TestRewriteSQLRemovesParenthesizedJSONGeneratedColumn(t *testing.T) {
	rewriter := newDefaultRewriterForTest()

	out, changed, err := rewriter.RewriteSQL(
		"CREATE TABLE t(j JSON, g JSON GENERATED ALWAYS AS ((JSON_EXTRACT(j, '$.a'))) VIRTUAL);",
	)
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, hasColumnOption(findColumn(parseCreateTable(t, out), "g"), ast.ColumnOptionGenerated))
}

func TestNewRewriterForFlavor(t *testing.T) {
	require.NotNil(t, NewRewriterForFlavor("mariadb"))
	require.NotNil(t, NewRewriterForFlavor("MariaDB"))
	require.Nil(t, NewRewriterForFlavor("mysql"))
}

func newDefaultRewriterForTest() *Rewriter {
	return NewRewriter(defaultRules...)
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

func hasJSONValidCheck(stmt *ast.CreateTableStmt) bool {
	for _, cons := range stmt.Constraints {
		if cons.Tp == ast.ConstraintCheck && isJSONValidExpr(cons.Expr) {
			return true
		}
	}
	return false
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
