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

package mariadb2tidb

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tiflow/dm/pkg/mariadb2tidb/config"
	"github.com/stretchr/testify/require"
)

func TestTransformSQLDefaultRules(t *testing.T) {
	converter := NewConverter(nil)
	input := `CREATE TABLE t (
  id INT DEFAULT uuid(),
  txt TEXT DEFAULT 'x',
  v VARCHAR(800),
  j JSON,
  g JSON GENERATED ALWAYS AS (JSON_EXTRACT(j, '$.a')) VIRTUAL,
  CHECK (json_valid(j)),
  KEY idx_txt (txt),
  KEY idx_v (v)
) DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;`

	output, err := converter.TransformSQL(input)
	require.NoError(t, err)

	stmt := parseCreateTableStmt(t, output)
	require.Equal(t, "utf8mb4", findTableOptionValue(stmt.Options, ast.TableOptionCharset))
	require.Equal(t, "utf8mb4_0900_ai_ci", findTableOptionValue(stmt.Options, ast.TableOptionCollate))

	idCol := findColumn(stmt, "id")
	require.NotNil(t, idCol)
	require.False(t, hasColumnOption(idCol, ast.ColumnOptionDefaultValue))

	txtCol := findColumn(stmt, "txt")
	require.NotNil(t, txtCol)
	require.False(t, hasColumnOption(txtCol, ast.ColumnOptionDefaultValue))

	vCol := findColumn(stmt, "v")
	require.NotNil(t, vCol)
	require.Equal(t, 768, vCol.Tp.GetFlen())

	gCol := findColumn(stmt, "g")
	require.NotNil(t, gCol)
	require.False(t, hasColumnOption(gCol, ast.ColumnOptionGenerated))

	require.False(t, hasJSONValidCheck(stmt))

	idxTxt := findIndex(stmt, "idx_txt")
	require.NotNil(t, idxTxt)
	require.Len(t, idxTxt.Keys, 1)
	require.Equal(t, 255, idxTxt.Keys[0].Length)

	idxV := findIndex(stmt, "idx_v")
	require.NotNil(t, idxV)
	require.Len(t, idxV.Keys, 1)
	require.Equal(t, 768, idxV.Keys[0].Length)
}

func TestTransformSQLNonStrictReturnsOriginal(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.StrictMode = false
	converter := NewConverter(cfg)

	input := "this is not valid sql"
	output, err := converter.TransformSQL(input)
	require.NoError(t, err)
	require.Equal(t, input, output)
}

func parseCreateTableStmt(t *testing.T, sql string) *ast.CreateTableStmt {
	t.Helper()
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	create, ok := stmt.(*ast.CreateTableStmt)
	require.True(t, ok)
	return create
}

func findTableOptionValue(options []*ast.TableOption, optionType ast.TableOptionType) string {
	for _, opt := range options {
		if opt.Tp == optionType {
			return strings.ToLower(opt.StrValue)
		}
	}
	return ""
}

func findColumn(stmt *ast.CreateTableStmt, name string) *ast.ColumnDef {
	lower := strings.ToLower(name)
	for _, col := range stmt.Cols {
		if col.Name.Name.L == lower {
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
	for _, constraint := range stmt.Constraints {
		if constraint.Tp == ast.ConstraintCheck && isJSONValidExpr(constraint.Expr) {
			return true
		}
	}
	return false
}

func isJSONValidExpr(expr ast.ExprNode) bool {
	fc, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return false
	}
	return strings.EqualFold(fc.FnName.O, "json_valid")
}

func findIndex(stmt *ast.CreateTableStmt, name string) *ast.Constraint {
	lower := strings.ToLower(name)
	for _, constraint := range stmt.Constraints {
		switch constraint.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq:
			if strings.EqualFold(constraint.Name, lower) {
				return constraint
			}
		}
	}
	return nil
}
