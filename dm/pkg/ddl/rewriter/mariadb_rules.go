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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	tidbtypes "github.com/pingcap/tidb/pkg/types"
)

// maxStringIndexPrefixLen keeps rewritten string index prefixes within TiDB's default maximum index length.
// TiDB limits an index to 3072 bytes, which is 768 characters with 4-byte UTF-8 encoding.
// See https://docs.pingcap.com/tidb/stable/tidb-limitations/#limitations-on-indexes.
const maxStringIndexPrefixLen = 768

// defaultBlobIndexPrefixLen follows a common MySQL/MariaDB prefix length for BLOB/TEXT indexes.
const defaultBlobIndexPrefixLen = 255

var mariaDBCompatibilityRules = []rule{
	indexPrefixRule{},
	textBlobDefaultRule{},
	functionDefaultRule{},
	jsonGeneratedRule{},
}

// indexPrefixRule adds explicit prefix lengths for plain secondary indexes that TiDB rejects.
type indexPrefixRule struct{}

func (r indexPrefixRule) Apply(node ast.Node) (bool, error) {
	stmt, ok := node.(*ast.CreateTableStmt)
	if !ok {
		return false, nil
	}
	colMap := make(map[string]*ast.ColumnDef, len(stmt.Cols))
	for _, col := range stmt.Cols {
		colMap[col.Name.Name.L] = col
	}

	changed := false
	for _, cons := range stmt.Constraints {
		switch cons.Tp {
		case ast.ConstraintKey, ast.ConstraintIndex:
		default:
			continue
		}
		for _, key := range cons.Keys {
			if key.Length > 0 {
				continue
			}
			if key.Column == nil {
				continue
			}
			col := colMap[key.Column.Name.L]
			if col == nil {
				continue
			}
			switch {
			case types.IsTypeBlob(col.Tp.GetType()):
				key.Length = defaultBlobIndexPrefixLen
				changed = true
			case (tidbtypes.IsTypeChar(col.Tp.GetType()) || tidbtypes.IsTypeVarchar(col.Tp.GetType())) &&
				col.Tp.GetFlen() > maxStringIndexPrefixLen:
				key.Length = maxStringIndexPrefixLen
				changed = true
			}
		}
	}
	return changed, nil
}

// textBlobDefaultRule removes non-NULL defaults from TEXT, BLOB, and JSON columns that TiDB rejects.
type textBlobDefaultRule struct{}

func (r textBlobDefaultRule) Apply(node ast.Node) (bool, error) {
	col, ok := node.(*ast.ColumnDef)
	if !ok || !isTextBlobOrJSON(col.Tp) {
		return false, nil
	}
	return filterColumnOptions(col, func(opt *ast.ColumnOption) bool {
		return opt.Tp == ast.ColumnOptionDefaultValue && !isNullValueExpr(opt.Expr)
	}), nil
}

// functionDefaultRule removes time-function defaults from column types that TiDB rejects.
type functionDefaultRule struct{}

func (r functionDefaultRule) Apply(node ast.Node) (bool, error) {
	col, ok := node.(*ast.ColumnDef)
	if !ok {
		return false, nil
	}
	return filterColumnOptions(col, func(opt *ast.ColumnOption) bool {
		return opt.Tp == ast.ColumnOptionDefaultValue && isUnsupportedTimeDefault(col, opt.Expr)
	}), nil
}

// jsonGeneratedRule rewrites MariaDB JSON_VALUE generated expressions to TiDB-supported JSON functions.
type jsonGeneratedRule struct{}

func (r jsonGeneratedRule) Apply(node ast.Node) (bool, error) {
	col, ok := node.(*ast.ColumnDef)
	if !ok {
		return false, nil
	}
	changed := false
	for _, opt := range col.Options {
		if opt.Tp != ast.ColumnOptionGenerated {
			continue
		}
		if expr, ok := rewriteJSONValueExpr(opt.Expr); ok {
			opt.Expr = expr
			changed = true
		}
	}
	return changed, nil
}

func isUnsupportedTimeDefault(col *ast.ColumnDef, expr ast.ExprNode) bool {
	expr = unwrapParentheses(expr)
	fn, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return false
	}
	switch fn.FnName.L {
	case ast.CurrentTimestamp, ast.Now, ast.LocalTime, ast.LocalTimestamp:
		return col.Tp.GetType() != mysql.TypeTimestamp && col.Tp.GetType() != mysql.TypeDatetime
	case ast.CurrentDate:
		return col.Tp.GetType() != mysql.TypeDate && col.Tp.GetType() != mysql.TypeDatetime
	case ast.CurrentTime:
		return col.Tp.GetType() != mysql.TypeDuration
	default:
		return false
	}
}

func rewriteJSONValueExpr(expr ast.ExprNode) (ast.ExprNode, bool) {
	fn, ok := unwrapParentheses(expr).(*ast.FuncCallExpr)
	if !ok || fn.FnName.L != "json_value" || len(fn.Args) != 2 {
		return expr, false
	}
	jsonExtract := &ast.FuncCallExpr{
		FnName: ast.NewCIStr(ast.JSONExtract),
		Args:   fn.Args,
	}
	return &ast.FuncCallExpr{
		FnName: ast.NewCIStr(ast.JSONUnquote),
		Args:   []ast.ExprNode{jsonExtract},
	}, true
}

func isNullValueExpr(expr ast.ExprNode) bool {
	expr = unwrapParentheses(expr)
	valExpr, ok := expr.(ast.ValueExpr)
	if !ok {
		return false
	}
	return valExpr.GetValue() == nil
}

func isTextBlobOrJSON(ft *types.FieldType) bool {
	return types.IsTypeBlob(ft.GetType()) || ft.GetType() == mysql.TypeJSON
}
