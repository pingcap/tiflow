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
)

// maxStringIndexPrefixLen keeps rewritten string index prefixes within TiDB's default maximum index length.
// TiDB limits an index to 3072 bytes, which is 768 characters with 4-byte UTF-8 encoding.
// See https://docs.pingcap.com/tidb/stable/tidb-limitations/#limitations-on-indexes.
const maxStringIndexPrefixLen = 768

// defaultBlobIndexPrefixLen follows a common MySQL/MariaDB prefix length for BLOB/TEXT indexes.
const defaultBlobIndexPrefixLen = 255

var mariaDBCompatibilityRules = []rule{
	secondaryIndexPrefixRule{},
	columnDefaultValueRule{},
	functionDefaultRule{},
	jsonValueRule{},
}

// secondaryIndexPrefixRule adds explicit prefix lengths for plain secondary indexes that TiDB rejects.
type secondaryIndexPrefixRule struct{}

func (r secondaryIndexPrefixRule) Apply(node ast.Node) bool {
	stmt, ok := node.(*ast.CreateTableStmt)
	if !ok {
		return false
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
			if key.Length > 0 || key.Column == nil {
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
			case types.IsTypeChar(col.Tp.GetType()):
				if col.Tp.GetFlen() > maxStringIndexPrefixLen {
					key.Length = maxStringIndexPrefixLen
					changed = true
				}
			}
		}
	}
	return changed
}

// columnDefaultValueRule removes literal non-NULL defaults from TEXT/BLOB/JSON columns.
type columnDefaultValueRule struct{}

func (r columnDefaultValueRule) Apply(node ast.Node) bool {
	col, ok := node.(*ast.ColumnDef)
	if !ok || !isTextBlobOrJSON(col.Tp) {
		return false
	}
	return filterColumnOptions(col, func(opt *ast.ColumnOption) bool {
		return opt.Tp == ast.ColumnOptionDefaultValue && isNonNullLiteralValueExpr(opt.Expr)
	})
}

func isNonNullLiteralValueExpr(expr ast.ExprNode) bool {
	expr = unwrapParentheses(expr)
	valExpr, ok := expr.(ast.ValueExpr)
	if !ok {
		return false
	}
	return valExpr.GetValue() != nil
}

// functionDefaultRule removes time-function defaults from column types that TiDB rejects.
type functionDefaultRule struct{}

func (r functionDefaultRule) Apply(node ast.Node) bool {
	col, ok := node.(*ast.ColumnDef)
	if !ok {
		return false
	}
	return filterColumnOptions(col, func(opt *ast.ColumnOption) bool {
		return opt.Tp == ast.ColumnOptionDefaultValue && isUnsupportedTimeDefault(col.Tp.GetType(), opt.Expr)
	})
}

func isUnsupportedTimeDefault(colType byte, expr ast.ExprNode) bool {
	expr = unwrapParentheses(expr)
	fn, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return false
	}
	switch fn.FnName.L {
	case ast.CurrentTimestamp, ast.Now, ast.LocalTime, ast.LocalTimestamp:
		return colType != mysql.TypeTimestamp && colType != mysql.TypeDatetime
	case ast.CurrentDate:
		return colType != mysql.TypeDate && colType != mysql.TypeDatetime
	case ast.CurrentTime:
		return colType != mysql.TypeDuration
	default:
		return false
	}
}

// jsonValueRule rewrites MariaDB JSON_VALUE in generated column expressions to supported JSON functions.
type jsonValueRule struct{}

func (r jsonValueRule) Apply(node ast.Node) bool {
	col, ok := node.(*ast.ColumnDef)
	if !ok {
		return false
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
	return changed
}

func rewriteJSONValueExpr(expr ast.ExprNode) (ast.ExprNode, bool) {
	visitor := &jsonValueExprRewriteVisitor{}
	node, ok := expr.Accept(visitor)
	if !ok {
		return expr, false
	}
	newExpr, ok := node.(ast.ExprNode)
	if !ok {
		return expr, false
	}
	return newExpr, visitor.changed
}

type jsonValueExprRewriteVisitor struct {
	changed bool
}

func (v *jsonValueExprRewriteVisitor) Enter(node ast.Node) (ast.Node, bool) {
	return node, false
}

func (v *jsonValueExprRewriteVisitor) Leave(node ast.Node) (ast.Node, bool) {
	fn, ok := node.(*ast.FuncCallExpr)
	if !ok || fn.FnName.L != "json_value" || len(fn.Args) != 2 {
		return node, true
	}
	v.changed = true
	jsonExtract := &ast.FuncCallExpr{
		FnName: ast.NewCIStr(ast.JSONExtract),
		Args:   fn.Args,
	}
	return &ast.FuncCallExpr{
		FnName: ast.NewCIStr(ast.JSONUnquote),
		Args:   []ast.ExprNode{jsonExtract},
	}, true
}

func isTextBlobOrJSON(ft *types.FieldType) bool {
	return types.IsTypeBlob(ft.GetType()) || ft.GetType() == mysql.TypeJSON
}
