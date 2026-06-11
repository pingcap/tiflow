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

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	tidbtypes "github.com/pingcap/tidb/pkg/types"
)

const maxVarcharLen = 768

var defaultRules = []Rule{
	collationRule{},
	zeroTimestampRule{},
	keyLengthRule{},
	indexPrefixRule{},
	integerWidthRule{},
	textBlobDefaultRule{},
	jsonCheckRule{},
	functionDefaultRule{},
	jsonGeneratedRule{},
}

type collationRule struct{}

func (r collationRule) Name() string { return "collation" }

func (r collationRule) Apply(node ast.Node) (bool, error) {
	switch n := node.(type) {
	case *ast.CreateDatabaseStmt:
		return rewriteDatabaseOptions(n.Options), nil
	case *ast.CreateTableStmt:
		return rewriteTableOptions(&n.Options), nil
	case *ast.ColumnDef:
		return rewriteColumnCollations(n), nil
	default:
		return false, nil
	}
}

type zeroTimestampRule struct{}

func (r zeroTimestampRule) Name() string { return "zero-timestamp" }

func (r zeroTimestampRule) Apply(node ast.Node) (bool, error) {
	col, ok := node.(*ast.ColumnDef)
	if !ok || !isTimeType(col.Tp.GetType()) {
		return false, nil
	}
	return filterColumnOptions(col, func(opt *ast.ColumnOption) bool {
		return opt.Tp == ast.ColumnOptionDefaultValue && isZeroTimeDefault(opt.Expr)
	}), nil
}

type keyLengthRule struct{}

func (r keyLengthRule) Name() string { return "key-length" }

func (r keyLengthRule) Apply(node ast.Node) (bool, error) {
	col, ok := node.(*ast.ColumnDef)
	if !ok || col.Tp.GetFlen() <= maxVarcharLen {
		return false, nil
	}
	switch col.Tp.GetType() {
	case mysql.TypeVarchar, mysql.TypeVarString:
		col.Tp.SetFlen(maxVarcharLen)
		return true, nil
	default:
		return false, nil
	}
}

type indexPrefixRule struct{}

func (r indexPrefixRule) Name() string { return "index-prefix" }

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
		case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq:
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
			case isTextOrBlob(col.Tp):
				key.Length = 255
				changed = true
			case isVarcharOrChar(col.Tp) && col.Tp.GetFlen() > 0:
				key.Length = col.Tp.GetFlen()
				changed = true
			}
		}
	}
	return changed, nil
}

type integerWidthRule struct{}

func (r integerWidthRule) Name() string { return "integer-width" }

func (r integerWidthRule) Apply(node ast.Node) (bool, error) {
	col, ok := node.(*ast.ColumnDef)
	if !ok || !mysql.IsIntegerType(col.Tp.GetType()) {
		return false, nil
	}
	if col.Tp.GetFlen() == types.UnspecifiedLength || col.Tp.GetFlen() <= 0 {
		return false, nil
	}
	col.Tp.SetFlen(types.UnspecifiedLength)
	return true, nil
}

type textBlobDefaultRule struct{}

func (r textBlobDefaultRule) Name() string { return "text-blob-default" }

func (r textBlobDefaultRule) Apply(node ast.Node) (bool, error) {
	col, ok := node.(*ast.ColumnDef)
	if !ok || !isTextBlobOrJSON(col.Tp) {
		return false, nil
	}
	return filterColumnOptions(col, func(opt *ast.ColumnOption) bool {
		return opt.Tp == ast.ColumnOptionDefaultValue
	}), nil
}

type jsonCheckRule struct{}

func (r jsonCheckRule) Name() string { return "json-check" }

func (r jsonCheckRule) Apply(node ast.Node) (bool, error) {
	switch n := node.(type) {
	case *ast.ColumnDef:
		return filterColumnOptions(n, func(opt *ast.ColumnOption) bool {
			return opt.Tp == ast.ColumnOptionCheck && isJSONValidExpr(opt.Expr)
		}), nil
	case *ast.CreateTableStmt:
		constraints := n.Constraints[:0]
		changed := false
		for _, cons := range n.Constraints {
			if cons.Tp == ast.ConstraintCheck && isJSONValidExpr(cons.Expr) {
				changed = true
				continue
			}
			constraints = append(constraints, cons)
		}
		n.Constraints = constraints
		return changed, nil
	default:
		return false, nil
	}
}

type functionDefaultRule struct{}

func (r functionDefaultRule) Name() string { return "function-default" }

func (r functionDefaultRule) Apply(node ast.Node) (bool, error) {
	col, ok := node.(*ast.ColumnDef)
	if !ok {
		return false, nil
	}
	return filterColumnOptions(col, func(opt *ast.ColumnOption) bool {
		return opt.Tp == ast.ColumnOptionDefaultValue && !keepDefaultExpr(col, opt.Expr)
	}), nil
}

type jsonGeneratedRule struct{}

func (r jsonGeneratedRule) Name() string { return "json-generated" }

func (r jsonGeneratedRule) Apply(node ast.Node) (bool, error) {
	col, ok := node.(*ast.ColumnDef)
	if !ok || !isJSONGenerated(col) {
		return false, nil
	}
	return filterColumnOptions(col, func(opt *ast.ColumnOption) bool {
		return opt.Tp == ast.ColumnOptionGenerated
	}), nil
}

func rewriteDatabaseOptions(options []*ast.DatabaseOption) bool {
	changed := false
	for _, opt := range options {
		switch opt.Tp {
		case ast.DatabaseOptionCharset:
			if strings.EqualFold(opt.Value, "latin1") {
				opt.Value = "utf8mb4"
				changed = true
			}
		case ast.DatabaseOptionCollate:
			if collation, ok := mapCollation(opt.Value); ok {
				opt.Value = collation
				changed = true
			}
		}
	}
	return changed
}

func rewriteTableOptions(options *[]*ast.TableOption) bool {
	changed := false
	needCollate := ""
	hasCollate := false
	for _, opt := range *options {
		switch opt.Tp {
		case ast.TableOptionCharset:
			if strings.EqualFold(opt.StrValue, "latin1") {
				opt.StrValue = "utf8mb4"
				needCollate = "utf8mb4_0900_ai_ci"
				changed = true
			}
		case ast.TableOptionCollate:
			hasCollate = true
			if collation, ok := mapCollation(opt.StrValue); ok {
				opt.StrValue = collation
				changed = true
			}
		}
	}
	if needCollate != "" && !hasCollate {
		*options = append(*options, &ast.TableOption{Tp: ast.TableOptionCollate, StrValue: needCollate})
		changed = true
	}
	return changed
}

func rewriteColumnCollations(col *ast.ColumnDef) bool {
	changed := false
	for _, opt := range col.Options {
		if opt.Tp != ast.ColumnOptionCollate {
			continue
		}
		if collation, ok := mapCollation(opt.StrValue); ok {
			opt.StrValue = collation
			changed = true
		}
	}
	return changed
}

func mapCollation(collation string) (string, bool) {
	name := strings.ToLower(collation)
	if name == "latin1_swedish_ci" {
		return "utf8mb4_0900_ai_ci", true
	}
	if strings.HasPrefix(name, "utf8mb4_unicode_") {
		return "utf8mb4_0900_ai_ci", true
	}
	return "", false
}

func filterColumnOptions(col *ast.ColumnDef, drop func(*ast.ColumnOption) bool) bool {
	options := col.Options[:0]
	changed := false
	for _, opt := range col.Options {
		if drop(opt) {
			changed = true
			continue
		}
		options = append(options, opt)
	}
	col.Options = options
	return changed
}

func keepDefaultExpr(col *ast.ColumnDef, expr ast.ExprNode) bool {
	expr = unwrapParentheses(expr)
	if _, ok := expr.(ast.ValueExpr); ok {
		return true
	}
	fn, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return false
	}
	return isTimeType(col.Tp.GetType()) && allowedTimeDefaultFuncs[fn.FnName.L]
}

var allowedTimeDefaultFuncs = map[string]bool{
	"current_timestamp": true,
	"current_date":      true,
	"current_time":      true,
	"now":               true,
	"localtime":         true,
	"localtimestamp":    true,
}

func isJSONValidExpr(expr ast.ExprNode) bool {
	fn, ok := expr.(*ast.FuncCallExpr)
	return ok && strings.EqualFold(fn.FnName.O, "json_valid")
}

func isJSONGenerated(col *ast.ColumnDef) bool {
	for _, opt := range col.Options {
		if opt.Tp != ast.ColumnOptionGenerated {
			continue
		}
		fn, ok := unwrapParentheses(opt.Expr).(*ast.FuncCallExpr)
		if ok && strings.HasPrefix(fn.FnName.L, "json") {
			return true
		}
	}
	return false
}

func unwrapParentheses(expr ast.ExprNode) ast.ExprNode {
	for {
		p, ok := expr.(*ast.ParenthesesExpr)
		if !ok {
			return expr
		}
		expr = p.Expr
	}
}

func isZeroTimeDefault(expr ast.ExprNode) bool {
	valExpr, ok := expr.(ast.ValueExpr)
	if !ok {
		return false
	}
	switch v := valExpr.GetValue().(type) {
	case tidbtypes.Time:
		return v.IsZero() || v.InvalidZero()
	case string:
		return isZeroTimeString(v)
	case []byte:
		return isZeroTimeString(string(v))
	case int:
		return v == 0
	case int64:
		return v == 0
	case uint64:
		return v == 0
	default:
		return false
	}
}

func isZeroTimeString(value string) bool {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "0000-00-00") {
		return false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(value, "0000-00-00"))
	if rest == "" || rest == "00:00:00" {
		return true
	}
	if !strings.HasPrefix(rest, "00:00:00.") {
		return false
	}
	return strings.Trim(rest[len("00:00:00."):], "0") == ""
}

func isTimeType(tp byte) bool {
	switch tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return true
	default:
		return false
	}
}

func isTextBlobOrJSON(ft *types.FieldType) bool {
	return types.IsTypeBlob(ft.GetType()) || ft.GetType() == mysql.TypeJSON
}

func isTextOrBlob(ft *types.FieldType) bool {
	switch ft.GetType() {
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		return true
	default:
		return false
	}
}

func isVarcharOrChar(ft *types.FieldType) bool {
	switch ft.GetType() {
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		return true
	default:
		return false
	}
}
