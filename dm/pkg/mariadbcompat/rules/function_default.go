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

package rules

import "github.com/pingcap/tidb/pkg/parser/ast"

// FunctionDefaultRule strips DEFAULT clauses TiDB cannot accept.
//
// DM never relies on TiDB-side default value computation (every row from the
// MariaDB binlog already carries a materialised value), so the rule keeps the
// minimum set TiDB is guaranteed to accept and discards everything else:
//
//   - literal DEFAULTs are kept on every column type;
//   - CURRENT_TIMESTAMP / NOW / CURRENT_DATE / CURRENT_TIME (and aliases) are
//     kept only on TIMESTAMP / DATETIME / DATE columns, where TiDB accepts them
//     unconditionally;
//   - any other function or expression default is dropped.
type FunctionDefaultRule struct{}

func (r *FunctionDefaultRule) Name() string { return "FunctionDefault" }

func (r *FunctionDefaultRule) Description() string {
	return "Strip DEFAULT expressions TiDB cannot accept; literals and time-function defaults on time columns are kept"
}

func (r *FunctionDefaultRule) Priority() int { return 600 }

func (r *FunctionDefaultRule) ShouldApply(node ast.Node) bool {
	col, ok := node.(*ast.ColumnDef)
	if !ok {
		return false
	}
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionDefaultValue && !keepDefaultExpr(col, opt.Expr) {
			return true
		}
	}
	return false
}

func (r *FunctionDefaultRule) Apply(node ast.Node) (ast.Node, error) {
	col := node.(*ast.ColumnDef)
	opts := col.Options[:0]
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionDefaultValue && !keepDefaultExpr(col, opt.Expr) {
			continue
		}
		opts = append(opts, opt)
	}
	col.Options = opts
	return col, nil
}

// keepDefaultExpr returns true when TiDB is guaranteed to accept the DEFAULT
// expression on the given column.
func keepDefaultExpr(col *ast.ColumnDef, expr ast.ExprNode) bool {
	// Some parser code paths may wrap the expression in parentheses; unwrap
	// defensively so the decision below is purely about the inner node.
	for {
		p, ok := expr.(*ast.ParenthesesExpr)
		if !ok {
			break
		}
		expr = p.Expr
	}
	if _, ok := expr.(ast.ValueExpr); ok {
		return true
	}
	fc, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return false
	}
	return isTimeType(col.Tp.GetType()) && allowedDefaultFuncs[fc.FnName.L]
}

// allowedDefaultFuncs lists time-function names TiDB accepts as DEFAULT on
// TIMESTAMP / DATETIME / DATE columns. MariaDB synonyms (LOCALTIME,
// LOCALTIMESTAMP) are normalised by the parser to one of these names.
var allowedDefaultFuncs = map[string]bool{
	"current_timestamp": true,
	"current_date":      true,
	"current_time":      true,
	"now":               true,
	"localtime":         true,
	"localtimestamp":    true,
}
