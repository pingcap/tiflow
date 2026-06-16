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
)

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

func isTimeType(tp byte) bool {
	switch tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return true
	default:
		return false
	}
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
