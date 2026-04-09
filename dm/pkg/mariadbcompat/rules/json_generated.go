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

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// JSONGeneratedRule converts generated columns using JSON functions into
// regular columns. TiDB does not allow expressions with JSON functions in
// generated columns. Reference: Step 12 in legacy universal_tidb_transform.sh.
// This rule strips the generated expression (and related options) while
// keeping the column definition intact so that column counts stay consistent
// between source and target databases.
type JSONGeneratedRule struct{}

// Name returns rule name
func (r *JSONGeneratedRule) Name() string { return "JsonGenerated" }

// Description returns description
func (r *JSONGeneratedRule) Description() string {
	return "Convert JSON-based generated columns to regular columns"
}

// Priority defines rule execution order
func (r *JSONGeneratedRule) Priority() int { return 700 }

// ShouldApply checks if any column in the table uses a JSON function in generated expression
func (r *JSONGeneratedRule) ShouldApply(node ast.Node) bool {
	stmt, ok := node.(*ast.CreateTableStmt)
	if !ok {
		return false
	}
	for _, col := range stmt.Cols {
		if isJSONGenerated(col) {
			return true
		}
	}
	return false
}

// Apply converts columns with JSON generated expressions into standard columns
// by removing the generated options while retaining the column and any
// associated constraints.
func (r *JSONGeneratedRule) Apply(node ast.Node) (ast.Node, error) {
	stmt := node.(*ast.CreateTableStmt)
	for _, col := range stmt.Cols {
		if !isJSONGenerated(col) {
			continue
		}
		opts := col.Options[:0]
		for _, opt := range col.Options {
			if opt.Tp == ast.ColumnOptionGenerated {
				continue
			}
			opts = append(opts, opt)
		}
		col.Options = opts
	}
	return stmt, nil
}

// isJSONGenerated checks if column has generated option with JSON function
func isJSONGenerated(col *ast.ColumnDef) bool {
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionGenerated {
			if fc, ok := opt.Expr.(*ast.FuncCallExpr); ok {
				name := strings.ToLower(fc.FnName.O)
				if strings.HasPrefix(name, "json") {
					return true
				}
			}
		}
	}
	return false
}
