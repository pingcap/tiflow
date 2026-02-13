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

// JsonCheckRule removes CHECK constraints that use JSON_VALID()
// Handles both column-level and table-level check constraints
// Reference: Step 5 in legacy universal_tidb_transform.sh
type JSONCheckRule struct{}

func (r *JSONCheckRule) Name() string { return "JsonCheck" }
func (r *JSONCheckRule) Description() string {
	return "Remove JSON_VALID check constraints"
}
func (r *JSONCheckRule) Priority() int { return 500 }

// ShouldApply determines if node contains JSON_VALID check
func (r *JSONCheckRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.ColumnDef:
		for _, opt := range n.Options {
			if opt.Tp == ast.ColumnOptionCheck && isJSONValidExpr(opt.Expr) {
				return true
			}
		}
	case *ast.CreateTableStmt:
		for _, c := range n.Constraints {
			if c.Tp == ast.ConstraintCheck && isJSONValidExpr(c.Expr) {
				return true
			}
		}
	}
	return false
}

// Apply removes JSON_VALID check constraints
func (r *JSONCheckRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.ColumnDef:
		opts := n.Options[:0]
		for _, opt := range n.Options {
			if opt.Tp == ast.ColumnOptionCheck && isJSONValidExpr(opt.Expr) {
				continue
			}
			opts = append(opts, opt)
		}
		n.Options = opts
		return n, nil
	case *ast.CreateTableStmt:
		constraints := n.Constraints[:0]
		for _, c := range n.Constraints {
			if c.Tp == ast.ConstraintCheck && isJSONValidExpr(c.Expr) {
				continue
			}
			constraints = append(constraints, c)
		}
		n.Constraints = constraints
		return n, nil
	}
	return node, nil
}

// isJSONValidExpr checks if expression is JSON_VALID function call
func isJSONValidExpr(expr ast.ExprNode) bool {
	fc, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return false
	}
	return strings.EqualFold(fc.FnName.O, "json_valid")
}
