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

// FunctionDefaultRule removes unsupported function-based default values.
// TiDB allows only a small set of deterministic functions in DEFAULT clauses
// (for example, CURRENT_TIMESTAMP). Any other function call is stripped.
// Reference: Step 6 in legacy universal_tidb_transform.sh
// Other function-based defaults are removed entirely for compatibility.
type FunctionDefaultRule struct{}

// Name returns rule name
func (r *FunctionDefaultRule) Name() string { return "FunctionDefault" }

// Description returns rule description
func (r *FunctionDefaultRule) Description() string {
	return "Remove unsupported function-based default values"
}

// Priority defines rule execution order
func (r *FunctionDefaultRule) Priority() int { return 600 }

// ShouldApply checks if the column has a function-based default that TiDB disallows
func (r *FunctionDefaultRule) ShouldApply(node ast.Node) bool {
	col, ok := node.(*ast.ColumnDef)
	if !ok {
		return false
	}
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionDefaultValue {
			if fc, ok := opt.Expr.(*ast.FuncCallExpr); ok {
				name := strings.ToLower(fc.FnName.O)
				if !allowedDefaultFuncs[name] {
					return true
				}
			}
		}
	}
	return false
}

// Apply removes the default clause if it uses a disallowed function
func (r *FunctionDefaultRule) Apply(node ast.Node) (ast.Node, error) {
	col := node.(*ast.ColumnDef)
	opts := col.Options[:0]
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionDefaultValue {
			if fc, ok := opt.Expr.(*ast.FuncCallExpr); ok {
				name := strings.ToLower(fc.FnName.O)
				if !allowedDefaultFuncs[name] {
					continue
				}
			}
		}
		opts = append(opts, opt)
	}
	col.Options = opts
	return col, nil
}

// allowedDefaultFuncs lists function names permitted in DEFAULT clauses
var allowedDefaultFuncs = map[string]bool{
	"current_timestamp": true,
	"current_date":      true,
	"current_time":      true,
	"now":               true,
}
