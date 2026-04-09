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
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// UUIDTypeRule normalizes UUID-related column definitions and index names.
type UUIDTypeRule struct{}

func (r *UUIDTypeRule) Name() string { return "UUIDType" }

func (r *UUIDTypeRule) Description() string {
	return "Normalize UUID columns and UUID index names"
}

func (r *UUIDTypeRule) Priority() int { return 80 }

func (r *UUIDTypeRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.ColumnDef:
		return isUUIDLikeColumn(n)
	case *ast.CreateTableStmt:
		for _, c := range n.Constraints {
			if isUUIDConstraintName(c) {
				return true
			}
		}
	}
	return false
}

func (r *UUIDTypeRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.ColumnDef:
		if isUUIDLikeColumn(n) && !hasDefaultOption(n.Options) {
			n.Options = append(n.Options, &ast.ColumnOption{
				Tp:   ast.ColumnOptionDefaultValue,
				Expr: ast.NewValueExpr("", "", ""),
			})
		}
		return n, nil
	case *ast.CreateTableStmt:
		for _, c := range n.Constraints {
			if isUUIDConstraintName(c) {
				c.Name = "uuid_key"
			}
		}
		return n, nil
	}
	return node, nil
}

func isUUIDLikeColumn(col *ast.ColumnDef) bool {
	if col == nil {
		return false
	}
	if col.Tp.GetType() != mysql.TypeString && col.Tp.GetType() != mysql.TypeVarchar && col.Tp.GetType() != mysql.TypeVarString {
		return false
	}
	if col.Tp.GetFlen() != 36 {
		return false
	}
	if !strings.EqualFold(col.Name.Name.O, "uuid") {
		return false
	}
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionNotNull {
			return true
		}
	}
	return false
}

func isUUIDConstraintName(c *ast.Constraint) bool {
	if c == nil {
		return false
	}
	switch c.Tp {
	case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		return strings.EqualFold(c.Name, "uuid")
	default:
		return false
	}
}

func hasDefaultOption(opts []*ast.ColumnOption) bool {
	for _, opt := range opts {
		if opt.Tp == ast.ColumnOptionDefaultValue {
			return true
		}
	}
	return false
}
