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

// ConstraintsRule drops foreign key constraints that TiDB doesn't support.
type ConstraintsRule struct{}

func (r *ConstraintsRule) Name() string { return "Constraints" }

func (r *ConstraintsRule) Description() string {
	return "Remove foreign key constraints for TiDB compatibility"
}

func (r *ConstraintsRule) Priority() int { return 100 }

func (r *ConstraintsRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		for _, c := range n.Constraints {
			if c.Tp == ast.ConstraintForeignKey {
				return true
			}
		}
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			if spec.Tp == ast.AlterTableAddConstraint && spec.Constraint != nil && spec.Constraint.Tp == ast.ConstraintForeignKey {
				return true
			}
			if spec.Tp == ast.AlterTableDropForeignKey {
				return true
			}
		}
	case *ast.ColumnDef:
		for _, opt := range n.Options {
			if opt.Tp == ast.ColumnOptionReference {
				return true
			}
		}
	}
	return false
}

func (r *ConstraintsRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		constraints := n.Constraints[:0]
		for _, c := range n.Constraints {
			if c.Tp == ast.ConstraintForeignKey {
				continue
			}
			constraints = append(constraints, c)
		}
		n.Constraints = constraints
		return n, nil
	case *ast.AlterTableStmt:
		specs := n.Specs[:0]
		for _, spec := range n.Specs {
			if spec.Tp == ast.AlterTableAddConstraint && spec.Constraint != nil && spec.Constraint.Tp == ast.ConstraintForeignKey {
				continue
			}
			if spec.Tp == ast.AlterTableDropForeignKey {
				continue
			}
			specs = append(specs, spec)
		}
		n.Specs = specs
		return n, nil
	case *ast.ColumnDef:
		opts := n.Options[:0]
		for _, opt := range n.Options {
			if opt.Tp == ast.ColumnOptionReference {
				continue
			}
			opts = append(opts, opt)
		}
		n.Options = opts
		return n, nil
	}
	return node, nil
}
