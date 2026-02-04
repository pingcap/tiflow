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

// ConstraintsRule strips unsupported foreign key clauses for TiDB compatibility.
type ConstraintsRule struct{}

func (r *ConstraintsRule) Name() string { return "Constraints" }

func (r *ConstraintsRule) Description() string {
	return "Strip unsupported foreign key clauses such as MATCH and SET DEFAULT"
}

func (r *ConstraintsRule) Priority() int { return 100 }

func (r *ConstraintsRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		for _, c := range n.Constraints {
			if needsReferenceCleanup(c) {
				return true
			}
		}
		for _, col := range n.Cols {
			if columnHasUnsupportedReference(col) {
				return true
			}
		}
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			if spec.Constraint != nil && needsReferenceCleanup(spec.Constraint) {
				return true
			}
			for _, constraint := range spec.NewConstraints {
				if needsReferenceCleanup(constraint) {
					return true
				}
			}
			for _, col := range spec.NewColumns {
				if columnHasUnsupportedReference(col) {
					return true
				}
			}
		}
	case *ast.ColumnDef:
		return columnHasUnsupportedReference(n)
	}
	return false
}

func (r *ConstraintsRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		for _, c := range n.Constraints {
			cleanupReferenceConstraint(c)
		}
		for _, col := range n.Cols {
			cleanupColumnReferences(col)
		}
		return n, nil
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			if spec.Constraint != nil {
				cleanupReferenceConstraint(spec.Constraint)
			}
			for _, constraint := range spec.NewConstraints {
				cleanupReferenceConstraint(constraint)
			}
			for _, col := range spec.NewColumns {
				cleanupColumnReferences(col)
			}
		}
		return n, nil
	case *ast.ColumnDef:
		cleanupColumnReferences(n)
		return n, nil
	}
	return node, nil
}

func needsReferenceCleanup(constraint *ast.Constraint) bool {
	if constraint == nil || constraint.Tp != ast.ConstraintForeignKey {
		return false
	}
	return referenceNeedsCleanup(constraint.Refer)
}

func columnHasUnsupportedReference(col *ast.ColumnDef) bool {
	if col == nil {
		return false
	}
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionReference && referenceNeedsCleanup(opt.Refer) {
			return true
		}
	}
	return false
}

func cleanupReferenceConstraint(constraint *ast.Constraint) {
	if constraint == nil || constraint.Tp != ast.ConstraintForeignKey {
		return
	}
	cleanupReferenceDef(constraint.Refer)
}

func cleanupColumnReferences(col *ast.ColumnDef) {
	if col == nil {
		return
	}
	for _, opt := range col.Options {
		if opt.Tp != ast.ColumnOptionReference {
			continue
		}
		cleanupReferenceDef(opt.Refer)
	}
}

func referenceNeedsCleanup(ref *ast.ReferenceDef) bool {
	if ref == nil {
		return false
	}
	if ref.Match != ast.MatchNone {
		return true
	}
	if ref.OnDelete != nil && ref.OnDelete.ReferOpt == ast.ReferOptionSetDefault {
		return true
	}
	if ref.OnUpdate != nil && ref.OnUpdate.ReferOpt == ast.ReferOptionSetDefault {
		return true
	}
	return false
}

func cleanupReferenceDef(ref *ast.ReferenceDef) {
	if ref == nil {
		return
	}
	if ref.Match != ast.MatchNone {
		ref.Match = ast.MatchNone
	}
	if ref.OnDelete != nil && ref.OnDelete.ReferOpt == ast.ReferOptionSetDefault {
		ref.OnDelete.ReferOpt = ast.ReferOptionNoOption
	}
	if ref.OnUpdate != nil && ref.OnUpdate.ReferOpt == ast.ReferOptionSetDefault {
		ref.OnUpdate.ReferOpt = ast.ReferOptionNoOption
	}
}
