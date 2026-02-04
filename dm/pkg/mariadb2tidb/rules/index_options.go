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

// IndexOptionsRule removes unsupported index options such as VECTOR indexes.
type IndexOptionsRule struct{}

func (r *IndexOptionsRule) Name() string { return "IndexOptions" }

func (r *IndexOptionsRule) Description() string {
	return "Normalize unsupported index options like VECTOR indexes"
}

func (r *IndexOptionsRule) Priority() int { return 210 }

func (r *IndexOptionsRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		return hasVectorConstraint(n.Constraints)
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			if hasVectorConstraint([]*ast.Constraint{spec.Constraint}) {
				return true
			}
			if hasVectorConstraint(spec.NewConstraints) {
				return true
			}
		}
	case *ast.CreateIndexStmt:
		return n.KeyType == ast.IndexKeyTypeVector
	}
	return false
}

func (r *IndexOptionsRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		normalizeVectorConstraints(n.Constraints)
		return n, nil
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			normalizeVectorConstraints([]*ast.Constraint{spec.Constraint})
			normalizeVectorConstraints(spec.NewConstraints)
		}
		return n, nil
	case *ast.CreateIndexStmt:
		if n.KeyType == ast.IndexKeyTypeVector {
			n.KeyType = ast.IndexKeyTypeNone
		}
		return n, nil
	}
	return node, nil
}

func hasVectorConstraint(constraints []*ast.Constraint) bool {
	for _, constraint := range constraints {
		if constraint != nil && constraint.Tp == ast.ConstraintVector {
			return true
		}
	}
	return false
}

func normalizeVectorConstraints(constraints []*ast.Constraint) {
	for _, constraint := range constraints {
		if constraint != nil && constraint.Tp == ast.ConstraintVector {
			constraint.Tp = ast.ConstraintIndex
		}
	}
}
