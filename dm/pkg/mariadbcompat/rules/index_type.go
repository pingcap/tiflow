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

// IndexTypeRule drops unsupported index type qualifiers.
type IndexTypeRule struct{}

func (r *IndexTypeRule) Name() string { return "IndexType" }

func (r *IndexTypeRule) Description() string {
	return "Remove unsupported index type qualifiers"
}

func (r *IndexTypeRule) Priority() int { return 160 }

func (r *IndexTypeRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		return hasUnsupportedIndexTypeInConstraints(n.Constraints)
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			if hasUnsupportedIndexTypeInConstraint(spec.Constraint) {
				return true
			}
			if hasUnsupportedIndexTypeInConstraints(spec.NewConstraints) {
				return true
			}
		}
	case *ast.CreateIndexStmt:
		return hasUnsupportedIndexTypeOption(n.IndexOption)
	}
	return false
}

func (r *IndexTypeRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		stripUnsupportedIndexTypes(n.Constraints)
		return n, nil
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			if spec.Constraint != nil {
				stripUnsupportedIndexType(spec.Constraint)
			}
			for _, constraint := range spec.NewConstraints {
				stripUnsupportedIndexType(constraint)
			}
		}
		return n, nil
	case *ast.CreateIndexStmt:
		stripUnsupportedIndexTypeOption(n.IndexOption)
		return n, nil
	}
	return node, nil
}

func hasUnsupportedIndexTypeInConstraints(constraints []*ast.Constraint) bool {
	for _, constraint := range constraints {
		if hasUnsupportedIndexTypeInConstraint(constraint) {
			return true
		}
	}
	return false
}

func hasUnsupportedIndexTypeInConstraint(constraint *ast.Constraint) bool {
	if constraint == nil {
		return false
	}
	return hasUnsupportedIndexTypeOption(constraint.Option)
}

func hasUnsupportedIndexTypeOption(option *ast.IndexOption) bool {
	if option == nil {
		return false
	}
	return option.Tp != ast.IndexTypeInvalid && option.Tp != ast.IndexTypeBtree
}

func stripUnsupportedIndexTypes(constraints []*ast.Constraint) {
	for _, constraint := range constraints {
		stripUnsupportedIndexType(constraint)
	}
}

func stripUnsupportedIndexType(constraint *ast.Constraint) {
	if constraint == nil {
		return
	}
	stripUnsupportedIndexTypeOption(constraint.Option)
}

func stripUnsupportedIndexTypeOption(option *ast.IndexOption) {
	if !hasUnsupportedIndexTypeOption(option) {
		return
	}
	option.Tp = ast.IndexTypeInvalid
}
