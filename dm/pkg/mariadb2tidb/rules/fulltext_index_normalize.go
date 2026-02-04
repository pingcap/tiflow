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

// FulltextIndexNormalizeRule normalizes FULLTEXT indexes to TiDB-supported forms.
type FulltextIndexNormalizeRule struct{}

func (r *FulltextIndexNormalizeRule) Name() string { return "FulltextIndexNormalize" }

func (r *FulltextIndexNormalizeRule) Description() string {
	return "Normalize FULLTEXT indexes to single-column form"
}

func (r *FulltextIndexNormalizeRule) Priority() int { return 220 }

func (r *FulltextIndexNormalizeRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		return needsFulltextNormalization(n.Constraints)
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			if needsFulltextNormalization([]*ast.Constraint{spec.Constraint}) {
				return true
			}
			if needsFulltextNormalization(spec.NewConstraints) {
				return true
			}
		}
	case *ast.CreateIndexStmt:
		if n.KeyType == ast.IndexKeyTypeFulltext && needsFulltextPartsNormalization(n.IndexPartSpecifications) {
			return true
		}
		return hasFulltextIndexOption(n.IndexOption)
	}
	return false
}

func (r *FulltextIndexNormalizeRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		normalizeFulltextConstraints(n.Constraints)
		return n, nil
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			normalizeFulltextConstraints([]*ast.Constraint{spec.Constraint})
			normalizeFulltextConstraints(spec.NewConstraints)
		}
		return n, nil
	case *ast.CreateIndexStmt:
		if n.KeyType == ast.IndexKeyTypeFulltext {
			n.IndexPartSpecifications = normalizeFulltextParts(n.IndexPartSpecifications)
			clearFulltextIndexOptions(n.IndexOption)
		}
		return n, nil
	}
	return node, nil
}

func needsFulltextNormalization(constraints []*ast.Constraint) bool {
	for _, constraint := range constraints {
		if constraint != nil && constraint.Tp == ast.ConstraintFulltext {
			if needsFulltextPartsNormalization(constraint.Keys) || hasFulltextIndexOption(constraint.Option) {
				return true
			}
		}
	}
	return false
}

func needsFulltextPartsNormalization(parts []*ast.IndexPartSpecification) bool {
	if len(parts) == 0 {
		return false
	}
	if len(parts) > 1 {
		return true
	}
	return parts[0].Length > 0 || parts[0].Desc
}

func hasFulltextIndexOption(option *ast.IndexOption) bool {
	return option != nil && option.ParserName.O != ""
}

func normalizeFulltextConstraints(constraints []*ast.Constraint) {
	for _, constraint := range constraints {
		if constraint == nil || constraint.Tp != ast.ConstraintFulltext {
			continue
		}
		constraint.Keys = normalizeFulltextParts(constraint.Keys)
		clearFulltextIndexOptions(constraint.Option)
	}
}

func normalizeFulltextParts(parts []*ast.IndexPartSpecification) []*ast.IndexPartSpecification {
	if len(parts) == 0 {
		return parts
	}
	parts = parts[:1]
	parts[0].Length = 0
	parts[0].Desc = false
	return parts
}

func clearFulltextIndexOptions(option *ast.IndexOption) {
	if option == nil {
		return
	}
	option.ParserName = ast.NewCIStr("")
}
