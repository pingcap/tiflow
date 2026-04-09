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

// AutoIncrementValuesRule normalizes non-positive AUTO_INCREMENT values.
type AutoIncrementValuesRule struct{}

func (r *AutoIncrementValuesRule) Name() string { return "AutoIncrementValues" }

func (r *AutoIncrementValuesRule) Description() string {
	return "Normalize non-positive AUTO_INCREMENT start values"
}

func (r *AutoIncrementValuesRule) Priority() int { return 140 }

func (r *AutoIncrementValuesRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		return hasInvalidAutoIncrementOption(n.Options)
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			if hasInvalidAutoIncrementOption(spec.Options) {
				return true
			}
		}
	}
	return false
}

func (r *AutoIncrementValuesRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		normalizeAutoIncrementOptions(n.Options)
		return n, nil
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			normalizeAutoIncrementOptions(spec.Options)
		}
		return n, nil
	}
	return node, nil
}

func hasInvalidAutoIncrementOption(options []*ast.TableOption) bool {
	for _, opt := range options {
		if opt.Tp == ast.TableOptionAutoIncrement && opt.UintValue < 1 {
			return true
		}
	}
	return false
}

func normalizeAutoIncrementOptions(options []*ast.TableOption) {
	for _, opt := range options {
		if opt.Tp == ast.TableOptionAutoIncrement && opt.UintValue < 1 {
			opt.UintValue = 1
		}
	}
}
