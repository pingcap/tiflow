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

// IgnoredClauseCleanupRule removes clauses that TiDB parses but ignores.
type IgnoredClauseCleanupRule struct{}

func (r *IgnoredClauseCleanupRule) Name() string { return "IgnoredClauseCleanup" }

func (r *IgnoredClauseCleanupRule) Description() string {
	return "Drop ignored clauses like NO_WRITE_TO_BINLOG and COLUMN_FORMAT"
}

func (r *IgnoredClauseCleanupRule) Priority() int { return 260 }

func (r *IgnoredClauseCleanupRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			if spec.NoWriteToBinlog {
				return true
			}
		}
	case *ast.ColumnDef:
		for _, opt := range n.Options {
			if opt.Tp == ast.ColumnOptionColumnFormat || opt.Tp == ast.ColumnOptionStorage {
				return true
			}
		}
	}
	return false
}

func (r *IgnoredClauseCleanupRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			spec.NoWriteToBinlog = false
		}
		return n, nil
	case *ast.ColumnDef:
		opts := n.Options[:0]
		for _, opt := range n.Options {
			if opt.Tp == ast.ColumnOptionColumnFormat || opt.Tp == ast.ColumnOptionStorage {
				continue
			}
			opts = append(opts, opt)
		}
		n.Options = opts
		return n, nil
	}
	return node, nil
}
