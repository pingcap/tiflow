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

// QualifiedNamesRule drops schema/table qualifiers from identifiers.
type QualifiedNamesRule struct{}

func (r *QualifiedNamesRule) Name() string { return "QualifiedNames" }

func (r *QualifiedNamesRule) Description() string {
	return "Normalize qualified table and column names"
}

func (r *QualifiedNamesRule) Priority() int { return 170 }

func (r *QualifiedNamesRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.TableName:
		return n.Schema.O != ""
	case *ast.ColumnName:
		return n.Schema.O != "" || n.Table.O != ""
	}
	return false
}

func (r *QualifiedNamesRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.TableName:
		if n.Schema.O != "" {
			n.Schema = ast.NewCIStr("")
		}
		return n, nil
	case *ast.ColumnName:
		if n.Schema.O != "" {
			n.Schema = ast.NewCIStr("")
		}
		if n.Table.O != "" {
			n.Table = ast.NewCIStr("")
		}
		return n, nil
	}
	return node, nil
}
