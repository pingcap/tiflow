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

// SpatialIndexDropRule downgrades SPATIAL indexes to normal indexes.
type SpatialIndexDropRule struct{}

func (r *SpatialIndexDropRule) Name() string { return "SpatialIndexDrop" }

func (r *SpatialIndexDropRule) Description() string {
	return "Replace SPATIAL indexes with regular indexes"
}

func (r *SpatialIndexDropRule) Priority() int { return 230 }

func (r *SpatialIndexDropRule) ShouldApply(node ast.Node) bool {
	create, ok := node.(*ast.CreateIndexStmt)
	return ok && create.KeyType == ast.IndexKeyTypeSpatial
}

func (r *SpatialIndexDropRule) Apply(node ast.Node) (ast.Node, error) {
	create := node.(*ast.CreateIndexStmt)
	if create.KeyType == ast.IndexKeyTypeSpatial {
		create.KeyType = ast.IndexKeyTypeNone
	}
	return create, nil
}
