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

// SystemVersioningRule removes unsupported system-versioning syntax before parsing.
type SystemVersioningRule struct{}

func (r *SystemVersioningRule) Name() string { return "SystemVersioning" }

func (r *SystemVersioningRule) Description() string {
	return "Strip system-versioning clauses that TiDB cannot parse"
}

func (r *SystemVersioningRule) Priority() int { return 180 }

func (r *SystemVersioningRule) ShouldApply(_ ast.Node) bool {
	return false
}

func (r *SystemVersioningRule) Apply(node ast.Node) (ast.Node, error) {
	return node, nil
}
