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

// VersionMacrosRule unwraps MariaDB/MySQL versioned comments before parsing.
type VersionMacrosRule struct{}

func (r *VersionMacrosRule) Name() string { return "VersionMacros" }

func (r *VersionMacrosRule) Description() string {
	return "Unwrap version-specific comment macros"
}

func (r *VersionMacrosRule) Priority() int { return 130 }

func (r *VersionMacrosRule) ShouldApply(_ ast.Node) bool {
	return false
}

func (r *VersionMacrosRule) Apply(node ast.Node) (ast.Node, error) {
	return node, nil
}
