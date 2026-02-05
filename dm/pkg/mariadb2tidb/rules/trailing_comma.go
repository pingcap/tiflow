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

// TrailingCommaRule removes trailing commas before parsing.
type TrailingCommaRule struct{}

func (r *TrailingCommaRule) Name() string { return "TrailingComma" }

func (r *TrailingCommaRule) Description() string {
	return "Remove trailing commas in column and index lists"
}

func (r *TrailingCommaRule) Priority() int { return 110 }

func (r *TrailingCommaRule) ShouldApply(node ast.Node) bool {
	raw, ok := node.(*rawStatement)
	if !ok {
		return false
	}
	return hasTrailingCommas(raw.Text())
}

func (r *TrailingCommaRule) Apply(node ast.Node) (ast.Node, error) {
	raw, ok := node.(*rawStatement)
	if !ok {
		return node, nil
	}
	updated, _ := stripTrailingCommas(raw.Text())
	if updated == raw.Text() {
		return raw, nil
	}
	return newRawStatement(updated), nil
}
