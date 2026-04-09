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

// ColumnAttributesRule removes unsupported column attributes before parsing.
type ColumnAttributesRule struct{}

func (r *ColumnAttributesRule) Name() string { return "ColumnAttributes" }

func (r *ColumnAttributesRule) Description() string {
	return "Strip unsupported column attributes such as INVISIBLE and COMPRESSED"
}

func (r *ColumnAttributesRule) Priority() int { return 200 }

func (r *ColumnAttributesRule) ShouldApply(node ast.Node) bool {
	raw, ok := node.(*rawStatement)
	if !ok {
		return false
	}
	return hasColumnAttributes(raw.Text())
}

func (r *ColumnAttributesRule) Apply(node ast.Node) (ast.Node, error) {
	raw, ok := node.(*rawStatement)
	if !ok {
		return node, nil
	}
	updated := stripColumnAttributes(raw.Text())
	if updated == raw.Text() {
		return raw, nil
	}
	return newRawStatement(updated), nil
}
