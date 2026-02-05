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

// CreateOrReplaceRule rewrites unsupported CREATE OR REPLACE statements before parsing.
type CreateOrReplaceRule struct{}

func (r *CreateOrReplaceRule) Name() string { return "CreateOrReplace" }

func (r *CreateOrReplaceRule) Description() string {
	return "Normalize unsupported CREATE OR REPLACE variants"
}

func (r *CreateOrReplaceRule) Priority() int { return 240 }

func (r *CreateOrReplaceRule) ShouldApply(node ast.Node) bool {
	raw, ok := node.(*rawStatement)
	if !ok {
		return false
	}
	return hasCreateOrReplace(raw.Text())
}

func (r *CreateOrReplaceRule) Apply(node ast.Node) (ast.Node, error) {
	raw, ok := node.(*rawStatement)
	if !ok {
		return node, nil
	}
	updated := rewriteCreateOrReplace(raw.Text())
	if updated == raw.Text() {
		return raw, nil
	}
	return newRawStatement(updated), nil
}
