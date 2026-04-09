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

// SequenceTypeRule removes MariaDB-only sequence type clauses before parsing.
type SequenceTypeRule struct{}

func (r *SequenceTypeRule) Name() string { return "SequenceType" }

func (r *SequenceTypeRule) Description() string {
	return "Strip unsupported AS <type> clauses from sequences"
}

func (r *SequenceTypeRule) Priority() int { return 190 }

func (r *SequenceTypeRule) ShouldApply(node ast.Node) bool {
	raw, ok := node.(*rawStatement)
	if !ok {
		return false
	}
	return hasSequenceType(raw.Text())
}

func (r *SequenceTypeRule) Apply(node ast.Node) (ast.Node, error) {
	raw, ok := node.(*rawStatement)
	if !ok {
		return node, nil
	}
	updated := stripSequenceType(raw.Text())
	if updated == raw.Text() {
		return raw, nil
	}
	return newRawStatement(updated), nil
}
