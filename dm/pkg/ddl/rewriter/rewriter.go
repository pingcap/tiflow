// Copyright 2026 PingCAP, Inc.
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

package rewriter

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type rule interface {
	Apply(ast.Node) (bool, error)
}

// Rewriter applies a fixed, ordered set of AST rules.
type Rewriter struct {
	rules []rule
}

// NewRewriter creates the default MariaDB compatibility AST rewriter.
func NewRewriter() *Rewriter {
	return &Rewriter{rules: append([]rule(nil), defaultRules...)}
}

// RewriteStmt applies all rules to stmt in place.
func (r *Rewriter) RewriteStmt(stmt ast.StmtNode) (bool, error) {
	if len(r.rules) == 0 || stmt == nil {
		return false, nil
	}
	visitor := &rewriteVisitor{rules: r.rules}
	stmt.Accept(visitor)
	return visitor.changed, visitor.err
}

type rewriteVisitor struct {
	rules   []rule
	changed bool
	err     error
}

func (v *rewriteVisitor) Enter(node ast.Node) (ast.Node, bool) {
	if v.err != nil {
		return node, true
	}
	return node, false
}

func (v *rewriteVisitor) Leave(node ast.Node) (ast.Node, bool) {
	if v.err != nil {
		return node, false
	}
	for _, rule := range v.rules {
		changed, err := rule.Apply(node)
		if err != nil {
			v.err = err
			return node, false
		}
		v.changed = v.changed || changed
	}
	return node, true
}
