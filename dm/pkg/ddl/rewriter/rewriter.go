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

type rewriteOptions struct {
	rules []rule
}

// Option configures the AST rules used by RewriteStmt.
type Option interface {
	apply(*rewriteOptions)
}

type optionFunc func(*rewriteOptions)

func (f optionFunc) apply(options *rewriteOptions) {
	f(options)
}

// WithMariaDBCompatibility enables MariaDB compatibility AST rewrite rules.
func WithMariaDBCompatibility() Option {
	return optionFunc(func(options *rewriteOptions) {
		options.rules = append(options.rules, mariaDBCompatibilityRules...)
	})
}

// RewriteStmt applies enabled rules to stmt in place.
// It is a best-effort compatibility layer for parsed AST nodes; parser failures and
// DDL failures that can be handled by downstream session settings, such as SQL mode,
// are intentionally left to the normal DM flow.
func RewriteStmt(stmt ast.StmtNode, opts ...Option) (bool, error) {
	options := rewriteOptions{}
	for _, opt := range opts {
		opt.apply(&options)
	}
	if stmt == nil || len(options.rules) == 0 {
		return false, nil
	}
	visitor := &rewriteVisitor{rules: options.rules}
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
