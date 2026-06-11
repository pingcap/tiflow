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
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // register parser driver
)

// Rule rewrites one AST node in place.
type Rule interface {
	Name() string
	Apply(ast.Node) (bool, error)
}

// Rewriter applies a fixed, ordered set of AST rules.
type Rewriter struct {
	rules []Rule
}

// NewRewriter creates a rewriter from explicit rules.
func NewRewriter(rules ...Rule) *Rewriter {
	return &Rewriter{rules: append([]Rule(nil), rules...)}
}

// NewRewriterForFlavor returns a default rewriter only for MariaDB upstreams.
func NewRewriterForFlavor(flavor string) *Rewriter {
	if !strings.EqualFold(flavor, "mariadb") {
		return nil
	}
	return NewRewriter(defaultRules...)
}

// RewriteStmt applies all rules to stmt in place.
func (r *Rewriter) RewriteStmt(stmt ast.StmtNode) (bool, error) {
	if r == nil || len(r.rules) == 0 || stmt == nil {
		return false, nil
	}
	visitor := &rewriteVisitor{rules: r.rules}
	stmt.Accept(visitor)
	return visitor.changed, visitor.err
}

// RewriteSQL parses, rewrites, and restores SQL. It is mainly intended for unit tests
// and small call sites that do not already have a parsed AST.
func (r *Rewriter) RewriteSQL(sql string) (string, bool, error) {
	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	if err != nil {
		return "", false, err
	}

	changed := false
	for _, stmt := range stmts {
		stmtChanged, err := r.RewriteStmt(stmt)
		if err != nil {
			return "", false, err
		}
		changed = changed || stmtChanged
	}
	if !changed {
		return sql, false, nil
	}

	out, err := restoreStatements(stmts)
	if err != nil {
		return "", false, err
	}
	return out, true, nil
}

type rewriteVisitor struct {
	rules   []Rule
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

func restoreStatements(stmts []ast.StmtNode) (string, error) {
	var out strings.Builder
	for i, stmt := range stmts {
		if i > 0 {
			out.WriteString(";\n")
		}
		err := stmt.Restore(&format.RestoreCtx{
			Flags: format.DefaultRestoreFlags | format.RestoreTiDBSpecialComment | format.RestoreStringWithoutDefaultCharset,
			In:    &out,
		})
		if err != nil {
			return "", err
		}
	}
	return out.String(), nil
}
