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

import (
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/collate"
)

// CollationFallbackRule removes unsupported collations to fall back to defaults.
type CollationFallbackRule struct{}

func (r *CollationFallbackRule) Name() string { return "CollationFallback" }

func (r *CollationFallbackRule) Description() string {
	return "Drop unsupported collations to use TiDB defaults"
}

func (r *CollationFallbackRule) Priority() int { return 270 }

func (r *CollationFallbackRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		return hasUnsupportedTableCollation(n.Options) || hasUnsupportedColumnCollation(n.Cols)
	case *ast.CreateDatabaseStmt:
		return hasUnsupportedDatabaseCollation(n.Options)
	case *ast.ColumnDef:
		return hasUnsupportedColumnCollation([]*ast.ColumnDef{n})
	}
	return false
}

func (r *CollationFallbackRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		n.Options = filterUnsupportedTableCollation(n.Options)
		for _, col := range n.Cols {
			filterColumnCollation(col)
		}
		return n, nil
	case *ast.CreateDatabaseStmt:
		n.Options = filterUnsupportedDatabaseCollation(n.Options)
		return n, nil
	case *ast.ColumnDef:
		filterColumnCollation(n)
		return n, nil
	}
	return node, nil
}

func hasUnsupportedTableCollation(options []*ast.TableOption) bool {
	for _, opt := range options {
		if opt.Tp == ast.TableOptionCollate && !isSupportedCollation(opt.StrValue) {
			return true
		}
	}
	return false
}

func hasUnsupportedDatabaseCollation(options []*ast.DatabaseOption) bool {
	for _, opt := range options {
		if opt.Tp == ast.DatabaseOptionCollate && !isSupportedCollation(opt.Value) {
			return true
		}
	}
	return false
}

func hasUnsupportedColumnCollation(cols []*ast.ColumnDef) bool {
	for _, col := range cols {
		for _, opt := range col.Options {
			if opt.Tp == ast.ColumnOptionCollate && !isSupportedCollation(opt.StrValue) {
				return true
			}
		}
	}
	return false
}

func filterUnsupportedTableCollation(options []*ast.TableOption) []*ast.TableOption {
	filtered := options[:0]
	for _, opt := range options {
		if opt.Tp == ast.TableOptionCollate && !isSupportedCollation(opt.StrValue) {
			continue
		}
		filtered = append(filtered, opt)
	}
	return filtered
}

func filterUnsupportedDatabaseCollation(options []*ast.DatabaseOption) []*ast.DatabaseOption {
	filtered := options[:0]
	for _, opt := range options {
		if opt.Tp == ast.DatabaseOptionCollate && !isSupportedCollation(opt.Value) {
			continue
		}
		filtered = append(filtered, opt)
	}
	return filtered
}

func filterColumnCollation(col *ast.ColumnDef) {
	opts := col.Options[:0]
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionCollate && !isSupportedCollation(opt.StrValue) {
			continue
		}
		opts = append(opts, opt)
	}
	col.Options = opts
}

var (
	supportedCollationsOnce sync.Once
	supportedCollations     map[string]struct{}
)

func isSupportedCollation(name string) bool {
	if name == "" {
		return true
	}
	name = strings.ToLower(name)
	if name == "default" {
		return true
	}
	supportedCollationsOnce.Do(func() {
		supportedCollations = make(map[string]struct{})
		for _, coll := range collate.GetSupportedCollations() {
			supportedCollations[strings.ToLower(coll.Name)] = struct{}{}
		}
	})
	_, ok := supportedCollations[name]
	return ok
}
