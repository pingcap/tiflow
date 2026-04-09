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

// EngineOptionsRule strips unsupported engine options.
type EngineOptionsRule struct{}

func (r *EngineOptionsRule) Name() string { return "EngineOptions" }

func (r *EngineOptionsRule) Description() string {
	return "Remove unsupported engine options"
}

func (r *EngineOptionsRule) Priority() int { return 250 }

func (r *EngineOptionsRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		return hasEngineOption(n.Options)
	case *ast.AlterTableStmt:
		for _, spec := range n.Specs {
			if hasEngineOption(spec.Options) {
				return true
			}
		}
	}
	return false
}

func (r *EngineOptionsRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		n.Options = filterEngineOptions(n.Options)
		return n, nil
	case *ast.AlterTableStmt:
		specs := n.Specs[:0]
		for _, spec := range n.Specs {
			spec.Options = filterEngineOptions(spec.Options)
			if len(spec.Options) == 0 && isOptionOnlyAlterSpec(spec) {
				continue
			}
			specs = append(specs, spec)
		}
		n.Specs = specs
		return n, nil
	}
	return node, nil
}

func hasEngineOption(options []*ast.TableOption) bool {
	for _, opt := range options {
		if opt.Tp == ast.TableOptionEngine {
			return true
		}
	}
	return false
}

func filterEngineOptions(options []*ast.TableOption) []*ast.TableOption {
	if len(options) == 0 {
		return options
	}
	filtered := options[:0]
	for _, opt := range options {
		if opt.Tp == ast.TableOptionEngine {
			continue
		}
		filtered = append(filtered, opt)
	}
	return filtered
}
