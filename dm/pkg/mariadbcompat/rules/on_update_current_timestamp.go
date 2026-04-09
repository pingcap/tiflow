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

// OnUpdateCurrentTimestampRule adds DEFAULT when ON UPDATE is specified without one.
type OnUpdateCurrentTimestampRule struct{}

func (r *OnUpdateCurrentTimestampRule) Name() string { return "OnUpdateCurrentTimestamp" }

func (r *OnUpdateCurrentTimestampRule) Description() string {
	return "Add DEFAULT CURRENT_TIMESTAMP when ON UPDATE is specified without one"
}

func (r *OnUpdateCurrentTimestampRule) Priority() int { return 150 }

func (r *OnUpdateCurrentTimestampRule) ShouldApply(node ast.Node) bool {
	col, ok := node.(*ast.ColumnDef)
	if !ok {
		return false
	}
	if !isTimeType(col.Tp.GetType()) {
		return false
	}
	if hasDefaultOption(col.Options) {
		return false
	}
	return findOnUpdateOption(col.Options) != nil
}

func (r *OnUpdateCurrentTimestampRule) Apply(node ast.Node) (ast.Node, error) {
	col := node.(*ast.ColumnDef)
	if !isTimeType(col.Tp.GetType()) {
		return col, nil
	}
	if hasDefaultOption(col.Options) {
		return col, nil
	}
	onUpdate := findOnUpdateOption(col.Options)
	if onUpdate == nil || onUpdate.Expr == nil {
		return col, nil
	}
	col.Options = append(col.Options, &ast.ColumnOption{
		Tp:   ast.ColumnOptionDefaultValue,
		Expr: onUpdate.Expr,
	})
	return col, nil
}

func findOnUpdateOption(options []*ast.ColumnOption) *ast.ColumnOption {
	for _, opt := range options {
		if opt.Tp == ast.ColumnOptionOnUpdate {
			return opt
		}
	}
	return nil
}
