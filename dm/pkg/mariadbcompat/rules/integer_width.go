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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// IntegerWidthRule removes integer display width specifications (e.g. INT(11) -> INT).
type IntegerWidthRule struct{}

func (r *IntegerWidthRule) Name() string { return "IntegerWidth" }

func (r *IntegerWidthRule) Description() string {
	return "Remove integer display width specifications"
}

func (r *IntegerWidthRule) Priority() int { return 400 }

func (r *IntegerWidthRule) ShouldApply(node ast.Node) bool {
	col, ok := node.(*ast.ColumnDef)
	if !ok {
		return false
	}
	if !mysql.IsIntegerType(col.Tp.GetType()) {
		return false
	}
	return col.Tp.GetFlen() != types.UnspecifiedLength && col.Tp.GetFlen() > 0
}

func (r *IntegerWidthRule) Apply(node ast.Node) (ast.Node, error) {
	col := node.(*ast.ColumnDef)
	if mysql.IsIntegerType(col.Tp.GetType()) {
		col.Tp.SetFlen(types.UnspecifiedLength)
	}
	return col, nil
}
