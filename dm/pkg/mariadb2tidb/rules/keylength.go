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
)

const maxVarcharLen = 768

// KeyLengthRule truncates oversized VARCHAR lengths to 768 characters
// to satisfy TiDB's key length limits.
type KeyLengthRule struct{}

// Name returns rule name
func (r *KeyLengthRule) Name() string { return "KeyLength" }

// Description returns description
func (r *KeyLengthRule) Description() string {
	return "Cap VARCHAR length at 768 for TiDB key limit"
}

// Priority determines rule order
func (r *KeyLengthRule) Priority() int { return 300 }

// ShouldApply checks if column is VARCHAR longer than allowed
func (r *KeyLengthRule) ShouldApply(node ast.Node) bool {
	col, ok := node.(*ast.ColumnDef)
	if !ok {
		return false
	}
	return col.Tp.GetType() == mysql.TypeVarchar && col.Tp.GetFlen() > maxVarcharLen
}

// Apply truncates VARCHAR length to maxVarcharLen
func (r *KeyLengthRule) Apply(node ast.Node) (ast.Node, error) {
	col := node.(*ast.ColumnDef)
	if col.Tp.GetType() == mysql.TypeVarchar && col.Tp.GetFlen() > maxVarcharLen {
		col.Tp.SetFlen(maxVarcharLen)
	}
	return col, nil
}
