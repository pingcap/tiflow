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

// TextBlobDefaultRule removes default values from TEXT/BLOB/JSON columns
// TiDB does not allow default values on these types
// Reference: Step 4 in legacy universal_tidb_transform.sh
// This rule also removes DEFAULT value expressions for JSON columns
// but does not handle CHECK constraints (handled by JsonCheckRule)
type TextBlobDefaultRule struct{}

// Name returns rule name
func (r *TextBlobDefaultRule) Name() string { return "TextBlobDefaults" }

// Description returns description
func (r *TextBlobDefaultRule) Description() string {
	return "Remove default values from TEXT/BLOB/JSON columns"
}

// Priority determines rule order
func (r *TextBlobDefaultRule) Priority() int { return 400 }

// ShouldApply checks if node is a ColumnDef with TEXT/BLOB/JSON type and has default value
func (r *TextBlobDefaultRule) ShouldApply(node ast.Node) bool {
	col, ok := node.(*ast.ColumnDef)
	if !ok {
		return false
	}
	if !isTextBlobOrJSON(col.Tp) {
		return false
	}
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionDefaultValue {
			return true
		}
	}
	return false
}

// Apply removes default options from column definition
func (r *TextBlobDefaultRule) Apply(node ast.Node) (ast.Node, error) {
	col := node.(*ast.ColumnDef)
	filtered := make([]*ast.ColumnOption, 0, len(col.Options))
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionDefaultValue {
			continue
		}
		filtered = append(filtered, opt)
	}
	col.Options = filtered
	return col, nil
}

// isTextBlobOrJSON checks if field type is TEXT, BLOB, or JSON
func isTextBlobOrJSON(ft *types.FieldType) bool {
	tp := ft.GetType()
	if types.IsTypeBlob(tp) {
		// TEXT types are represented as blob types with a charset, so this check
		// covers all TEXT/BLOB variants
		return true
	}
	return tp == mysql.TypeJSON
}
