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

// IndexPrefixRule ensures indexed TEXT/BLOB/VARCHAR columns have prefix lengths
// For TEXT/BLOB columns without a specified length, it adds a 255 character prefix.
// For VARCHAR/CHAR columns, it uses the column length as the prefix length.
type IndexPrefixRule struct{}

// Name returns rule name
func (r *IndexPrefixRule) Name() string { return "IndexPrefix" }

// Description returns rule description
func (r *IndexPrefixRule) Description() string {
	return "Add prefix length to indexed TEXT/BLOB/VARCHAR columns"
}

// Priority determines rule order
func (r *IndexPrefixRule) Priority() int { return 350 }

// ShouldApply checks if table has indexed TEXT/BLOB/VARCHAR columns without prefix length
func (r *IndexPrefixRule) ShouldApply(node ast.Node) bool {
	tbl, ok := node.(*ast.CreateTableStmt)
	if !ok {
		return false
	}
	colMap := buildColumnMap(tbl.Cols)
	for _, cons := range tbl.Constraints {
		switch cons.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq:
			for _, key := range cons.Keys {
				if key.Length > 0 {
					continue
				}
				if col, ok := colMap[key.Column.Name.L]; ok {
					if needsPrefix(col.Tp) {
						return true
					}
				}
			}
		}
	}
	return false
}

// Apply sets prefix lengths for indexed TEXT/BLOB/VARCHAR columns
func (r *IndexPrefixRule) Apply(node ast.Node) (ast.Node, error) {
	tbl := node.(*ast.CreateTableStmt)
	colMap := buildColumnMap(tbl.Cols)
	for _, cons := range tbl.Constraints {
		switch cons.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq:
			for _, key := range cons.Keys {
				if key.Length > 0 {
					continue
				}
				if col, ok := colMap[key.Column.Name.L]; ok {
					if isTextOrBlob(col.Tp) {
						key.Length = 255
					} else if isVarcharOrChar(col.Tp) {
						flen := col.Tp.GetFlen()
						if flen > 0 {
							key.Length = flen
						}
					}
				}
			}
		}
	}
	return tbl, nil
}

func buildColumnMap(cols []*ast.ColumnDef) map[string]*ast.ColumnDef {
	m := make(map[string]*ast.ColumnDef, len(cols))
	for _, col := range cols {
		m[col.Name.Name.L] = col
	}
	return m
}

func needsPrefix(ft *types.FieldType) bool {
	return isTextOrBlob(ft) || isVarcharOrChar(ft)
}

func isTextOrBlob(ft *types.FieldType) bool {
	switch ft.GetType() {
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		return true
	}
	return false
}

func isVarcharOrChar(ft *types.FieldType) bool {
	switch ft.GetType() {
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		return true
	}
	return false
}
