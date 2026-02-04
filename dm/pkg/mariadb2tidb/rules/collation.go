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
	"path/filepath"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tiflow/dm/pkg/mariadb2tidb/config"
)

// CollationRule transforms collation and charset specifications based on configuration
type CollationRule struct {
	charsetMap   map[string]config.CharsetMapping
	collationMap map[string]string
}

// NewCollationRule creates a CollationRule with the provided configuration
func NewCollationRule(cfg *config.Config) *CollationRule {
	cr := &CollationRule{
		charsetMap:   make(map[string]config.CharsetMapping),
		collationMap: make(map[string]string),
	}
	if cfg != nil {
		for k, v := range cfg.CharsetMappings {
			cr.charsetMap[strings.ToLower(k)] = v
		}
		for k, v := range cfg.CollationMappings {
			cr.collationMap[strings.ToLower(k)] = v
		}
	}
	return cr
}

// Name returns the unique name of the rule
func (r *CollationRule) Name() string {
	return "Collation"
}

// Description returns a human-readable description of what the rule does
func (r *CollationRule) Description() string {
	return "Transform collation specifications for TiDB compatibility: utf8mb4_unicode_* and latin1_swedish_ci → utf8mb4_0900_ai_ci"
}

// Priority returns the priority of the rule (lower number = higher priority)
func (r *CollationRule) Priority() int {
	return 100 // Highest priority - collations affect many other transformations
}

// ShouldApply checks if the rule should be applied to the given AST node
func (r *CollationRule) ShouldApply(node ast.Node) bool {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		// Check table options for charset/collate
		if r.hasTargetCollation(n.Options) {
			return true
		}
		// Check column definitions for collations
		for _, col := range n.Cols {
			if r.hasTargetCollationInColumn(col) {
				return true
			}
		}
		return false
	case *ast.CreateDatabaseStmt:
		if r.hasTargetDatabaseCollation(n.Options) {
			return true
		}
		return false
	case *ast.ColumnDef:
		return r.hasTargetCollationInColumn(n)
	default:
		return false
	}
}

// Apply applies the transformation to the AST node and returns the modified node
func (r *CollationRule) Apply(node ast.Node) (ast.Node, error) {
	switch n := node.(type) {
	case *ast.CreateTableStmt:
		return r.transformCreateTable(n), nil
	case *ast.CreateDatabaseStmt:
		return r.transformCreateDatabase(n), nil
	case *ast.ColumnDef:
		return r.transformColumnDef(n), nil
	default:
		return node, nil
	}
}

// hasTargetCollation checks if table options contain target collations
func (r *CollationRule) hasTargetCollation(options []*ast.TableOption) bool {
	for _, opt := range options {
		if opt.Tp == ast.TableOptionCharset {
			if _, ok := r.mapCharset(opt.StrValue); ok {
				return true
			}
		}
		if opt.Tp == ast.TableOptionCollate {
			if r.isTargetCollation(opt.StrValue) {
				return true
			}
		}
	}
	return false
}

// hasTargetDatabaseCollation checks if database options contain target collations
func (r *CollationRule) hasTargetDatabaseCollation(options []*ast.DatabaseOption) bool {
	for _, opt := range options {
		if opt.Tp == ast.DatabaseOptionCharset {
			if _, ok := r.mapCharset(opt.Value); ok {
				return true
			}
		}
		if opt.Tp == ast.DatabaseOptionCollate {
			if r.isTargetCollation(opt.Value) {
				return true
			}
		}
	}
	return false
}

// hasTargetCollationInColumn checks if column definition contains target collations
func (r *CollationRule) hasTargetCollationInColumn(col *ast.ColumnDef) bool {
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionCollate {
			if r.isTargetCollation(opt.StrValue) {
				return true
			}
		}
	}
	// Check field type options - note: TiDB parser may not expose charset/collate directly in FieldType
	// We primarily rely on column options for collation detection
	return false
}

// isTargetCollation checks if a collation should be transformed
func (r *CollationRule) isTargetCollation(collation string) bool {
	_, ok := r.mapCollation(collation)
	return ok
}

func (r *CollationRule) mapCharset(charset string) (config.CharsetMapping, bool) {
	m, ok := r.charsetMap[strings.ToLower(charset)]
	return m, ok
}

func (r *CollationRule) mapCollation(collation string) (string, bool) {
	c := strings.ToLower(collation)
	if v, ok := r.collationMap[c]; ok {
		return v, true
	}
	for pattern, v := range r.collationMap {
		if strings.Contains(pattern, "*") {
			if ok, _ := filepath.Match(pattern, c); ok {
				return v, true
			}
		}
	}
	return "", false
}

// transformCreateTable applies collation transformations to CREATE TABLE statement
func (r *CollationRule) transformCreateTable(stmt *ast.CreateTableStmt) *ast.CreateTableStmt {
	// Transform table-level options
	for _, opt := range stmt.Options {
		if opt.Tp == ast.TableOptionCharset {
			if mapping, ok := r.mapCharset(opt.StrValue); ok {
				opt.StrValue = mapping.TargetCharset
				if mapping.TargetCollation != "" {
					r.ensureTableCollateOption(stmt, mapping.TargetCollation)
				}
			}
		}
		if opt.Tp == ast.TableOptionCollate {
			if newColl, ok := r.mapCollation(opt.StrValue); ok {
				opt.StrValue = newColl
			}
		}
	}

	// Transform column-level collations
	for _, col := range stmt.Cols {
		r.transformColumnDef(col)
	}

	return stmt
}

// transformCreateDatabase applies collation transformations to CREATE DATABASE statement
func (r *CollationRule) transformCreateDatabase(stmt *ast.CreateDatabaseStmt) *ast.CreateDatabaseStmt {
	for _, opt := range stmt.Options {
		if opt.Tp == ast.DatabaseOptionCharset {
			if mapping, ok := r.mapCharset(opt.Value); ok {
				opt.Value = mapping.TargetCharset
				if mapping.TargetCollation != "" {
					r.ensureDatabaseCollateOption(stmt, mapping.TargetCollation)
				}
			}
		}
		if opt.Tp == ast.DatabaseOptionCollate {
			if newColl, ok := r.mapCollation(opt.Value); ok {
				opt.Value = newColl
			}
		}
	}
	return stmt
}

// transformColumnDef applies collation transformations to column definition
func (r *CollationRule) transformColumnDef(col *ast.ColumnDef) *ast.ColumnDef {
	// Transform column options
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionCollate {
			if newColl, ok := r.mapCollation(opt.StrValue); ok {
				opt.StrValue = newColl
			}
		}
	}

	// Transform field type options - note: TiDB parser FieldType charset/collate may not be directly accessible
	// Collation transformation is primarily handled through column options

	return col
}

// ensureTableCollateOption adds COLLATE option if not present, or ensures it's set correctly
func (r *CollationRule) ensureTableCollateOption(stmt *ast.CreateTableStmt, collation string) {
	// Check if COLLATE option already exists
	for _, opt := range stmt.Options {
		if opt.Tp == ast.TableOptionCollate {
			opt.StrValue = collation
			return
		}
	}

	// Add new COLLATE option
	collateOpt := &ast.TableOption{
		Tp:       ast.TableOptionCollate,
		StrValue: collation,
	}
	stmt.Options = append(stmt.Options, collateOpt)
}

// ensureDatabaseCollateOption adds COLLATE option if not present, or ensures it's set correctly
func (r *CollationRule) ensureDatabaseCollateOption(stmt *ast.CreateDatabaseStmt, collation string) {
	for _, opt := range stmt.Options {
		if opt.Tp == ast.DatabaseOptionCollate {
			opt.Value = collation
			return
		}
	}

	collateOpt := &ast.DatabaseOption{
		Tp:    ast.DatabaseOptionCollate,
		Value: collation,
	}
	stmt.Options = append(stmt.Options, collateOpt)
}
