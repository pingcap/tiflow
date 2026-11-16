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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // Initialize parser AST constructors.
)

// RedactSQL produces diagnostic SQL only. It preserves object names and type
// declarations, replacing literal expressions and comments with placeholders.
// Never use its result for execution, checkpointing or schema comparison.
func RedactSQL(sql string) string {
	if !IsRedactLogEnabled() || sql == "" {
		return sql
	}
	// Without the source SQL mode, a backslash followed by a quote can change
	// token boundaries under NO_BACKSLASH_ESCAPES. Do not expose a potentially
	// misclassified literal as an identifier in this ambiguous case.
	if strings.Contains(sql, `\`) {
		return "(SQL redacted: ambiguous backslash escaping)"
	}
	statements, _, err := parser.New().ParseSQL(sql)
	if err != nil || len(statements) == 0 {
		return normalizeSQLDiagnostic(sql)
	}
	var output strings.Builder
	changed := false
	for i, statement := range statements {
		switch statement.(type) {
		case *ast.SelectStmt, *ast.SetOprStmt, *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt,
			*ast.CreateTableStmt, *ast.AlterTableStmt, *ast.DropTableStmt, *ast.CreateIndexStmt,
			*ast.DropIndexStmt, *ast.TruncateTableStmt, *ast.RenameTableStmt,
			*ast.CreateDatabaseStmt, *ast.AlterDatabaseStmt, *ast.DropDatabaseStmt:
		default:
			return normalizeSQLDiagnostic(sql)
		}
		visitor := &sqlLiteralRedactor{}
		redacted, ok := statement.Accept(visitor)
		if !ok || visitor.useNormalizer {
			return normalizeSQLDiagnostic(sql)
		}
		changed = changed || visitor.changed
		if i > 0 {
			output.WriteString("; ")
		}
		if err := redacted.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &output)); err != nil {
			return normalizeSQLDiagnostic(sql)
		}
	}
	// Keep already parameterized SQL and DDL without data literals verbatim.
	// Ordinary comments do not appear in the AST and must still be discarded.
	if !changed && !strings.Contains(sql, "/*") && !strings.Contains(sql, "--") && !strings.Contains(sql, "#") {
		return sql
	}
	return output.String()
}

// RedactSQLs applies the same policy before callers join or truncate statements.
// It does not mutate the input slice, which can still be used for execution.
func RedactSQLs(statements []string) []string {
	if !IsRedactLogEnabled() || len(statements) == 0 {
		return statements
	}
	output := make([]string, len(statements))
	for i, statement := range statements {
		output[i] = RedactSQL(statement)
	}
	return output
}

func normalizeSQLDiagnostic(sql string) string {
	// The lexer-based normalizer also handles unsupported or incomplete syntax.
	// It never falls back to the original SQL after an error. Unlike AST restore,
	// it can collapse lists and normalize numeric type parameters and hints.
	normalized := parser.Normalize(sql, "ON")
	if normalized == "" {
		return "(unparseable SQL; redacted)"
	}
	return "(normalized SQL) " + normalized
}

type sqlLiteralRedactor struct {
	useNormalizer bool
	changed       bool
}

func (v *sqlLiteralRedactor) Enter(node ast.Node) (ast.Node, bool) {
	switch n := node.(type) {
	case ast.ParamMarkerExpr:
		return node, true
	case ast.ValueExpr:
		if n.GetValue() == nil {
			return node, true
		}
		v.changed = true
		return ast.NewParamMarkerExpr(0), true
	case *ast.AlterTableSpec:
		switch n.Tp {
		case ast.AlterTableOption, ast.AlterTableAddColumns, ast.AlterTableAddConstraint,
			ast.AlterTableDropColumn, ast.AlterTableDropPrimaryKey, ast.AlterTableDropIndex,
			ast.AlterTableDropForeignKey, ast.AlterTableModifyColumn, ast.AlterTableChangeColumn,
			ast.AlterTableRenameColumn, ast.AlterTableRenameTable, ast.AlterTableAlterColumn,
			ast.AlterTableRenameIndex:
		default:
			// Other ALTER forms can store literal payloads outside expression nodes.
			v.useNormalizer = true
		}
		if n.Comment != "" {
			n.Comment = "?"
			v.changed = true
		}
	case *ast.CreateDatabaseStmt:
		v.databaseOptions(n.Options)
	case *ast.AlterDatabaseStmt:
		v.databaseOptions(n.Options)
	case *ast.Limit:
		// LIMIT/OFFSET are query controls, not row values.
		return node, true
	case *ast.ColumnDef:
		elements := n.Tp.GetElems()
		if len(elements) > 0 {
			redacted := make([]string, len(elements))
			for i := range redacted {
				redacted[i] = "?"
			}
			n.Tp.SetElems(redacted)
			v.changed = true
		}
	case *ast.ColumnOption:
		if n.Tp != ast.ColumnOptionCollate && n.StrValue != "" {
			n.StrValue = "?"
			v.changed = true
		}
		if n.SecondaryEngineAttr != "" {
			n.SecondaryEngineAttr = "?"
			v.changed = true
		}
	case *ast.TableOption:
		switch n.Tp {
		case ast.TableOptionEngine, ast.TableOptionCharset, ast.TableOptionCollate:
		default:
			if n.StrValue != "" {
				n.StrValue = "?"
				v.changed = true
			}
		}
	case *ast.IndexOption:
		if n.Comment != "" {
			n.Comment = "?"
			v.changed = true
		}
		if n.SecondaryEngineAttr != "" {
			n.SecondaryEngineAttr = "?"
			v.changed = true
		}
	case *ast.TableOptimizerHint:
		// HintData can embed strings outside ValueExpr. The normalizer removes hints.
		v.useNormalizer = true
	case *ast.SelectStmt:
		if n.SelectIntoOpt != nil {
			v.useNormalizer = true
		}
	}
	return node, false
}

func (*sqlLiteralRedactor) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}

// RedactSQLValue handles the string or string-slice forms used by retry logs.
func RedactSQLValue(value any) any {
	if !IsRedactLogEnabled() {
		return value
	}
	switch sql := value.(type) {
	case string:
		return RedactSQL(sql)
	case []string:
		return RedactSQLs(sql)
	default:
		return "(unsupported SQL diagnostic value; redacted)"
	}
}

func (v *sqlLiteralRedactor) databaseOptions(options []*ast.DatabaseOption) {
	for _, option := range options {
		switch option.Tp {
		case ast.DatabaseOptionCharset, ast.DatabaseOptionCollate:
		default:
			v.useNormalizer = true
		}
	}
}
