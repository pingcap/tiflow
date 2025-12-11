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

package dispatcher

import (
	"fmt"
	"regexp"

	"github.com/pingcap/tiflow/pkg/config"
)

const (
	// SchemaPlaceholder is the placeholder string for schema names in routing expressions.
	// When used in a SchemaRule or TableRule, it will be replaced with the source schema name.
	SchemaPlaceholder = "{schema}"
	// TablePlaceholder is the placeholder string for table names in routing expressions.
	// When used in a SchemaRule or TableRule, it will be replaced with the source table name.
	TablePlaceholder = "{table}"
)

var (
	// schemaRE is used to match substring '{schema}' in expressions
	schemaRE = regexp.MustCompile(`\{schema\}`)
	// tableRE is used to match substring '{table}' in expressions
	tableRE = regexp.MustCompile(`\{table\}`)
)

// ValidateExpression validates a schema or table routing expression.
// This is a convenience wrapper around config.ValidateRoutingExpression.
func ValidateExpression(expr string) error {
	return config.ValidateRoutingExpression(expr)
}

// Dispatcher is an abstraction for routing schema and table names.
// It determines the target schema and table name for a given source schema/table.
type Dispatcher interface {
	fmt.Stringer
	// Substitute returns the target schema and table names for the given source schema/table.
	Substitute(sourceSchema, sourceTable string) (targetSchema, targetTable string)
}

// DynamicSchemaDispatcher is a dispatcher which routes schema and table names
// based on configured expressions.
type DynamicSchemaDispatcher struct {
	schemaExpr string
	tableExpr  string
}

// NewDynamicSchemaDispatcher creates a DynamicSchemaDispatcher.
// If schemaExpr is empty, source schema is used.
// If tableExpr is empty, source table is used.
func NewDynamicSchemaDispatcher(schemaExpr, tableExpr string) *DynamicSchemaDispatcher {
	return &DynamicSchemaDispatcher{
		schemaExpr: schemaExpr,
		tableExpr:  tableExpr,
	}
}

// Substitute converts source schema/table names using the configured expressions.
// Supports placeholders: {schema} and {table}
// Example: schemaExpr="{schema}", tableExpr="{table}" -> no routing (pass through)
// Example: schemaExpr="new_db", tableExpr="{table}" -> route all to new_db schema
// Example: schemaExpr="{schema}_backup", tableExpr="archive_{table}" -> transform both
func (d *DynamicSchemaDispatcher) Substitute(sourceSchema, sourceTable string) (string, string) {
	targetSchema := d.substituteExpression(d.schemaExpr, sourceSchema, sourceTable)
	if targetSchema == "" {
		targetSchema = sourceSchema
	}

	targetTable := d.substituteExpression(d.tableExpr, sourceSchema, sourceTable)
	if targetTable == "" {
		targetTable = sourceTable
	}

	return targetSchema, targetTable
}

// substituteExpression replaces {schema} and {table} placeholders in the expression.
func (d *DynamicSchemaDispatcher) substituteExpression(expr, sourceSchema, sourceTable string) string {
	if expr == "" {
		return ""
	}

	result := schemaRE.ReplaceAllString(expr, sourceSchema)
	result = tableRE.ReplaceAllString(result, sourceTable)
	return result
}

func (d *DynamicSchemaDispatcher) String() string {
	return fmt.Sprintf("dynamic(schema:%s,table:%s)", d.schemaExpr, d.tableExpr)
}
