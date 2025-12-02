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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDynamicSchemaDispatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		schemaExpr   string
		tableExpr    string
		sourceSchema string
		sourceTable  string
		wantSchema   string
		wantTable    string
	}{
		{
			name:         "passthrough with placeholders",
			schemaExpr:   "{schema}",
			tableExpr:    "{table}",
			sourceSchema: "source_db",
			sourceTable:  "source_table",
			wantSchema:   "source_db",
			wantTable:    "source_table",
		},
		{
			name:         "static schema routing",
			schemaExpr:   "target_db",
			tableExpr:    "{table}",
			sourceSchema: "source_db",
			sourceTable:  "my_table",
			wantSchema:   "target_db",
			wantTable:    "my_table",
		},
		{
			name:         "static table routing",
			schemaExpr:   "{schema}",
			tableExpr:    "routed_table",
			sourceSchema: "source_db",
			sourceTable:  "my_table",
			wantSchema:   "source_db",
			wantTable:    "routed_table",
		},
		{
			name:         "both schema and table static routing",
			schemaExpr:   "target_db",
			tableExpr:    "target_table",
			sourceSchema: "source_db",
			sourceTable:  "source_table",
			wantSchema:   "target_db",
			wantTable:    "target_table",
		},
		{
			name:         "schema prefix transformation",
			schemaExpr:   "backup_{schema}",
			tableExpr:    "{table}",
			sourceSchema: "prod",
			sourceTable:  "users",
			wantSchema:   "backup_prod",
			wantTable:    "users",
		},
		{
			name:         "schema suffix transformation",
			schemaExpr:   "{schema}_archive",
			tableExpr:    "{table}",
			sourceSchema: "logs",
			sourceTable:  "events",
			wantSchema:   "logs_archive",
			wantTable:    "events",
		},
		{
			name:         "table prefix transformation",
			schemaExpr:   "{schema}",
			tableExpr:    "v2_{table}",
			sourceSchema: "app",
			sourceTable:  "config",
			wantSchema:   "app",
			wantTable:    "v2_config",
		},
		{
			name:         "table suffix transformation",
			schemaExpr:   "{schema}",
			tableExpr:    "{table}_bak",
			sourceSchema: "data",
			sourceTable:  "metrics",
			wantSchema:   "data",
			wantTable:    "metrics_bak",
		},
		{
			name:         "complex transformation with both placeholders",
			schemaExpr:   "mirror_{schema}",
			tableExpr:    "copy_{table}_v2",
			sourceSchema: "production",
			sourceTable:  "orders",
			wantSchema:   "mirror_production",
			wantTable:    "copy_orders_v2",
		},
		{
			name:         "empty schema expr uses source schema",
			schemaExpr:   "",
			tableExpr:    "new_table",
			sourceSchema: "my_db",
			sourceTable:  "old_table",
			wantSchema:   "my_db",
			wantTable:    "new_table",
		},
		{
			name:         "empty table expr uses source table",
			schemaExpr:   "new_db",
			tableExpr:    "",
			sourceSchema: "old_db",
			sourceTable:  "my_table",
			wantSchema:   "new_db",
			wantTable:    "my_table",
		},
		{
			name:         "both expressions empty uses source values",
			schemaExpr:   "",
			tableExpr:    "",
			sourceSchema: "src_db",
			sourceTable:  "src_table",
			wantSchema:   "src_db",
			wantTable:    "src_table",
		},
		{
			name:         "schema placeholder in table expr",
			schemaExpr:   "{schema}",
			tableExpr:    "{schema}_{table}",
			sourceSchema: "mydb",
			sourceTable:  "mytable",
			wantSchema:   "mydb",
			wantTable:    "mydb_mytable",
		},
		{
			name:         "table placeholder in schema expr",
			schemaExpr:   "{schema}_{table}",
			tableExpr:    "{table}",
			sourceSchema: "db",
			sourceTable:  "tbl",
			wantSchema:   "db_tbl",
			wantTable:    "tbl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDynamicSchemaDispatcher(tt.schemaExpr, tt.tableExpr)
			gotSchema, gotTable := d.Substitute(tt.sourceSchema, tt.sourceTable)
			require.Equal(t, tt.wantSchema, gotSchema, "schema mismatch")
			require.Equal(t, tt.wantTable, gotTable, "table mismatch")
		})
	}
}

func TestDynamicSchemaDispatcherString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		schemaExpr string
		tableExpr  string
		want       string
	}{
		{
			schemaExpr: "{schema}",
			tableExpr:  "{table}",
			want:       "dynamic(schema:{schema},table:{table})",
		},
		{
			schemaExpr: "target_db",
			tableExpr:  "{table}",
			want:       "dynamic(schema:target_db,table:{table})",
		},
		{
			schemaExpr: "",
			tableExpr:  "",
			want:       "dynamic(schema:,table:)",
		},
	}

	for _, tt := range tests {
		d := NewDynamicSchemaDispatcher(tt.schemaExpr, tt.tableExpr)
		require.Equal(t, tt.want, d.String())
	}
}

func TestValidateExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		// Valid expressions
		{name: "empty expression", expr: "", wantErr: false},
		{name: "schema placeholder only", expr: "{schema}", wantErr: false},
		{name: "table placeholder only", expr: "{table}", wantErr: false},
		{name: "both placeholders", expr: "{schema}{table}", wantErr: false},
		{name: "static value", expr: "my_database", wantErr: false},
		{name: "prefix with schema", expr: "prefix_{schema}", wantErr: false},
		{name: "suffix with schema", expr: "{schema}_suffix", wantErr: false},
		{name: "prefix and suffix with schema", expr: "pre_{schema}_suf", wantErr: false},
		{name: "prefix with table", expr: "prefix_{table}", wantErr: false},
		{name: "suffix with table", expr: "{table}_suffix", wantErr: false},
		{name: "both with prefix", expr: "p_{schema}_{table}", wantErr: false},
		{name: "alphanumeric", expr: "db123", wantErr: false},
		{name: "with underscore", expr: "my_db", wantErr: false},
		{name: "with dot", expr: "my.db", wantErr: false},
		{name: "with hyphen", expr: "my-db", wantErr: false},
		{name: "complex valid", expr: "backup.{schema}_v2", wantErr: false},

		// Invalid expressions
		{name: "invalid placeholder", expr: "{invalid}", wantErr: true},
		{name: "unclosed brace", expr: "{schema", wantErr: true},
		{name: "special char space", expr: "my db", wantErr: true},
		{name: "special char at", expr: "my@db", wantErr: true},
		{name: "special char hash", expr: "my#db", wantErr: true},
		{name: "table before schema", expr: "{table}{schema}", wantErr: true},
		{name: "duplicate schema", expr: "{schema}{schema}", wantErr: true},
		{name: "duplicate table", expr: "{table}{table}", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateExpression(tt.expr)
			if tt.wantErr {
				require.Error(t, err, "expected error for expr: %s", tt.expr)
			} else {
				require.NoError(t, err, "unexpected error for expr: %s", tt.expr)
			}
		})
	}
}

func TestDispatcherInterface(t *testing.T) {
	t.Parallel()

	// Verify DynamicSchemaDispatcher implements the Dispatcher interface
	var _ Dispatcher = (*DynamicSchemaDispatcher)(nil)

	// Test that it can be used through the interface
	d := NewDynamicSchemaDispatcher("target", "{table}")

	// Should not panic
	_ = d.String()
	_, _ = d.Substitute("schema", "table")
}
