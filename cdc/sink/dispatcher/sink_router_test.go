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

	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNewSinkRouterNilConfig(t *testing.T) {
	t.Parallel()

	router, err := NewSinkRouter(nil)
	require.NoError(t, err)
	require.Nil(t, router)
}

func TestNewSinkRouterNilSinkConfig(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		Sink: nil,
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.Nil(t, router)
}

func TestNewSinkRouterNoRoutingRules(t *testing.T) {
	t.Parallel()

	// Config with dispatch rules but no schema/table routing
	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:        []string{"test.*"},
					DispatcherRule: "ts",
					// No SchemaRule or TableRule
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.Nil(t, router, "router should be nil when no routing rules configured")
}

func TestNewSinkRouterWithSchemaRouting(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"source_db.*"},
					SchemaRule: "target_db",
					TableRule:  "{table}",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)
}

func TestNewSinkRouterWithTableRouting(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"db.source_table"},
					SchemaRule: "{schema}",
					TableRule:  "target_table",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)
}

func TestNewSinkRouterInvalidSchemaExpression(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"db.*"},
					SchemaRule: "{invalid}",
					TableRule:  "{table}",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.Error(t, err)
	require.Nil(t, router)
}

func TestNewSinkRouterInvalidTableExpression(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"db.*"},
					SchemaRule: "{schema}",
					TableRule:  "invalid space",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.Error(t, err)
	require.Nil(t, router)
}

func TestNewSinkRouterInvalidMatcher(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"[invalid"},
					SchemaRule: "target",
					TableRule:  "{table}",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.Error(t, err)
	require.Nil(t, router)
}

func TestSinkRouterRouteNilRouter(t *testing.T) {
	t.Parallel()

	var router *SinkRouter
	schema, table := router.Route("source_db", "source_table")
	require.Equal(t, "source_db", schema)
	require.Equal(t, "source_table", table)
}

func TestSinkRouterRouteSchemaOnly(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"source_db.*"},
					SchemaRule: "target_db",
					TableRule:  "{table}",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	tests := []struct {
		name         string
		sourceSchema string
		sourceTable  string
		wantSchema   string
		wantTable    string
	}{
		{
			name:         "matching table",
			sourceSchema: "source_db",
			sourceTable:  "users",
			wantSchema:   "target_db",
			wantTable:    "users",
		},
		{
			name:         "another matching table",
			sourceSchema: "source_db",
			sourceTable:  "orders",
			wantSchema:   "target_db",
			wantTable:    "orders",
		},
		{
			name:         "non-matching schema",
			sourceSchema: "other_db",
			sourceTable:  "users",
			wantSchema:   "other_db",
			wantTable:    "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSchema, gotTable := router.Route(tt.sourceSchema, tt.sourceTable)
			require.Equal(t, tt.wantSchema, gotSchema)
			require.Equal(t, tt.wantTable, gotTable)
		})
	}
}

func TestSinkRouterRouteTableOnly(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"mydb.source_table"},
					SchemaRule: "{schema}",
					TableRule:  "target_table",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	tests := []struct {
		name         string
		sourceSchema string
		sourceTable  string
		wantSchema   string
		wantTable    string
	}{
		{
			name:         "matching table",
			sourceSchema: "mydb",
			sourceTable:  "source_table",
			wantSchema:   "mydb",
			wantTable:    "target_table",
		},
		{
			name:         "non-matching table",
			sourceSchema: "mydb",
			sourceTable:  "other_table",
			wantSchema:   "mydb",
			wantTable:    "other_table",
		},
		{
			name:         "non-matching schema",
			sourceSchema: "otherdb",
			sourceTable:  "source_table",
			wantSchema:   "otherdb",
			wantTable:    "source_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSchema, gotTable := router.Route(tt.sourceSchema, tt.sourceTable)
			require.Equal(t, tt.wantSchema, gotSchema)
			require.Equal(t, tt.wantTable, gotTable)
		})
	}
}

func TestSinkRouterRouteSchemaAndTable(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"src_db.src_table"},
					SchemaRule: "dst_db",
					TableRule:  "dst_table",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	// Matching
	schema, table := router.Route("src_db", "src_table")
	require.Equal(t, "dst_db", schema)
	require.Equal(t, "dst_table", table)

	// Non-matching
	schema, table = router.Route("src_db", "other_table")
	require.Equal(t, "src_db", schema)
	require.Equal(t, "other_table", table)
}

func TestSinkRouterRouteWithTransformation(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"prod.*"},
					SchemaRule: "backup_{schema}",
					TableRule:  "{table}_archive",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	schema, table := router.Route("prod", "users")
	require.Equal(t, "backup_prod", schema)
	require.Equal(t, "users_archive", table)
}

func TestSinkRouterMultipleRules(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"db1.*"},
					SchemaRule: "target1",
					TableRule:  "{table}",
				},
				{
					Matcher:    []string{"db2.*"},
					SchemaRule: "target2",
					TableRule:  "{table}",
				},
				{
					Matcher:    []string{"db3.specific_table"},
					SchemaRule: "target3",
					TableRule:  "renamed_table",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	tests := []struct {
		sourceSchema string
		sourceTable  string
		wantSchema   string
		wantTable    string
	}{
		{"db1", "table1", "target1", "table1"},
		{"db1", "table2", "target1", "table2"},
		{"db2", "table1", "target2", "table1"},
		{"db3", "specific_table", "target3", "renamed_table"},
		{"db3", "other_table", "db3", "other_table"}, // No match
		{"db4", "table1", "db4", "table1"},           // No match
	}

	for _, tt := range tests {
		gotSchema, gotTable := router.Route(tt.sourceSchema, tt.sourceTable)
		require.Equal(t, tt.wantSchema, gotSchema, "schema mismatch for %s.%s", tt.sourceSchema, tt.sourceTable)
		require.Equal(t, tt.wantTable, gotTable, "table mismatch for %s.%s", tt.sourceSchema, tt.sourceTable)
	}
}

func TestSinkRouterFirstMatchWins(t *testing.T) {
	t.Parallel()

	// When multiple rules could match, first one wins
	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"db.*"},
					SchemaRule: "first_target",
					TableRule:  "{table}",
				},
				{
					Matcher:    []string{"db.specific"},
					SchemaRule: "second_target",
					TableRule:  "{table}",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	// "db.specific" matches both rules, but first rule wins
	schema, table := router.Route("db", "specific")
	require.Equal(t, "first_target", schema)
	require.Equal(t, "specific", table)
}

func TestSinkRouterCaseInsensitiveSchema(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		CaseSensitive: false, // Case insensitive
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"MyDB.*"},
					SchemaRule: "target_db",
					TableRule:  "{table}",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	// Should match regardless of schema case
	schema, table := router.Route("mydb", "table1")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "table1", table)

	schema, table = router.Route("MYDB", "table2")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "table2", table)

	schema, table = router.Route("MyDb", "table3")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "table3", table)
}

func TestSinkRouterCaseInsensitiveTable(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		CaseSensitive: false, // Case insensitive
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"mydb.MyTable"},
					SchemaRule: "target_db",
					TableRule:  "target_table",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	// Should match regardless of table case
	schema, table := router.Route("mydb", "mytable")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "target_table", table)

	schema, table = router.Route("mydb", "MYTABLE")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "target_table", table)

	schema, table = router.Route("mydb", "MyTable")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "target_table", table)

	// Non-matching table should pass through
	schema, table = router.Route("mydb", "other_table")
	require.Equal(t, "mydb", schema)
	require.Equal(t, "other_table", table)
}

func TestSinkRouterCaseInsensitiveBoth(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		CaseSensitive: false, // Case insensitive
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"MyDB.MyTable"},
					SchemaRule: "target_db",
					TableRule:  "target_table",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	// Should match regardless of case for both schema and table
	tests := []struct {
		sourceSchema string
		sourceTable  string
		shouldMatch  bool
	}{
		{"MyDB", "MyTable", true},
		{"mydb", "mytable", true},
		{"MYDB", "MYTABLE", true},
		{"Mydb", "Mytable", true},
		{"MyDB", "other", false},
		{"other", "MyTable", false},
	}

	for _, tt := range tests {
		schema, table := router.Route(tt.sourceSchema, tt.sourceTable)
		if tt.shouldMatch {
			require.Equal(t, "target_db", schema, "schema mismatch for %s.%s", tt.sourceSchema, tt.sourceTable)
			require.Equal(t, "target_table", table, "table mismatch for %s.%s", tt.sourceSchema, tt.sourceTable)
		} else {
			require.Equal(t, tt.sourceSchema, schema, "should pass through for %s.%s", tt.sourceSchema, tt.sourceTable)
			require.Equal(t, tt.sourceTable, table, "should pass through for %s.%s", tt.sourceSchema, tt.sourceTable)
		}
	}
}

func TestSinkRouterCaseSensitiveSchema(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		CaseSensitive: true, // Case sensitive
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"MyDB.*"},
					SchemaRule: "target_db",
					TableRule:  "{table}",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	// Should only match exact case
	schema, table := router.Route("MyDB", "table1")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "table1", table)

	// Should not match different case
	schema, table = router.Route("mydb", "table2")
	require.Equal(t, "mydb", schema)
	require.Equal(t, "table2", table)

	schema, table = router.Route("MYDB", "table3")
	require.Equal(t, "MYDB", schema)
	require.Equal(t, "table3", table)
}

func TestSinkRouterCaseSensitiveTable(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		CaseSensitive: true, // Case sensitive
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"mydb.MyTable"},
					SchemaRule: "target_db",
					TableRule:  "target_table",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	// Should only match exact table case
	schema, table := router.Route("mydb", "MyTable")
	require.Equal(t, "target_db", schema)
	require.Equal(t, "target_table", table)

	// Should not match different table case
	schema, table = router.Route("mydb", "mytable")
	require.Equal(t, "mydb", schema)
	require.Equal(t, "mytable", table)

	schema, table = router.Route("mydb", "MYTABLE")
	require.Equal(t, "mydb", schema)
	require.Equal(t, "MYTABLE", table)
}

func TestSinkRouterCaseSensitiveBoth(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		CaseSensitive: true, // Case sensitive
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"MyDB.MyTable"},
					SchemaRule: "target_db",
					TableRule:  "target_table",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	// Should only match exact case for both schema and table
	tests := []struct {
		sourceSchema string
		sourceTable  string
		shouldMatch  bool
	}{
		{"MyDB", "MyTable", true},  // Exact match
		{"mydb", "mytable", false}, // Both wrong case
		{"MYDB", "MYTABLE", false}, // Both wrong case
		{"MyDB", "mytable", false}, // Table wrong case
		{"mydb", "MyTable", false}, // Schema wrong case
	}

	for _, tt := range tests {
		schema, table := router.Route(tt.sourceSchema, tt.sourceTable)
		if tt.shouldMatch {
			require.Equal(t, "target_db", schema, "schema mismatch for %s.%s", tt.sourceSchema, tt.sourceTable)
			require.Equal(t, "target_table", table, "table mismatch for %s.%s", tt.sourceSchema, tt.sourceTable)
		} else {
			require.Equal(t, tt.sourceSchema, schema, "should pass through for %s.%s", tt.sourceSchema, tt.sourceTable)
			require.Equal(t, tt.sourceTable, table, "should pass through for %s.%s", tt.sourceSchema, tt.sourceTable)
		}
	}
}

func TestSinkRouterWildcardMatchers(t *testing.T) {
	t.Parallel()

	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"*.*"},
					SchemaRule: "all_to_one",
					TableRule:  "{table}",
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	// All tables should be routed
	schema, table := router.Route("any_db", "any_table")
	require.Equal(t, "all_to_one", schema)
	require.Equal(t, "any_table", table)

	schema, table = router.Route("other_db", "other_table")
	require.Equal(t, "all_to_one", schema)
	require.Equal(t, "other_table", table)
}

func TestSinkRouterMixedRulesWithAndWithoutRouting(t *testing.T) {
	t.Parallel()

	// Some dispatch rules have routing, some don't
	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:        []string{"db1.*"},
					DispatcherRule: "ts",
					// No routing - just partition dispatch
				},
				{
					Matcher:    []string{"db2.*"},
					SchemaRule: "routed_db",
					TableRule:  "{table}",
				},
				{
					Matcher:        []string{"db3.*"},
					DispatcherRule: "rowid",
					// No routing
				},
			},
		},
	}
	router, err := NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router, "router should exist because db2 has routing")

	// db1 has no routing rule - should pass through
	schema, table := router.Route("db1", "table1")
	require.Equal(t, "db1", schema)
	require.Equal(t, "table1", table)

	// db2 has routing - should be routed
	schema, table = router.Route("db2", "table1")
	require.Equal(t, "routed_db", schema)
	require.Equal(t, "table1", table)

	// db3 has no routing rule - should pass through
	schema, table = router.Route("db3", "table1")
	require.Equal(t, "db3", schema)
	require.Equal(t, "table1", table)
}
