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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaRouteValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		route       *SchemaRoute
		expectError bool
	}{
		{
			name: "valid route",
			route: &SchemaRoute{
				SourceSchema: "source_db",
				TargetSchema: "target_db",
			},
			expectError: false,
		},
		{
			name: "empty source schema",
			route: &SchemaRoute{
				SourceSchema: "",
				TargetSchema: "target_db",
			},
			expectError: true,
		},
		{
			name: "empty target schema",
			route: &SchemaRoute{
				SourceSchema: "source_db",
				TargetSchema: "",
			},
			expectError: true,
		},
		{
			name: "both empty",
			route: &SchemaRoute{
				SourceSchema: "",
				TargetSchema: "",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.route.Validate()
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSchemaRouteRoute(t *testing.T) {
	t.Parallel()

	route := &SchemaRoute{
		SourceSchema: "source_db",
		TargetSchema: "target_db",
	}

	tests := []struct {
		name           string
		inputSchema    string
		expectedSchema string
		expectedMatch  bool
	}{
		{
			name:           "matching schema",
			inputSchema:    "source_db",
			expectedSchema: "target_db",
			expectedMatch:  true,
		},
		{
			name:           "non-matching schema",
			inputSchema:    "other_db",
			expectedSchema: "other_db",
			expectedMatch:  false,
		},
		{
			name:           "empty schema",
			inputSchema:    "",
			expectedSchema: "",
			expectedMatch:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultSchema, matched := route.Route(tt.inputSchema)
			require.Equal(t, tt.expectedSchema, resultSchema)
			require.Equal(t, tt.expectedMatch, matched)
		})
	}
}

func TestValidateSchemaRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		schemaRoutes     map[string]*SchemaRoute
		schemaRouteRules []string
		expectError      bool
		errorContains    string
	}{
		{
			name: "valid configuration",
			schemaRoutes: map[string]*SchemaRoute{
				"route1": {
					SourceSchema: "source_db1",
					TargetSchema: "target_db1",
				},
				"route2": {
					SourceSchema: "source_db2",
					TargetSchema: "target_db2",
				},
			},
			schemaRouteRules: []string{"route1", "route2"},
			expectError:      false,
		},
		{
			name:             "empty configuration",
			schemaRoutes:     map[string]*SchemaRoute{},
			schemaRouteRules: []string{},
			expectError:      false,
		},
		{
			name: "referenced rule does not exist",
			schemaRoutes: map[string]*SchemaRoute{
				"route1": {
					SourceSchema: "source_db",
					TargetSchema: "target_db",
				},
			},
			schemaRouteRules: []string{"route1", "nonexistent"},
			expectError:      true,
			errorContains:    "nonexistent",
		},
		{
			name: "invalid route - empty source",
			schemaRoutes: map[string]*SchemaRoute{
				"route1": {
					SourceSchema: "",
					TargetSchema: "target_db",
				},
			},
			schemaRouteRules: []string{"route1"},
			expectError:      true,
			errorContains:    "source-schema cannot be empty",
		},
		{
			name: "invalid route - empty target",
			schemaRoutes: map[string]*SchemaRoute{
				"route1": {
					SourceSchema: "source_db",
					TargetSchema: "",
				},
			},
			schemaRouteRules: []string{"route1"},
			expectError:      true,
			errorContains:    "target-schema cannot be empty",
		},
		{
			name: "duplicate source schemas",
			schemaRoutes: map[string]*SchemaRoute{
				"route1": {
					SourceSchema: "source_db",
					TargetSchema: "target_db1",
				},
				"route2": {
					SourceSchema: "source_db",
					TargetSchema: "target_db2",
				},
			},
			schemaRouteRules: []string{"route1", "route2"},
			expectError:      true,
			errorContains:    "duplicate source-schema",
		},
		{
			name: "unused route definition",
			schemaRoutes: map[string]*SchemaRoute{
				"route1": {
					SourceSchema: "source_db",
					TargetSchema: "target_db",
				},
				"unused": {
					SourceSchema: "unused_db",
					TargetSchema: "unused_target",
				},
			},
			schemaRouteRules: []string{"route1"},
			expectError:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSchemaRouting(tt.schemaRoutes, tt.schemaRouteRules)
			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					require.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBuildSchemaRouter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		schemaRoutes     map[string]*SchemaRoute
		schemaRouteRules []string
		expectError      bool
		expectNil        bool
	}{
		{
			name: "valid router",
			schemaRoutes: map[string]*SchemaRoute{
				"route1": {
					SourceSchema: "source_db",
					TargetSchema: "target_db",
				},
			},
			schemaRouteRules: []string{"route1"},
			expectError:      false,
			expectNil:        false,
		},
		{
			name:             "no rules - returns nil",
			schemaRoutes:     map[string]*SchemaRoute{},
			schemaRouteRules: []string{},
			expectError:      false,
			expectNil:        true,
		},
		{
			name: "referenced rule does not exist",
			schemaRoutes: map[string]*SchemaRoute{
				"route1": {
					SourceSchema: "source_db",
					TargetSchema: "target_db",
				},
			},
			schemaRouteRules: []string{"nonexistent"},
			expectError:      true,
			expectNil:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router, err := BuildSchemaRouter(tt.schemaRoutes, tt.schemaRouteRules)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.expectNil {
					require.Nil(t, router)
				} else {
					require.NotNil(t, router)
				}
			}
		})
	}
}

func TestSchemaRouterRoute(t *testing.T) {
	t.Parallel()

	schemaRoutes := map[string]*SchemaRoute{
		"route1": {
			SourceSchema: "source_db1",
			TargetSchema: "target_db1",
		},
		"route2": {
			SourceSchema: "source_db2",
			TargetSchema: "target_db2",
		},
	}
	schemaRouteRules := []string{"route1", "route2"}

	router, err := BuildSchemaRouter(schemaRoutes, schemaRouteRules)
	require.NoError(t, err)
	require.NotNil(t, router)

	tests := []struct {
		name           string
		inputSchema    string
		expectedSchema string
	}{
		{
			name:           "match first rule",
			inputSchema:    "source_db1",
			expectedSchema: "target_db1",
		},
		{
			name:           "match second rule",
			inputSchema:    "source_db2",
			expectedSchema: "target_db2",
		},
		{
			name:           "no match - return original",
			inputSchema:    "unmapped_db",
			expectedSchema: "unmapped_db",
		},
		{
			name:           "empty schema",
			inputSchema:    "",
			expectedSchema: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := router.Route(tt.inputSchema)
			require.Equal(t, tt.expectedSchema, result)
		})
	}
}

func TestSchemaRouterNil(t *testing.T) {
	t.Parallel()

	var router *SchemaRouter = nil

	// Nil router should return original schema
	result := router.Route("any_schema")
	require.Equal(t, "any_schema", result)
}

func TestSchemaRouterEmpty(t *testing.T) {
	t.Parallel()

	router := NewSchemaRouter([]*SchemaRoute{})

	// Empty router should return original schema
	result := router.Route("any_schema")
	require.Equal(t, "any_schema", result)
}
