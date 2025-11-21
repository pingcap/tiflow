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
	"fmt"

	"github.com/pingcap/tiflow/pkg/errors"
)

// SchemaRoute defines a schema name routing rule that maps a source schema
// to a target schema. This is used to replicate data from one schema to a
// different schema name in the downstream.
type SchemaRoute struct {
	// SourceSchema is the upstream schema name to match
	SourceSchema string `toml:"source-schema" json:"source-schema"`
	// TargetSchema is the downstream schema name to use
	TargetSchema string `toml:"target-schema" json:"target-schema"`
}

// Validate validates the schema route rule
func (r *SchemaRoute) Validate() error {
	if r.SourceSchema == "" {
		return errors.ErrInvalidReplicaConfig.GenWithStackByArgs(
			"source-schema cannot be empty in schema route rule")
	}
	if r.TargetSchema == "" {
		return errors.ErrInvalidReplicaConfig.GenWithStackByArgs(
			"target-schema cannot be empty in schema route rule")
	}
	return nil
}

// Route applies the routing rule to a schema name
func (r *SchemaRoute) Route(schema string) (string, bool) {
	if r.SourceSchema == schema {
		return r.TargetSchema, true
	}
	return schema, false
}

// SchemaRouter manages schema routing rules
type SchemaRouter struct {
	// rules is the ordered list of routing rules to apply
	rules []*SchemaRoute
}

// NewSchemaRouter creates a new schema router with the given rules
func NewSchemaRouter(rules []*SchemaRoute) *SchemaRouter {
	return &SchemaRouter{
		rules: rules,
	}
}

// Route applies routing rules to a schema name and returns the target schema name
func (r *SchemaRouter) Route(schema string) string {
	if r == nil || len(r.rules) == 0 {
		return schema
	}

	for _, rule := range r.rules {
		if targetSchema, matched := rule.Route(schema); matched {
			return targetSchema
		}
	}

	return schema
}

// ValidateSchemaRouting validates the schema routing configuration
func ValidateSchemaRouting(schemaRoutes map[string]*SchemaRoute, schemaRouteRules []string) error {
	// Check that all referenced route rules exist
	for _, ruleName := range schemaRouteRules {
		if _, exists := schemaRoutes[ruleName]; !exists {
			return errors.ErrInvalidReplicaConfig.GenWithStackByArgs(
				fmt.Sprintf("schema route rule '%s' is referenced but not defined", ruleName))
		}
	}

	// Validate each route rule
	for name, rule := range schemaRoutes {
		if err := rule.Validate(); err != nil {
			return errors.ErrInvalidReplicaConfig.GenWithStackByArgs(
				fmt.Sprintf("invalid schema route rule '%s': %v", name, err))
		}
	}

	// Check for duplicate source schemas in active rules
	seenSourceSchemas := make(map[string]string)
	for _, ruleName := range schemaRouteRules {
		rule := schemaRoutes[ruleName]
		if existingRuleName, exists := seenSourceSchemas[rule.SourceSchema]; exists {
			return errors.ErrInvalidReplicaConfig.GenWithStackByArgs(
				fmt.Sprintf("duplicate source-schema '%s' in route rules '%s' and '%s'",
					rule.SourceSchema, existingRuleName, ruleName))
		}
		seenSourceSchemas[rule.SourceSchema] = ruleName
	}

	return nil
}

// BuildSchemaRouter builds a schema router from the config
func BuildSchemaRouter(schemaRoutes map[string]*SchemaRoute, schemaRouteRules []string) (*SchemaRouter, error) {
	if len(schemaRouteRules) == 0 {
		return nil, nil
	}

	// Build ordered list of rules based on schemaRouteRules
	rules := make([]*SchemaRoute, 0, len(schemaRouteRules))
	for _, ruleName := range schemaRouteRules {
		rule, exists := schemaRoutes[ruleName]
		if !exists {
			return nil, errors.ErrInvalidReplicaConfig.GenWithStackByArgs(
				fmt.Sprintf("schema route rule '%s' is referenced but not defined", ruleName))
		}
		rules = append(rules, rule)
	}

	return NewSchemaRouter(rules), nil
}
