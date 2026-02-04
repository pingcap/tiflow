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
)

// StubRule provides a base implementation for rules that are not yet implemented
type StubRule struct {
	name        string
	description string
	priority    int
}

// NewStubRule creates a new stub rule
func NewStubRule(name, description string, priority int) *StubRule {
	return &StubRule{
		name:        name,
		description: description,
		priority:    priority,
	}
}

// Name implements Rule interface
func (r *StubRule) Name() string {
	return r.name
}

// Description implements Rule interface
func (r *StubRule) Description() string {
	return r.description + " (stub implementation)"
}

// Priority implements Rule interface
func (r *StubRule) Priority() int {
	return r.priority
}

// ShouldApply implements Rule interface - stub always returns false
func (r *StubRule) ShouldApply(_ ast.Node) bool {
	return false // Stub rules don't apply to anything
}

// Apply implements Rule interface - stub returns node unchanged
func (r *StubRule) Apply(node ast.Node) (ast.Node, error) {
	return node, nil // Stub rules don't modify anything
}

// CreateStubRules creates stub implementations for all planned rules
func CreateStubRules() []Rule {
	return []Rule{
		NewStubRule("SystemVersioning", "Remove or rewrite system-versioning syntax", 800),
		NewStubRule("SequenceType", "Normalize sequence type clauses", 810),
		NewStubRule("ColumnAttributes", "Strip unsupported column attributes", 820),
		NewStubRule("IndexOptions", "Remove unsupported index options", 830),
		NewStubRule("FulltextIndexNormalize", "Normalize FULLTEXT indexes", 840),
		NewStubRule("SpatialIndexDrop", "Drop unsupported SPATIAL indexes", 850),
		NewStubRule("CreateOrReplace", "Rewrite unsupported CREATE OR REPLACE variants", 860),
		NewStubRule("EngineOptions", "Strip unsupported engine and table options", 870),
		NewStubRule("IgnoredClauseCleanup", "Remove parsed-but-ignored clauses", 880),
		NewStubRule("CollationFallback", "Map unsupported collations to defaults", 890),
	}
}
