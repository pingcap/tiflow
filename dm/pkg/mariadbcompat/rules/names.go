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
	"fmt"
	"sort"
	"strings"
)

// SupportedRuleNames returns the canonical names of all built-in transformation rules.
func SupportedRuleNames() []string {
	registry := NewRegistry(nil)
	ruleList := registry.GetRules()
	names := make([]string, 0, len(ruleList))
	seen := make(map[string]struct{}, len(ruleList))
	for _, rule := range ruleList {
		name := rule.Name()
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// NormalizeConfiguredRuleNames validates configured rule names and rewrites them to canonical names.
func NormalizeConfiguredRuleNames(names []string) ([]string, error) {
	if len(names) == 0 {
		return names, nil
	}

	supported := SupportedRuleNames()
	normalizedNames := make(map[string]string, len(supported))
	for _, name := range supported {
		normalizedNames[strings.ToLower(name)] = name
	}

	normalized := make([]string, 0, len(names))
	for _, name := range names {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			return nil, fmt.Errorf("mariadb-compat rule name must not be empty")
		}

		canonical, ok := normalizedNames[strings.ToLower(trimmed)]
		if !ok {
			return nil, fmt.Errorf(
				"unknown mariadb-compat rule %q, available rules: %s",
				name,
				strings.Join(supported, ", "),
			)
		}
		normalized = append(normalized, canonical)
	}
	return normalized, nil
}
