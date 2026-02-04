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
	"os"

	yaml "gopkg.in/yaml.v3"
)

// Config holds configuration for the transformation engine
type Config struct {
	// InputDir specifies the directory to read SQL files from
	InputDir string `yaml:"input_dir"`

	// OutputDir specifies the directory to write transformed SQL files
	OutputDir string `yaml:"output_dir"`

	// EnabledRules is a list of rule names to enable
	EnabledRules []string `yaml:"enabled_rules"`

	// DisabledRules is a list of rule names to disable
	DisabledRules []string `yaml:"disabled_rules"`

	// StrictMode determines if transformation should fail on any error
	StrictMode bool `yaml:"strict_mode"`

	// PreserveComments determines if comments should be preserved
	PreserveComments bool `yaml:"preserve_comments"`

	// FormattingOptions controls output formatting
	FormattingOptions FormattingConfig `yaml:"formatting"`

	// CharsetMappings defines how to convert character sets
	CharsetMappings map[string]CharsetMapping `yaml:"charset_mappings"`

	// CollationMappings defines how to convert collations
	CollationMappings map[string]string `yaml:"collation_mappings"`
}

// FormattingConfig controls SQL output formatting
type FormattingConfig struct {
	// IndentSize specifies the number of spaces for indentation
	IndentSize int `yaml:"indent_size"`

	// UppercaseKeywords determines if SQL keywords should be uppercase
	UppercaseKeywords bool `yaml:"uppercase_keywords"`

	// BackquoteIdentifiers determines if identifiers should be backtick-quoted
	BackquoteIdentifiers bool `yaml:"backquote_identifiers"`

	// SingleQuoteStrings determines if strings should use single quotes
	SingleQuoteStrings bool `yaml:"single_quote_strings"`
}

// CharsetMapping defines target charset and collation for a source charset
type CharsetMapping struct {
	TargetCharset   string `yaml:"target_charset"`
	TargetCollation string `yaml:"target_collation"`
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		InputDir:         "",
		OutputDir:        "",
		EnabledRules:     []string{}, // Empty means all rules enabled
		DisabledRules:    []string{},
		StrictMode:       true,
		PreserveComments: true,
		FormattingOptions: FormattingConfig{
			IndentSize:           2,
			UppercaseKeywords:    true,
			BackquoteIdentifiers: true,
			SingleQuoteStrings:   true,
		},
		CharsetMappings: map[string]CharsetMapping{
			"latin1": {
				TargetCharset:   "utf8mb4",
				TargetCollation: "utf8mb4_0900_ai_ci",
			},
		},
		CollationMappings: map[string]string{
			"latin1_swedish_ci": "utf8mb4_0900_ai_ci",
			"utf8mb4_unicode_*": "utf8mb4_0900_ai_ci",
		},
	}
}

// IsRuleEnabled checks if a rule is enabled based on configuration
func (c *Config) IsRuleEnabled(ruleName string) bool {
	// If explicitly disabled, return false
	for _, disabled := range c.DisabledRules {
		if disabled == ruleName {
			return false
		}
	}

	// If EnabledRules is empty, all rules are enabled (except disabled ones)
	if len(c.EnabledRules) == 0 {
		return true
	}

	// If EnabledRules is specified, only those rules are enabled
	for _, enabled := range c.EnabledRules {
		if enabled == ruleName {
			return true
		}
	}

	return false
}

// LoadConfig reads configuration from a YAML file. If path is empty, returns DefaultConfig.
func LoadConfig(path string) (*Config, error) {
	if path == "" {
		return DefaultConfig(), nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
