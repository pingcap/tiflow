// Copyright 2021 PingCAP, Inc.
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

package pb

import (
	"bytes"
	"io"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"
)

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (m *StartTaskRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	clone := *m
	clone.Task = redactYAMLConfig(m.Task)
	enc.AddString("HidePasswordObject", clone.String())
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (m *CheckTaskRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	clone := *m
	clone.Task = redactYAMLConfig(m.Task)
	enc.AddString("HidePasswordObject", clone.String())
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (m *OperateSourceRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	clone := *m
	clone.Config = make([]string, len(m.Config))
	for i, cfg := range m.Config {
		clone.Config[i] = redactYAMLConfig(cfg)
	}
	enc.AddString("HidePasswordObject", clone.String())
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (m *CheckSubtasksCanUpdateRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	clone := *m
	clone.SubtaskCfgTomlString = redactTOMLConfig(m.SubtaskCfgTomlString)
	enc.AddString("HidePasswordObject", clone.String())
	return nil
}

const invalidConfigForLog = "(invalid config; redacted)"

// Decode generic mappings instead of config structs so diagnostics retain unknown
// fields without applying defaults. Never include decoder errors: they can quote
// sensitive input. Only the log representation is changed.
func redactYAMLConfig(input string) string {
	if input == "" {
		return input
	}
	var cfg map[string]any
	decoder := yaml.NewDecoder(strings.NewReader(input))
	decoder.SetStrict(true)
	if err := decoder.Decode(&cfg); err != nil {
		return invalidConfigForLog
	}
	// Task and source configs are single documents. Reject trailing documents
	// rather than accidentally logging only part of a request.
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return invalidConfigForLog
	}
	redactConfigFields(cfg)
	output, err := yaml.Marshal(cfg)
	if err != nil {
		return invalidConfigForLog
	}
	return string(output)
}

func redactTOMLConfig(input string) string {
	if input == "" {
		return input
	}
	var cfg map[string]any
	if _, err := toml.Decode(input, &cfg); err != nil {
		return invalidConfigForLog
	}
	redactConfigFields(cfg)
	var output bytes.Buffer
	if err := toml.NewEncoder(&output).Encode(cfg); err != nil {
		return invalidConfigForLog
	}
	return output.String()
}

func isSensitiveConfigField(key string) bool {
	switch strings.ToLower(key) {
	case "password", "ssl-ca-bytes", "ssl-cert-bytes", "ssl-key-bytes",
		"ssl-ca-base64", "ssl-cert-base64", "ssl-key-base64":
		return true
	default:
		return false
	}
}

func redactConfigFields(value any) {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			if isSensitiveConfigField(key) {
				value[key] = log.RedactedCredential
			} else {
				redactConfigFields(child)
			}
		}
	case map[any]any: // Nested YAML mappings.
		for key, child := range value {
			if name, ok := key.(string); ok && isSensitiveConfigField(name) {
				value[key] = log.RedactedCredential
			} else {
				redactConfigFields(child)
			}
		}
	case []any:
		for _, child := range value {
			redactConfigFields(child)
		}
	case []map[string]any: // TOML arrays of tables.
		for _, child := range value {
			redactConfigFields(child)
		}
	}
}
