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
// See the License for the specific language governing permissions and
// limitations under the License.

package master

import (
	"fmt"
	"strings"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/redact"
	"gopkg.in/yaml.v2"
)

const redactedTLSContent = "REDACTED"

// sensitiveConfigKeys are the config fields whose values must never be logged.
var sensitiveConfigKeys = map[string]struct{}{
	"password":          {},
	"plaintext":         {},
	"secret-key":        {},
	"ssl-ca-bytes":      {},
	"ssl-cert-bytes":    {},
	"ssl-key-bytes":     {},
	"ssl-ca-base64":     {},
	"ssl-cert-base64":   {},
	"ssl-key-base64":    {},
	"access-key":        {},
	"secret-access-key": {},
	"session-token":     {},
}

// redactConfigYAML parses a YAML config and replaces the values of the sensitive
// fields, so the payload can still be logged for diagnostics.
func redactConfigYAML(content string) string {
	if strings.TrimSpace(content) == "" {
		return content
	}
	var parsed any
	if err := yaml.Unmarshal([]byte(content), &parsed); err != nil {
		return "(invalid config; redacted)"
	}
	redactConfigValue(parsed)
	data, err := yaml.Marshal(parsed)
	if err != nil {
		return "(invalid config; redacted)"
	}
	return redact.Text(string(data))
}

func redactConfigValue(value any) {
	switch v := value.(type) {
	case map[any]any:
		for key, child := range v {
			name, _ := key.(string)
			if _, ok := sensitiveConfigKeys[strings.ToLower(name)]; ok {
				v[key] = redactedConfigValue(name, child)
				continue
			}
			redactConfigValue(child)
		}
	case map[string]any:
		for key, child := range v {
			if _, ok := sensitiveConfigKeys[strings.ToLower(key)]; ok {
				v[key] = redactedConfigValue(key, child)
				continue
			}
			redactConfigValue(child)
		}
	case []any:
		for _, child := range v {
			redactConfigValue(child)
		}
	}
}

func redactedConfigValue(key string, value any) string {
	switch strings.ToLower(key) {
	case "ssl-ca-bytes", "ssl-cert-bytes", "ssl-key-bytes",
		"ssl-ca-base64", "ssl-cert-base64", "ssl-key-base64":
		return redactedTLSContent
	default:
		return config.ObfuscatedPasswordForFeedback
	}
}

// redactedPayload renders an RPC request for logging: configuration strings are
// redacted, while the non-sensitive fields stay visible.
func redactedPayload(req interface{}) string {
	switch r := req.(type) {
	case *pb.StartTaskRequest:
		return fmt.Sprintf("task: %s, sources: %v", redactConfigYAML(r.Task), r.Sources)
	case *pb.UpdateTaskRequest:
		return fmt.Sprintf("task: %s", redactConfigYAML(r.Task))
	case *pb.OperateSourceRequest:
		configs := make([]string, 0, len(r.Config))
		for _, cfg := range r.Config {
			configs = append(configs, redactConfigYAML(cfg))
		}
		return fmt.Sprintf("op: %v, source id: %v, worker: %s, config: %v",
			r.Op, r.SourceID, r.WorkerName, configs)
	case *pb.EncryptRequest:
		return "(plaintext hidden)"
	default:
		return fmt.Sprintf("(%T)", req)
	}
}
