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

const invalidConfigForLog = "(invalid config; redacted)"

// redactedTaskYAML decodes a task config and renders its redacted form, so the
// payload can be logged without the target DB password and inline TLS material.
func redactedTaskYAML(content string) string {
	if strings.TrimSpace(content) == "" {
		return content
	}
	cfg := &config.TaskConfig{}
	if err := yaml.Unmarshal([]byte(content), cfg); err != nil {
		return invalidConfigForLog
	}
	data, err := yaml.Marshal(cfg.Redacted())
	if err != nil {
		return invalidConfigForLog
	}
	return redact.Text(string(data))
}

// redactedSourceYAML decodes a source config and renders its redacted form.
func redactedSourceYAML(content string) string {
	if strings.TrimSpace(content) == "" {
		return content
	}
	cfg := &config.SourceConfig{}
	if err := yaml.Unmarshal([]byte(content), cfg); err != nil {
		return invalidConfigForLog
	}
	data, err := yaml.Marshal(cfg.Redacted())
	if err != nil {
		return invalidConfigForLog
	}
	return redact.Text(string(data))
}

// redactedPayload renders an RPC request for logging: configuration strings are
// replaced with their redacted form, the rest of the request stays visible.
func redactedPayload(req interface{}) string {
	switch r := req.(type) {
	case *pb.StartTaskRequest:
		return fmt.Sprintf("task: %s, sources: %v", redactedTaskYAML(r.Task), r.Sources)
	case *pb.UpdateTaskRequest:
		return fmt.Sprintf("task: %s", redactedTaskYAML(r.Task))
	case *pb.OperateSourceRequest:
		configs := make([]string, 0, len(r.Config))
		for _, cfg := range r.Config {
			configs = append(configs, redactedSourceYAML(cfg))
		}
		return fmt.Sprintf("op: %v, source id: %v, worker: %s, config: %v",
			r.Op, r.SourceID, r.WorkerName, configs)
	case *pb.EncryptRequest:
		return "(plaintext hidden)"
	default:
		return fmt.Sprintf("(%T)", req)
	}
}
