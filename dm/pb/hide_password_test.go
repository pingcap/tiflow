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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"
)

func TestYAMLRequestLogRedaction(t *testing.T) {
	configs := map[string]string{
		"no trailing newline": "target-database:\n  host: db.example\n  password: secret-password",
		"quoted key":          "target-database: {host: db.example, 'password': secret-password}",
		"double quoted key":   "target-database: {host: db.example, \"password\": \"secret-password\"}",
		"block scalar":        "target-database:\n  host: db.example\n  password: |\n    secret-password\n",
		"alias":               "db: &db {host: db.example, password: secret-password}\ntarget-database: *db\n",
		"nested sequence":     "sources:\n  - from: {host: db.example, password: secret-password}\n",
		"PEM content": `target-database:
  host: db.example
  security:
    ssl-key-bytes: |
      -----BEGIN PRIVATE KEY-----
      secret-private-key
      -----END PRIVATE KEY-----
`,
		"TLS content": `target-database:
  host: db.example
  password: secret-password
  security:
    ssl-key: /certs/client-key.pem
    ssl-key-base64: c2VjcmV0LXByaXZhdGUta2V5
    ssl-key-bytes: [115, 101, 99, 114, 101, 116]
    ssl-ca-base64: secret-ca
    ssl-cert-base64: secret-cert
    ssl-ca-bytes: secret-ca-bytes
    ssl-cert-bytes: secret-cert-bytes
`,
	}
	for name, cfg := range configs {
		t.Run(name, func(t *testing.T) {
			requests := []fmt.Stringer{
				&StartTaskRequest{Task: cfg, Sources: []string{"source-one"}, RemoveMeta: true},
				&CheckTaskRequest{Task: cfg, ErrCnt: 17, WarnCnt: 19},
				&OperateSourceRequest{
					Op: SourceOp_StartSource, Config: []string{cfg, cfg}, WorkerName: "worker-one",
				},
			}
			for _, req := range requests {
				t.Run(fmt.Sprintf("%T", req), func(t *testing.T) {
					before := req.String()
					output := requestLog(t, zap.Any("payload", req))
					require.Contains(t, output, "db.example")
					require.Contains(t, output, log.RedactedCredential)
					for _, secret := range []string{
						"secret-password", "secret-private-key", "c2VjcmV0LXByaXZhdGUta2V5",
						"secret-ca", "secret-cert", "115",
					} {
						require.NotContains(t, output, secret)
					}
					if name == "TLS content" {
						require.Contains(t, output, "/certs/client-key.pem")
					}
					switch req.(type) {
					case *StartTaskRequest:
						require.Contains(t, output, "source-one")
						require.Contains(t, output, "removeMeta:true")
					case *CheckTaskRequest:
						require.Contains(t, output, "errCnt:17")
						require.Contains(t, output, "warnCnt:19")
					case *OperateSourceRequest:
						require.Contains(t, output, "worker-one")
					}
					require.Equal(t, before, req.String(), "logging must not mutate the RPC request")
				})
			}
		})
	}
}

func TestSubtaskRequestLogRedaction(t *testing.T) {
	const cfg = `name = "task-one"
dir = "s3://internal-bucket/dump"
[from]
host = "source.example"
password = "source-secret"
[to]
host = "target.example"
password = "target-secret"
[to.security]
ssl-key = "/certs/client-key.pem"
ssl-key-base64 = "private-key-base64"
ssl-key-bytes = [115, 101, 99, 114, 101, 116]
[[extra]]
name = "diagnostic-field"
password = "nested-secret"
`
	req := &CheckSubtasksCanUpdateRequest{SubtaskCfgTomlString: cfg}
	// The worker explicitly uses zap.Object; zap.Any must also select the
	// marshaler when a request is logged by shared RPC handling.
	for _, field := range []zap.Field{zap.Object("payload", req), zap.Any("payload", req)} {
		output := requestLog(t, field)
		for _, secret := range []string{
			"source-secret", "target-secret", "private-key-base64", "nested-secret", "115",
		} {
			require.NotContains(t, output, secret)
		}
		for _, retained := range []string{
			"task-one", "source.example", "target.example", "/certs/client-key.pem",
			"s3://internal-bucket/dump", "diagnostic-field", log.RedactedCredential,
		} {
			require.Contains(t, output, retained)
		}
		require.Equal(t, cfg, req.SubtaskCfgTomlString)
	}
}

func TestInvalidRequestConfigLogRedaction(t *testing.T) {
	for _, cfg := range []string{
		"target-database: [secret-password",
		"password: secret-password\npassword: another-secret\n",
		"password: secret-password\n---\npassword: another-secret\n",
		"secret-password",
	} {
		for _, req := range []any{
			&StartTaskRequest{Task: cfg},
			&CheckTaskRequest{Task: cfg},
			&OperateSourceRequest{Config: []string{cfg}},
		} {
			output := requestLog(t, zap.Any("payload", req))
			require.Contains(t, output, invalidConfigForLog)
			require.NotContains(t, output, "secret-password")
			require.NotContains(t, output, "another-secret")
		}
	}
	req := &CheckSubtasksCanUpdateRequest{SubtaskCfgTomlString: "password = secret-password"}
	output := requestLog(t, zap.Object("payload", req))
	require.Contains(t, output, invalidConfigForLog)
	require.NotContains(t, output, "secret-password")
}

func TestConfigLogPreservesDiagnosticFields(t *testing.T) {
	const yamlConfig = `name: task-one
threads: 4
enabled: true
extra: {future-option: [one, two]}
where: "id > 10"
dir: s3://internal-bucket/dump
`
	var yamlBefore, yamlAfter map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(yamlConfig), &yamlBefore))
	require.NoError(t, yaml.Unmarshal([]byte(redactYAMLConfig(yamlConfig)), &yamlAfter))
	require.Equal(t, yamlBefore, yamlAfter)

	const tomlConfig = "name = 'task-one'\nthreads = 4\nenabled = true\n[extra]\nfuture-option = ['one', 'two']\n"
	var tomlBefore, tomlAfter map[string]any
	_, err := toml.Decode(tomlConfig, &tomlBefore)
	require.NoError(t, err)
	_, err = toml.Decode(redactTOMLConfig(tomlConfig), &tomlAfter)
	require.NoError(t, err)
	require.Equal(t, tomlBefore, tomlAfter)
	require.Empty(t, redactYAMLConfig(""))
	require.Empty(t, redactTOMLConfig(""))
}

func requestLog(t *testing.T, field zap.Field) string {
	t.Helper()
	var output bytes.Buffer
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{MessageKey: "msg"}),
		zapcore.AddSync(&output),
		zap.InfoLevel,
	))
	logger.Info("request", field)
	require.NotContains(t, output.String(), "payloadError")
	return output.String()
}
