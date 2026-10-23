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

package redact

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestStorageConfigRedaction(t *testing.T) {
	url := "s3://bucket/path?access-key=private-ak&secret-access-key=private-sk&session-token=private-token&region=us-east-1"
	redactJSON := func(input string) string {
		return ConfigFieldWithRedactedURL("config", json.RawMessage(input)).String
	}
	for _, tc := range []struct {
		name, input string
		redact      func(string) string
	}{
		{"yaml", "name: task-one\nloaders:\n  global:\n    dir: '" + url + "'\n", YAMLConfigWithRedactedURL},
		{"yaml-list", "name: task-one\nmysql-instances:\n- loader:\n    dir: '" + url + "'\n", YAMLConfigWithRedactedURL},
		{"json", `{"name":"task-one","dir":"` + url + `"}`, redactJSON},
		{"toml", "name = 'task-one'\n[[loaders]]\ndir = '" + url + "'\n", TOMLConfigWithRedactedURL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := tc.redact(tc.input)
			require.NotContains(t, output, "private-")
			require.Contains(t, output, "task-one")
			require.Contains(t, tc.input, "private-sk")
			require.Contains(t, output, "bucket/path")
			require.Contains(t, output, "region=us-east-1")
		})
	}
	for _, url := range []string{
		"s3://bucket:bad/path?secret-access-key=private-sk",
		"s3://private-ak:private-sk@bucket/path",
		"azure://container/path?account-key=private-sk",
	} {
		field := ConfigFieldWithRedactedURL("config", map[string]string{"dir": url})
		require.NotContains(t, field.String, "private-")
	}
}

func TestStorageConfigPreservesOriginal(t *testing.T) {
	cfg := map[string]any{
		"name": "task-one",
		"dir":  "s3://bucket/path?secret-access-key=private-sk",
		"meta": map[string]any{"position": uint64(9007199254740993)},
	}
	before, err := json.Marshal(cfg)
	require.NoError(t, err)
	field := ConfigFieldWithRedactedURL("config", cfg)
	require.Equal(t, zapcore.StringType, field.Type)
	require.NotContains(t, field.String, "private-sk")
	require.Contains(t, field.String, "9007199254740993")
	after, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.JSONEq(t, `{"dir":"./dumped data","unknown":42}`,
		ConfigFieldWithRedactedURL("config", map[string]any{"dir": "./dumped data", "unknown": 42}).String)
}

func TestInvalidStorageConfig(t *testing.T) {
	redactJSON := func(input string) string {
		return ConfigFieldWithRedactedURL("config", json.RawMessage(input)).String
	}
	for _, tc := range []struct {
		input  string
		redact func(string) string
	}{
		{"dir: [private-sk", YAMLConfigWithRedactedURL},
		{"dir: local\n---\ndir: private-sk", YAMLConfigWithRedactedURL},
		{`dir = "private-sk`, TOMLConfigWithRedactedURL},
		{`{"dir":"private-sk"`, redactJSON},
		{`{} {"dir":"private-sk"}`, redactJSON},
	} {
		require.Equal(t, invalidConfig, tc.redact(tc.input))
	}
	require.Equal(t, invalidConfig,
		ConfigFieldWithRedactedURL("config", map[string]any{"dir": "private-sk", "invalid": make(chan int)}).String)
}

func TestStorageConfigURLFields(t *testing.T) {
	raw := "s3://bucket/path?access-key=private-ak&secret-access-key=private-sk&region=us-east-1"
	for _, key := range []string{"dir", "DIR", "data-dir", "data_dir", "output-dir", "outputdir"} {
		cfg := map[string]any{"nested": []any{map[string]any{key: raw, "unknown": 42}}}
		before, err := json.Marshal(cfg)
		require.NoError(t, err)
		field := ConfigFieldWithRedactedURL("config", cfg)
		require.NotContains(t, field.String, "private-")
		require.Contains(t, field.String, "region=us-east-1")
		require.Contains(t, field.String, `"unknown":42`)
		after, err := json.Marshal(cfg)
		require.NoError(t, err)
		require.Equal(t, before, after)
	}
}

func TestURLTextCredentials(t *testing.T) {
	for _, text := range []string{
		"AccessDenied: s3://bucket/path?access-key=private-ak&secret-access-key=private-sk request id: test-id",
		"Get \"https://bucket/path?X-Amz-Signature=private-sig\": status code: 403 request id: test-id",
		"lstat s3:/bucket/path?secret-access-key=private-key: no such file request id: test-id",
		"read https://private-user:private-pass@host/path request id: test-id",
		"s3://bad:port/path?secret-access-key=private-sk request id: test-id",
	} {
		output := TextWithRedactedURL(text)
		require.NotContains(t, output, "private-")
		require.Contains(t, output, "request id: test-id")
	}
	require.Equal(t, "ordinary error 403", TextWithRedactedURL("ordinary error 403"))
	require.Equal(t, "read s3://bucket/path", TextWithRedactedURL("read s3://bucket/path"))
	err := errors.New("403 https://bucket/path?sig=private-signature")
	core, logs := observer.New(zap.ErrorLevel)
	zap.New(core).Error("storage failed", ErrorFieldWithRedactedURL(err))
	fields := logs.All()[0].ContextMap()
	require.NotContains(t, fields["error"], "private-")
	require.NotContains(t, fields, "errorVerbose")
	require.Contains(t, err.Error(), "private-signature")
}

func TestURLTextUsesTiDBPolicy(t *testing.T) {
	for _, raw := range []string{
		"s3://bucket/path?region=us-east-1&secret-access-key=private-sk",
		"s3:/bucket/path?secret-access-key=private-sk",
		"azure://container/path?sas-token=private-token",
		"https://bucket/path?X-Amz-Signature=private-signature",
		"https://private-user:private-password@host/path",
		"s3://bucket:bad/path?secret-access-key=private-sk",
	} {
		require.Equal(t, "storage failed: "+objstore.RedactURL(raw)+" request id: 123",
			TextWithRedactedURL("storage failed: "+raw+" request id: 123"))
	}
	require.Contains(t, TextWithRedactedURL("s3://bucket/path?region=us-east-1&secret-access-key=private-sk"),
		"region=us-east-1")
}

func TestProxyURL(t *testing.T) {
	for _, raw := range []string{
		"http://private-user:private-password@proxy.invalid:8080",
		"private-user:private-password@proxy.invalid:8080",
		"https://proxy.invalid/?token=private-token",
		"http://private-user:private-password@proxy.invalid:bad-port",
	} {
		require.NotContains(t, RedactedProxyURL(raw), "private-")
	}
	require.Equal(t, "proxy.invalid:8080", RedactedProxyURL("proxy.invalid:8080"))
	require.Equal(t, "http://proxy.invalid:8080", RedactedProxyURL("http://proxy.invalid:8080"))
}
