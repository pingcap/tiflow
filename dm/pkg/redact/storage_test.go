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
	"go.uber.org/zap/zaptest/observer"
)

func TestStorageConfigRedaction(t *testing.T) {
	url := "s3://bucket/path?access-key=private-ak&secret-access-key=private-sk&session-token=private-token&region=us-east-1"
	for _, tc := range []struct {
		name, input string
		redact      func(string) string
	}{
		{"yaml", "name: task-one\nloaders:\n  global:\n    dir: '" + url + "'\n", YAMLConfigWithRedactedURL},
		{"yaml-list", "name: task-one\nmysql-instances:\n- loader:\n    dir: '" + url + "'\n", YAMLConfigWithRedactedURL},
		{"yaml-numeric-key", "name: task-one\nloaders:\n  123:\n    dir: '" + url + "'\n", YAMLConfigWithRedactedURL},
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
		require.NotContains(t, YAMLConfigWithRedactedURL("dir: '"+url+"'"), "private-")
	}
}

func TestInvalidStorageConfig(t *testing.T) {
	for _, tc := range []struct {
		input  string
		redact func(string) string
	}{
		{"dir: [private-sk", YAMLConfigWithRedactedURL},
		{"dir: local\n---\ndir: private-sk", YAMLConfigWithRedactedURL},
	} {
		require.Equal(t, invalidConfig, tc.redact(tc.input))
	}
}

func TestStorageConfigURLFields(t *testing.T) {
	raw := "s3://bucket/path?access-key=private-ak&secret-access-key=private-sk&region=us-east-1"
	for _, key := range []string{"dir", "DIR", "data-dir", "data_dir", "output-dir", "outputdir"} {
		output := YAMLConfigWithRedactedURL("nested:\n- " + key + ": '" + raw + "'\n  unknown: 42\n")
		require.NotContains(t, output, "private-")
		require.Contains(t, output, "region=us-east-1")
		require.Contains(t, output, "unknown: 42")
	}
}

func TestStorageConfigInvalidURLFields(t *testing.T) {
	raw := "s3://bucket/path?secret-access-key=private-sk"
	for _, input := range []string{
		"name: task-one\ndir: ['" + raw + "']",
		"name: task-one\ndir: {unexpected: '" + raw + "'}",
	} {
		output := YAMLConfigWithRedactedURL(input)
		require.NotContains(t, output, "private-sk")
		require.Contains(t, output, "task-one")
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
		"s3://bucket/customer's-dump?access-key=private-ak&secret-access-key=private-sk",
		"https://private'user:private-password@host/path",
	} {
		require.Equal(t, "storage failed: "+objstore.RedactURL(raw)+" request id: 123",
			TextWithRedactedURL("storage failed: "+raw+" request id: 123"))
	}
	require.Contains(t, TextWithRedactedURL("s3://bucket/path?region=us-east-1&secret-access-key=private-sk"),
		"region=us-east-1")
}

func TestURLTextJSONEscaped(t *testing.T) {
	// encoding/json escapes '&' as \u0026, for example in checker results.
	// The sensitive parameters are not first, so they must be split correctly.
	raw := "s3://bucket/path?region=us-east-1&secret-access-key=private-sk&access-key=private-ak"
	once, err := json.Marshal(map[string]string{"dir": raw})
	require.NoError(t, err)
	require.Contains(t, string(once), `\u0026`)
	twice, err := json.Marshal(map[string]string{"result": string(once)})
	require.NoError(t, err)

	output := TextWithRedactedURL(string(once))
	require.NotContains(t, output, "private-")
	var decoded map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &decoded))
	require.Equal(t, objstore.RedactURL(raw), decoded["dir"])

	output = TextWithRedactedURL(string(twice))
	require.NotContains(t, output, "private-")
	var outer map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &outer))
	require.NoError(t, json.Unmarshal([]byte(outer["result"]), &decoded))
	require.Equal(t, objstore.RedactURL(raw), decoded["dir"])
}

func TestURLTextApostrophe(t *testing.T) {
	raw := "s3://bucket/customer's-dump?access-key=private-ak&secret-access-key=private-sk"
	output := TextWithRedactedURL("storage failed: " + raw)
	require.NotContains(t, output, "private-")
	require.Contains(t, output, "customer's-dump")

	// net/url.Error quotes the URL and keeps apostrophes in userinfo.
	output = TextWithRedactedURL(`Get "https://private'user:private-password@host/path": EOF`)
	require.NotContains(t, output, "private")
	require.Contains(t, output, `"https://host/path"`)

	// Quoted URLs keep their quotes, and adjacent ones are still redacted.
	require.Equal(t, "dir: '"+objstore.RedactURL("s3://bucket/path?secret-access-key=private-sk")+"'\nnext",
		TextWithRedactedURL("dir: 's3://bucket/path?secret-access-key=private-sk'\nnext"))
	output = TextWithRedactedURL("['s3://a/path?region=us-east-1','s3://b/path?secret-access-key=private-sk']")
	require.NotContains(t, output, "private-")

	once, err := json.Marshal(map[string]string{"dir": raw})
	require.NoError(t, err)
	require.NotContains(t, TextWithRedactedURL(string(once)), "private-")
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
