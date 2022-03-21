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

package util

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestProxyFields(t *testing.T) {
	defer testleak.AfterTest(t)()
	revIndex := map[string]int{
		"http_proxy":  0,
		"https_proxy": 1,
		"no_proxy":    2,
	}
	envs := []string{"http_proxy", "https_proxy", "no_proxy"}
	envPreset := []string{"http://127.0.0.1:8080", "https://127.0.0.1:8443", "localhost,127.0.0.1"}

	// Exhaust all combinations of those environment variables' selection.
	// Each bit of the mask decided whether this index of `envs` would be set.
	for mask := 0; mask <= 0b111; mask++ {
		for _, env := range envs {
			require.Nil(t, os.Unsetenv(env))
		}

		for i := 0; i < 3; i++ {
			if (1<<i)&mask != 0 {
				require.Nil(t, os.Setenv(envs[i], envPreset[i]))
			}
		}

		for _, field := range findProxyFields() {
			idx, ok := revIndex[field.Key]
			require.True(t, ok)
			require.NotEqual(t, 0, (1<<idx)&mask)
			require.Equal(t, envPreset[idx], field.String)
		}
	}
}

func TestVerifyPdEndpoint(t *testing.T) {
	defer testleak.AfterTest(t)()
	// empty URL.
	url := ""
	require.Error(t, VerifyPdEndpoint(url, false), ".*PD endpoint should be a valid http or https URL.*")

	// invalid URL.
	url = "\n hi"
	require.Error(t, VerifyPdEndpoint(url, false), ".*invalid control character in URL.*")

	// http URL without host.
	url = "http://"
	require.Error(t, VerifyPdEndpoint(url, false), ".*PD endpoint should be a valid http or https URL.*")

	// https URL without host.
	url = "https://"
	require.Error(t, VerifyPdEndpoint(url, false), ".*PD endpoint should be a valid http or https URL.*")

	// postgres scheme.
	url = "postgres://postgres@localhost/cargo_registry"
	require.Error(t, VerifyPdEndpoint(url, false), ".*PD endpoint should be a valid http or https URL.*")

	// https scheme without TLS.
	url = "https://aa"
	require.Error(t, VerifyPdEndpoint(url, false), ".*PD endpoint scheme is https, please provide certificate.*")

	// http scheme with TLS.
	url = "http://aa"
	require.Error(t, VerifyPdEndpoint(url, true), ".*PD endpoint scheme should be https.*")

	// valid http URL.
	require.Nil(t, VerifyPdEndpoint("http://aa", false))

	// valid https URL with TLS.
	require.Nil(t, VerifyPdEndpoint("https://aa", true))
}

func TestStrictDecodeValidFile(t *testing.T) {
	defer testleak.AfterTest(t)()
	dataDir := t.TempDir()
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "ticdc.toml")
	configContent := fmt.Sprintf(`
addr = "128.0.0.1:1234"
advertise-addr = "127.0.0.1:1111"

log-file = "/root/cdc1.log"
log-level = "warn"

data-dir = "%+v"
gc-ttl = 500
tz = "US"
capture-session-ttl = 10

owner-flush-interval = "600ms"
processor-flush-interval = "600ms"

[log.file]
max-size = 200
max-days = 1
max-backups = 1

[sorter]
chunk-size-limit = 10000000
max-memory-consumption = 2000000
max-memory-percentage = 3
num-concurrent-worker = 4
num-workerpool-goroutine = 5
sort-dir = "/tmp/just_a_test"

[security]
ca-path = "aa"
cert-path = "bb"
key-path = "cc"
cert-allowed-cn = ["dd","ee"]
`, dataDir)
	err := os.WriteFile(configPath, []byte(configContent), 0o644)
	require.Nil(t, err)

	conf := config.GetDefaultServerConfig()
	err = StrictDecodeFile(configPath, "test", conf)
	require.Nil(t, err)
}

func TestStrictDecodeInvalidFile(t *testing.T) {
	defer testleak.AfterTest(t)()
	dataDir := t.TempDir()
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "ticdc.toml")
	configContent := fmt.Sprintf(`
unknown = "128.0.0.1:1234"
data-dir = "%+v"

[log.unkown]
max-size = 200
max-days = 1
max-backups = 1
`, dataDir)
	err := os.WriteFile(configPath, []byte(configContent), 0o644)
	require.Nil(t, err)

	conf := config.GetDefaultServerConfig()
	err = StrictDecodeFile(configPath, "test", conf)
	require.Error(t, err, ".*contained unknown configuration options.*")
}

func TestAndWriteExampleReplicaTOML(t *testing.T) {
	defer testleak.AfterTest(t)()
	cfg := config.GetDefaultReplicaConfig()
	err := StrictDecodeFile("changefeed.toml", "cdc", &cfg)
	require.Nil(t, err)

	require.True(t, cfg.CaseSensitive)
	require.Equal(t, &config.FilterConfig{
		IgnoreTxnStartTs: []uint64{1, 2},
		Rules:            []string{"*.*", "!test.*"},
	}, cfg.Filter)
	require.Equal(t, &config.MounterConfig{
		WorkerNum: 16,
	}, cfg.Mounter)
	require.Equal(t, &config.SinkConfig{
		DispatchRules: []*config.DispatchRule{
			{PartitionRule: "ts", TopicRule: "hello_{schema}", Matcher: []string{"test1.*", "test2.*"}},
			{PartitionRule: "rowid", TopicRule: "{schema}_world", Matcher: []string{"test3.*", "test4.*"}},
		},
		ColumnSelectors: []*config.ColumnSelector{
			{Matcher: []string{"test1.*", "test2.*"}, Columns: []string{"column1", "column2"}},
			{Matcher: []string{"test3.*", "test4.*"}, Columns: []string{"!a", "column3"}},
		},
		Protocol: "open-protocol",
	}, cfg.Sink)
	require.Equal(t, &config.CyclicConfig{
		Enable:          false,
		ReplicaID:       1,
		FilterReplicaID: []uint64{2, 3},
		SyncDDL:         true,
	}, cfg.Cyclic)

}

func TestAndWriteExampleServerTOML(t *testing.T) {
	defer testleak.AfterTest(t)()
	cfg := config.GetDefaultServerConfig()
	err := StrictDecodeFile("ticdc.toml", "cdc", &cfg)
	require.Nil(t, err)
	defcfg := config.GetDefaultServerConfig()
	defcfg.AdvertiseAddr = "127.0.0.1:8300"
	defcfg.LogFile = "/tmp/ticdc/ticdc.log"
	require.Equal(t, defcfg, cfg)
}

func TestJSONPrint(t *testing.T) {
	defer testleak.AfterTest(t)()
	cmd := new(cobra.Command)
	type testStruct struct {
		A string `json:"a"`
	}

	data := testStruct{
		A: "string",
	}

	var b bytes.Buffer
	cmd.SetOut(&b)

	err := JSONPrint(cmd, &data)
	require.Nil(t, err)

	output := `{
  "a": "string"
}
`
	require.Equal(t, output, b.String())
}

func TestIgnoreStrictCheckItem(t *testing.T) {
	defer testleak.AfterTest(t)()
	dataDir := t.TempDir()
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "ticdc.toml")
	configContent := fmt.Sprintf(`
data-dir = "%+v"
[unknown]
max-size = 200
max-days = 1
max-backups = 1
`, dataDir)
	err := os.WriteFile(configPath, []byte(configContent), 0o644)
	require.Nil(t, err)

	conf := config.GetDefaultServerConfig()
	err = StrictDecodeFile(configPath, "test", conf, "unknown")
	require.Nil(t, err)

	configContent = fmt.Sprintf(`
data-dir = "%+v"
[unknown]
max-size = 200
max-days = 1
max-backups = 1
[unknown2]
max-size = 200
max-days = 1
max-backups = 1
`, dataDir)
	err = os.WriteFile(configPath, []byte(configContent), 0o644)
	require.Nil(t, err)

	err = StrictDecodeFile(configPath, "test", conf, "unknown")
	require.Error(t, err, ".*contained unknown configuration options: unknown2.*")

	configContent = fmt.Sprintf(`
data-dir = "%+v"
[debug]
unknown = 1
`, dataDir)
	err = os.WriteFile(configPath, []byte(configContent), 0o644)
	require.Nil(t, err)

	err = StrictDecodeFile(configPath, "test", conf, "debug")
	require.Nil(t, err)
}
