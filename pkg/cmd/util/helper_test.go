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
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestProxyFields(t *testing.T) {
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
			require.Equal(t, field.String, envPreset[idx])
		}
	}
}

func TestVerifyPdEndpoint(t *testing.T) {
	// empty URL.
	url := ""
	require.Regexp(t, ".*PD endpoint should be a valid http or https URL.*",
		VerifyPdEndpoint(url, false))

	// invalid URL.
	url = "\n hi"
	require.Regexp(t, ".*invalid control character in URL.*",
		VerifyPdEndpoint(url, false))

	// http URL without host.
	url = "http://"
	require.Regexp(t, ".*PD endpoint should be a valid http or https URL.*",
		VerifyPdEndpoint(url, false))

	// https URL without host.
	url = "https://"
	require.Regexp(t, ".*PD endpoint should be a valid http or https URL.*",
		VerifyPdEndpoint(url, false))

	// postgres scheme.
	url = "postgres://postgres@localhost/cargo_registry"
	require.Regexp(t, ".*PD endpoint should be a valid http or https URL.*",
		VerifyPdEndpoint(url, false))

	// https scheme without TLS.
	url = "https://aa"
	require.Regexp(t, ".*PD endpoint scheme is https, please provide certificate.*",
		VerifyPdEndpoint(url, false))

	// http scheme with TLS.
	url = "http://aa"
	require.Regexp(t, ".*PD endpoint scheme should be https.*", VerifyPdEndpoint(url, true))

	// valid http URL.
	require.Nil(t, VerifyPdEndpoint("http://aa", false))

	// valid https URL with TLS.
	require.Nil(t, VerifyPdEndpoint("https://aa", true))
}

func TestStrictDecodeValidFile(t *testing.T) {
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
	require.Contains(t, err.Error(), "contained unknown configuration options")
}

func TestAndWriteExampleReplicaTOML(t *testing.T) {
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

	sinkURL, err := url.Parse("kafka://127.0.0.1:9092")
	require.NoError(t, err)

	err = cfg.ValidateAndAdjust(sinkURL)
	require.NoError(t, err)
	require.Equal(t, &config.SinkConfig{
		EncoderConcurrency: util.AddressOf(config.DefaultEncoderGroupConcurrency),
		DispatchRules: []*config.DispatchRule{
			{PartitionRule: "ts", TopicRule: "hello_{schema}", Matcher: []string{"test1.*", "test2.*"}},
			{PartitionRule: "rowid", TopicRule: "{schema}_world", Matcher: []string{"test3.*", "test4.*"}},
		},
		ColumnSelectors: []*config.ColumnSelector{
			{Matcher: []string{"test1.*", "test2.*"}, Columns: []string{"column1", "column2"}},
			{Matcher: []string{"test3.*", "test4.*"}, Columns: []string{"!a", "column3"}},
		},
		CSVConfig: &config.CSVConfig{
			Quote:                string(config.DoubleQuoteChar),
			Delimiter:            string(config.Comma),
			NullString:           config.NULL,
			BinaryEncodingMethod: config.BinaryEncodingBase64,
		},
		Terminator:                       util.AddressOf("\r\n"),
		DateSeparator:                    util.AddressOf(config.DateSeparatorDay.String()),
		EnablePartitionSeparator:         util.AddressOf(true),
		EnableKafkaSinkV2:                util.AddressOf(false),
		OnlyOutputUpdatedColumns:         util.AddressOf(false),
		DeleteOnlyOutputHandleKeyColumns: util.AddressOf(false),
		Protocol:                         util.AddressOf("open-protocol"),
		AdvanceTimeoutInSec:              util.AddressOf(uint(150)),
		SendBootstrapIntervalInSec:       util.AddressOf(int64(120)),
		SendBootstrapInMsgCount:          util.AddressOf(int32(10000)),
		SendBootstrapToAllPartition:      util.AddressOf(true),
		OpenProtocol:                     &config.OpenProtocolConfig{OutputOldValue: true},
	}, cfg.Sink)
}

func TestAndWriteStorageSinkTOML(t *testing.T) {
	cfg := config.GetDefaultReplicaConfig()
	err := StrictDecodeFile("changefeed_storage_sink.toml", "cdc", &cfg)
	require.NoError(t, err)

	sinkURL, err := url.Parse("s3://127.0.0.1:9092")
	require.NoError(t, err)

	cfg.Sink.Protocol = util.AddressOf(config.ProtocolCanalJSON.String())
	err = cfg.ValidateAndAdjust(sinkURL)
	require.NoError(t, err)
	require.Equal(t, &config.SinkConfig{
		Protocol:                 util.AddressOf(config.ProtocolCanalJSON.String()),
		EncoderConcurrency:       util.AddressOf(config.DefaultEncoderGroupConcurrency),
		Terminator:               util.AddressOf(config.CRLF),
		TxnAtomicity:             util.AddressOf(config.AtomicityLevel("")),
		DateSeparator:            util.AddressOf("day"),
		EnablePartitionSeparator: util.AddressOf(true),
		FileIndexWidth:           util.AddressOf(config.DefaultFileIndexWidth),
		EnableKafkaSinkV2:        util.AddressOf(false),
		CSVConfig: &config.CSVConfig{
			Delimiter:            ",",
			Quote:                "\"",
			NullString:           "\\N",
			IncludeCommitTs:      false,
			BinaryEncodingMethod: config.BinaryEncodingBase64,
		},
		OnlyOutputUpdatedColumns:         util.AddressOf(false),
		DeleteOnlyOutputHandleKeyColumns: util.AddressOf(false),
		AdvanceTimeoutInSec:              util.AddressOf(uint(150)),
		SendBootstrapIntervalInSec:       util.AddressOf(int64(120)),
		SendBootstrapInMsgCount:          util.AddressOf(int32(10000)),
		SendBootstrapToAllPartition:      util.AddressOf(true),
		OpenProtocol:                     &config.OpenProtocolConfig{OutputOldValue: true},
	}, cfg.Sink)
}

func TestAndWriteExampleServerTOML(t *testing.T) {
	cfg := config.GetDefaultServerConfig()
	err := StrictDecodeFile("ticdc.toml", "cdc", &cfg)
	require.Nil(t, err)
	defcfg := config.GetDefaultServerConfig()
	defcfg.AdvertiseAddr = "127.0.0.1:8300"
	defcfg.LogFile = "/tmp/ticdc/ticdc.log"
	require.Equal(t, defcfg, cfg)
}

func TestJSONPrint(t *testing.T) {
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
	require.Contains(t, err.Error(), "contained unknown configuration options: unknown2")

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

func TestInitSignalHandlingGracefulShutdown(t *testing.T) {
	shutdownCh := make(chan struct{}, 1)
	shutdown := func() <-chan struct{} { return shutdownCh }
	cancelCh := make(chan struct{}, 1)
	cancel := func() { cancelCh <- struct{}{} }
	InitSignalHandling(shutdown, cancel)
	self, err := os.FindProcess(os.Getpid())
	require.Nil(t, err)

	// First signal for preparing shutdown.
	err = self.Signal(syscall.SIGTERM)
	require.Nil(t, err)
	select {
	case <-shutdownCh:
		require.Fail(t, "unexpected")
	case <-cancelCh:
		require.Fail(t, "unexpected")
	case <-time.After(100 * time.Millisecond):
	}

	// Graceful shutdown complete.
	shutdownCh <- struct{}{}
	select {
	case <-cancelCh:
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "timeout")
	}
}

func TestInitSignalHandlingForceShutdown(t *testing.T) {
	shutdownCh := make(chan struct{}, 1)
	shutdown := func() <-chan struct{} { return shutdownCh }
	cancelCh := make(chan struct{}, 1)
	cancel := func() { cancelCh <- struct{}{} }
	InitSignalHandling(shutdown, cancel)
	self, err := os.FindProcess(os.Getpid())
	require.Nil(t, err)
	err = self.Signal(syscall.SIGTERM)
	require.Nil(t, err)
	// Second signal for force shutdown.
	// We use another signal, to avoid lost signal, because sending a signal
	// is setting a bit in Unix.
	err = self.Signal(syscall.SIGQUIT)
	require.Nil(t, err)
	select {
	case <-cancelCh:
	case <-time.After(1 * time.Second):
		require.Fail(t, "timeout")
	}
}
