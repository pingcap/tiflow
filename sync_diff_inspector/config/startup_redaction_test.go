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

package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/log"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestParseCleanupFatalRedaction(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can traverse directories without execute permission")
	}
	cases := []struct {
		name   string
		config string
		flags  []string
		redact bool
	}{
		{name: "disabled by default"},
		{name: "enabled by config", config: "redact-info-log = true\n", redact: true},
		{name: "CLI enables", config: "redact-info-log = false\n", flags: []string{"--redact-info-log"}, redact: true},
		{name: "CLI disables", config: "redact-info-log = true\n", flags: []string{"--redact-info-log=false"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseDir := t.TempDir()
			configPath := filepath.Join(baseDir, "config.toml")
			require.NoError(t, os.WriteFile(configPath, []byte(tc.config), 0o600))
			blockedDir := filepath.Join(baseDir, "private-temp-directory")
			require.NoError(t, os.Mkdir(blockedDir, 0o700))
			t.Setenv("TMPDIR", blockedDir)
			if os.TempDir() != blockedDir {
				t.Skip("platform does not use TMPDIR")
			}
			require.NoError(t, os.Chmod(blockedDir, 0))
			t.Cleanup(func() { require.NoError(t, os.Chmod(blockedDir, 0o700)) })
			if err := os.RemoveAll(filepath.Join(blockedDir, "probe")); !os.IsPermission(err) {
				t.Skipf("filesystem does not enforce directory permissions: %v", err)
			}

			previous := dmlog.IsRedactLogEnabled()
			// The parser must use its resolved setting before main applies it
			// to the global logger, even when the current mode is opposite.
			dmlog.SetRedactLog(!tc.redact)
			t.Cleanup(func() { dmlog.SetRedactLog(previous) })
			core, observed := observer.New(zap.FatalLevel)
			logger := zap.New(core, zap.WithFatalHook(zapcore.WriteThenPanic))
			t.Cleanup(log.ReplaceGlobals(logger, &log.ZapProperties{}))
			cfg := NewConfig()
			args := append([]string{"--config", configPath}, tc.flags...)
			require.PanicsWithValue(t, "fail to remove the temp directory", func() { _ = cfg.Parse(args) })
			require.Equal(t, tc.redact, cfg.RedactInfoLog)
			require.Equal(t, !tc.redact, dmlog.IsRedactLogEnabled())
			require.Equal(t, blockedDir, filepath.Dir(cfg.Task.OutputDir))
			entries := observed.All()
			require.Len(t, entries, 1)
			fields := entries[0].ContextMap()
			if tc.redact {
				require.Equal(t, "?", fields["path"])
				require.Equal(t, "?", fields["error"])
				encoded, err := json.Marshal(fields)
				require.NoError(t, err)
				require.NotContains(t, string(encoded), "private-temp-directory")
			} else {
				err := os.RemoveAll(cfg.Task.OutputDir)
				require.Error(t, err)
				require.Equal(t, cfg.Task.OutputDir, fields["path"])
				require.Equal(t, err.Error(), fields["error"])
			}
			require.NotContains(t, fields, "errorVerbose")
		})
	}
}

func TestTaskInitFilterErrorRedaction(t *testing.T) {
	const secret = "PrivateFilterReviewMarker"
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("redact=%t", enabled), func(t *testing.T) {
			previous := dmlog.IsRedactLogEnabled()
			dmlog.SetRedactLog(enabled)
			t.Cleanup(func() { dmlog.SetRedactLog(previous) })
			core, observed := observer.New(zap.ErrorLevel)
			t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{}))
			task := TaskConfig{Target: "target", CheckTables: []string{"/[" + secret + "/.*"}}
			err := task.Init(map[string]*DataSource{"target": {}}, nil)
			require.ErrorContains(t, err, secret)
			require.Equal(t, []string{"/[" + secret + "/.*"}, task.CheckTables)
			entries := observed.All()
			require.Len(t, entries, 1)
			require.Equal(t, "parse check tables failed", entries[0].Message)
			fields := entries[0].ContextMap()
			if enabled {
				require.Equal(t, "?", fields["error"])
				require.NotContains(t, fields, "errorVerbose")
				encoded, err := json.Marshal(fields)
				require.NoError(t, err)
				require.NotContains(t, string(encoded), secret)
			} else {
				require.Contains(t, fields["error"], secret)
			}
		})
	}
}
