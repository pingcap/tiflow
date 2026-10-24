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

package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/stretchr/testify/require"
)

func TestStartupErrorRedaction(t *testing.T) {
	const helperEnv = "TIFLOW_TEST_STARTUP_ERROR"
	if os.Getenv(helperEnv) == "1" {
		os.Args = append([]string{"sync_diff_inspector"}, flag.Args()...)
		main()
		return
	}

	const secret = "PrivateStartupReviewMarker"
	for _, tc := range []struct {
		name    string
		flags   []string
		filter  string
		message string
	}{
		{name: "template", flags: []string{"--template", secret}, message: "unexpect template name"},
		{name: "log-level", flags: []string{"--log-level", secret}, message: "Log init failed!"},
		{name: "log-file", message: "Log init failed!"},
		{name: "config-init", filter: "/[" + secret + "/.*", message: "Fail to initialize config."},
		{name: "config-check", flags: []string{"--check-thread-count=0"}, message: "There is something wrong with your config"},
	} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/redact=%t", tc.name, enabled), func(t *testing.T) {
				dir := t.TempDir()
				outputDir := filepath.Join(dir, secret)
				if tc.name == "log-file" {
					require.NoError(t, os.WriteFile(outputDir, nil, 0o600))
				}
				filter := tc.filter
				if filter == "" {
					filter = "*.*"
				}
				configPath := filepath.Join(dir, "config.toml")
				content := fmt.Sprintf("[data-sources.target]\n[task]\noutput-dir = %q\ntarget-instance = 'target'\ntarget-check-tables = [%q]\n", outputDir, filter)
				require.NoError(t, os.WriteFile(configPath, []byte(content), 0o600))
				args := []string{"-test.run=^TestStartupErrorRedaction$", "--", "--config", configPath, fmt.Sprintf("--redact-info-log=%t", enabled)}
				cmd := exec.Command(os.Args[0], append(args, tc.flags...)...)
				cmd.Env = append(os.Environ(), helperEnv+"=1")
				output, err := cmd.CombinedOutput()
				var exitErr *exec.ExitError
				require.ErrorAs(t, err, &exitErr, "%s", output)
				require.Equal(t, 2, exitErr.ExitCode(), "%s", output)
				text := string(output)
				if tc.name == "template" && enabled {
					require.Equal(t, "?\n", text)
				} else {
					require.Contains(t, text, tc.message)
				}
				if tc.name == "config-init" || tc.name == "config-check" {
					// Inspect the file as well as stdout/stderr so an earlier
					// diagnostic cannot expose the same value before startup exits.
					logOutput, err := os.ReadFile(filepath.Join(outputDir, config.LogFileName))
					require.NoError(t, err)
					text += string(logOutput)
				}
				if enabled {
					require.NotContains(t, text, secret)
				} else {
					require.Contains(t, text, secret)
				}
			})
		}
	}
}

func TestConfigParseErrorRedaction(t *testing.T) {
	// Run the real entry point in a subprocess because parse failures call os.Exit.
	const helperEnv = "TIFLOW_TEST_CONFIG_PARSE_ERROR"
	if os.Getenv(helperEnv) == "1" {
		os.Args = append([]string{"sync_diff_inspector"}, flag.Args()...)
		main()
		return
	}

	const secret = "PrivatePasswordReviewMarker"
	const malformed = "[data-sources.mysql]\npassword = " + secret + "\n"
	tests := []struct {
		name   string
		config string
		flags  []string
		redact bool
	}{
		{name: "disabled by default", config: malformed},
		{name: "explicitly disabled", config: malformed, flags: []string{"--redact-info-log=false"}},
		{name: "enabled", config: malformed, flags: []string{"--redact-info-log"}, redact: true},
		{
			name: "enabled with config opt-out", config: "redact-info-log = false\n" + malformed,
			flags: []string{"--redact-info-log"}, redact: true,
		},
		{
			name: "disabled with config opt-in", config: "redact-info-log = true\n" + malformed,
			flags: []string{"--redact-info-log=false"},
		},
		{
			name: "enabled by config with unknown key", config: "redact-info-log = true\n" + secret + " = true\n",
			redact: true,
		},
		{
			name: "enabled with unknown key", config: "redact-info-log = false\n" + secret + " = true\n",
			flags: []string{"--redact-info-log"}, redact: true,
		},
		{
			name: "disabled with unknown key", config: "redact-info-log = true\n" + secret + " = true\n",
			flags: []string{"--redact-info-log=false"},
		},
		{
			name: "last flag enables redaction", config: malformed,
			flags: []string{"--redact-info-log=false", "--redact-info-log"}, redact: true,
		},
		{
			name: "last flag disables redaction", config: malformed,
			flags: []string{"--redact-info-log", "--redact-info-log=false"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.toml")
			require.NoError(t, os.WriteFile(configPath, []byte(test.config), 0o600))
			for _, configFirst := range []bool{false, true} {
				args := []string{"--config", configPath}
				if configFirst {
					args = append(args, test.flags...)
				} else {
					args = append(append([]string{}, test.flags...), args...)
				}
				cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestConfigParseErrorRedaction$", "--"}, args...)...)
				cmd.Env = append(os.Environ(), helperEnv+"=1")
				output, err := cmd.CombinedOutput()
				var exitErr *exec.ExitError
				require.ErrorAs(t, err, &exitErr, "%s", output)
				require.Equal(t, 2, exitErr.ExitCode(), "%s", output)
				text := string(output)
				require.Contains(t, text, "--redact-info-log")
				if test.redact {
					require.Contains(t, text, "Error: ?\n")
					require.NotContains(t, text, secret)
				} else {
					require.Contains(t, text, secret)
					require.NotContains(t, text, "Error: ?\n")
				}
			}
		})
	}
}
