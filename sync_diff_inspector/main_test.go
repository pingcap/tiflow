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

func TestStartupDiagnostics(t *testing.T) {
	// Parse and startup failures exit, so exercise the real entry point separately.
	const helperEnv = "TIFLOW_TEST_STARTUP_DIAGNOSTICS"
	if os.Getenv(helperEnv) == "1" {
		os.Args = append([]string{"sync_diff_inspector"}, flag.Args()...)
		main()
		return
	}
	const secret = "PrivatePasswordReviewMarker"
	for _, tc := range []struct {
		name, want string
		flags      []string
	}{
		{"malformed visible", secret, []string{"--redact-info-log=false"}},
		{"malformed hidden", "Error: details redacted", []string{"--redact-info-log"}},
		{"missing config", "argument --config is required", []string{"--redact-info-log"}},
		{"invalid template", "Use --template dm or --template norm", []string{"--redact-info-log", "--template", secret}},
		{"log path", "please check log info in", []string{"--redact-info-log", "--check-thread-count=0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			outputDir := filepath.Join(dir, "output")
			args := append([]string{}, tc.flags...)
			if tc.name != "missing config" {
				content := "[data-sources.target]\npassword = " + secret
				if tc.name == "log path" {
					content = fmt.Sprintf("[data-sources.target]\n[task]\noutput-dir = %q\ntarget-instance = 'target'\ntarget-check-tables = ['*.*']\n", outputDir)
				}
				path := filepath.Join(dir, "config.toml")
				require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
				args = append(args, "--config", path)
			}
			cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestStartupDiagnostics$", "--"}, args...)...)
			cmd.Env = append(os.Environ(), helperEnv+"=1")
			output, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, "%s", output)
			require.Equal(t, 2, exitErr.ExitCode(), "%s", output)
			require.Contains(t, string(output), tc.want)
			if tc.name != "malformed visible" {
				require.NotContains(t, string(output), secret)
			}
			if tc.name == "log path" {
				require.Contains(t, string(output), filepath.Join(outputDir, config.LogFileName))
			}
		})
	}
}
