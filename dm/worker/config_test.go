// Copyright 2019 PingCAP, Inc.
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

package worker

import (
	"flag"
	"os"
	"strings"
	"testing"

	"github.com/kami-zh/go-capturer"
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

var (
	defaultConfigFile = "./dm-worker.toml"
	_                 = check.Suite(&testConfigSuite{})
)

type testConfigSuite struct{}

func (t *testConfigSuite) TestAdjustAddr(c *check.C) {
	cfg := NewConfig()
	c.Assert(cfg.configFromFile(defaultConfigFile), check.IsNil)
	c.Assert(cfg.adjust(), check.IsNil)

	// invalid `advertise-addr`
	cfg.AdvertiseAddr = "127.0.0.1"
	c.Assert(terror.ErrWorkerHostPortNotValid.Equal(cfg.adjust()), check.IsTrue)
	cfg.AdvertiseAddr = "0.0.0.0:8262"
	c.Assert(terror.ErrWorkerHostPortNotValid.Equal(cfg.adjust()), check.IsTrue)

	// clear `advertise-addr`, still invalid because no `host` in `worker-addr`.
	cfg.AdvertiseAddr = ""
	c.Assert(terror.ErrWorkerHostPortNotValid.Equal(cfg.adjust()), check.IsTrue)

	// TICASE-956
	cfg.WorkerAddr = "127.0.0.1:8262"
	c.Assert(cfg.adjust(), check.IsNil)
	c.Assert(cfg.AdvertiseAddr, check.Equals, cfg.WorkerAddr)
}

func (t *testConfigSuite) TestPrintSampleConfig(c *check.C) {
	buf, err := os.ReadFile(defaultConfigFile)
	c.Assert(err, check.IsNil)

	// test print sample config
	out := capturer.CaptureStdout(func() {
		cfg := NewConfig()
		err = cfg.Parse([]string{"-print-sample-config"})
		c.Assert(err, check.ErrorMatches, flag.ErrHelp.Error())
	})
	c.Assert(strings.TrimSpace(out), check.Equals, strings.TrimSpace(string(buf)))
}

func TestRedactInfoLog(t *testing.T) {
	require.True(t, NewConfig().RedactInfoLog)

	for _, tc := range []struct {
		name    string
		config  string
		args    []string
		enabled bool
	}{
		{name: "default", enabled: true},
		{name: "flag true", args: []string{"--redact-info-log=true"}, enabled: true},
		{name: "flag false", args: []string{"--redact-info-log=false"}},
		{name: "file omitted", config: "log-level = \"info\"", enabled: true},
		{name: "file true", config: "redact-info-log = true", enabled: true},
		{name: "file false", config: "redact-info-log = false"},
		{name: "flag overrides file true", config: "redact-info-log = true", args: []string{"--redact-info-log=false"}},
		{name: "flag overrides file false", config: "redact-info-log = false", args: []string{"--redact-info-log=true"}, enabled: true},
		{name: "omitted file preserves flag", config: "log-level = \"info\"", args: []string{"--redact-info-log=false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"--worker-addr=127.0.0.1:8262"}
			if tc.config != "" {
				path := t.TempDir() + "/worker.toml"
				require.NoError(t, os.WriteFile(path, []byte(tc.config), 0o600))
				args = append(args, "--config", path)
			}
			args = append(args, tc.args...)
			cfg := NewConfig()
			require.NoError(t, cfg.Parse(args))
			require.Equal(t, tc.enabled, cfg.RedactInfoLog)
		})
	}
}
