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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/spf13/cobra"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type utilsSuite struct{}

var _ = check.Suite(&utilsSuite{})

func (s *utilsSuite) TestProxyFields(c *check.C) {
	defer testleak.AfterTest(c)()
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
			c.Assert(os.Unsetenv(env), check.IsNil)
		}

		for i := 0; i < 3; i++ {
			if (1<<i)&mask != 0 {
				c.Assert(os.Setenv(envs[i], envPreset[i]), check.IsNil)
			}
		}

		for _, field := range findProxyFields() {
			idx, ok := revIndex[field.Key]
			c.Assert(ok, check.IsTrue)
			c.Assert((1<<idx)&mask, check.Not(check.Equals), 0)
			c.Assert(field.String, check.Equals, envPreset[idx])
		}
	}
}

func (s *utilsSuite) TestVerifyPdEndpoint(c *check.C) {
	defer testleak.AfterTest(c)()
	// empty URL.
	url := ""
	c.Assert(VerifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// invalid URL.
	url = "\n hi"
	c.Assert(VerifyPdEndpoint(url, false), check.ErrorMatches, ".*invalid control character in URL.*")

	// http URL without host.
	url = "http://"
	c.Assert(VerifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// https URL without host.
	url = "https://"
	c.Assert(VerifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// postgres scheme.
	url = "postgres://postgres@localhost/cargo_registry"
	c.Assert(VerifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// https scheme without TLS.
	url = "https://aa"
	c.Assert(VerifyPdEndpoint(url, false), check.ErrorMatches, ".*PD endpoint scheme is https, please provide certificate.*")

	// http scheme with TLS.
	url = "http://aa"
	c.Assert(VerifyPdEndpoint(url, true), check.ErrorMatches, ".*PD endpoint scheme should be https.*")

	// valid http URL.
	c.Assert(VerifyPdEndpoint("http://aa", false), check.IsNil)

	// valid https URL with TLS.
	c.Assert(VerifyPdEndpoint("https://aa", true), check.IsNil)
}

func (s *utilsSuite) TestStrictDecodeValidFile(c *check.C) {
	defer testleak.AfterTest(c)()
	dataDir := c.MkDir()
	tmpDir := c.MkDir()
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
	err := ioutil.WriteFile(configPath, []byte(configContent), 0o644)
	c.Assert(err, check.IsNil)

	conf := config.GetDefaultServerConfig()
	err = StrictDecodeFile(configPath, "test", conf)
	c.Assert(err, check.IsNil)
}

func (s *utilsSuite) TestStrictDecodeInvalidFile(c *check.C) {
	defer testleak.AfterTest(c)()
	dataDir := c.MkDir()
	tmpDir := c.MkDir()
	configPath := filepath.Join(tmpDir, "ticdc.toml")
	configContent := fmt.Sprintf(`
unknown = "128.0.0.1:1234"
data-dir = "%+v"

[log.unkown]
max-size = 200
max-days = 1
max-backups = 1
`, dataDir)
	err := ioutil.WriteFile(configPath, []byte(configContent), 0o644)
	c.Assert(err, check.IsNil)

	conf := config.GetDefaultServerConfig()
	err = StrictDecodeFile(configPath, "test", conf)
	c.Assert(err, check.ErrorMatches, ".*contained unknown configuration options.*")
}

func (s *utilsSuite) TestAndWriteExampleReplicaTOML(c *check.C) {
	defer testleak.AfterTest(c)()
	cfg := config.GetDefaultReplicaConfig()
	err := StrictDecodeFile("changefeed.toml", "cdc", &cfg)
	c.Assert(err, check.IsNil)

	c.Assert(cfg.CaseSensitive, check.IsTrue)
	c.Assert(cfg.Filter, check.DeepEquals, &config.FilterConfig{
		IgnoreTxnStartTs: []uint64{1, 2},
		Rules:            []string{"*.*", "!test.*"},
	})
	c.Assert(cfg.Mounter, check.DeepEquals, &config.MounterConfig{
		WorkerNum: 16,
	})
	c.Assert(cfg.Sink, check.DeepEquals, &config.SinkConfig{
		DispatchRules: []*config.DispatchRule{
			{Dispatcher: "ts", Matcher: []string{"test1.*", "test2.*"}},
			{Dispatcher: "rowid", Matcher: []string{"test3.*", "test4.*"}},
		},
		Protocol: "default",
	})
	c.Assert(cfg.Cyclic, check.DeepEquals, &config.CyclicConfig{
		Enable:          false,
		ReplicaID:       1,
		FilterReplicaID: []uint64{2, 3},
		SyncDDL:         true,
	})
}

func (s *utilsSuite) TestAndWriteExampleServerTOML(c *check.C) {
	defer testleak.AfterTest(c)()
	cfg := config.GetDefaultServerConfig()
	err := StrictDecodeFile("ticdc.toml", "cdc", &cfg)
	c.Assert(err, check.IsNil)
	defcfg := config.GetDefaultServerConfig()
	defcfg.AdvertiseAddr = "127.0.0.1:8300"
	defcfg.LogFile = "/tmp/ticdc/ticdc.log"
	c.Assert(cfg, check.DeepEquals, defcfg)
}

func (s *utilsSuite) TestJSONPrint(c *check.C) {
	defer testleak.AfterTest(c)()
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
	c.Assert(err, check.IsNil)

	output := `{
  "a": "string"
}
`
	c.Assert(b.String(), check.Equals, output)
}
