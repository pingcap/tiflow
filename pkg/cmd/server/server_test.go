// Copyright 2020 PingCAP, Inc.
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

package server

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	ticonfig "github.com/pingcap/tidb/config"
	"github.com/spf13/cobra"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type serverSuite struct{}

var _ = check.Suite(&serverSuite{})

func (s *serverSuite) TestPatchTiDBConf(c *check.C) {
	defer testleak.AfterTest(c)()
	patchTiDBConf()
	cfg := ticonfig.GetGlobalConfig()
	c.Assert(cfg.TiKVClient.MaxBatchSize, check.Equals, uint(0))
}

func (s *serverSuite) TestValidateWithEmptyPdAddress(c *check.C) {
	defer testleak.AfterTest(c)()
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	c.Assert(cmd.ParseFlags([]string{"--pd="}), check.IsNil)
	err := o.complete(cmd)
	c.Assert(err, check.IsNil)
	err = o.validate()
	c.Assert(err, check.ErrorMatches, ".*empty PD address.*")
}

func (s *serverSuite) TestValidateWithInvalidPdAddress(c *check.C) {
	defer testleak.AfterTest(c)()
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	c.Assert(cmd.ParseFlags([]string{"--pd=aa"}), check.IsNil)
	err := o.complete(cmd)
	c.Assert(err, check.IsNil)
	err = o.validate()
	c.Assert(err, check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")
}

func (s *serverSuite) TestValidateWithInvalidPdAddressWithoutHost(c *check.C) {
	defer testleak.AfterTest(c)()
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	c.Assert(cmd.ParseFlags([]string{"--pd=http://"}), check.IsNil)
	err := o.complete(cmd)
	c.Assert(err, check.IsNil)
	err = o.validate()
	c.Assert(err, check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")
}

func (s *serverSuite) TestValidateWithHttpsPdAddressWithoutCertificate(c *check.C) {
	defer testleak.AfterTest(c)()
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	c.Assert(cmd.ParseFlags([]string{"--pd=https://aa"}), check.IsNil)
	err := o.complete(cmd)
	c.Assert(err, check.IsNil)
	err = o.validate()
	c.Assert(err, check.ErrorMatches, ".*PD endpoint scheme is https, please provide certificate.*")
}

func (s *serverSuite) TestAddUnknownFlag(c *check.C) {
	defer testleak.AfterTest(c)()
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	c.Assert(cmd.ParseFlags([]string{"--PD="}), check.ErrorMatches, ".*unknown flag: --PD.*")
}

func (s *serverSuite) TestDefaultCfg(c *check.C) {
	defer testleak.AfterTest(c)()
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	c.Assert(cmd.ParseFlags([]string{}), check.IsNil)
	err := o.complete(cmd)
	c.Assert(err, check.IsNil)

	defaultCfg := config.GetDefaultServerConfig()
	c.Assert(defaultCfg.ValidateAndAdjust(), check.IsNil)
	c.Assert(o.serverConfig, check.DeepEquals, defaultCfg)
	c.Assert(o.serverPdAddr, check.Equals, "http://127.0.0.1:2379")
}

func (s *serverSuite) TestParseCfg(c *check.C) {
	defer testleak.AfterTest(c)()
	dataDir := c.MkDir()
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	c.Assert(cmd.ParseFlags([]string{
		"--addr", "127.5.5.1:8833",
		"--advertise-addr", "127.5.5.1:7777",
		"--log-file", "/root/cdc.log",
		"--log-level", "debug",
		"--data-dir", dataDir,
		"--gc-ttl", "10",
		"--tz", "UTC",
		"--owner-flush-interval", "150ms",
		"--processor-flush-interval", "150ms",
		"--cert", "bb",
		"--key", "cc",
		"--cert-allowed-cn", "dd,ee",
		"--sorter-chunk-size-limit", "50000000",
		"--sorter-max-memory-consumption", "60000",
		"--sorter-max-memory-percentage", "70",
		"--sorter-num-concurrent-worker", "80",
		"--sorter-num-workerpool-goroutine", "90",
		"--sort-dir", "/tmp/just_a_test",
	}), check.IsNil)

	err := o.complete(cmd)
	c.Assert(err, check.IsNil)
	err = o.validate()
	c.Assert(err, check.IsNil)
	c.Assert(o.serverConfig, check.DeepEquals, &config.ServerConfig{
		Addr:          "127.5.5.1:8833",
		AdvertiseAddr: "127.5.5.1:7777",
		LogFile:       "/root/cdc.log",
		LogLevel:      "debug",
		Log: &config.LogConfig{
			File: &config.LogFileConfig{
				MaxSize:    300,
				MaxDays:    0,
				MaxBackups: 0,
			},
		},
		DataDir:                dataDir,
		GcTTL:                  10,
		TZ:                     "UTC",
		CaptureSessionTTL:      10,
		OwnerFlushInterval:     config.TomlDuration(150 * time.Millisecond),
		ProcessorFlushInterval: config.TomlDuration(150 * time.Millisecond),
		Sorter: &config.SorterConfig{
			NumConcurrentWorker:    80,
			ChunkSizeLimit:         50000000,
			MaxMemoryPressure:      70,
			MaxMemoryConsumption:   60000,
			NumWorkerPoolGoroutine: 90,
			SortDir:                config.DefaultSortDir,
		},
		Security: &config.SecurityConfig{
			CertPath:      "bb",
			KeyPath:       "cc",
			CertAllowedCN: []string{"dd", "ee"},
		},
		PerTableMemoryQuota: 20 * 1024 * 1024, // 20M
		KVClient: &config.KVClientConfig{
			WorkerConcurrent: 8,
			WorkerPoolSize:   0,
			RegionScanLimit:  40,
		},
		SchedulerV2: &config.SchedulerV2Config{
			Enabled:                      true,
			ProcessorCheckpointInterval:  config.TomlDuration(time.Millisecond * 200),
			ClientMaxBatchInterval:       config.TomlDuration(time.Millisecond * 100),
			ClientMaxBatchSize:           8192,
			ClientRetryRateLimit:         1.0,
			ServerMaxPendingMessageCount: 102400,
			ServerAckInterval:            config.TomlDuration(time.Millisecond * 100),
			ServerWorkerPoolSize:         4,
		},
	})
}

func (s *serverSuite) TestDecodeCfg(c *check.C) {
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

[scheduler-v2]
enabled = true
processor-checkpoint-interval = "15ms"
client-retry-rate-limit = 0.2
`, dataDir)
	err := ioutil.WriteFile(configPath, []byte(configContent), 0o644)
	c.Assert(err, check.IsNil)

	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	c.Assert(cmd.ParseFlags([]string{"--config", configPath}), check.IsNil)

	err = o.complete(cmd)
	c.Assert(err, check.IsNil)
	err = o.validate()
	c.Assert(err, check.IsNil)
	c.Assert(o.serverConfig, check.DeepEquals, &config.ServerConfig{
		Addr:          "128.0.0.1:1234",
		AdvertiseAddr: "127.0.0.1:1111",
		LogFile:       "/root/cdc1.log",
		LogLevel:      "warn",
		Log: &config.LogConfig{
			File: &config.LogFileConfig{
				MaxSize:    200,
				MaxDays:    1,
				MaxBackups: 1,
			},
		},
		DataDir:                dataDir,
		GcTTL:                  500,
		TZ:                     "US",
		CaptureSessionTTL:      10,
		OwnerFlushInterval:     config.TomlDuration(600 * time.Millisecond),
		ProcessorFlushInterval: config.TomlDuration(600 * time.Millisecond),
		Sorter: &config.SorterConfig{
			NumConcurrentWorker:    4,
			ChunkSizeLimit:         10000000,
			MaxMemoryPressure:      3,
			MaxMemoryConsumption:   2000000,
			NumWorkerPoolGoroutine: 5,
			SortDir:                config.DefaultSortDir,
		},
		Security:            &config.SecurityConfig{},
		PerTableMemoryQuota: 20 * 1024 * 1024, // 20M
		KVClient: &config.KVClientConfig{
			WorkerConcurrent: 8,
			WorkerPoolSize:   0,
			RegionScanLimit:  40,
		},
		SchedulerV2: &config.SchedulerV2Config{
			Enabled:                      true,
			ProcessorCheckpointInterval:  config.TomlDuration(time.Millisecond*15),
			ClientMaxBatchInterval:       config.TomlDuration(time.Millisecond * 100),
			ClientMaxBatchSize:           8192,
			ClientRetryRateLimit:         0.2,
			ServerMaxPendingMessageCount: 102400,
			ServerAckInterval:            config.TomlDuration(time.Millisecond * 100),
			ServerWorkerPoolSize:         4,
		},
	})
}

func (s *serverSuite) TestDecodeCfgWithFlags(c *check.C) {
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

[scheduler-v2]
enabled = true
processor-checkpoint-interval = "15ms"
client-retry-rate-limit = 0.2
`, dataDir)
	err := ioutil.WriteFile(configPath, []byte(configContent), 0o644)
	c.Assert(err, check.IsNil)

	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	c.Assert(cmd.ParseFlags([]string{
		"--addr", "127.5.5.1:8833",
		"--log-file", "/root/cdc.log",
		"--log-level", "debug",
		"--data-dir", dataDir,
		"--gc-ttl", "10",
		"--tz", "UTC",
		"--owner-flush-interval", "150ms",
		"--processor-flush-interval", "150ms",
		"--ca", "",
		"--sorter-chunk-size-limit", "50000000",
		"--sorter-max-memory-consumption", "60000000",
		"--sorter-max-memory-percentage", "70",
		"--sorter-num-concurrent-worker", "3",
		"--config", configPath,
	}), check.IsNil)

	err = o.complete(cmd)
	c.Assert(err, check.IsNil)
	err = o.validate()
	c.Assert(err, check.IsNil)
	c.Assert(o.serverConfig, check.DeepEquals, &config.ServerConfig{
		Addr:          "127.5.5.1:8833",
		AdvertiseAddr: "127.0.0.1:1111",
		LogFile:       "/root/cdc.log",
		LogLevel:      "debug",
		Log: &config.LogConfig{
			File: &config.LogFileConfig{
				MaxSize:    200,
				MaxDays:    1,
				MaxBackups: 1,
			},
		},
		DataDir:                dataDir,
		GcTTL:                  10,
		TZ:                     "UTC",
		CaptureSessionTTL:      10,
		OwnerFlushInterval:     config.TomlDuration(150 * time.Millisecond),
		ProcessorFlushInterval: config.TomlDuration(150 * time.Millisecond),
		Sorter: &config.SorterConfig{
			NumConcurrentWorker:    3,
			ChunkSizeLimit:         50000000,
			MaxMemoryPressure:      70,
			MaxMemoryConsumption:   60000000,
			NumWorkerPoolGoroutine: 5,
			SortDir:                config.DefaultSortDir,
		},
		Security: &config.SecurityConfig{
			CertPath:      "bb",
			KeyPath:       "cc",
			CertAllowedCN: []string{"dd", "ee"},
		},
		PerTableMemoryQuota: 20 * 1024 * 1024, // 20M
		KVClient: &config.KVClientConfig{
			WorkerConcurrent: 8,
			WorkerPoolSize:   0,
			RegionScanLimit:  40,
		},
		SchedulerV2: &config.SchedulerV2Config{
			Enabled:                      true,
			ProcessorCheckpointInterval:  config.TomlDuration(time.Millisecond*15),
			ClientMaxBatchInterval:       config.TomlDuration(time.Millisecond * 100),
			ClientMaxBatchSize:           8192,
			ClientRetryRateLimit:         0.2,
			ServerMaxPendingMessageCount: 102400,
			ServerAckInterval:            config.TomlDuration(time.Millisecond * 100),
			ServerWorkerPoolSize:         4,
		},
	})
}
