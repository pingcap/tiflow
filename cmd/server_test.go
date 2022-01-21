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

package cmd

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/pingcap/check"
	ticonfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/spf13/cobra"
)

type serverSuite struct{}

var _ = check.Suite(&serverSuite{})

func (s *serverSuite) TestPatchTiDBConf(c *check.C) {
	defer testleak.AfterTest(c)()
	patchTiDBConf()
	cfg := ticonfig.GetGlobalConfig()
	c.Assert(cfg.TiKVClient.MaxBatchSize, check.Equals, uint(0))
}

func (s *serverSuite) TestDataDirServerConfig(c *check.C) {
	defer testleak.AfterTest(c)()
	cmd := new(cobra.Command)
	initServerCmd(cmd)
	c.Assert(cmd.ParseFlags([]string{}), check.IsNil)
	cfg, err := loadAndVerifyServerConfig(cmd)
	c.Assert(err, check.IsNil)
	c.Assert(cfg, check.NotNil)
	// data dir default to ""
	c.Assert(cfg.DataDir, check.Equals, "")
	c.Assert(cfg.Sorter.SortDir, check.Equals, filepath.Join("", "/tmp/sorter"))

	dataDir := c.MkDir()
	cmd = new(cobra.Command)
	initServerCmd(cmd)
	c.Assert(cmd.ParseFlags([]string{"--data-dir=" + dataDir}), check.IsNil)
	cfg, err = loadAndVerifyServerConfig(cmd)
	c.Assert(err, check.IsNil)
	c.Assert(cfg, check.NotNil)
	c.Assert(cfg.DataDir, check.Equals, dataDir)
	// sorter-dir is not set yet
	c.Assert(cfg.Sorter.SortDir, check.Equals, "/tmp/sorter")
}

func (s *serverSuite) TestLoadAndVerifyServerConfig(c *check.C) {
	defer testleak.AfterTest(c)()
	// test default flag values
	cmd := new(cobra.Command)
	initServerCmd(cmd)
	c.Assert(cmd.ParseFlags([]string{}), check.IsNil)
	cfg, err := loadAndVerifyServerConfig(cmd)
	c.Assert(err, check.IsNil)
	c.Assert(cfg, check.NotNil)

	defcfg := config.GetDefaultServerConfig()
	c.Assert(defcfg.ValidateAndAdjust(), check.IsNil)
	c.Assert(cfg, check.DeepEquals, defcfg)
	c.Assert(serverPdAddr, check.Equals, "http://127.0.0.1:2379")

	// test empty PD address
	cmd = new(cobra.Command)
	initServerCmd(cmd)
	c.Assert(cmd.ParseFlags([]string{"--pd="}), check.IsNil)
	_, err = loadAndVerifyServerConfig(cmd)
	c.Assert(err, check.ErrorMatches, ".*empty PD address.*")

	// test invalid PD address
	cmd = new(cobra.Command)
	initServerCmd(cmd)
	c.Assert(cmd.ParseFlags([]string{"--pd=aa"}), check.IsNil)
	_, err = loadAndVerifyServerConfig(cmd)
	c.Assert(err, check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// test invalid PD address(without host)
	cmd = new(cobra.Command)
	initServerCmd(cmd)
	c.Assert(cmd.ParseFlags([]string{"--pd=http://"}), check.IsNil)
	_, err = loadAndVerifyServerConfig(cmd)
	c.Assert(err, check.ErrorMatches, ".*PD endpoint should be a valid http or https URL.*")

	// test missing certificate
	cmd = new(cobra.Command)
	initServerCmd(cmd)
	c.Assert(cmd.ParseFlags([]string{"--pd=https://aa"}), check.IsNil)
	_, err = loadAndVerifyServerConfig(cmd)
	c.Assert(err, check.ErrorMatches, ".*PD endpoint scheme is https, please provide certificate.*")

	// test undefined flag
	cmd = new(cobra.Command)
	initServerCmd(cmd)
	c.Assert(cmd.ParseFlags([]string{"--PD="}), check.ErrorMatches, ".*unknown flag: --PD.*")
	_, err = loadAndVerifyServerConfig(cmd)
	c.Assert(err, check.IsNil)

	// test flags without config file
	dataDir := c.MkDir()
	cmd = new(cobra.Command)
	initServerCmd(cmd)
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
	cfg, err = loadAndVerifyServerConfig(cmd)
	c.Assert(err, check.IsNil)
	c.Assert(cfg, check.DeepEquals, &config.ServerConfig{
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
		PerTableMemoryQuota: 10 * 1024 * 1024, // 10M
		KVClient: &config.KVClientConfig{
			WorkerConcurrent: 8,
			WorkerPoolSize:   0,
			RegionScanLimit:  40,
		},
	})

	// test decode config file
	dataDir = c.MkDir()
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
`, dataDir)
	err = ioutil.WriteFile(configPath, []byte(configContent), 0o644)
	c.Assert(err, check.IsNil)
	cmd = new(cobra.Command)
	initServerCmd(cmd)
	c.Assert(cmd.ParseFlags([]string{"--config", configPath}), check.IsNil)
	cfg, err = loadAndVerifyServerConfig(cmd)
	c.Assert(err, check.IsNil)
	c.Assert(cfg, check.DeepEquals, &config.ServerConfig{
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
		PerTableMemoryQuota: 10 * 1024 * 1024, // 10M
		KVClient: &config.KVClientConfig{
			WorkerConcurrent: 8,
			WorkerPoolSize:   0,
			RegionScanLimit:  40,
		},
	})

	configContent = configContent + `
[security]
ca-path = "aa"
cert-path = "bb"
key-path = "cc"
cert-allowed-cn = ["dd","ee"]
`
	err = ioutil.WriteFile(configPath, []byte(configContent), 0o644)
	c.Assert(err, check.IsNil)
	cmd = new(cobra.Command)
	initServerCmd(cmd)
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
	cfg, err = loadAndVerifyServerConfig(cmd)
	c.Assert(err, check.IsNil)
	c.Assert(cfg, check.DeepEquals, &config.ServerConfig{
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
		PerTableMemoryQuota: 10 * 1024 * 1024, // 10M
		KVClient: &config.KVClientConfig{
			WorkerConcurrent: 8,
			WorkerPoolSize:   0,
			RegionScanLimit:  40,
		},
	})
}
