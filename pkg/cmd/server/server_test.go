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
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	ticonfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestPatchTiDBConf(t *testing.T) {
	t.TempDir()
	patchTiDBConf()
	cfg := ticonfig.GetGlobalConfig()
	require.Equal(t, uint(0), cfg.TiKVClient.MaxBatchSize)
}

func TestValidateWithEmptyPdAddress(t *testing.T) {
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	require.Nil(t, cmd.ParseFlags([]string{"--pd="}))
	err := o.complete(cmd)
	require.Nil(t, err)
	err = o.validate()
	require.Regexp(t, ".*empty PD address.*", err.Error())
}

func TestValidateWithInvalidPdAddress(t *testing.T) {
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	require.Nil(t, cmd.ParseFlags([]string{"--pd=aa"}))
	err := o.complete(cmd)
	require.Nil(t, err)
	err = o.validate()
	require.Regexp(t, ".*PD endpoint should be a valid http or https URL.*", err.Error())
}

func TestValidateWithInvalidPdAddressWithoutHost(t *testing.T) {
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	require.Nil(t, cmd.ParseFlags([]string{"--pd=http://"}))
	err := o.complete(cmd)
	require.Nil(t, err)
	err = o.validate()
	require.Regexp(t, ".*PD endpoint should be a valid http or https URL.*", err.Error())
}

func TestValidateWithHttpsPdAddressWithoutCertificate(t *testing.T) {
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	require.Nil(t, cmd.ParseFlags([]string{"--pd=https://aa"}))
	err := o.complete(cmd)
	require.Nil(t, err)
	err = o.validate()
	require.Regexp(t, ".*PD endpoint scheme is https, please provide certificate.*", err.Error())
}

func TestAddUnknownFlag(t *testing.T) {
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	require.Regexp(t, ".*unknown flag: --PD.*", cmd.ParseFlags([]string{"--PD="}).Error())
}

func TestDefaultCfg(t *testing.T) {
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	require.Nil(t, cmd.ParseFlags([]string{}))
	err := o.complete(cmd)
	require.Nil(t, err)

	defaultCfg := config.GetDefaultServerConfig()
	require.Nil(t, defaultCfg.ValidateAndAdjust())
	require.Equal(t, defaultCfg, o.serverConfig)
	require.Equal(t, "http://127.0.0.1:2379", o.serverPdAddr)
}

func TestParseCfg(t *testing.T) {
	dataDir := t.TempDir()
	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	require.Nil(t, cmd.ParseFlags([]string{
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
		"--sort-dir", "/tmp/just_a_test",
	}))

	err := o.complete(cmd)
	require.Nil(t, err)
	err = o.validate()
	require.Nil(t, err)
	require.Equal(t, &config.ServerConfig{
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
			InternalErrOutput: "stderr",
		},
		DataDir:                dataDir,
		GcTTL:                  10,
		TZ:                     "UTC",
		CaptureSessionTTL:      10,
		OwnerFlushInterval:     config.TomlDuration(150 * time.Millisecond),
		ProcessorFlushInterval: config.TomlDuration(150 * time.Millisecond),
		Sorter: &config.SorterConfig{
			SortDir:             config.DefaultSortDir,
			CacheSizeInMB:       128,
			MaxMemoryPercentage: 10,

			NumConcurrentWorker:    4,
			ChunkSizeLimit:         128 * 1024 * 1024,
			MaxMemoryConsumption:   16 * 1024 * 1024 * 1024,
			NumWorkerPoolGoroutine: 16,
		},
		Security: &config.SecurityConfig{
			CertPath:      "bb",
			KeyPath:       "cc",
			CertAllowedCN: []string{"dd", "ee"},
		},
		PerTableMemoryQuota: config.DefaultTableMemoryQuota,
		KVClient: &config.KVClientConfig{
			WorkerConcurrent:    8,
			WorkerPoolSize:      0,
			RegionScanLimit:     40,
			RegionRetryDuration: config.TomlDuration(time.Minute),
		},
		Debug: &config.DebugConfig{
			TableActor: &config.TableActorConfig{
				EventBatchSize: 32,
			},
			EnableDBSorter:      true,
			EnableNewScheduler:  true,
			EnablePullBasedSink: true,
			DB: &config.DBConfig{
				Count:                       8,
				Concurrency:                 128,
				MaxOpenFiles:                10000,
				BlockSize:                   65536,
				WriterBufferSize:            8388608,
				Compression:                 "snappy",
				WriteL0PauseTrigger:         math.MaxInt32,
				CompactionL0Trigger:         160,
				CompactionDeletionThreshold: 10485760,
				CompactionPeriod:            1800,
				IteratorMaxAliveDuration:    10000,
				IteratorSlowReadDuration:    256,
			},
			// We expect the default configuration here.
			Messages: &config.MessagesConfig{
				ClientMaxBatchInterval:       config.TomlDuration(time.Millisecond * 10),
				ClientMaxBatchSize:           8 * 1024 * 1024,
				ClientMaxBatchCount:          128,
				ClientRetryRateLimit:         1.0,
				ServerMaxPendingMessageCount: 102400,
				ServerAckInterval:            config.TomlDuration(time.Millisecond * 100),
				ServerWorkerPoolSize:         4,
				MaxRecvMsgSize:               256 * 1024 * 1024,
				KeepAliveTimeout:             config.TomlDuration(time.Second * 10),
				KeepAliveTime:                config.TomlDuration(time.Second * 30),
			},
			Scheduler: &config.SchedulerConfig{
				HeartbeatTick:        2,
				CollectStatsTick:     200,
				MaxTaskConcurrency:   10,
				CheckBalanceInterval: 60000000000,
				AddTableBatchSize:    50,
			},
			EnableNewSink: true,
		},
		ClusterID:           "default",
		MaxMemoryPercentage: config.DisableMemoryLimit,
	}, o.serverConfig)
}

func TestDecodeCfg(t *testing.T) {
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
sort-dir = "/tmp/just_a_test"

[kv-client]
region-retry-duration = "3s"

[debug]
enable-db-sorter = true
enable-pull-based-sink = true
[debug.db]
count = 5
concurrency = 6
max-open-files = 7
block-size = 32768 # 32 KB
block-cache-size = 8
writer-buffer-size = 9
compression = "none"
target-file-size-base = 10
compaction-l0-trigger = 11
compaction-deletion-threshold = 15
compaction-period = 16
write-l0-slowdown-trigger = 12
write-l0-pause-trigger = 13

[debug.messages]
client-max-batch-interval = "500ms"
client-max-batch-size = 999
client-max-batch-count = 888
client-retry-rate-limit = 100.0
server-max-pending-message-count = 1024
server-ack-interval = "1s"
server-worker-pool-size = 16
max-recv-msg-size = 4
[debug.scheduler]
heartbeat-tick = 3
collect-stats-tick = 201
max-task-concurrency = 11
check-balance-interval = "10s"
`, dataDir)
	err := os.WriteFile(configPath, []byte(configContent), 0o644)
	require.Nil(t, err)

	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	require.Nil(t, cmd.ParseFlags([]string{"--config", configPath}))

	err = o.complete(cmd)
	require.Nil(t, err)
	err = o.validate()
	require.Nil(t, err)
	require.Equal(t, &config.ServerConfig{
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
			InternalErrOutput: "stderr",
		},
		DataDir:                dataDir,
		GcTTL:                  500,
		TZ:                     "US",
		CaptureSessionTTL:      10,
		OwnerFlushInterval:     config.TomlDuration(600 * time.Millisecond),
		ProcessorFlushInterval: config.TomlDuration(600 * time.Millisecond),
		Sorter: &config.SorterConfig{
			SortDir:             config.DefaultSortDir,
			CacheSizeInMB:       128,
			MaxMemoryPercentage: 10,

			NumConcurrentWorker:    4,
			ChunkSizeLimit:         128 * 1024 * 1024,
			MaxMemoryConsumption:   16 * 1024 * 1024 * 1024,
			NumWorkerPoolGoroutine: 16,
		},
		Security:            &config.SecurityConfig{},
		PerTableMemoryQuota: config.DefaultTableMemoryQuota,
		KVClient: &config.KVClientConfig{
			WorkerConcurrent:    8,
			WorkerPoolSize:      0,
			RegionScanLimit:     40,
			RegionRetryDuration: config.TomlDuration(3 * time.Second),
		},
		Debug: &config.DebugConfig{
			TableActor: &config.TableActorConfig{
				EventBatchSize: 32,
			},
			EnableDBSorter:      true,
			EnablePullBasedSink: true,
			EnableNewScheduler:  true,
			DB: &config.DBConfig{
				Count:                       5,
				Concurrency:                 6,
				MaxOpenFiles:                7,
				BlockSize:                   32768,
				WriterBufferSize:            9,
				Compression:                 "none",
				CompactionL0Trigger:         11,
				WriteL0PauseTrigger:         13,
				IteratorMaxAliveDuration:    10000,
				IteratorSlowReadDuration:    256,
				CompactionDeletionThreshold: 15,
				CompactionPeriod:            16,
			},
			Messages: &config.MessagesConfig{
				ClientMaxBatchInterval:       config.TomlDuration(500 * time.Millisecond),
				ClientMaxBatchSize:           999,
				ClientMaxBatchCount:          888,
				ClientRetryRateLimit:         100.0,
				ServerMaxPendingMessageCount: 1024,
				ServerAckInterval:            config.TomlDuration(1 * time.Second),
				ServerWorkerPoolSize:         16,
				MaxRecvMsgSize:               4,
				KeepAliveTimeout:             config.TomlDuration(time.Second * 10),
				KeepAliveTime:                config.TomlDuration(time.Second * 30),
			},
			Scheduler: &config.SchedulerConfig{
				HeartbeatTick:        3,
				CollectStatsTick:     201,
				MaxTaskConcurrency:   11,
				CheckBalanceInterval: config.TomlDuration(10 * time.Second),
				AddTableBatchSize:    50,
			},
			EnableNewSink: true,
		},
		ClusterID:           "default",
		MaxMemoryPercentage: config.DisableMemoryLimit,
	}, o.serverConfig)
}

func TestDecodeCfgWithFlags(t *testing.T) {
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
sort-dir = "/tmp/just_a_test"

[security]
ca-path = "aa"
cert-path = "bb"
key-path = "cc"
cert-allowed-cn = ["dd","ee"]
`, dataDir)
	err := os.WriteFile(configPath, []byte(configContent), 0o644)
	require.Nil(t, err)

	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	require.Nil(t, cmd.ParseFlags([]string{
		"--addr", "127.5.5.1:8833",
		"--log-file", "/root/cdc.log",
		"--log-level", "debug",
		"--data-dir", dataDir,
		"--gc-ttl", "10",
		"--tz", "UTC",
		"--owner-flush-interval", "150ms",
		"--processor-flush-interval", "150ms",
		"--ca", "",
		"--config", configPath,
	}))

	err = o.complete(cmd)
	require.Nil(t, err)
	err = o.validate()
	require.Nil(t, err)
	require.Equal(t, &config.ServerConfig{
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
			InternalErrOutput: "stderr",
		},
		DataDir:                dataDir,
		GcTTL:                  10,
		TZ:                     "UTC",
		CaptureSessionTTL:      10,
		OwnerFlushInterval:     config.TomlDuration(150 * time.Millisecond),
		ProcessorFlushInterval: config.TomlDuration(150 * time.Millisecond),
		Sorter: &config.SorterConfig{
			SortDir:             config.DefaultSortDir,
			CacheSizeInMB:       128,
			MaxMemoryPercentage: 10,

			NumConcurrentWorker:    4,
			ChunkSizeLimit:         128 * 1024 * 1024,
			MaxMemoryConsumption:   16 * 1024 * 1024 * 1024,
			NumWorkerPoolGoroutine: 16,
		},
		Security: &config.SecurityConfig{
			CertPath:      "bb",
			KeyPath:       "cc",
			CertAllowedCN: []string{"dd", "ee"},
		},
		PerTableMemoryQuota: config.DefaultTableMemoryQuota,
		KVClient: &config.KVClientConfig{
			WorkerConcurrent:    8,
			WorkerPoolSize:      0,
			RegionScanLimit:     40,
			RegionRetryDuration: config.TomlDuration(time.Minute),
		},
		Debug: &config.DebugConfig{
			TableActor: &config.TableActorConfig{
				EventBatchSize: 32,
			},
			EnableDBSorter:      true,
			EnableNewScheduler:  true,
			EnablePullBasedSink: true,
			DB: &config.DBConfig{
				Count:                       8,
				Concurrency:                 128,
				MaxOpenFiles:                10000,
				BlockSize:                   65536,
				WriterBufferSize:            8388608,
				Compression:                 "snappy",
				WriteL0PauseTrigger:         math.MaxInt32,
				CompactionL0Trigger:         160,
				CompactionDeletionThreshold: 10485760,
				CompactionPeriod:            1800,
				IteratorMaxAliveDuration:    10000,
				IteratorSlowReadDuration:    256,
			},
			// We expect the default configuration here.
			Messages: &config.MessagesConfig{
				ClientMaxBatchInterval:       config.TomlDuration(time.Millisecond * 10),
				ClientMaxBatchSize:           8 * 1024 * 1024,
				ClientMaxBatchCount:          128,
				ClientRetryRateLimit:         1.0,
				ServerMaxPendingMessageCount: 102400,
				ServerAckInterval:            config.TomlDuration(time.Millisecond * 100),
				ServerWorkerPoolSize:         4,
				MaxRecvMsgSize:               256 * 1024 * 1024,
				KeepAliveTimeout:             config.TomlDuration(time.Second * 10),
				KeepAliveTime:                config.TomlDuration(time.Second * 30),
			},
			Scheduler: &config.SchedulerConfig{
				HeartbeatTick:        2,
				CollectStatsTick:     200,
				MaxTaskConcurrency:   10,
				CheckBalanceInterval: 60000000000,
				AddTableBatchSize:    50,
			},
			EnableNewSink: true,
		},
		ClusterID:           "default",
		MaxMemoryPercentage: config.DisableMemoryLimit,
	}, o.serverConfig)
}

func TestDecodeUnkownDebugCfg(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "ticdc.toml")
	configContent := `
[debug]
unknown1 = 1
[debug.unknown2]
unknown3 = 3
`
	err := os.WriteFile(configPath, []byte(configContent), 0o644)
	require.Nil(t, err)

	cmd := new(cobra.Command)
	o := newOptions()
	o.addFlags(cmd)

	require.Nil(t, cmd.ParseFlags([]string{"--config", configPath}))

	err = o.complete(cmd)
	require.Nil(t, err)
	err = o.validate()
	require.Nil(t, err)
	require.Equal(t, &config.DebugConfig{
		TableActor: &config.TableActorConfig{
			EventBatchSize: 32,
		},
		EnableDBSorter:      true,
		EnableNewScheduler:  true,
		EnablePullBasedSink: true,
		DB: &config.DBConfig{
			Count:                       8,
			Concurrency:                 128,
			MaxOpenFiles:                10000,
			BlockSize:                   65536,
			WriterBufferSize:            8388608,
			Compression:                 "snappy",
			WriteL0PauseTrigger:         math.MaxInt32,
			CompactionL0Trigger:         160,
			CompactionDeletionThreshold: 10485760,
			CompactionPeriod:            1800,
			IteratorMaxAliveDuration:    10000,
			IteratorSlowReadDuration:    256,
		},
		// We expect the default configuration here.
		Messages: &config.MessagesConfig{
			ClientMaxBatchInterval:       config.TomlDuration(time.Millisecond * 10),
			ClientMaxBatchSize:           8 * 1024 * 1024,
			ClientMaxBatchCount:          128,
			ClientRetryRateLimit:         1.0,
			ServerMaxPendingMessageCount: 102400,
			ServerAckInterval:            config.TomlDuration(time.Millisecond * 100),
			ServerWorkerPoolSize:         4,
			MaxRecvMsgSize:               256 * 1024 * 1024,
			KeepAliveTimeout:             config.TomlDuration(time.Second * 10),
			KeepAliveTime:                config.TomlDuration(time.Second * 30),
		},
		Scheduler: &config.SchedulerConfig{
			HeartbeatTick:        2,
			CollectStatsTick:     200,
			MaxTaskConcurrency:   10,
			CheckBalanceInterval: 60000000000,
			AddTableBatchSize:    50,
		},
		EnableNewSink: true,
	}, o.serverConfig.Debug)
}
