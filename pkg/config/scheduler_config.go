// Copyright 2022 PingCAP, Inc.
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

package config

import (
	"errors"
	"time"

	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// ChangefeedSchedulerConfig is per changefeed scheduler settings.
type ChangefeedSchedulerConfig struct {
	// EnableTableAcrossNodes set true to split one table to multiple spans and
	// distribute to multiple TiCDC nodes.
	EnableTableAcrossNodes bool `toml:"enable-table-across-nodes" json:"enable-table-across-nodes"`
	// RegionThreshold is the region count threshold of splitting a table.
	RegionThreshold int `toml:"region-threshold" json:"region-threshold"`
	// WriteKeyThreshold is the written keys threshold of splitting a table.
	WriteKeyThreshold int `toml:"write-key-threshold" json:"write-key-threshold"`
	// Deprecated.
	RegionPerSpan int `toml:"region-per-span" json:"region-per-span"`
}

// Validate validates the config.
func (c *ChangefeedSchedulerConfig) Validate() error {
	if !c.EnableTableAcrossNodes {
		return nil
	}
	if c.RegionThreshold < 0 {
		return errors.New("region-threshold must be larger than 0")
	}
	if c.WriteKeyThreshold < 0 {
		return errors.New("write-key-threshold must be larger than 0")
	}
	return nil
}

// SchedulerConfig configs TiCDC scheduler.
type SchedulerConfig struct {
	// HeartbeatTick is the number of owner tick to initial a heartbeat to captures.
	HeartbeatTick int `toml:"heartbeat-tick" json:"heartbeat-tick"`
	// CollectStatsTick is the number of owner tick to collect stats.
	CollectStatsTick int `toml:"collect-stats-tick" json:"collect-stats-tick"`
	// MaxTaskConcurrency the maximum of concurrent running schedule tasks.
	MaxTaskConcurrency int `toml:"max-task-concurrency" json:"max-task-concurrency"`
	// CheckBalanceInterval the interval of balance tables between each capture.
	CheckBalanceInterval TomlDuration `toml:"check-balance-interval" json:"check-balance-interval"`
	// AddTableBatchSize is the batch size of adding tables on each tick,
	// used by the `BasicScheduler`.
	// When the new owner in power, other captures may not online yet, there might have hundreds of
	// tables need to be dispatched, add tables in a batch way to prevent suddenly resource usage
	// spikes, also wait for other captures join the cluster
	// When there are only 2 captures, and a large number of tables, this can be helpful to prevent
	// oom caused by all tables dispatched to only one capture.
	AddTableBatchSize int `toml:"add-table-batch-size" json:"add-table-batch-size"`

	// ChangefeedSettings is setting by changefeed.
	ChangefeedSettings *ChangefeedSchedulerConfig `toml:"-" json:"-"`
}

// NewDefaultSchedulerConfig return the default scheduler configuration.
func NewDefaultSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		HeartbeatTick: 2,
		// By default, owner ticks every 50ms, we want to low the frequency of
		// collecting stats to reduce memory allocation and CPU usage.
		CollectStatsTick:   200, // 200 * 50ms = 10s.
		MaxTaskConcurrency: 10,
		// TODO: no need to check balance each minute, relax the interval.
		CheckBalanceInterval: TomlDuration(time.Minute),
		AddTableBatchSize:    50,
	}
}

// ValidateAndAdjust verifies that each parameter is valid.
func (c *SchedulerConfig) ValidateAndAdjust() error {
	if c.HeartbeatTick <= 0 {
		return cerror.ErrInvalidServerOption.GenWithStackByArgs(
			"heartbeat-tick must be larger than 0")
	}
	if c.CollectStatsTick <= 0 {
		return cerror.ErrInvalidServerOption.GenWithStackByArgs(
			"collect-stats-tick must be larger than 0")
	}
	if c.MaxTaskConcurrency <= 0 {
		return cerror.ErrInvalidServerOption.GenWithStackByArgs(
			"max-task-concurrency must be larger than 0")
	}
	if time.Duration(c.CheckBalanceInterval) <= time.Second {
		return cerror.ErrInvalidServerOption.GenWithStackByArgs(
			"check-balance-interval must be larger than 1s")
	}
	if c.AddTableBatchSize <= 0 {
		return cerror.ErrInvalidServerOption.GenWithStackByArgs(
			"add-table-batch-size must be large than 0")
	}
	return nil
}
