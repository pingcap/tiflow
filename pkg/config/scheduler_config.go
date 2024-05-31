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
	"fmt"
	"time"

	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	// DefaultAddTableBatchSize is set to 0, which means that AddTableBatchSize
	// could be adjusted by scheduler based on statistics information.
	DefaultAddTableBatchSize = 0
	// MinAdjustedAddTableBatchSize is the minimum batch size of adding tables when adjusted.
	MinAdjustedAddTableBatchSize = 50
	// MaxAdjustedAddTableBatchSize is the maximum batch size of adding tables when adjusted.
	MaxAdjustedAddTableBatchSize = 1000
	// DefaultMaxTableCount is the default maximum number of tables that a capture can be scheduled.
	// TODO(CharlesCheung): find a more reasonable value.
	DefaultMaxTableCount = 30000
)

// ChangefeedSchedulerConfig is per changefeed scheduler settings.
type ChangefeedSchedulerConfig struct {
	// AddTableBatchSize is the batch size of adding tables on each tick,
	// used by the `BasicScheduler`.
	// When the new owner in power, other captures may not online yet, there might have hundreds of
	// tables need to be dispatched, add tables in a batch way to prevent suddenly resource usage
	// spikes, also wait for other captures join the cluster
	// When there are only 2 captures, and a large number of tables, this can be helpful to prevent
	// oom caused by all tables dispatched to only one capture.
	AddTableBatchSize int `toml:"add-table-batch-size" json:"add-table-batch-size"`
	// MaxTableCount is the maximum number of tables that a capture can be scheduled.
	MaxTableCount int `toml:"max-table-count" json:"max-table-count"`
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
	if c.AddTableBatchSize < 0 {
		return errors.New("add-table-batch-size must not be negative")
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
	// Deprecated. Use `ChangefeedSettings.AddTableBatchSize` instead.
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
		// Disabled by default since it has been replaced by `ChangefeedSettings.AddTableBatchSize`.
		AddTableBatchSize: DefaultAddTableBatchSize,
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
	if c.AddTableBatchSize != 0 {
		log.Warn("debug.scheduler.add-table-batch-size is deprecated, use changefeed configuration instead")
		if c.AddTableBatchSize < 0 {
			return cerror.ErrInvalidServerOption.GenWithStackByArgs(
				fmt.Sprintf("add-table-batch-size must not be negative: %d", c.AddTableBatchSize))
		}
	}
	return nil
}

// GetAddTableBatchSize is used to keep backward compatibility. And c.ChangefeedSettings.AddTableBatchSize
// is take procedure over c.AddTableBatchSize, since the latter is deprecated.
func (c *SchedulerConfig) GetAddTableBatchSize() int {
	batch := c.ChangefeedSettings.AddTableBatchSize
	if batch == 0 {
		batch = c.AddTableBatchSize
	}
	return batch
}

// GetMaxTableCount is used to keep backward compatibility.
func (c *SchedulerConfig) GetMaxTableCount() int {
	if c.ChangefeedSettings.MaxTableCount <= 0 {
		return DefaultMaxTableCount
	}
	return c.ChangefeedSettings.MaxTableCount
}
