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
	"time"

	cerrors "github.com/pingcap/tiflow/pkg/errors"
)

// SchedulerConfig configs TiCDC scheduler.
type SchedulerConfig struct {
	HeartbeatTick        int          `toml:"heartbeat-tick" json:"heartbeat-tick"`
	MaxTaskConcurrency   int          `toml:"max-task-concurrency" json:"max-task-concurrency"`
	CheckBalanceInterval TomlDuration `toml:"check-balance-interval" json:"check-balance-interval"`
}

func NewDefaultSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		HeartbeatTick:      2,
		MaxTaskConcurrency: 10,
		// TODO: no need to check balance each minute, relax the interval.
		CheckBalanceInterval: TomlDuration(time.Minute),
	}
}

// ValidateAndAdjust verifies that each parameter is valid.
func (c *SchedulerConfig) ValidateAndAdjust() error {
	if c.HeartbeatTick <= 0 {
		return cerrors.ErrInvalidServerOption.GenWithStackByArgs(
			"heartbeat-tick must be larger than 0")
	}
	if c.MaxTaskConcurrency <= 0 {
		return cerrors.ErrInvalidServerOption.GenWithStackByArgs(
			"max-task-concurrency must be larger than 0")
	}
	if time.Duration(c.CheckBalanceInterval) <= time.Second {
		return cerrors.ErrInvalidServerOption.GenWithStackByArgs(
			"check-balance-interval must be larger than 1s")
	}
	return nil
}
