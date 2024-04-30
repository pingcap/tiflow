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

package config

import (
	"github.com/pingcap/errors"
)

// DebugConfig represents config for ticdc unexposed feature configurations
type DebugConfig struct {
	DB *DBConfig `toml:"db" json:"db"`

	Messages *MessagesConfig `toml:"messages" json:"messages"`

	// Scheduler is the configuration of the two-phase scheduler.
	Scheduler *SchedulerConfig `toml:"scheduler" json:"scheduler"`

	// CDCV2 enables ticdc version 2 implementation with new metastore
	CDCV2 *CDCV2 `toml:"cdc-v2" json:"cdc-v2"`

	// Puller is the configuration of the puller.
	Puller *PullerConfig `toml:"puller" json:"puller"`
}

// ValidateAndAdjust validates and adjusts the debug configuration
func (c *DebugConfig) ValidateAndAdjust() error {
	if err := c.Messages.ValidateAndAdjust(); err != nil {
		return errors.Trace(err)
	}
	if err := c.DB.ValidateAndAdjust(); err != nil {
		return errors.Trace(err)
	}
	if err := c.Scheduler.ValidateAndAdjust(); err != nil {
		return errors.Trace(err)
	}
	if err := c.CDCV2.ValidateAndAdjust(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// PullerConfig represents config for puller
type PullerConfig struct {
	// EnableResolvedTsStuckDetection is used to enable resolved ts stuck detection.
	EnableResolvedTsStuckDetection bool `toml:"enable-resolved-ts-stuck-detection" json:"enable-resolved-ts-stuck-detection"`
	// ResolvedTsStuckInterval is the interval of checking resolved ts stuck.
	ResolvedTsStuckInterval TomlDuration `toml:"resolved-ts-stuck-interval" json:"resolved-ts-stuck-interval"`
	// LogRegionDetails determines whether logs Region details or not in puller and kv-client.
	LogRegionDetails bool `toml:"log-region-details" json:"log-region-details"`
}
