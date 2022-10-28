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

import "github.com/pingcap/errors"

// DebugConfig represents config for ticdc unexposed feature configurations
type DebugConfig struct {
	TableActor *TableActorConfig `toml:"table-actor" json:"table-actor"`

	// EnablePullBasedSink enables pull-based sink, false by default.
	//
	// NOTE: currently it can only be enabled with EnableDBSorter, because unified
	// sorter hasn't been transformed into the new interface.
	//
	// TODO(qupeng): we need to transform unified sorter into EventSortEngine to remove
	// the above limit.
	EnablePullBasedSink bool `toml:"enable-pull-based-sink" json:"enable-pull-based-sink"`

	// EnableDBSorter enables db sorter.
	//
	// The default value is true.
	EnableDBSorter bool      `toml:"enable-db-sorter" json:"enable-db-sorter"`
	DB             *DBConfig `toml:"db" json:"db"`

	// EnableNewScheduler enables the peer-messaging based new scheduler.
	// The default value is true.
	EnableNewScheduler bool            `toml:"enable-new-scheduler" json:"enable-new-scheduler"`
	Messages           *MessagesConfig `toml:"messages" json:"messages"`

	// EnableSchedulerV3 enables the two-phase scheduler.
	// The default value is true.
	EnableSchedulerV3 bool `toml:"enable-scheduler-v3" json:"enable-scheduler-v3"`
	// Scheduler is the configuration of the two-phase scheduler.
	Scheduler *SchedulerConfig `toml:"scheduler" json:"scheduler"`

	// EnableNewSink enables the new sink.
	// The default value is true.
	EnableNewSink bool `toml:"enable-new-sink" json:"enable-new-sink"`
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
	return nil
}
