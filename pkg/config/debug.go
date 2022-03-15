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

import "github.com/pingcap/log"

// DebugConfig represents config for ticdc unexposed feature configurations
// Note that any embedded config should be "omitempty",
// and shall not be removed for compatibility.
type DebugConfig struct {
	// identify if the table actor is enabled for table pipeline
	// TODO: turn on after GA.
	EnableTableActor bool              `toml:"enable-table-actor" json:"enable-table-actor"`
	TableActor       *TableActorConfig `toml:"table-actor,omitempty" json:"table-actor,omitempty"`

	// EnableDBSorter enables db sorter.
	//
	// The default value is false.
	EnableDBSorter bool      `toml:"enable-db-sorter" json:"enable-db-sorter"`
	DB             *DBConfig `toml:"db,omitempty" json:"db,omitempty"`

	// EnableNewScheduler enables the peer-messaging based new scheduler.
	// The default value is false.
	EnableNewScheduler bool            `toml:"enable-new-scheduler" json:"enable-new-scheduler"`
	Messages           *MessagesConfig `toml:"messages,omitempty" json:"messages,omitempty"`
}

// ValidateAndAdjust validates and adjusts the debug configuration
func (c *DebugConfig) ValidateAndAdjust() error {
	if c.DB != nil {
		log.Warn("use deprecated configuration `debug.db`, please use `db` instead.")
	}
	if c.Messages != nil {
		log.Warn("use deprecated configuration `debug.messages`, please use `messages` instead.")
	}
	return nil
}
