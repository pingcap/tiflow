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

package config

// DefaultMaxMessageBytes sets the default value for max-message-bytes
const DefaultMaxMessageBytes = 10 * 1024 * 1024 // 10M

// SinkConfig represents sink config for a changefeed
type SinkConfig struct {
	DispatchRules []*DispatchRule `toml:"dispatchers" json:"dispatchers"`
	Protocol      string          `toml:"protocol" json:"protocol"`
}

// DispatchRule represents partition rule for a table
type DispatchRule struct {
	Matcher    []string `toml:"matcher" json:"matcher"`
	Dispatcher string   `toml:"dispatcher" json:"dispatcher"`
}
