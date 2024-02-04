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

// SyncedStatusConfig represents synced check interval config for a changefeed
type SyncedStatusConfig struct {
	// The minimum interval between the latest synced ts and now required to reach synced state
	SyncedCheckInterval int64 `toml:"synced-check-interval" json:"synced-check-interval"`
	// The maximum interval between latest checkpoint ts and now or
	// between latest sink's checkpoint ts and puller's checkpoint ts required to reach synced state
	CheckpointInterval int64 `toml:"checkpoint-interval" json:"checkpoint-interval"`
}
