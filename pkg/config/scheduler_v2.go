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

// SchedulerV2Config represents configurations for the new scheduler,
// i.e. scheduling by peer messages
type SchedulerV2Config struct {
	Enabled                     bool         `toml:"enabled" json:"enabled"`
	ProcessorCheckpointInterval TomlDuration `toml:"processor-checkpoint-interval" json:"processor-checkpoint-interval"`

	ClientMaxBatchInterval TomlDuration `toml:"client-max-batch-interval" json:"client-max-batch-interval"`
	ClientMaxBatchSize     int          `toml:"client-max-batch-size" json:"client-max-batch-size"`
	ClientRetryRateLimit   float64      `toml:"client-retry-rate-limit" json:"client-retry-rate-limit"`

	ServerMaxPendingMessageCount int          `toml:"server-max-pending-message-count" json:"server-max-pending-message-count"`
	ServerAckInterval            TomlDuration `toml:"server-ack-interval" json:"server-ack-interval"`
	ServerWorkerPoolSize         int          `toml:"server-worker-pool-size" json:"server-worker-pool-size"`
}
