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
	"time"

	"github.com/pingcap/tiflow/pkg/errors"
)

// KVClientConfig represents config for kv client
type KVClientConfig struct {
	EnableMultiplexing bool `toml:"enable-multiplexing" json:"enable-multiplexing"`
	// how many workers will be used for a single region worker
	WorkerConcurrent uint `toml:"worker-concurrent" json:"worker-concurrent"`
	// how many grpc streams will be established to every TiKV node
	GrpcStreamConcurrent uint `toml:"grpc-stream-concurrent" json:"grpc-stream-concurrent"`
	// Advance table ResolvedTs interval.
	AdvanceIntervalInMs uint `toml:"advance-interval-in-ms" json:"advance-interval-in-ms"`
	// how many goroutines to maintain frontiers.
	FrontierConcurrent uint `toml:"frontier-concurrent" json:"frontier-concurrent"`
	// background workerpool size, the workrpool is shared by all goroutines in cdc server
	WorkerPoolSize int `toml:"worker-pool-size" json:"worker-pool-size"`
	// region incremental scan limit for one table in a single store
	RegionScanLimit int `toml:"region-scan-limit" json:"region-scan-limit"`
	// the total retry duration of connecting a region
	RegionRetryDuration TomlDuration `toml:"region-retry-duration" json:"region-retry-duration"`
}

// NewDefaultKVClientConfig return the default kv client configuration
func NewDefaultKVClientConfig() *KVClientConfig {
	return &KVClientConfig{
		EnableMultiplexing:   true,
		WorkerConcurrent:     8,
		GrpcStreamConcurrent: 1,
		AdvanceIntervalInMs:  300,
		FrontierConcurrent:   8,
		WorkerPoolSize:       0, // 0 will use NumCPU() * 2
		RegionScanLimit:      40,
		// The default TiKV region election timeout is [10s, 20s],
		// Use 1 minute to cover region leader missing.
		RegionRetryDuration: TomlDuration(time.Minute),
	}
}

// ValidateAndAdjust validates and adjusts the kv client configuration
func (c *KVClientConfig) ValidateAndAdjust() error {
	if c.RegionScanLimit <= 0 {
		return errors.ErrInvalidServerOption.GenWithStackByArgs(
			"region-scan-limit should be at least 1")
	}
	if c.RegionRetryDuration <= 0 {
		return errors.ErrInvalidServerOption.GenWithStackByArgs(
			"region-scan-limit should be positive")
	}
	return nil
}
