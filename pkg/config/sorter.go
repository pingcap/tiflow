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

import "sync"

// SorterConfig represents sorter config for a changefeed
type SorterConfig struct {
	// number of concurrent heap sorts
	NumConcurrentWorker int `toml:"num-concurrent-workers" json:"num-concurrent-workers"`
	// maximum size for a heap
	ChunkSizeLimit uint64 `toml:"chunk-size-limit" json:"chunk-size-limit"`
	// the maximum memory use percentage that allows in-memory sorting
	MaxMemoryPressure int `toml:"max-memory-pressure" json:"max-memory-pressure"`
	// the maximum memory consumption allowed for in-memory sorting
	MaxMemoryConsumption uint64 `toml:"max-memory-consumption" json:"max-memory-consumption"`
}

var (
	sorterConfig *SorterConfig
	mu           sync.Mutex
)

// GetSorterConfig returns the process-local sorter config
func GetSorterConfig() *SorterConfig {
	mu.Lock()
	defer mu.Unlock()
	return sorterConfig
}

// SetSorterConfig sets the process-local sorter config
func SetSorterConfig(config *SorterConfig) {
	mu.Lock()
	defer mu.Unlock()
	sorterConfig = config
}
