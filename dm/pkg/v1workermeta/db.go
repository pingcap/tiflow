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

package v1workermeta

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/pingcap/dm/pkg/terror"
)

// kvConfig is the configuration of goleveldb.
type kvConfig struct {
	BlockCacheCapacity            int     `toml:"block-cache-capacity" json:"block-cache-capacity"`
	BlockRestartInterval          int     `toml:"block-restart-interval" json:"block-restart-interval"`
	BlockSize                     int     `toml:"block-size" json:"block-size"`
	CompactionL0Trigger           int     `toml:"compaction-L0-trigger" json:"compaction-L0-trigger"`
	CompactionTableSize           int     `toml:"compaction-table-size" json:"compaction-table-size"`
	CompactionTotalSize           int     `toml:"compaction-total-size" json:"compaction-total-size"`
	CompactionTotalSizeMultiplier float64 `toml:"compaction-total-size-multiplier" json:"compaction-total-size-multiplier"`
	WriteBuffer                   int     `toml:"write-buffer" json:"write-buffer"`
	WriteL0PauseTrigger           int     `toml:"write-L0-pause-trigger" json:"write-L0-pause-trigger"`
	WriteL0SlowdownTrigger        int     `toml:"write-L0-slowdown-trigger" json:"write-L0-slowdown-trigger"`
}

// default leveldb config.
var defaultKVConfig = &kvConfig{
	BlockCacheCapacity:            8388608,
	BlockRestartInterval:          16,
	BlockSize:                     4096,
	CompactionL0Trigger:           8,
	CompactionTableSize:           67108864,
	CompactionTotalSize:           536870912,
	CompactionTotalSizeMultiplier: 8,
	WriteBuffer:                   67108864,
	WriteL0PauseTrigger:           24,
	WriteL0SlowdownTrigger:        17,
}

// openDB opens a levelDB.
func openDB(kvDir string, config *kvConfig) (*leveldb.DB, error) {
	var opts opt.Options
	opts.BlockCacheCapacity = config.BlockCacheCapacity
	opts.BlockRestartInterval = config.BlockRestartInterval
	opts.BlockSize = config.BlockSize
	opts.CompactionL0Trigger = config.CompactionL0Trigger
	opts.CompactionTableSize = config.CompactionTableSize
	opts.CompactionTotalSize = config.CompactionTotalSize
	opts.CompactionTotalSizeMultiplier = config.CompactionTotalSizeMultiplier
	opts.WriteBuffer = config.WriteBuffer
	opts.WriteL0PauseTrigger = config.WriteL0PauseTrigger
	opts.WriteL0SlowdownTrigger = config.WriteL0SlowdownTrigger

	db, err := leveldb.OpenFile(kvDir, &opts)
	return db, terror.ErrWorkerOpenKVDBFile.Delegate(err)
}
