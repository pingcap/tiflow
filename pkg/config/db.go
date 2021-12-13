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

import cerror "github.com/pingcap/ticdc/pkg/errors"

// DBConfig represents leveldb sorter config.
type DBConfig struct {
	// Count is the number of leveldb count.
	//
	// The default value is 16.
	Count int `toml:"count" json:"count"`
	// Concurrency is the maximum write and read concurrency.
	//
	// The default value is 256.
	Concurrency int `toml:"concurrency" json:"concurrency"`
	// MaxOpenFiles is the maximum number of open FD by leveldb sorter.
	//
	// The default value is 10000.
	MaxOpenFiles int `toml:"max-open-files" json:"max-open-files"`
	// BlockSize the block size of leveldb sorter.
	//
	// The default value is 65536, 64KB.
	BlockSize int `toml:"block-size" json:"block-size"`
	// BlockCacheSize is the capacity of leveldb block cache.
	//
	// The default value is 4294967296, 4GB.
	BlockCacheSize int `toml:"block-cache-size" json:"block-cache-size"`
	// WriterBufferSize is the size of memory table of leveldb.
	//
	// The default value is 8388608, 8MB.
	WriterBufferSize int `toml:"writer-buffer-size" json:"writer-buffer-size"`
	// Compression is the compression algorithm that is used by leveldb.
	// Valid values are "none" or "snappy".
	//
	// The default value is "snappy".
	Compression string `toml:"compression" json:"compression"`
	// TargetFileSizeBase limits size of leveldb sst file that compaction generates.
	//
	// The default value is 8388608, 8MB.
	TargetFileSizeBase int `toml:"target-file-size-base" json:"target-file-size-base"`
	// CompactionL0Trigger defines number of leveldb sst file at level-0 that will
	// trigger compaction.
	//
	// The default value is 160.
	CompactionL0Trigger int `toml:"compaction-l0-trigger" json:"compaction-l0-trigger"`
	// WriteL0SlowdownTrigger defines number of leveldb sst file at level-0 that
	// will trigger write slowdown.
	//
	// The default value is 1<<31 - 1.
	WriteL0SlowdownTrigger int `toml:"write-l0-slowdown-trigger" json:"write-l0-slowdown-trigger"`
	// WriteL0PauseTrigger defines number of leveldb sst file at level-0 that will
	// pause write.
	//
	// The default value is 1<<31 - 1.
	WriteL0PauseTrigger int `toml:"write-l0-pause-trigger" json:"write-l0-pause-trigger"`
	// CleanupSpeedLimit limits clean up speed, based on key value entry count.
	//
	// The default value is 10000.
	CleanupSpeedLimit int `toml:"cleanup-speed-limit" json:"cleanup-speed-limit"`
}

// ValidateAndAdjust validates and adjusts the db configuration
func (c *DBConfig) ValidateAndAdjust() error {
	if c.Compression != "none" && c.Compression != "snappy" {
		return cerror.ErrIllegalSorterParameter.GenWithStackByArgs("sorter.leveldb.compression must be \"none\" or \"snappy\"")
	}
	if c.CleanupSpeedLimit <= 1 {
		return cerror.ErrIllegalSorterParameter.GenWithStackByArgs("sorter.leveldb.cleanup-speed-limit must be larger than 1")
	}

	return nil
}
