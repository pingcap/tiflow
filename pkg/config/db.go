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
	"math"

	"github.com/pingcap/tiflow/pkg/errors"
)

// DBConfig represents db sorter config.
type DBConfig struct {
	// Count is the number of db count.
	//
	// The default value is 8.
	Count int `toml:"count" json:"count"`
	// MaxOpenFiles is the maximum number of open FD by db sorter.
	//
	// The default value is 10000.
	MaxOpenFiles int `toml:"max-open-files" json:"max-open-files"`
	// BlockSize the block size of db sorter.
	//
	// The default value is 65536, 64KB.
	BlockSize int `toml:"block-size" json:"block-size"`
	// WriterBufferSize is the size of memory table of db.
	//
	// The default value is 8388608, 8MB.
	WriterBufferSize int `toml:"writer-buffer-size" json:"writer-buffer-size"`
	// Compression is the compression algorithm that is used by db.
	// Valid values are "none" or "snappy".
	//
	// The default value is "snappy".
	Compression string `toml:"compression" json:"compression"`
	// WriteL0PauseTrigger defines number of db sst file at level-0 that will
	// pause write.
	//
	// The default value is 1<<31 - 1.
	WriteL0PauseTrigger int `toml:"write-l0-pause-trigger" json:"write-l0-pause-trigger"`

	// CompactionL0Trigger defines number of db sst file at level-0 that will
	// trigger compaction.
	//
	// The default value is 16, which is based on a performance test on 4K tables.
	CompactionL0Trigger int `toml:"compaction-l0-trigger" json:"compaction-l0-trigger"`
}

// NewDefaultDBConfig return the default db configuration
func NewDefaultDBConfig() *DBConfig {
	return &DBConfig{
		Count: 8,
		// Following configs are optimized for write/read throughput.
		// Users should not change them.
		MaxOpenFiles:        10000,
		BlockSize:           65536,
		WriterBufferSize:    8388608,
		Compression:         "snappy",
		WriteL0PauseTrigger: math.MaxInt32,
		CompactionL0Trigger: 16, // Based on a performance test on 4K tables.
	}
}

// ValidateAndAdjust validates and adjusts the db configuration
func (c *DBConfig) ValidateAndAdjust() error {
	if c.Compression != "none" && c.Compression != "snappy" {
		return errors.ErrIllegalSorterParameter.GenWithStackByArgs(
			"sorter.leveldb.compression must be \"none\" or \"snappy\"")
	}

	return nil
}
