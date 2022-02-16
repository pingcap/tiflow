// Copyright 2022 PingCAP, Inc.
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

package codec

import "github.com/pingcap/tiflow/pkg/config"

// defaultMaxBatchSize sets the default value for max-batch-size
const defaultMaxBatchSize int = 16

// Config use to create the encoder
type Config struct {
	maxMessageBytes int
	maxBatchSize    int

	// canal-json
	enableTiDBExtension bool

	// Avro
	avroRegistry string
}

func NewConfig() *Config {
	return &Config{
		maxMessageBytes:     config.DefaultMaxMessageBytes,
		maxBatchSize:        defaultMaxBatchSize,
		enableTiDBExtension: false,
	}
}

func (c *Config) WithMaxMessageBytes(bytes int) *Config {
	c.maxBatchSize = bytes
	return c
}

func (c *Config) WithMaxBatchSize(size int) *Config {
	c.maxBatchSize = size
	return c
}

func (c *Config) WithEnableTiDBExtension() *Config {
	c.enableTiDBExtension = true
	return c
}

func (c *Config) WithAvroRegistry(registry string) *Config {
	c.avroRegistry = registry
	return c
}
