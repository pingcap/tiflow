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

import (
	"net/url"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// defaultMaxBatchSize sets the default value for max-batch-size
const defaultMaxBatchSize int = 16

// Config use to create the encoder
type Config struct {
	protocol config.Protocol

	// control batch behavior, only for `open-protocol` and `craft` at the moment.
	maxMessageBytes int
	maxBatchSize    int

	// canal-json only
	enableTiDBExtension bool

	// avro only
	avroRegistry string
	tz           *time.Location
}

// NewConfig return a Config for codec
func NewConfig(protocol config.Protocol, tz *time.Location) *Config {
	return &Config{
		protocol: protocol,

		maxMessageBytes: config.DefaultMaxMessageBytes,
		maxBatchSize:    defaultMaxBatchSize,

		enableTiDBExtension: false,
		avroRegistry:        "",
		tz:                  tz,
	}
}

const (
	codecOPTEnableTiDBExtension = "enable-tidb-extension"
	codecOPTMaxBatchSize        = "max-batch-size"
	codecOPTMaxMessageBytes     = "max-message-bytes"
	codecAvroRegistry           = "registry"
)

// Apply fill the Config
func (c *Config) Apply(sinkURI *url.URL, opts map[string]string) error {
	params := sinkURI.Query()
	if s := params.Get(codecOPTEnableTiDBExtension); s != "" {
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		c.enableTiDBExtension = b
	}

	if s := params.Get(codecOPTMaxBatchSize); s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.maxBatchSize = a
	}

	if s := params.Get(codecOPTMaxMessageBytes); s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.maxMessageBytes = a
	}

	if s, ok := opts[codecAvroRegistry]; ok {
		c.avroRegistry = s
	}

	return nil
}

// WithMaxMessageBytes set the `maxMessageBytes`
func (c *Config) WithMaxMessageBytes(bytes int) *Config {
	c.maxMessageBytes = bytes
	return c
}

// Validate the Config
func (c *Config) Validate() error {
	if c.protocol != config.ProtocolCanalJSON && c.enableTiDBExtension {
		return cerror.ErrMQCodecInvalidConfig.GenWithStack(`enable-tidb-extension only support canal-json protocol`)
	}

	if c.protocol == config.ProtocolAvro {
		if c.avroRegistry == "" {
			return cerror.ErrMQCodecInvalidConfig.GenWithStack(`Avro protocol requires parameter "registry"`)
		}

		if c.tz == nil {
			return cerror.ErrMQCodecInvalidConfig.GenWithStack("Avro protocol requires timezone to be set")
		}
	}

	if c.maxMessageBytes <= 0 {
		return cerror.ErrMQCodecInvalidConfig.Wrap(errors.Errorf("invalid max-message-bytes %d", c.maxMessageBytes))
	}

	if c.maxBatchSize <= 0 {
		return cerror.ErrMQCodecInvalidConfig.Wrap(errors.Errorf("invalid max-batch-size %d", c.maxBatchSize))
	}

	return nil
}

// MaxMessageBytes return the maxMessageBytes for the codec
func (c *Config) MaxMessageBytes() int {
	return c.maxMessageBytes
}

// Protocol return the protocol for the codec
func (c *Config) Protocol() config.Protocol {
	return c.protocol
}
