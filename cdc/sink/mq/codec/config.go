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
	avroSchemaRegistry             string
	avroDecimalHandlingMode        string
	avroBigintUnsignedHandlingMode string
}

// NewConfig return a Config for codec
func NewConfig(protocol config.Protocol) *Config {
	return &Config{
		protocol: protocol,

		maxMessageBytes: config.DefaultMaxMessageBytes,
		maxBatchSize:    defaultMaxBatchSize,

		enableTiDBExtension:            false,
		avroSchemaRegistry:             "",
		avroDecimalHandlingMode:        "precise",
		avroBigintUnsignedHandlingMode: "long",
	}
}

const (
	codecOPTEnableTiDBExtension            = "enable-tidb-extension"
	codecOPTMaxBatchSize                   = "max-batch-size"
	codecOPTMaxMessageBytes                = "max-message-bytes"
	codecOPTAvroDecimalHandlingMode        = "avro-decimal-handling-mode"
	codecOPTAvroBigintUnsignedHandlingMode = "avro-bigint-unsigned-handling-mode"
	codecOPTAvroSchemaRegistry             = "schema-registry"
)

const (
	decimalHandlingModeString        = "string"
	decimalHandlingModePrecise       = "precise"
	bigintUnsignedHandlingModeString = "string"
	bigintUnsignedHandlingModeLong   = "long"
)

// Apply fill the Config
func (c *Config) Apply(sinkURI *url.URL, config *config.ReplicaConfig) error {
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

	if s := params.Get(codecOPTAvroDecimalHandlingMode); s != "" {
		c.avroDecimalHandlingMode = s
	}

	if s := params.Get(codecOPTAvroBigintUnsignedHandlingMode); s != "" {
		c.avroBigintUnsignedHandlingMode = s
	}

	if config.Sink != nil && config.Sink.SchemaRegistry != "" {
		c.avroSchemaRegistry = config.Sink.SchemaRegistry
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
	if c.enableTiDBExtension &&
		!(c.protocol == config.ProtocolCanalJSON || c.protocol == config.ProtocolAvro) {
		return cerror.ErrMQCodecInvalidConfig.GenWithStack(
			`enable-tidb-extension only supports canal-json/avro protocol`,
		)
	}

	if c.protocol == config.ProtocolAvro {
		if c.avroSchemaRegistry == "" {
			return cerror.ErrMQCodecInvalidConfig.GenWithStack(
				`Avro protocol requires parameter "%s"`,
				codecOPTAvroSchemaRegistry,
			)
		}

		if c.avroDecimalHandlingMode != decimalHandlingModePrecise &&
			c.avroDecimalHandlingMode != decimalHandlingModeString {
			return cerror.ErrMQCodecInvalidConfig.GenWithStack(
				`%s value could only be "%s" or "%s"`,
				codecOPTAvroDecimalHandlingMode,
				decimalHandlingModeString,
				decimalHandlingModePrecise,
			)
		}

		if c.avroBigintUnsignedHandlingMode != bigintUnsignedHandlingModeLong &&
			c.avroBigintUnsignedHandlingMode != bigintUnsignedHandlingModeString {
			return cerror.ErrMQCodecInvalidConfig.GenWithStack(
				`%s value could only be "%s" or "%s"`,
				codecOPTAvroBigintUnsignedHandlingMode,
				bigintUnsignedHandlingModeLong,
				bigintUnsignedHandlingModeString,
			)
		}
	}

	if c.maxMessageBytes <= 0 {
		return cerror.ErrMQCodecInvalidConfig.Wrap(
			errors.Errorf("invalid max-message-bytes %d", c.maxMessageBytes),
		)
	}

	if c.maxBatchSize <= 0 {
		return cerror.ErrMQCodecInvalidConfig.Wrap(
			errors.Errorf("invalid max-batch-size %d", c.maxBatchSize),
		)
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
