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
	MaxMessageBytes int
	MaxBatchSize    int

	// canal-json only
	EnableTiDBExtension bool

	// avro only
	AvroSchemaRegistry             string
	AvroDecimalHandlingMode        string
	AvroBigintUnsignedHandlingMode string
}

// NewConfig return a Config for codec
func NewConfig(protocol config.Protocol) *Config {
	return &Config{
		protocol: protocol,

		MaxMessageBytes: config.DefaultMaxMessageBytes,
		MaxBatchSize:    defaultMaxBatchSize,

		EnableTiDBExtension:            false,
		AvroSchemaRegistry:             "",
		AvroDecimalHandlingMode:        "precise",
		AvroBigintUnsignedHandlingMode: "long",
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
	DecimalHandlingModeString        = "string"
	DecimalHandlingModePrecise       = "precise"
	BigintUnsignedHandlingModeString = "string"
	BigintUnsignedHandlingModeLong   = "long"
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
		c.MaxBatchSize = a
	}

	if s := params.Get(codecOPTMaxMessageBytes); s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.MaxMessageBytes = a
	}

	if s := params.Get(codecOPTAvroDecimalHandlingMode); s != "" {
		c.AvroDecimalHandlingMode = s
	}

	if s := params.Get(codecOPTAvroBigintUnsignedHandlingMode); s != "" {
		c.AvroBigintUnsignedHandlingMode = s
	}

	if config.Sink != nil && config.Sink.SchemaRegistry != "" {
		c.AvroSchemaRegistry = config.Sink.SchemaRegistry
	}

	return nil
}

// WithMaxMessageBytes set the `maxMessageBytes`
func (c *Config) WithMaxMessageBytes(bytes int) *Config {
	c.MaxMessageBytes = bytes
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
		if c.AvroSchemaRegistry == "" {
			return cerror.ErrMQCodecInvalidConfig.GenWithStack(
				`Avro protocol requires parameter "%s"`,
				codecOPTAvroSchemaRegistry,
			)
		}

		if c.AvroDecimalHandlingMode != DecimalHandlingModePrecise &&
			c.AvroDecimalHandlingMode != DecimalHandlingModeString {
			return cerror.ErrMQCodecInvalidConfig.GenWithStack(
				`%s value could only be "%s" or "%s"`,
				codecOPTAvroDecimalHandlingMode,
				DecimalHandlingModeString,
				DecimalHandlingModePrecise,
			)
		}

		if c.AvroBigintUnsignedHandlingMode != BigintUnsignedHandlingModeLong &&
			c.AvroBigintUnsignedHandlingMode != BigintUnsignedHandlingModeString {
			return cerror.ErrMQCodecInvalidConfig.GenWithStack(
				`%s value could only be "%s" or "%s"`,
				codecOPTAvroBigintUnsignedHandlingMode,
				BigintUnsignedHandlingModeLong,
				BigintUnsignedHandlingModeString,
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

// GetMaxMessageBytes return the axMessageBytes for the codec
func (c *Config) GetMaxMessageBytes() int {
	return c.maxMessageBytes
}

// Protocol return the protocol for the codec
func (c *Config) Protocol() config.Protocol {
	return c.protocol
}
