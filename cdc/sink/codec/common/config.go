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

package common

import (
	"net/url"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// defaultMaxBatchSize sets the default value for max-batch-size
const defaultMaxBatchSize int = 16

// Config use to create the encoder
type Config struct {
	Protocol config.Protocol

	DeleteOnlyHandleKeyColumns bool

	// control batch behavior, only for `open-protocol` and `craft` at the moment.
	MaxMessageBytes int
	MaxBatchSize    int

	// canal-json only
	EnableTiDBExtension bool
	ContentCompatible   bool

	// avro only
	AvroSchemaRegistry             string
	AvroDecimalHandlingMode        string
	AvroBigintUnsignedHandlingMode string

	// for sinking to cloud storage
	Delimiter            string
	Quote                string
	NullString           string
	IncludeCommitTs      bool
	Terminator           string
	BinaryEncodingMethod string

	LargeMessageHandle *config.LargeMessageHandleConfig
}

// NewConfig return a Config for codec
func NewConfig(protocol config.Protocol) *Config {
	return &Config{
		Protocol: protocol,

		MaxMessageBytes: config.DefaultMaxMessageBytes,
		MaxBatchSize:    defaultMaxBatchSize,

		EnableTiDBExtension:            false,
		AvroSchemaRegistry:             "",
		AvroDecimalHandlingMode:        "precise",
		AvroBigintUnsignedHandlingMode: "long",

		LargeMessageHandle: config.NewDefaultLargeMessageHandleConfig(),
	}
}

const (
	codecOPTEnableTiDBExtension            = "enable-tidb-extension"
	codecOPTMaxBatchSize                   = "max-batch-size"
	codecContentCompatible                 = "content-compatible"
	codecOPTMaxMessageBytes                = "max-message-bytes"
	codecOPTAvroDecimalHandlingMode        = "avro-decimal-handling-mode"
	codecOPTAvroBigintUnsignedHandlingMode = "avro-bigint-unsigned-handling-mode"
	codecOPTAvroSchemaRegistry             = "schema-registry"
)

const (
	// DecimalHandlingModeString is the string mode for decimal handling
	DecimalHandlingModeString = "string"
	// DecimalHandlingModePrecise is the precise mode for decimal handling
	DecimalHandlingModePrecise = "precise"
	// BigintUnsignedHandlingModeString is the string mode for unsigned bigint handling
	BigintUnsignedHandlingModeString = "string"
	// BigintUnsignedHandlingModeLong is the long mode for unsigned bigint handling
	BigintUnsignedHandlingModeLong = "long"
)

// Apply fill the Config
func (c *Config) Apply(sinkURI *url.URL, config *config.ReplicaConfig) error {
	params := sinkURI.Query()
	if s := params.Get(codecOPTEnableTiDBExtension); s != "" {
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		c.EnableTiDBExtension = b
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

	if s := params.Get(codecContentCompatible); s != "" {
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		c.ContentCompatible = b
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

	if config.Sink != nil {
		c.Terminator = config.Sink.Terminator
		if config.Sink.CSVConfig != nil {
			c.Delimiter = config.Sink.CSVConfig.Delimiter
			c.Quote = config.Sink.CSVConfig.Quote
			c.NullString = config.Sink.CSVConfig.NullString
			c.IncludeCommitTs = config.Sink.CSVConfig.IncludeCommitTs
			c.BinaryEncodingMethod = config.Sink.CSVConfig.BinaryEncodingMethod
		}

		if config.Sink.KafkaConfig != nil {
			c.LargeMessageHandle = config.Sink.KafkaConfig.LargeMessageHandle
			if c.LargeMessageHandle.HandleKeyOnly() && config.ForceReplicate {
				return cerror.ErrCodecInvalidConfig.GenWithStack(
					`force-replicate must be disabled, when the large message handle option is set to "handle-key-only"`)
			}
		}

	}

	c.DeleteOnlyHandleKeyColumns = !config.EnableOldValue
	return nil
}

// WithMaxMessageBytes set the `maxMessageBytes`
func (c *Config) WithMaxMessageBytes(bytes int) *Config {
	c.MaxMessageBytes = bytes
	return c
}

// Validate the Config
func (c *Config) Validate() error {
	if c.EnableTiDBExtension &&
		!(c.Protocol == config.ProtocolCanalJSON || c.Protocol == config.ProtocolAvro) {
		log.Warn("ignore invalid config, enable-tidb-extension"+
			"only supports canal-json/avro protocol",
			zap.Bool("enableTidbExtension", c.EnableTiDBExtension),
			zap.String("protocol", c.Protocol.String()))
	}

	if c.Protocol == config.ProtocolAvro {
		if c.AvroSchemaRegistry == "" {
			return cerror.ErrCodecInvalidConfig.GenWithStack(
				`Avro protocol requires parameter "%s"`,
				codecOPTAvroSchemaRegistry,
			)
		}

		if c.AvroDecimalHandlingMode != DecimalHandlingModePrecise &&
			c.AvroDecimalHandlingMode != DecimalHandlingModeString {
			return cerror.ErrCodecInvalidConfig.GenWithStack(
				`%s value could only be "%s" or "%s"`,
				codecOPTAvroDecimalHandlingMode,
				DecimalHandlingModeString,
				DecimalHandlingModePrecise,
			)
		}

		if c.AvroBigintUnsignedHandlingMode != BigintUnsignedHandlingModeLong &&
			c.AvroBigintUnsignedHandlingMode != BigintUnsignedHandlingModeString {
			return cerror.ErrCodecInvalidConfig.GenWithStack(
				`%s value could only be "%s" or "%s"`,
				codecOPTAvroBigintUnsignedHandlingMode,
				BigintUnsignedHandlingModeLong,
				BigintUnsignedHandlingModeString,
			)
		}
	}

	if c.MaxMessageBytes <= 0 {
		return cerror.ErrCodecInvalidConfig.Wrap(
			errors.Errorf("invalid max-message-bytes %d", c.MaxMessageBytes),
		)
	}

	if c.MaxBatchSize <= 0 {
		return cerror.ErrCodecInvalidConfig.Wrap(
			errors.Errorf("invalid max-batch-size %d", c.MaxBatchSize),
		)
	}

	if c.LargeMessageHandle != nil {
		err := c.LargeMessageHandle.Validate(c.Protocol, c.EnableTiDBExtension)
		if err != nil {
			return err
		}
	}

	return nil
}
