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
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin/binding"
	"github.com/imdario/mergo"
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

	// control batch behavior, only for `open-protocol` and `craft` at the moment.
	MaxMessageBytes int
	MaxBatchSize    int

	// onlyHandleKeyColumns is true, for the delete event only output the handle key columns.
	DeleteOnlyHandleKeyColumns bool

	EnableTiDBExtension bool
	EnableRowChecksum   bool

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

	// for open protocol
	OnlyOutputUpdatedColumns bool

	LargeMessageHandle *config.LargeMessageHandleConfig
}

// NewConfig return a Config for codec
func NewConfig(protocol config.Protocol) *Config {
	return &Config{
		Protocol: protocol,

		MaxMessageBytes: config.DefaultMaxMessageBytes,
		MaxBatchSize:    defaultMaxBatchSize,

		EnableTiDBExtension:            false,
		EnableRowChecksum:              false,
		AvroSchemaRegistry:             "",
		AvroDecimalHandlingMode:        "precise",
		AvroBigintUnsignedHandlingMode: "long",

		OnlyOutputUpdatedColumns: false,

		LargeMessageHandle: config.NewDefaultLargeMessageHandleConfig(),
	}
}

const (
	codecOPTEnableTiDBExtension            = "enable-tidb-extension"
	codecOPTAvroDecimalHandlingMode        = "avro-decimal-handling-mode"
	codecOPTAvroBigintUnsignedHandlingMode = "avro-bigint-unsigned-handling-mode"
	codecOPTAvroSchemaRegistry             = "schema-registry"

	codecOPTOnlyOutputUpdatedColumns = "only-output-updated-columns"
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

type urlConfig struct {
	EnableTiDBExtension            *bool   `form:"enable-tidb-extension"`
	MaxBatchSize                   *int    `form:"max-batch-size"`
	MaxMessageBytes                *int    `form:"max-message-bytes"`
	AvroDecimalHandlingMode        *string `form:"avro-decimal-handling-mode"`
	AvroBigintUnsignedHandlingMode *string `form:"avro-bigint-unsigned-handling-mode"`

	AvroSchemaRegistry       string `form:"schema-registry"`
	OnlyOutputUpdatedColumns *bool  `form:"only-output-updated-columns"`
}

// Apply fill the Config
func (c *Config) Apply(sinkURI *url.URL, replicaConfig *config.ReplicaConfig) error {
	req := &http.Request{URL: sinkURI}
	var err error
	urlParameter := &urlConfig{}
	if err := binding.Query.Bind(req, urlParameter); err != nil {
		return cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	if urlParameter, err = mergeConfig(replicaConfig, urlParameter); err != nil {
		return err
	}

	if urlParameter.EnableTiDBExtension != nil {
		c.EnableTiDBExtension = *urlParameter.EnableTiDBExtension
	}

	if urlParameter.MaxBatchSize != nil {
		c.MaxBatchSize = *urlParameter.MaxBatchSize
	}

	if urlParameter.MaxMessageBytes != nil {
		c.MaxMessageBytes = *urlParameter.MaxMessageBytes
	}

	if urlParameter.AvroDecimalHandlingMode != nil &&
		*urlParameter.AvroDecimalHandlingMode != "" {
		c.AvroDecimalHandlingMode = *urlParameter.AvroDecimalHandlingMode
	}
	if urlParameter.AvroBigintUnsignedHandlingMode != nil &&
		*urlParameter.AvroBigintUnsignedHandlingMode != "" {
		c.AvroBigintUnsignedHandlingMode = *urlParameter.AvroBigintUnsignedHandlingMode
	}

	if urlParameter.AvroSchemaRegistry != "" {
		c.AvroSchemaRegistry = urlParameter.AvroSchemaRegistry
	}

	if replicaConfig.Sink != nil {
		c.Terminator = replicaConfig.Sink.Terminator
		if replicaConfig.Sink.CSVConfig != nil {
			c.Delimiter = replicaConfig.Sink.CSVConfig.Delimiter
			c.Quote = replicaConfig.Sink.CSVConfig.Quote
			c.NullString = replicaConfig.Sink.CSVConfig.NullString
			c.IncludeCommitTs = replicaConfig.Sink.CSVConfig.IncludeCommitTs
			c.BinaryEncodingMethod = replicaConfig.Sink.CSVConfig.BinaryEncodingMethod
		}

		if replicaConfig.Sink.KafkaConfig != nil {
			if replicaConfig.Sink.KafkaConfig.LargeMessageHandle != nil {
				c.LargeMessageHandle = replicaConfig.Sink.KafkaConfig.LargeMessageHandle
			}
			if c.LargeMessageHandle.HandleKeyOnly() && replicaConfig.ForceReplicate {
				return cerror.ErrCodecInvalidConfig.GenWithStack(
					`force-replicate must be disabled, when the large message handle option is set to "handle-key-only"`)
			}
		}
	}
	if urlParameter.OnlyOutputUpdatedColumns != nil {
		c.OnlyOutputUpdatedColumns = *urlParameter.OnlyOutputUpdatedColumns
	}
	if c.OnlyOutputUpdatedColumns && !replicaConfig.EnableOldValue {
		return cerror.ErrCodecInvalidConfig.GenWithStack(
			`old value must be enabled when configuration "%s" is true.`,
			codecOPTOnlyOutputUpdatedColumns,
		)
	}

	if replicaConfig.Integrity != nil {
		c.EnableRowChecksum = replicaConfig.Integrity.Enabled()
	}

	c.DeleteOnlyHandleKeyColumns = !replicaConfig.EnableOldValue

	return nil
}

func mergeConfig(
	replicaConfig *config.ReplicaConfig,
	urlParameters *urlConfig,
) (*urlConfig, error) {
	dest := &urlConfig{}
	if replicaConfig.Sink != nil {
		dest.AvroSchemaRegistry = replicaConfig.Sink.SchemaRegistry
		dest.OnlyOutputUpdatedColumns = replicaConfig.Sink.OnlyOutputUpdatedColumns
		if replicaConfig.Sink.KafkaConfig != nil {
			dest.MaxMessageBytes = replicaConfig.Sink.KafkaConfig.MaxMessageBytes
			if replicaConfig.Sink.KafkaConfig.CodecConfig != nil {
				codecConfig := replicaConfig.Sink.KafkaConfig.CodecConfig
				dest.EnableTiDBExtension = codecConfig.EnableTiDBExtension
				dest.MaxBatchSize = codecConfig.MaxBatchSize
				dest.AvroDecimalHandlingMode = codecConfig.AvroDecimalHandlingMode
				dest.AvroBigintUnsignedHandlingMode = codecConfig.AvroBigintUnsignedHandlingMode
			}
		}
	}
	if err := mergo.Merge(dest, urlParameters, mergo.WithOverride); err != nil {
		return nil, err
	}
	return dest, nil
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

		if c.EnableRowChecksum {
			if !(c.EnableTiDBExtension && c.AvroDecimalHandlingMode == DecimalHandlingModeString &&
				c.AvroBigintUnsignedHandlingMode == BigintUnsignedHandlingModeString) {
				return cerror.ErrCodecInvalidConfig.GenWithStack(
					`Avro protocol with row level checksum,
					should set "%s" to "%s", and set "%s" to "%s" and "%s" to "%s"`,
					codecOPTEnableTiDBExtension, "true",
					codecOPTAvroDecimalHandlingMode, DecimalHandlingModeString,
					codecOPTAvroBigintUnsignedHandlingMode, BigintUnsignedHandlingModeString)
			}
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
		err := c.LargeMessageHandle.AdjustAndValidate(c.Protocol, c.EnableTiDBExtension)
		if err != nil {
			return err
		}
	}

	return nil
}
