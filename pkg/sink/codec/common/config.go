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
	"time"

	"github.com/gin-gonic/gin/binding"
	"github.com/imdario/mergo"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// defaultMaxBatchSize sets the default value for max-batch-size
const defaultMaxBatchSize int = 16

// Config use to create the encoder
type Config struct {
	ChangefeedID model.ChangeFeedID

	Protocol config.Protocol

	// control batch behavior, only for `open-protocol` and `craft` at the moment.
	MaxMessageBytes int
	MaxBatchSize    int

	// DeleteOnlyHandleKeyColumns is true, for the delete event only output the handle key columns.
	DeleteOnlyHandleKeyColumns bool

	LargeMessageHandle *config.LargeMessageHandleConfig

	EnableTiDBExtension bool
	EnableRowChecksum   bool

	// avro only
	AvroConfluentSchemaRegistry    string
	AvroDecimalHandlingMode        string
	AvroBigintUnsignedHandlingMode string
	AvroGlueSchemaRegistry         *config.GlueSchemaRegistryConfig
	// EnableWatermarkEvent set to true, avro encode DDL and checkpoint event
	// and send to the downstream kafka, they cannot be consumed by the confluent official consumer
	// and would cause error, so this is only used for ticdc internal testing purpose, should not be
	// exposed to the outside users.
	AvroEnableWatermark bool

	// canal-json only
	ContentCompatible bool

	// for sinking to cloud storage
	Delimiter            string
	Quote                string
	NullString           string
	IncludeCommitTs      bool
	Terminator           string
	BinaryEncodingMethod string

	// for open protocol
	OnlyOutputUpdatedColumns bool

	TimeZone *time.Location
	// for the simple protocol, can be "json" and "avro", default to "json"
	EncodingFormat EncodingFormatType
	// Whether old value should be excluded in the output.
	OpenOutputOldValue bool
}

// EncodingFormatType is the type of encoding format
type EncodingFormatType string

const (
	// EncodingFormatJSON is the json format
	EncodingFormatJSON EncodingFormatType = "json"
	// EncodingFormatAvro is the avro format
	EncodingFormatAvro EncodingFormatType = "avro"
)

// NewConfig return a Config for codec
func NewConfig(protocol config.Protocol) *Config {
	return &Config{
		Protocol: protocol,

		MaxMessageBytes: config.DefaultMaxMessageBytes,
		MaxBatchSize:    defaultMaxBatchSize,

		EnableTiDBExtension: false,
		EnableRowChecksum:   false,

		AvroConfluentSchemaRegistry:    "",
		AvroDecimalHandlingMode:        "precise",
		AvroBigintUnsignedHandlingMode: "long",
		AvroEnableWatermark:            false,

		OnlyOutputUpdatedColumns:   false,
		DeleteOnlyHandleKeyColumns: false,
		LargeMessageHandle:         config.NewDefaultLargeMessageHandleConfig(),

		EncodingFormat: EncodingFormatJSON,
		TimeZone:       time.Local,

		// default value is true
		OpenOutputOldValue: true,
	}
}

const (
	codecOPTEnableTiDBExtension            = "enable-tidb-extension"
	codecOPTAvroDecimalHandlingMode        = "avro-decimal-handling-mode"
	codecOPTAvroBigintUnsignedHandlingMode = "avro-bigint-unsigned-handling-mode"
	codecOPTAvroSchemaRegistry             = "schema-registry"
	coderOPTAvroGlueSchemaRegistry         = "glue-schema-registry"
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

	// AvroEnableWatermark is the option for enabling watermark in avro protocol
	// only used for internal testing, do not set this in the production environment since the
	// confluent official consumer cannot handle watermark.
	AvroEnableWatermark *bool `form:"avro-enable-watermark"`

	AvroSchemaRegistry       string `form:"schema-registry"`
	OnlyOutputUpdatedColumns *bool  `form:"only-output-updated-columns"`
	// EncodingFormatType is only works for the simple protocol,
	// can be `json` and `avro`, default to `json`.
	EncodingFormatType *string `form:"encoding-format"`
	ContentCompatible  *bool   `form:"content-compatible"`
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

	// avro related
	if urlParameter.AvroDecimalHandlingMode != nil &&
		*urlParameter.AvroDecimalHandlingMode != "" {
		c.AvroDecimalHandlingMode = *urlParameter.AvroDecimalHandlingMode
	}
	if urlParameter.AvroBigintUnsignedHandlingMode != nil &&
		*urlParameter.AvroBigintUnsignedHandlingMode != "" {
		c.AvroBigintUnsignedHandlingMode = *urlParameter.AvroBigintUnsignedHandlingMode
	}
	if urlParameter.AvroEnableWatermark != nil {
		if c.EnableTiDBExtension && c.Protocol == config.ProtocolAvro {
			c.AvroEnableWatermark = *urlParameter.AvroEnableWatermark
		}
	}
	if urlParameter.AvroSchemaRegistry != "" {
		c.AvroConfluentSchemaRegistry = urlParameter.AvroSchemaRegistry
	}
	if replicaConfig.Sink.KafkaConfig != nil &&
		replicaConfig.Sink.KafkaConfig.GlueSchemaRegistryConfig != nil {
		c.AvroGlueSchemaRegistry = replicaConfig.Sink.KafkaConfig.GlueSchemaRegistryConfig
	}
	if c.Protocol == config.ProtocolAvro && replicaConfig.ForceReplicate {
		return cerror.ErrCodecInvalidConfig.GenWithStack(
			`force-replicate must be disabled, when using avro protocol`)
	}

	if replicaConfig.Sink != nil {
		c.Terminator = util.GetOrZero(replicaConfig.Sink.Terminator)
		if replicaConfig.Sink.CSVConfig != nil {
			c.Delimiter = replicaConfig.Sink.CSVConfig.Delimiter
			c.Quote = replicaConfig.Sink.CSVConfig.Quote
			c.NullString = replicaConfig.Sink.CSVConfig.NullString
			c.IncludeCommitTs = replicaConfig.Sink.CSVConfig.IncludeCommitTs
			c.BinaryEncodingMethod = replicaConfig.Sink.CSVConfig.BinaryEncodingMethod
		}
		if replicaConfig.Sink.KafkaConfig != nil && replicaConfig.Sink.KafkaConfig.LargeMessageHandle != nil {
			c.LargeMessageHandle = replicaConfig.Sink.KafkaConfig.LargeMessageHandle
		}
		if !c.LargeMessageHandle.Disabled() && replicaConfig.ForceReplicate {
			return cerror.ErrCodecInvalidConfig.GenWithStack(
				`force-replicate must be disabled, when the large message handle is enabled, large message handle: "%s"`,
				c.LargeMessageHandle.LargeMessageHandleOption)
		}
		if replicaConfig.Sink.OpenProtocol != nil {
			c.OpenOutputOldValue = replicaConfig.Sink.OpenProtocol.OutputOldValue
		}
	}
	if urlParameter.OnlyOutputUpdatedColumns != nil {
		c.OnlyOutputUpdatedColumns = *urlParameter.OnlyOutputUpdatedColumns
	}

	if replicaConfig.Integrity != nil {
		c.EnableRowChecksum = replicaConfig.Integrity.Enabled()
	}

	c.DeleteOnlyHandleKeyColumns = util.GetOrZero(replicaConfig.Sink.DeleteOnlyOutputHandleKeyColumns)
	if c.DeleteOnlyHandleKeyColumns && replicaConfig.ForceReplicate {
		return cerror.ErrCodecInvalidConfig.GenWithStack(
			`force-replicate must be disabled when configuration "delete-only-output-handle-key-columns" is true.`)
	}

	if c.Protocol == config.ProtocolSimple {
		if urlParameter.EncodingFormatType != nil {
			s := *urlParameter.EncodingFormatType
			if s != "" {
				encodingFormat := EncodingFormatType(s)
				switch encodingFormat {
				case EncodingFormatJSON, EncodingFormatAvro:
					c.EncodingFormat = encodingFormat
				default:
					return cerror.ErrCodecInvalidConfig.GenWithStack(
						"unsupported encoding format type: %s for the simple protocol", encodingFormat)
				}
			}
		}
	}
	if c.Protocol == config.ProtocolCanalJSON {
		c.ContentCompatible = util.GetOrZero(urlParameter.ContentCompatible)
		if c.ContentCompatible {
			c.OnlyOutputUpdatedColumns = true
		}
	}

	return nil
}

func mergeConfig(
	replicaConfig *config.ReplicaConfig,
	urlParameters *urlConfig,
) (*urlConfig, error) {
	dest := &urlConfig{}
	if replicaConfig.Sink != nil {
		dest.AvroSchemaRegistry = util.GetOrZero(replicaConfig.Sink.SchemaRegistry)
		dest.OnlyOutputUpdatedColumns = replicaConfig.Sink.OnlyOutputUpdatedColumns
		dest.ContentCompatible = replicaConfig.Sink.ContentCompatible
		if util.GetOrZero(dest.ContentCompatible) {
			dest.OnlyOutputUpdatedColumns = util.AddressOf(true)
		}
		if replicaConfig.Sink.KafkaConfig != nil {
			dest.MaxMessageBytes = replicaConfig.Sink.KafkaConfig.MaxMessageBytes
			if replicaConfig.Sink.KafkaConfig.CodecConfig != nil {
				codecConfig := replicaConfig.Sink.KafkaConfig.CodecConfig
				dest.EnableTiDBExtension = codecConfig.EnableTiDBExtension
				dest.MaxBatchSize = codecConfig.MaxBatchSize
				dest.AvroEnableWatermark = codecConfig.AvroEnableWatermark
				dest.AvroDecimalHandlingMode = codecConfig.AvroDecimalHandlingMode
				dest.AvroBigintUnsignedHandlingMode = codecConfig.AvroBigintUnsignedHandlingMode
				dest.EncodingFormatType = codecConfig.EncodingFormat
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

// WithChangefeedID set the `changefeedID`
func (c *Config) WithChangefeedID(id model.ChangeFeedID) *Config {
	c.ChangefeedID = id
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
		if c.AvroConfluentSchemaRegistry != "" && c.AvroGlueSchemaRegistry != nil {
			return cerror.ErrCodecInvalidConfig.GenWithStack(
				`Avro protocol requires only one of "%s" or "%s" to specify the schema registry`,
				codecOPTAvroSchemaRegistry,
				coderOPTAvroGlueSchemaRegistry,
			)
		}

		if c.AvroConfluentSchemaRegistry == "" && c.AvroGlueSchemaRegistry == nil {
			return cerror.ErrCodecInvalidConfig.GenWithStack(
				`Avro protocol requires parameter "%s" or "%s" to specify the schema registry`,
				codecOPTAvroSchemaRegistry,
				coderOPTAvroGlueSchemaRegistry,
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

const (
	// SchemaRegistryTypeConfluent is the type of Confluent Schema Registry
	SchemaRegistryTypeConfluent = "confluent"
	// SchemaRegistryTypeGlue is the type of AWS Glue Schema Registry
	SchemaRegistryTypeGlue = "glue"
)

// SchemaRegistryType returns the type of schema registry
func (c *Config) SchemaRegistryType() string {
	if c.AvroConfluentSchemaRegistry != "" {
		return SchemaRegistryTypeConfluent
	}
	if c.AvroGlueSchemaRegistry != nil {
		return SchemaRegistryTypeGlue
	}
	return "unknown"
}
