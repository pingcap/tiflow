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

package config

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

const (
	// DefaultMaxMessageBytes sets the default value for max-message-bytes.
	DefaultMaxMessageBytes = 10 * 1024 * 1024 // 10M
	// DefaultAdvanceTimeoutInSec sets the default value for advance-timeout-in-sec.
	DefaultAdvanceTimeoutInSec = uint(150)

	// TxnAtomicityKey specifies the key of the transaction-atomicity in the SinkURI.
	TxnAtomicityKey = "transaction-atomicity"
	// defaultTxnAtomicity is the default atomicity level.
	defaultTxnAtomicity = noneTxnAtomicity
	// unknownTxnAtomicity is an invalid atomicity level and will be treated as
	// defaultTxnAtomicity when initializing sink in processor.
	unknownTxnAtomicity AtomicityLevel = ""
	// noneTxnAtomicity means atomicity of transactions is not guaranteed
	noneTxnAtomicity AtomicityLevel = "none"
	// tableTxnAtomicity means atomicity of single table transactions is guaranteed.
	tableTxnAtomicity AtomicityLevel = "table"

	// Comma is a constant for ','
	Comma = ","
	// CR is an abbreviation for carriage return
	CR = '\r'
	// LF is an abbreviation for line feed
	LF = '\n'
	// CRLF is an abbreviation for '\r\n'
	CRLF = "\r\n"
	// DoubleQuoteChar is a constant for '"'
	DoubleQuoteChar = '"'
	// Backslash is a constant for '\'
	Backslash = '\\'
	// NULL is a constant for '\N'
	NULL = "\\N"

	// MinFileIndexWidth is the minimum width of file index.
	MinFileIndexWidth = 6 // enough for 2^19 files
	// MaxFileIndexWidth is the maximum width of file index.
	MaxFileIndexWidth = 20 // enough for 2^64 files
	// DefaultFileIndexWidth is the default width of file index.
	DefaultFileIndexWidth = MaxFileIndexWidth

	// BinaryEncodingHex encodes binary data to hex string.
	BinaryEncodingHex = "hex"
	// BinaryEncodingBase64 encodes binary data to base64 string.
	BinaryEncodingBase64 = "base64"

	// DefaultPulsarProducerCacheSize is the default size of the cache for producers
	// 10240 producers maybe cost 1.1G memory
	DefaultPulsarProducerCacheSize = 10240

	// DefaultEncoderGroupConcurrency is the default concurrency of encoder group.
	DefaultEncoderGroupConcurrency = 32

	// DefaultSendBootstrapIntervalInSec is the default interval to send bootstrap message.
	DefaultSendBootstrapIntervalInSec = int64(120)
	// DefaultSendBootstrapInMsgCount is the default number of messages to send bootstrap message.
	DefaultSendBootstrapInMsgCount = int32(10000)
	// DefaultSendBootstrapToAllPartition is the default value of
	// whether to send bootstrap message to all partitions.
	DefaultSendBootstrapToAllPartition = true

	// DefaultMaxReconnectToPulsarBroker is the default max reconnect times to pulsar broker.
	// The pulsar client uses an exponential backoff with jitter to reconnect to the broker.
	// Based on test, when the max reconnect times is 3,
	// the total time of reconnecting to brokers is about 30 seconds.
	DefaultMaxReconnectToPulsarBroker = 3
)

// AtomicityLevel represents the atomicity level of a changefeed.
type AtomicityLevel string

// ShouldSplitTxn returns whether the sink should split txn.
func (l AtomicityLevel) ShouldSplitTxn() bool {
	if l == unknownTxnAtomicity {
		l = defaultTxnAtomicity
	}
	return l == noneTxnAtomicity
}

func (l AtomicityLevel) validate(scheme string) error {
	switch l {
	case unknownTxnAtomicity:
	case noneTxnAtomicity:
		// Do nothing here to avoid modifying the persistence parameters.
	case tableTxnAtomicity:
		// MqSink only support `noneTxnAtomicity`.
		if sink.IsMQScheme(scheme) {
			errMsg := fmt.Sprintf("%s level atomicity is not supported by %s scheme", l, scheme)
			return cerror.ErrSinkURIInvalid.GenWithStackByArgs(errMsg)
		}
	default:
		errMsg := fmt.Sprintf("%s level atomicity is not supported by %s scheme", l, scheme)
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs(errMsg)
	}
	return nil
}

// SinkConfig represents sink config for a changefeed
type SinkConfig struct {
	TxnAtomicity *AtomicityLevel `toml:"transaction-atomicity" json:"transaction-atomicity,omitempty"`
	// Protocol is NOT available when the downstream is DB.
	Protocol *string `toml:"protocol" json:"protocol,omitempty"`

	// DispatchRules is only available when the downstream is MQ.
	DispatchRules []*DispatchRule `toml:"dispatchers" json:"dispatchers,omitempty"`
	// CSVConfig is only available when the downstream is Storage.
	CSVConfig       *CSVConfig        `toml:"csv" json:"csv,omitempty"`
	ColumnSelectors []*ColumnSelector `toml:"column-selectors" json:"column-selectors,omitempty"`
	// SchemaRegistry is only available when the downstream is MQ using avro protocol.
	SchemaRegistry *string `toml:"schema-registry" json:"schema-registry,omitempty"`
	// EncoderConcurrency is only available when the downstream is MQ.
	EncoderConcurrency *int `toml:"encoder-concurrency" json:"encoder-concurrency,omitempty"`
	// Terminator is NOT available when the downstream is DB.
	Terminator *string `toml:"terminator" json:"terminator,omitempty"`
	// DateSeparator is only available when the downstream is Storage.
	DateSeparator *string `toml:"date-separator" json:"date-separator,omitempty"`
	// EnablePartitionSeparator is only available when the downstream is Storage.
	EnablePartitionSeparator *bool `toml:"enable-partition-separator" json:"enable-partition-separator,omitempty"`
	// FileIndexWidth is only available when the downstream is Storage
	FileIndexWidth *int `toml:"file-index-digit,omitempty" json:"file-index-digit,omitempty"`

	// EnableKafkaSinkV2 enabled then the kafka-go sink will be used.
	// It is only available when the downstream is MQ.
	EnableKafkaSinkV2 *bool `toml:"enable-kafka-sink-v2" json:"enable-kafka-sink-v2,omitempty"`

	// OnlyOutputUpdatedColumns is only available when the downstream is MQ.
	OnlyOutputUpdatedColumns *bool `toml:"only-output-updated-columns" json:"only-output-updated-columns,omitempty"`

	// DeleteOnlyOutputHandleKeyColumns is only available when the downstream is MQ.
	DeleteOnlyOutputHandleKeyColumns *bool `toml:"delete-only-output-handle-key-columns" json:"delete-only-output-handle-key-columns,omitempty"`

	// TiDBSourceID is the source ID of the upstream TiDB,
	// which is used to set the `tidb_cdc_write_source` session variable.
	// Note: This field is only used internally and only used in the MySQL sink.
	TiDBSourceID uint64 `toml:"-" json:"-"`

	// SafeMode is only available when the downstream is DB.
	SafeMode           *bool               `toml:"safe-mode" json:"safe-mode,omitempty"`
	KafkaConfig        *KafkaConfig        `toml:"kafka-config" json:"kafka-config,omitempty"`
	PulsarConfig       *PulsarConfig       `toml:"pulsar-config" json:"pulsar-config,omitempty"`
	MySQLConfig        *MySQLConfig        `toml:"mysql-config" json:"mysql-config,omitempty"`
	CloudStorageConfig *CloudStorageConfig `toml:"cloud-storage-config" json:"cloud-storage-config,omitempty"`

	// AdvanceTimeoutInSec is a duration in second. If a table sink progress hasn't been
	// advanced for this given duration, the sink will be canceled and re-established.
	// Deprecated since v8.1.1
	AdvanceTimeoutInSec *uint `toml:"advance-timeout-in-sec" json:"advance-timeout-in-sec,omitempty"`

	// Simple Protocol only config, use to control the behavior of sending bootstrap message.
	// Note: When one of the following conditions is set to negative value,
	// bootstrap sending function will be disabled.
	// SendBootstrapIntervalInSec is the interval in seconds to send bootstrap message.
	SendBootstrapIntervalInSec *int64 `toml:"send-bootstrap-interval-in-sec" json:"send-bootstrap-interval-in-sec,omitempty"`
	// SendBootstrapInMsgCount means bootstrap messages are being sent every SendBootstrapInMsgCount row change messages.
	SendBootstrapInMsgCount *int32 `toml:"send-bootstrap-in-msg-count" json:"send-bootstrap-in-msg-count,omitempty"`
	// SendBootstrapToAllPartition determines whether to send bootstrap message to all partitions.
	// If set to false, bootstrap message will only be sent to the first partition of each topic.
	// Default value is true.
	SendBootstrapToAllPartition *bool `toml:"send-bootstrap-to-all-partition" json:"send-bootstrap-to-all-partition,omitempty"`

	// OpenProtocol related configurations
	OpenProtocol *OpenProtocolConfig `toml:"open" json:"open,omitempty"`
}

// MaskSensitiveData masks sensitive data in SinkConfig
func (s *SinkConfig) MaskSensitiveData() {
	if s.SchemaRegistry != nil {
		s.SchemaRegistry = aws.String(util.MaskSensitiveDataInURI(*s.SchemaRegistry))
	}
	if s.KafkaConfig != nil {
		s.KafkaConfig.MaskSensitiveData()
	}
	if s.PulsarConfig != nil {
		s.PulsarConfig.MaskSensitiveData()
	}
}

// ShouldSendBootstrapMsg returns whether the sink should send bootstrap message.
// Only enable bootstrap sending function for simple protocol
// and when both send-bootstrap-interval-in-sec and send-bootstrap-in-msg-count are > 0
func (s *SinkConfig) ShouldSendBootstrapMsg() bool {
	if s == nil {
		return false
	}
	protocol := util.GetOrZero(s.Protocol)

	return protocol == ProtocolSimple.String() &&
		util.GetOrZero(s.SendBootstrapIntervalInSec) > 0 &&
		util.GetOrZero(s.SendBootstrapInMsgCount) > 0
}

// CSVConfig defines a series of configuration items for csv codec.
type CSVConfig struct {
	// delimiter between fields
	Delimiter string `toml:"delimiter" json:"delimiter"`
	// quoting character
	Quote string `toml:"quote" json:"quote"`
	// representation of null values
	NullString string `toml:"null" json:"null"`
	// whether to include commit ts
	IncludeCommitTs bool `toml:"include-commit-ts" json:"include-commit-ts"`
	// encoding method of binary type
	BinaryEncodingMethod string `toml:"binary-encoding-method" json:"binary-encoding-method"`
}

func (c *CSVConfig) validateAndAdjust() error {
	if c == nil {
		return nil
	}

	// validate quote
	if len(c.Quote) > 1 {
		return cerror.WrapError(cerror.ErrSinkInvalidConfig,
			errors.New("csv config quote contains more than one character"))
	}
	if len(c.Quote) == 1 {
		quote := c.Quote[0]
		if quote == CR || quote == LF {
			return cerror.WrapError(cerror.ErrSinkInvalidConfig,
				errors.New("csv config quote cannot be line break character"))
		}
	}

	// validate delimiter
	switch len(c.Delimiter) {
	case 0:
		return cerror.WrapError(cerror.ErrSinkInvalidConfig,
			errors.New("csv config delimiter cannot be empty"))
	case 1:
		if strings.ContainsRune(c.Delimiter, CR) || strings.ContainsRune(c.Delimiter, LF) {
			return cerror.WrapError(cerror.ErrSinkInvalidConfig,
				errors.New("csv config delimiter contains line break characters"))
		}
	default:
		return cerror.WrapError(cerror.ErrSinkInvalidConfig,
			errors.New("csv config delimiter contains more than one character"))
	}

	if len(c.Quote) > 0 && strings.Contains(c.Delimiter, c.Quote) {
		return cerror.WrapError(cerror.ErrSinkInvalidConfig,
			errors.New("csv config quote and delimiter cannot be the same"))
	}

	// validate binary encoding method
	switch c.BinaryEncodingMethod {
	case BinaryEncodingHex, BinaryEncodingBase64:
	default:
		return cerror.WrapError(cerror.ErrSinkInvalidConfig,
			errors.New("csv config binary-encoding-method can only be hex or base64"))
	}

	return nil
}

// DateSeparator specifies the date separator in storage destination path
type DateSeparator int

// Enum types of DateSeparator
const (
	DateSeparatorNone DateSeparator = iota
	DateSeparatorYear
	DateSeparatorMonth
	DateSeparatorDay
)

// FromString converts the separator from string to DateSeperator enum type.
func (d *DateSeparator) FromString(separator string) error {
	switch strings.ToLower(separator) {
	case "none":
		*d = DateSeparatorNone
	case "year":
		*d = DateSeparatorYear
	case "month":
		*d = DateSeparatorMonth
	case "day":
		*d = DateSeparatorDay
	default:
		return cerror.ErrStorageSinkInvalidDateSeparator.GenWithStackByArgs(separator)
	}

	return nil
}

// GetPattern returns the pattern of the date separator.
func (d DateSeparator) GetPattern() string {
	switch d {
	case DateSeparatorNone:
		return ""
	case DateSeparatorYear:
		return `\d{4}`
	case DateSeparatorMonth:
		return `\d{4}-\d{2}`
	case DateSeparatorDay:
		return `\d{4}-\d{2}-\d{2}`
	default:
		return ""
	}
}

func (d DateSeparator) String() string {
	switch d {
	case DateSeparatorNone:
		return "none"
	case DateSeparatorYear:
		return "year"
	case DateSeparatorMonth:
		return "month"
	case DateSeparatorDay:
		return "day"
	default:
		return "unknown"
	}
}

// DispatchRule represents partition rule for a table.
type DispatchRule struct {
	Matcher []string `toml:"matcher" json:"matcher"`
	// Deprecated, please use PartitionRule.
	DispatcherRule string `toml:"dispatcher" json:"dispatcher"`
	// PartitionRule is an alias added for DispatcherRule to mitigate confusions.
	// In the future release, the DispatcherRule is expected to be removed .
	PartitionRule string `toml:"partition" json:"partition"`

	// IndexName is set when using index-value dispatcher with specified index.
	IndexName string `toml:"index" json:"index"`

	// Columns are set when using columns dispatcher.
	Columns []string `toml:"columns" json:"columns"`

	TopicRule string `toml:"topic" json:"topic"`
}

// ColumnSelector represents a column selector for a table.
type ColumnSelector struct {
	Matcher []string `toml:"matcher" json:"matcher"`
	Columns []string `toml:"columns" json:"columns"`
}

// CodecConfig represents a MQ codec configuration
type CodecConfig struct {
	EnableTiDBExtension            *bool   `toml:"enable-tidb-extension" json:"enable-tidb-extension,omitempty"`
	MaxBatchSize                   *int    `toml:"max-batch-size" json:"max-batch-size,omitempty"`
	AvroEnableWatermark            *bool   `toml:"avro-enable-watermark" json:"avro-enable-watermark"`
	AvroDecimalHandlingMode        *string `toml:"avro-decimal-handling-mode" json:"avro-decimal-handling-mode,omitempty"`
	AvroBigintUnsignedHandlingMode *string `toml:"avro-bigint-unsigned-handling-mode" json:"avro-bigint-unsigned-handling-mode,omitempty"`
	EncodingFormat                 *string `toml:"encoding-format" json:"encoding-format,omitempty"`
}

// KafkaConfig represents a kafka sink configuration
type KafkaConfig struct {
	PartitionNum                 *int32                    `toml:"partition-num" json:"partition-num,omitempty"`
	ReplicationFactor            *int16                    `toml:"replication-factor" json:"replication-factor,omitempty"`
	KafkaVersion                 *string                   `toml:"kafka-version" json:"kafka-version,omitempty"`
	MaxMessageBytes              *int                      `toml:"max-message-bytes" json:"max-message-bytes,omitempty"`
	Compression                  *string                   `toml:"compression" json:"compression,omitempty"`
	KafkaClientID                *string                   `toml:"kafka-client-id" json:"kafka-client-id,omitempty"`
	AutoCreateTopic              *bool                     `toml:"auto-create-topic" json:"auto-create-topic,omitempty"`
	DialTimeout                  *string                   `toml:"dial-timeout" json:"dial-timeout,omitempty"`
	WriteTimeout                 *string                   `toml:"write-timeout" json:"write-timeout,omitempty"`
	ReadTimeout                  *string                   `toml:"read-timeout" json:"read-timeout,omitempty"`
	RequiredAcks                 *int                      `toml:"required-acks" json:"required-acks,omitempty"`
	SASLUser                     *string                   `toml:"sasl-user" json:"sasl-user,omitempty"`
	SASLPassword                 *string                   `toml:"sasl-password" json:"sasl-password,omitempty"`
	SASLMechanism                *string                   `toml:"sasl-mechanism" json:"sasl-mechanism,omitempty"`
	SASLGssAPIAuthType           *string                   `toml:"sasl-gssapi-auth-type" json:"sasl-gssapi-auth-type,omitempty"`
	SASLGssAPIKeytabPath         *string                   `toml:"sasl-gssapi-keytab-path" json:"sasl-gssapi-keytab-path,omitempty"`
	SASLGssAPIKerberosConfigPath *string                   `toml:"sasl-gssapi-kerberos-config-path" json:"sasl-gssapi-kerberos-config-path,omitempty"`
	SASLGssAPIServiceName        *string                   `toml:"sasl-gssapi-service-name" json:"sasl-gssapi-service-name,omitempty"`
	SASLGssAPIUser               *string                   `toml:"sasl-gssapi-user" json:"sasl-gssapi-user,omitempty"`
	SASLGssAPIPassword           *string                   `toml:"sasl-gssapi-password" json:"sasl-gssapi-password,omitempty"`
	SASLGssAPIRealm              *string                   `toml:"sasl-gssapi-realm" json:"sasl-gssapi-realm,omitempty"`
	SASLGssAPIDisablePafxfast    *bool                     `toml:"sasl-gssapi-disable-pafxfast" json:"sasl-gssapi-disable-pafxfast,omitempty"`
	SASLOAuthClientID            *string                   `toml:"sasl-oauth-client-id" json:"sasl-oauth-client-id,omitempty"`
	SASLOAuthClientSecret        *string                   `toml:"sasl-oauth-client-secret" json:"sasl-oauth-client-secret,omitempty"`
	SASLOAuthTokenURL            *string                   `toml:"sasl-oauth-token-url" json:"sasl-oauth-token-url,omitempty"`
	SASLOAuthScopes              []string                  `toml:"sasl-oauth-scopes" json:"sasl-oauth-scopes,omitempty"`
	SASLOAuthGrantType           *string                   `toml:"sasl-oauth-grant-type" json:"sasl-oauth-grant-type,omitempty"`
	SASLOAuthAudience            *string                   `toml:"sasl-oauth-audience" json:"sasl-oauth-audience,omitempty"`
	EnableTLS                    *bool                     `toml:"enable-tls" json:"enable-tls,omitempty"`
	CA                           *string                   `toml:"ca" json:"ca,omitempty"`
	Cert                         *string                   `toml:"cert" json:"cert,omitempty"`
	Key                          *string                   `toml:"key" json:"key,omitempty"`
	InsecureSkipVerify           *bool                     `toml:"insecure-skip-verify" json:"insecure-skip-verify,omitempty"`
	CodecConfig                  *CodecConfig              `toml:"codec-config" json:"codec-config,omitempty"`
	LargeMessageHandle           *LargeMessageHandleConfig `toml:"large-message-handle" json:"large-message-handle,omitempty"`
	GlueSchemaRegistryConfig     *GlueSchemaRegistryConfig `toml:"glue-schema-registry-config" json:"glue-schema-registry-config"`

	// OutputRawChangeEvent controls whether to split the update pk/uk events.
	OutputRawChangeEvent *bool `toml:"output-raw-change-event" json:"output-raw-change-event,omitempty"`
}

// GetOutputRawChangeEvent returns the value of OutputRawChangeEvent
func (k *KafkaConfig) GetOutputRawChangeEvent() bool {
	if k == nil || k.OutputRawChangeEvent == nil {
		return false
	}
	return *k.OutputRawChangeEvent
}

// MaskSensitiveData masks sensitive data in KafkaConfig
func (k *KafkaConfig) MaskSensitiveData() {
	k.SASLPassword = aws.String("******")
	k.SASLGssAPIPassword = aws.String("******")
	k.SASLOAuthClientSecret = aws.String("******")
	k.Key = aws.String("******")
	if k.GlueSchemaRegistryConfig != nil {
		k.GlueSchemaRegistryConfig.AccessKey = "******"
		k.GlueSchemaRegistryConfig.Token = "******"
		k.GlueSchemaRegistryConfig.SecretAccessKey = "******"
	}
	if k.SASLOAuthTokenURL != nil {
		k.SASLOAuthTokenURL = aws.String(util.MaskSensitiveDataInURI(*k.SASLOAuthTokenURL))
	}
}

// PulsarCompressionType is the compression type for pulsar
type PulsarCompressionType string

// Value returns the pulsar compression type
func (p *PulsarCompressionType) Value() pulsar.CompressionType {
	if p == nil {
		return 0
	}
	switch strings.ToLower(string(*p)) {
	case "lz4":
		return pulsar.LZ4
	case "zlib":
		return pulsar.ZLib
	case "zstd":
		return pulsar.ZSTD
	default:
		return 0 // default is no compression
	}
}

// TimeMill is the time in milliseconds
type TimeMill int

// Duration returns the time in seconds as a duration
func (t *TimeMill) Duration() time.Duration {
	if t == nil {
		return 0
	}
	return time.Duration(*t) * time.Millisecond
}

// NewTimeMill returns a new time in milliseconds
func NewTimeMill(x int) *TimeMill {
	t := TimeMill(x)
	return &t
}

// TimeSec is the time in seconds
type TimeSec int

// Duration returns the time in seconds as a duration
func (t *TimeSec) Duration() time.Duration {
	if t == nil {
		return 0
	}
	return time.Duration(*t) * time.Second
}

// NewTimeSec returns a new time in seconds
func NewTimeSec(x int) *TimeSec {
	t := TimeSec(x)
	return &t
}

// OAuth2 is the configuration for OAuth2
type OAuth2 struct {
	// OAuth2IssuerURL  the URL of the authorization server.
	OAuth2IssuerURL string `toml:"oauth2-issuer-url" json:"oauth2-issuer-url,omitempty"`
	// OAuth2Audience  the URL of the resource server.
	OAuth2Audience string `toml:"oauth2-audience" json:"oauth2-audience,omitempty"`
	// OAuth2PrivateKey the private key used to sign the server.
	OAuth2PrivateKey string `toml:"oauth2-private-key" json:"oauth2-private-key,omitempty"`
	// OAuth2ClientID  the client ID of the application.
	OAuth2ClientID string `toml:"oauth2-client-id" json:"oauth2-client-id,omitempty"`
	// OAuth2Scope scope
	OAuth2Scope string `toml:"oauth2-scope" json:"oauth2-scope,omitempty"`
}

func (o *OAuth2) validate() (err error) {
	if o == nil {
		return nil
	}
	if len(o.OAuth2IssuerURL) == 0 || len(o.OAuth2ClientID) == 0 || len(o.OAuth2PrivateKey) == 0 ||
		len(o.OAuth2Audience) == 0 {
		return fmt.Errorf("issuer-url and audience and private-key and client-id not be empty")
	}
	return nil
}

// PulsarConfig pulsar sink configuration
type PulsarConfig struct {
	TLSKeyFilePath        *string `toml:"tls-key-file-path" json:"tls-key-file-path,omitempty"`
	TLSCertificateFile    *string `toml:"tls-certificate-file" json:"tls-certificate-file,omitempty"`
	TLSTrustCertsFilePath *string `toml:"tls-trust-certs-file-path" json:"tls-trust-certs-file-path,omitempty"`

	// PulsarProducerCacheSize is the size of the cache of pulsar producers
	PulsarProducerCacheSize *int32 `toml:"pulsar-producer-cache-size" json:"pulsar-producer-cache-size,omitempty"`

	// PulsarVersion print the version of pulsar
	PulsarVersion *string `toml:"pulsar-version" json:"pulsar-version,omitempty"`

	// pulsar client compression
	CompressionType *PulsarCompressionType `toml:"compression-type" json:"compression-type,omitempty"`

	// AuthenticationToken the token for the Pulsar server
	AuthenticationToken *string `toml:"authentication-token" json:"authentication-token,omitempty"`

	// ConnectionTimeout Timeout for the establishment of a TCP connection (default: 5 seconds)
	ConnectionTimeout *TimeSec `toml:"connection-timeout" json:"connection-timeout,omitempty"`

	// Set the operation timeout (default: 30 seconds)
	// Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
	// operation will be marked as failed
	OperationTimeout *TimeSec `toml:"operation-timeout" json:"operation-timeout,omitempty"`

	// BatchingMaxMessages specifies the maximum number of messages permitted in a batch. (default: 1000)
	BatchingMaxMessages *uint `toml:"batching-max-messages" json:"batching-max-messages,omitempty"`

	// BatchingMaxPublishDelay specifies the time period within which the messages sent will be batched (default: 10ms)
	// if batch messages are enabled. If set to a non zero value, messages will be queued until this time
	// interval or until
	BatchingMaxPublishDelay *TimeMill `toml:"batching-max-publish-delay" json:"batching-max-publish-delay,omitempty"`

	// SendTimeout specifies the timeout for a message that has not been acknowledged by the server since sent.
	// Send and SendAsync returns an error after timeout.
	// default: 30s
	SendTimeout *TimeSec `toml:"send-timeout" json:"send-timeout,omitempty"`

	// TokenFromFile Authentication from the file token,
	// the path name of the file (the third priority authentication method)
	TokenFromFile *string `toml:"token-from-file" json:"token-from-file,omitempty"`

	// BasicUserName Account name for pulsar basic authentication (the second priority authentication method)
	BasicUserName *string `toml:"basic-user-name" json:"basic-user-name,omitempty"`
	// BasicPassword with account
	BasicPassword *string `toml:"basic-password" json:"basic-password,omitempty"`

	// AuthTLSCertificatePath  create new pulsar authentication provider with specified TLS certificate and private key
	AuthTLSCertificatePath *string `toml:"auth-tls-certificate-path" json:"auth-tls-certificate-path,omitempty"`
	// AuthTLSPrivateKeyPath private key
	AuthTLSPrivateKeyPath *string `toml:"auth-tls-private-key-path" json:"auth-tls-private-key-path,omitempty"`

	// Oauth2 include  oauth2-issuer-url oauth2-audience oauth2-private-key oauth2-client-id
	// and 'type' always use 'client_credentials'
	OAuth2 *OAuth2 `toml:"oauth2" json:"oauth2,omitempty"`

	// OutputRawChangeEvent controls whether to split the update pk/uk events.
	OutputRawChangeEvent *bool `toml:"output-raw-change-event" json:"output-raw-change-event,omitempty"`

	// BrokerURL is used to configure service brokerUrl for the Pulsar service.
	// This parameter is a part of the `sink-uri`. Internal use only.
	BrokerURL string `toml:"-" json:"-"`
	// SinkURI is the parsed sinkURI. Internal use only.
	SinkURI *url.URL `toml:"-" json:"-"`
}

// GetOutputRawChangeEvent returns the value of OutputRawChangeEvent
func (c *PulsarConfig) GetOutputRawChangeEvent() bool {
	if c == nil || c.OutputRawChangeEvent == nil {
		return false
	}
	return *c.OutputRawChangeEvent
}

// MaskSensitiveData masks sensitive data in PulsarConfig
func (c *PulsarConfig) MaskSensitiveData() {
	if c.AuthenticationToken != nil {
		c.AuthenticationToken = aws.String("******")
	}
	if c.BasicPassword != nil {
		c.BasicPassword = aws.String("******")
	}
	if c.OAuth2 != nil {
		c.OAuth2.OAuth2PrivateKey = "******"
	}
}

// Check get broker url
func (c *PulsarConfig) validate() (err error) {
	if c.OAuth2 != nil {
		if err = c.OAuth2.validate(); err != nil {
			return err
		}
	}
	return nil
}

// GetDefaultTopicName get default topic name
func (c *PulsarConfig) GetDefaultTopicName() string {
	topicName := c.SinkURI.Path
	return topicName[1:]
}

// MySQLConfig represents a MySQL sink configuration
type MySQLConfig struct {
	WorkerCount                  *int    `toml:"worker-count" json:"worker-count,omitempty"`
	MaxTxnRow                    *int    `toml:"max-txn-row" json:"max-txn-row,omitempty"`
	MaxMultiUpdateRowSize        *int    `toml:"max-multi-update-row-size" json:"max-multi-update-row-size,omitempty"`
	MaxMultiUpdateRowCount       *int    `toml:"max-multi-update-row" json:"max-multi-update-row,omitempty"`
	TiDBTxnMode                  *string `toml:"tidb-txn-mode" json:"tidb-txn-mode,omitempty"`
	SSLCa                        *string `toml:"ssl-ca" json:"ssl-ca,omitempty"`
	SSLCert                      *string `toml:"ssl-cert" json:"ssl-cert,omitempty"`
	SSLKey                       *string `toml:"ssl-key" json:"ssl-key,omitempty"`
	TimeZone                     *string `toml:"time-zone" json:"time-zone,omitempty"`
	WriteTimeout                 *string `toml:"write-timeout" json:"write-timeout,omitempty"`
	ReadTimeout                  *string `toml:"read-timeout" json:"read-timeout,omitempty"`
	Timeout                      *string `toml:"timeout" json:"timeout,omitempty"`
	EnableBatchDML               *bool   `toml:"enable-batch-dml" json:"enable-batch-dml,omitempty"`
	EnableMultiStatement         *bool   `toml:"enable-multi-statement" json:"enable-multi-statement,omitempty"`
	EnableCachePreparedStatement *bool   `toml:"enable-cache-prepared-statement" json:"enable-cache-prepared-statement,omitempty"`
}

// CloudStorageConfig represents a cloud storage sink configuration
type CloudStorageConfig struct {
	WorkerCount   *int    `toml:"worker-count" json:"worker-count,omitempty"`
	FlushInterval *string `toml:"flush-interval" json:"flush-interval,omitempty"`
	FileSize      *int    `toml:"file-size" json:"file-size,omitempty"`

	OutputColumnID      *bool   `toml:"output-column-id" json:"output-column-id,omitempty"`
	FileExpirationDays  *int    `toml:"file-expiration-days" json:"file-expiration-days,omitempty"`
	FileCleanupCronSpec *string `toml:"file-cleanup-cron-spec" json:"file-cleanup-cron-spec,omitempty"`
	FlushConcurrency    *int    `toml:"flush-concurrency" json:"flush-concurrency,omitempty"`

	// OutputRawChangeEvent controls whether to split the update pk/uk events.
	OutputRawChangeEvent *bool `toml:"output-raw-change-event" json:"output-raw-change-event,omitempty"`
}

// GetOutputRawChangeEvent returns the value of OutputRawChangeEvent
func (c *CloudStorageConfig) GetOutputRawChangeEvent() bool {
	if c == nil || c.OutputRawChangeEvent == nil {
		return false
	}
	return *c.OutputRawChangeEvent
}

func (s *SinkConfig) validateAndAdjust(sinkURI *url.URL) error {
	if err := s.validateAndAdjustSinkURI(sinkURI); err != nil {
		return err
	}

	if sink.IsMySQLCompatibleScheme(sinkURI.Scheme) {
		return nil
	}

	protocol, _ := ParseSinkProtocolFromString(util.GetOrZero(s.Protocol))

	if s.KafkaConfig != nil && s.KafkaConfig.LargeMessageHandle != nil {
		var (
			enableTiDBExtension bool
			err                 error
		)
		if s := sinkURI.Query().Get("enable-tidb-extension"); s != "" {
			enableTiDBExtension, err = strconv.ParseBool(s)
			if err != nil {
				return errors.Trace(err)
			}
		}
		err = s.KafkaConfig.LargeMessageHandle.AdjustAndValidate(protocol, enableTiDBExtension)
		if err != nil {
			return err
		}
	}

	if s.SchemaRegistry != nil &&
		(s.KafkaConfig != nil && s.KafkaConfig.GlueSchemaRegistryConfig != nil) {
		return cerror.ErrInvalidReplicaConfig.
			GenWithStackByArgs("schema-registry and glue-schema-registry-config" +
				"cannot be set at the same time," +
				"schema-registry is used by confluent schema registry, " +
				"glue-schema-registry-config is used by aws glue schema registry")
	}

	if s.KafkaConfig != nil && s.KafkaConfig.GlueSchemaRegistryConfig != nil {
		err := s.KafkaConfig.GlueSchemaRegistryConfig.Validate()
		if err != nil {
			return err
		}
	}

	if sink.IsPulsarScheme(sinkURI.Scheme) && s.PulsarConfig == nil {
		s.PulsarConfig = &PulsarConfig{
			SinkURI: sinkURI,
		}
	}
	if s.PulsarConfig != nil {
		if err := s.PulsarConfig.validate(); err != nil {
			return err
		}
	}

	for _, rule := range s.DispatchRules {
		if rule.DispatcherRule != "" && rule.PartitionRule != "" {
			log.Error("dispatcher and partition cannot be configured both", zap.Any("rule", rule))
			return cerror.WrapError(cerror.ErrSinkInvalidConfig,
				errors.New(fmt.Sprintf("dispatcher and partition cannot be "+
					"configured both for rule:%v", rule)))
		}
		// After `validate()` is called, we only use PartitionRule to represent a partition
		// dispatching rule. So when DispatcherRule is not empty, we assign its
		// value to PartitionRule and clear itself.
		if rule.DispatcherRule != "" {
			rule.PartitionRule = rule.DispatcherRule
			rule.DispatcherRule = ""
		}
	}

	if util.GetOrZero(s.EncoderConcurrency) < 0 {
		return cerror.ErrSinkInvalidConfig.GenWithStack(
			"encoder-concurrency should greater than 0, but got %d", s.EncoderConcurrency)
	}

	// validate terminator
	if s.Terminator == nil {
		s.Terminator = util.AddressOf(CRLF)
	}

	if util.GetOrZero(s.DeleteOnlyOutputHandleKeyColumns) && protocol == ProtocolCsv {
		return cerror.ErrSinkInvalidConfig.GenWithStack(
			"CSV protocol always output all columns for the delete event, " +
				"do not set `delete-only-output-handle-key-columns` to true")
	}

	// validate storage sink related config
	if sinkURI != nil && sink.IsStorageScheme(sinkURI.Scheme) {
		// validate date separator
		if len(util.GetOrZero(s.DateSeparator)) > 0 {
			var separator DateSeparator
			if err := separator.FromString(util.GetOrZero(s.DateSeparator)); err != nil {
				return cerror.WrapError(cerror.ErrSinkInvalidConfig, err)
			}
		}

		// File index width should be in [minFileIndexWidth, maxFileIndexWidth].
		// In most scenarios, the user does not need to change this configuration,
		// so the default value of this parameter is not set and just make silent
		// adjustments here.
		if util.GetOrZero(s.FileIndexWidth) < MinFileIndexWidth ||
			util.GetOrZero(s.FileIndexWidth) > MaxFileIndexWidth {
			s.FileIndexWidth = util.AddressOf(DefaultFileIndexWidth)
		}

		if err := s.CSVConfig.validateAndAdjust(); err != nil {
			return err
		}
	}

	if util.GetOrZero(s.AdvanceTimeoutInSec) == 0 {
		log.Warn(fmt.Sprintf("advance-timeout-in-sec is not set, use default value: %d seconds", DefaultAdvanceTimeoutInSec))
		s.AdvanceTimeoutInSec = util.AddressOf(DefaultAdvanceTimeoutInSec)
	}

	return nil
}

// validateAndAdjustSinkURI validate and adjust `Protocol` and `TxnAtomicity` by sinkURI.
func (s *SinkConfig) validateAndAdjustSinkURI(sinkURI *url.URL) error {
	if sinkURI == nil {
		return nil
	}

	if err := s.applyParameterBySinkURI(sinkURI); err != nil {
		if !cerror.ErrIncompatibleSinkConfig.Equal(err) {
			return err
		}
		// Ignore `ErrIncompatibleSinkConfig` here to:
		// 1. Keep compatibility with old version.
		// 2. Avoid throwing error when create changefeed.
		log.Warn("sink-uri is not compatible with the sink config, "+
			"the configuration in sink URI will be used", zap.Error(err))
	}

	// validate that TxnAtomicity is valid and compatible with the scheme.
	if err := util.GetOrZero(s.TxnAtomicity).validate(sinkURI.Scheme); err != nil {
		return err
	}

	log.Info("succeed to parse parameter from sink uri",
		zap.String("protocol", util.GetOrZero(s.Protocol)),
		zap.String("txnAtomicity", string(util.GetOrZero(s.TxnAtomicity))))

	// Check that protocol config is compatible with the scheme.
	if sink.IsMySQLCompatibleScheme(sinkURI.Scheme) && s.Protocol != nil {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("protocol %s "+
			"is incompatible with %s scheme", util.GetOrZero(s.Protocol), sinkURI.Scheme))
	}
	// For testing purposes, any protocol should be legal for blackhole.
	if sink.IsMQScheme(sinkURI.Scheme) || sink.IsStorageScheme(sinkURI.Scheme) {
		return s.ValidateProtocol(sinkURI.Scheme)
	}
	return nil
}

// ValidateProtocol validates the protocol configuration.
func (s *SinkConfig) ValidateProtocol(scheme string) error {
	protocol, err := ParseSinkProtocolFromString(util.GetOrZero(s.Protocol))
	if err != nil {
		return err
	}
	outputOldValue := false
	switch protocol {
	case ProtocolOpen:
		if s.OpenProtocol != nil {
			outputOldValue = s.OpenProtocol.OutputOldValue
		}
	case ProtocolAvro:
		outputOldValue = false
	default:
		return nil
	}

	outputRawChangeEvent := false
	switch scheme {
	case sink.KafkaScheme, sink.KafkaSSLScheme:
		outputRawChangeEvent = s.KafkaConfig.GetOutputRawChangeEvent()
	case sink.PulsarScheme, sink.PulsarSSLScheme:
		outputRawChangeEvent = s.PulsarConfig.GetOutputRawChangeEvent()
	default:
		outputRawChangeEvent = s.CloudStorageConfig.GetOutputRawChangeEvent()
	}

	if outputRawChangeEvent && !outputOldValue {
		// TODO: return error if we do not need to keep backward compatibility.
		log.Warn(fmt.Sprintf("TiCDC will not split the update pk/uk events if output-raw-change-event is true(scheme: %s).", scheme) +
			fmt.Sprintf("It is not recommended to set output-old-value to false(protocol: %s) in this case.", protocol) +
			"Otherwise, there may be data consistency issues in update pk/uk scenarios due to lack of old value information.")
	}
	return nil
}

// applyParameterBySinkURI parse sinkURI and set `Protocol` and `TxnAtomicity` to `SinkConfig`.
// Return:
// - ErrIncompatibleSinkConfig to terminate `updated` changefeed operation.
func (s *SinkConfig) applyParameterBySinkURI(sinkURI *url.URL) error {
	if sinkURI == nil {
		return nil
	}

	cfgInSinkURI := map[string]string{}
	cfgInFile := map[string]string{}
	params := sinkURI.Query()

	txnAtomicityFromURI := AtomicityLevel(params.Get(TxnAtomicityKey))
	if txnAtomicityFromURI != unknownTxnAtomicity {
		if util.GetOrZero(s.TxnAtomicity) != unknownTxnAtomicity && util.GetOrZero(s.TxnAtomicity) != txnAtomicityFromURI {
			cfgInSinkURI[TxnAtomicityKey] = string(txnAtomicityFromURI)
			cfgInFile[TxnAtomicityKey] = string(util.GetOrZero(s.TxnAtomicity))
		}
		s.TxnAtomicity = util.AddressOf(txnAtomicityFromURI)
	}

	protocolFromURI := params.Get(ProtocolKey)
	if protocolFromURI != "" {
		if s.Protocol != nil && util.GetOrZero(s.Protocol) != protocolFromURI {
			cfgInSinkURI[ProtocolKey] = protocolFromURI
			cfgInFile[ProtocolKey] = util.GetOrZero(s.Protocol)
		}
		s.Protocol = util.AddressOf(protocolFromURI)
	}

	getError := func() error {
		if len(cfgInSinkURI) != len(cfgInFile) {
			log.Panic("inconsistent configuration items in sink uri and configuration file",
				zap.Any("cfgInSinkURI", cfgInSinkURI), zap.Any("cfgInFile", cfgInFile))
		}
		if len(cfgInSinkURI) == 0 && len(cfgInFile) == 0 {
			return nil
		}
		getErrMsg := func(cfgIn map[string]string) string {
			var errMsg strings.Builder
			for k, v := range cfgIn {
				errMsg.WriteString(fmt.Sprintf("%s=%s, ", k, v))
			}
			return errMsg.String()[0 : errMsg.Len()-2]
		}
		return cerror.ErrIncompatibleSinkConfig.GenWithStackByArgs(
			getErrMsg(cfgInSinkURI), getErrMsg(cfgInFile))
	}
	return getError()
}

// CheckCompatibilityWithSinkURI check whether the sinkURI is compatible with the sink config.
func (s *SinkConfig) CheckCompatibilityWithSinkURI(
	oldSinkConfig *SinkConfig, sinkURIStr string,
) error {
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	cfgParamsChanged := s.Protocol != oldSinkConfig.Protocol ||
		s.TxnAtomicity != oldSinkConfig.TxnAtomicity

	isURIParamsChanged := func(oldCfg SinkConfig) bool {
		err := oldCfg.applyParameterBySinkURI(sinkURI)
		return cerror.ErrIncompatibleSinkConfig.Equal(err)
	}
	uriParamsChanged := isURIParamsChanged(*oldSinkConfig)

	if !uriParamsChanged && !cfgParamsChanged {
		return nil
	}

	compatibilityError := s.applyParameterBySinkURI(sinkURI)
	if uriParamsChanged && cerror.ErrIncompatibleSinkConfig.Equal(compatibilityError) {
		// Ignore compatibility error if the sinkURI make such changes.
		return nil
	}
	return compatibilityError
}

// GlueSchemaRegistryConfig represents a Glue Schema Registry configuration
type GlueSchemaRegistryConfig struct {
	// Name of the schema registry
	RegistryName string `toml:"registry-name" json:"registry-name"`
	// Region of the schema registry
	Region string `toml:"region" json:"region"`
	// AccessKey of the schema registry
	AccessKey string `toml:"access-key" json:"access-key,omitempty"`
	// SecretAccessKey of the schema registry
	SecretAccessKey string `toml:"secret-access-key" json:"secret-access-key,omitempty"`
	Token           string `toml:"token" json:"token,omitempty"`
}

// Validate the GlueSchemaRegistryConfig.
func (g *GlueSchemaRegistryConfig) Validate() error {
	if g.RegistryName == "" {
		return cerror.ErrInvalidGlueSchemaRegistryConfig.
			GenWithStack("registry-name is empty, is must be set")
	}
	if g.Region == "" {
		return cerror.ErrInvalidGlueSchemaRegistryConfig.
			GenWithStack("region is empty, is must be set")
	}
	if g.AccessKey != "" && g.SecretAccessKey == "" {
		return cerror.ErrInvalidGlueSchemaRegistryConfig.
			GenWithStack("access-key is set, but access-key-secret is empty, they must be set together")
	}
	return nil
}

// NoCredentials returns true if no credentials are set.
func (g *GlueSchemaRegistryConfig) NoCredentials() bool {
	return g.AccessKey == "" && g.SecretAccessKey == "" && g.Token == ""
}

// OpenProtocolConfig represents the configurations for open protocol encoding
type OpenProtocolConfig struct {
	OutputOldValue bool `toml:"output-old-value" json:"output-old-value"`
}
