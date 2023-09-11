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
	"strings"

	"github.com/aws/aws-sdk-go/aws"
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

	// DefaultAdvanceTimeoutInSec is the default sink advance timeout, in second.
	DefaultAdvanceTimeoutInSec uint = 150
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

// ForceEnableOldValueProtocols specifies which protocols need to be forced to enable old value.
var ForceEnableOldValueProtocols = map[string]struct{}{
	ProtocolCanal.String():     {},
	ProtocolCanalJSON.String(): {},
	ProtocolMaxwell.String():   {},
}

// ForceDisableOldValueProtocols specifies protocols need to be forced to disable old value.
var ForceDisableOldValueProtocols = map[string]struct{}{
	ProtocolAvro.String(): {},
	ProtocolCsv.String():  {},
}

// SinkConfig represents sink config for a changefeed
type SinkConfig struct {
	TxnAtomicity AtomicityLevel `toml:"transaction-atomicity" json:"transaction-atomicity"`
	Protocol     string         `toml:"protocol" json:"protocol"`

	DispatchRules            []*DispatchRule   `toml:"dispatchers" json:"dispatchers"`
	CSVConfig                *CSVConfig        `toml:"csv" json:"csv"`
	ColumnSelectors          []*ColumnSelector `toml:"column-selectors" json:"column-selectors"`
	SchemaRegistry           string            `toml:"schema-registry" json:"schema-registry"`
	EncoderConcurrency       int               `toml:"encoder-concurrency" json:"encoder-concurrency"`
	Terminator               string            `toml:"terminator" json:"terminator"`
	DateSeparator            string            `toml:"date-separator" json:"date-separator"`
	EnablePartitionSeparator bool              `toml:"enable-partition-separator" json:"enable-partition-separator"`
	FileIndexWidth           int               `toml:"file-index-digit,omitempty" json:"file-index-digit,omitempty"`

	KafkaConfig *KafkaConfig `toml:"kafka-config" json:"kafka-config,omitempty"`

	// TiDBSourceID is the source ID of the upstream TiDB,
	// which is used to set the `tidb_cdc_write_source` session variable.
	// Note: This field is only used internally and only used in the MySQL sink.
	TiDBSourceID uint64 `toml:"-" json:"-"`

	// AdvanceTimeoutInSec is a duration in second. If a table sink progress hasn't been
	// advanced for this given duration, the sink will be canceled and re-established.
	AdvanceTimeoutInSec uint `toml:"advance-timeout-in-sec" json:"advance-timeout-in-sec,omitempty"`
}

// KafkaConfig represents a kafka sink configuration
type KafkaConfig struct {
	SASLMechanism         *string  `toml:"sasl-mechanism" json:"sasl-mechanism,omitempty"`
	SASLOAuthClientID     *string  `toml:"sasl-oauth-client-id" json:"sasl-oauth-client-id,omitempty"`
	SASLOAuthClientSecret *string  `toml:"sasl-oauth-client-secret" json:"sasl-oauth-client-secret,omitempty"`
	SASLOAuthTokenURL     *string  `toml:"sasl-oauth-token-url" json:"sasl-oauth-token-url,omitempty"`
	SASLOAuthScopes       []string `toml:"sasl-oauth-scopes" json:"sasl-oauth-scopes,omitempty"`
	SASLOAuthGrantType    *string  `toml:"sasl-oauth-grant-type" json:"sasl-oauth-grant-type,omitempty"`
	SASLOAuthAudience     *string  `toml:"sasl-oauth-audience" json:"sasl-oauth-audience,omitempty"`

	LargeMessageHandle *LargeMessageHandleConfig `toml:"large-message-handle" json:"large-message-handle,omitempty"`
}

// MaskSensitiveData masks sensitive data in SinkConfig
func (s *SinkConfig) MaskSensitiveData() {
	if s.SchemaRegistry != "" {
		s.SchemaRegistry = util.MaskSensitiveDataInURI(s.SchemaRegistry)
	}
	if s.KafkaConfig != nil {
		s.KafkaConfig.MaskSensitiveData()
	}
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
	if len(c.Delimiter) == 0 {
		return cerror.WrapError(cerror.ErrSinkInvalidConfig,
			errors.New("csv config delimiter cannot be empty"))
	}
	if strings.ContainsRune(c.Delimiter, CR) ||
		strings.ContainsRune(c.Delimiter, LF) {
		return cerror.WrapError(cerror.ErrSinkInvalidConfig,
			errors.New("csv config delimiter contains line break characters"))
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
	TopicRule     string `toml:"topic" json:"topic"`
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
}

// MaskSensitiveData masks sensitive data in KafkaConfig
func (k *KafkaConfig) MaskSensitiveData() {
	k.SASLOAuthClientSecret = aws.String("******")
	if k.SASLOAuthTokenURL != nil {
		k.SASLOAuthTokenURL = aws.String(util.MaskSensitiveDataInURI(*k.SASLOAuthTokenURL))
	}
}

func (s *SinkConfig) validateAndAdjust(sinkURI *url.URL) error {
	if err := s.validateAndAdjustSinkURI(sinkURI); err != nil {
		return err
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

	if s.EncoderConcurrency < 0 {
		return cerror.ErrSinkInvalidConfig.GenWithStack(
			"encoder-concurrency should greater than 0, but got %d", s.EncoderConcurrency)
	}

	// validate terminator
	if len(s.Terminator) == 0 {
		s.Terminator = CRLF
	}

	// validate storage sink related config
	if sinkURI != nil && sink.IsStorageScheme(sinkURI.Scheme) {
		// validate date separator
		if len(s.DateSeparator) > 0 {
			var separator DateSeparator
			if err := separator.FromString(s.DateSeparator); err != nil {
				return cerror.WrapError(cerror.ErrSinkInvalidConfig, err)
			}
		}

		// File index width should be in [minFileIndexWidth, maxFileIndexWidth].
		// In most scenarios, the user does not need to change this configuration,
		// so the default value of this parameter is not set and just make silent
		// adjustments here.
		if s.FileIndexWidth < MinFileIndexWidth || s.FileIndexWidth > MaxFileIndexWidth {
			s.FileIndexWidth = DefaultFileIndexWidth
		}

		if err := s.CSVConfig.validateAndAdjust(); err != nil {
			return err
		}
	}

	if s.AdvanceTimeoutInSec == 0 {
		log.Warn(fmt.Sprintf("advance-timeout-in-sec is not set, use default value: %d seconds", DefaultAdvanceTimeoutInSec))
		s.AdvanceTimeoutInSec = DefaultAdvanceTimeoutInSec
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
	if err := s.TxnAtomicity.validate(sinkURI.Scheme); err != nil {
		return err
	}

	// Validate that protocol is compatible with the scheme. For testing purposes,
	// any protocol should be legal for blackhole.
	if sink.IsMQScheme(sinkURI.Scheme) || sink.IsStorageScheme(sinkURI.Scheme) {
		_, err := ParseSinkProtocolFromString(s.Protocol)
		if err != nil {
			return err
		}
	} else if sink.IsMySQLCompatibleScheme(sinkURI.Scheme) && s.Protocol != "" {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("protocol %s "+
			"is incompatible with %s scheme", s.Protocol, sinkURI.Scheme))
	}

	log.Info("succeed to parse parameter from sink uri",
		zap.String("protocol", s.Protocol),
		zap.String("txnAtomicity", string(s.TxnAtomicity)))
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
		if s.TxnAtomicity != unknownTxnAtomicity && s.TxnAtomicity != txnAtomicityFromURI {
			cfgInSinkURI[TxnAtomicityKey] = string(txnAtomicityFromURI)
			cfgInFile[TxnAtomicityKey] = string(s.TxnAtomicity)
		}
		s.TxnAtomicity = txnAtomicityFromURI
	}

	protocolFromURI := params.Get(ProtocolKey)
	if protocolFromURI != "" {
		if s.Protocol != "" && s.Protocol != protocolFromURI {
			cfgInSinkURI[ProtocolKey] = protocolFromURI
			cfgInFile[ProtocolKey] = s.Protocol
		}
		s.Protocol = protocolFromURI
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

const (
	// LargeMessageHandleOptionNone means not handling large message.
	LargeMessageHandleOptionNone string = "none"
	// LargeMessageHandleOptionHandleKeyOnly means handling large message by sending only handle key columns.
	LargeMessageHandleOptionHandleKeyOnly string = "handle-key-only"
)

// LargeMessageHandleConfig is the configuration for handling large message.
type LargeMessageHandleConfig struct {
	LargeMessageHandleOption string `toml:"large-message-handle-option" json:"large-message-handle-option"`
}

// NewDefaultLargeMessageHandleConfig return the default LargeMessageHandleConfig.
func NewDefaultLargeMessageHandleConfig() *LargeMessageHandleConfig {
	return &LargeMessageHandleConfig{
		LargeMessageHandleOption: LargeMessageHandleOptionNone,
	}
}

// Validate the LargeMessageHandleConfig.
func (c *LargeMessageHandleConfig) Validate(protocol Protocol, enableTiDBExtension bool) error {
	if c.LargeMessageHandleOption == LargeMessageHandleOptionNone {
		return nil
	}

	switch protocol {
	case ProtocolOpen:
		return nil
	case ProtocolCanalJSON:
		if !enableTiDBExtension {
			return cerror.ErrInvalidReplicaConfig.GenWithStack(
				"large message handle is set to %s, protocol is %s, but enable-tidb-extension is false",
				c.LargeMessageHandleOption, protocol.String())
		}
		return nil
	default:
	}
	return cerror.ErrInvalidReplicaConfig.GenWithStack(
		"large message handle is set to %s, protocol is %s, it's not supported",
		c.LargeMessageHandleOption, protocol.String())
}

// HandleKeyOnly returns true if handle large message by encoding handle key only.
func (c *LargeMessageHandleConfig) HandleKeyOnly() bool {
	if c == nil {
		return false
	}
	return c.LargeMessageHandleOption == LargeMessageHandleOptionHandleKeyOnly
}

// Disabled returns true if disable large message handle.
func (c *LargeMessageHandleConfig) Disabled() bool {
	if c == nil {
		return true
	}
	return c.LargeMessageHandleOption == LargeMessageHandleOptionNone
}
