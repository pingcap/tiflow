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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"go.uber.org/zap"
)

// DefaultMaxMessageBytes sets the default value for max-message-bytes.
const DefaultMaxMessageBytes = 10 * 1024 * 1024 // 10M

// AtomicityLevel represents the atomicity level of a changefeed.
type AtomicityLevel string

const (
	// unknownTxnAtomicity is the default atomicity level, which is invalid and will
	// be set to a valid value when initializing sink in processor.
	unknownTxnAtomicity AtomicityLevel = ""

	// noneTxnAtomicity means atomicity of transactions is not guaranteed
	noneTxnAtomicity AtomicityLevel = "none"

	// tableTxnAtomicity means atomicity of single table transactions is guaranteed.
	tableTxnAtomicity AtomicityLevel = "table"

	defaultMqTxnAtomicity    = noneTxnAtomicity
	defaultMysqlTxnAtomicity = tableTxnAtomicity
)

const (
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
)

// ShouldSplitTxn returns whether the sink should split txn.
func (l AtomicityLevel) ShouldSplitTxn() bool {
	return l == noneTxnAtomicity
}

// ForceEnableOldValueProtocols specifies which protocols need to be forced to enable old value.
var ForceEnableOldValueProtocols = []string{
	ProtocolCanal.String(),
	ProtocolCanalJSON.String(),
	ProtocolMaxwell.String(),
}

// SinkConfig represents sink config for a changefeed
type SinkConfig struct {
	DispatchRules   []*DispatchRule   `toml:"dispatchers" json:"dispatchers"`
	CSVConfig       *CSVConfig        `toml:"csv" json:"csv"`
	Protocol        string            `toml:"protocol" json:"protocol"`
	ColumnSelectors []*ColumnSelector `toml:"column-selectors" json:"column-selectors"`
	SchemaRegistry  string            `toml:"schema-registry" json:"schema-registry"`
	TxnAtomicity    AtomicityLevel    `toml:"transaction-atomicity" json:"transaction-atomicity"`
}

// CSVConfig defines a series of configuration items for csv codec.
type CSVConfig struct {
	Delimiter       string `toml:"delimiter" json:"delimiter"`
	Quote           string `toml:"quote" json:"quote"`
	Terminator      string `toml:"terminator" json:"terminator"`
	NullString      string `toml:"null" json:"null"`
	DateSeparator   string `toml:"date-separator" json:"date-separator"`
	IncludeCommitTs bool   `toml:"include-commit-ts" json:"include-commit-ts"`
}

// DateSeparator sepecifies the date separator in storage destination path
type DateSeparator int

// Enum types of DateSeperator
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

func (s *SinkConfig) validateAndAdjust(sinkURI *url.URL, enableOldValue bool) error {
	if err := s.applyParameter(sinkURI); err != nil {
		return err
	}

	if !enableOldValue {
		for _, protocolStr := range ForceEnableOldValueProtocols {
			if protocolStr == s.Protocol {
				log.Error(fmt.Sprintf("Old value is not enabled when using `%s` protocol. "+
					"Please update changefeed config", s.Protocol))
				return cerror.WrapError(cerror.ErrKafkaInvalidConfig,
					errors.New(fmt.Sprintf("%s protocol requires old value to be enabled", s.Protocol)))
			}
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

	if s.CSVConfig != nil {
		return s.validateAndAdjustCSVConfig()
	}

	return nil
}

func (s *SinkConfig) validateAndAdjustCSVConfig() error {
	// validate quote
	if len(s.CSVConfig.Quote) > 1 {
		return cerror.WrapError(cerror.ErrSinkInvalidConfig,
			errors.New("csv config quote contains more than one character"))
	}
	if len(s.CSVConfig.Quote) == 1 {
		quote := s.CSVConfig.Quote[0]
		if quote == CR || quote == LF {
			return cerror.WrapError(cerror.ErrSinkInvalidConfig,
				errors.New("csv config quote cannot be line break character"))
		}
	}

	// validate delimiter
	if len(s.CSVConfig.Delimiter) == 0 {
		return cerror.WrapError(cerror.ErrSinkInvalidConfig,
			errors.New("csv config delimiter cannot be empty"))
	}
	if strings.ContainsRune(s.CSVConfig.Delimiter, CR) ||
		strings.ContainsRune(s.CSVConfig.Delimiter, LF) {
		return cerror.WrapError(cerror.ErrSinkInvalidConfig,
			errors.New("csv config delimiter contains line break characters"))
	}
	if len(s.CSVConfig.Quote) > 0 && strings.Contains(s.CSVConfig.Delimiter, s.CSVConfig.Quote) {
		return cerror.WrapError(cerror.ErrSinkInvalidConfig,
			errors.New("csv config quote and delimiter cannot be the same"))
	}

	// validate terminator
	if len(s.CSVConfig.Terminator) == 0 {
		s.CSVConfig.Terminator = CRLF
	}

	// validate date separator
	if len(s.CSVConfig.DateSeparator) > 0 {
		var separator DateSeparator
		if err := separator.FromString(s.CSVConfig.DateSeparator); err != nil {
			return cerror.WrapError(cerror.ErrSinkInvalidConfig, err)
		}
	}

	return nil
}

// applyParameter fill the `ReplicaConfig` and `TxnAtomicity` by sinkURI.
func (s *SinkConfig) applyParameter(sinkURI *url.URL) error {
	if sinkURI == nil {
		return nil
	}
	params := sinkURI.Query()

	txnAtomicity := params.Get("transaction-atomicity")
	switch AtomicityLevel(txnAtomicity) {
	case unknownTxnAtomicity:
		// Set default value according to scheme.
		if sink.IsMQScheme(sinkURI.Scheme) {
			s.TxnAtomicity = defaultMqTxnAtomicity
		} else {
			s.TxnAtomicity = defaultMysqlTxnAtomicity
		}
	case noneTxnAtomicity:
		s.TxnAtomicity = noneTxnAtomicity
	case tableTxnAtomicity:
		// MqSink only support `noneTxnAtomicity`.
		if sink.IsMQScheme(sinkURI.Scheme) {
			log.Warn("The configuration of transaction-atomicity is incompatible with scheme",
				zap.Any("txnAtomicity", s.TxnAtomicity),
				zap.String("scheme", sinkURI.Scheme),
				zap.String("protocol", s.Protocol))
			s.TxnAtomicity = defaultMqTxnAtomicity
		} else {
			s.TxnAtomicity = tableTxnAtomicity
		}
	default:
		errMsg := fmt.Sprintf("%s level atomicity is not supported by %s scheme",
			txnAtomicity, sinkURI.Scheme)
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs(errMsg)
	}

	protocolFromURI := params.Get(ProtocolKey)
	if protocolFromURI != "" {
		if s.Protocol != "" {
			log.Warn(
				fmt.Sprintf("protocol is specified in both sink URI and config file "+
					"the value in sink URI will be used "+
					"protocol in sink URI:%s, protocol in config file:%s",
					protocolFromURI, s.Protocol))
		}
		s.Protocol = protocolFromURI
	}

	// validate that protocol is compatible with the scheme
	if sink.IsMQScheme(sinkURI.Scheme) {
		_, err := ParseSinkProtocolFromString(s.Protocol)
		if err != nil {
			return err
		}
	} else if s.Protocol != "" {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("protocol %s "+
			"is incompatible with %s scheme", s.Protocol, sinkURI.Scheme))
	}

	log.Info("succeed to parse parameter from sink uri",
		zap.String("protocol", s.Protocol),
		zap.String("txnAtomicity", string(s.TxnAtomicity)))
	return nil
}
