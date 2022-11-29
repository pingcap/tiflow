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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// DefaultMaxMessageBytes sets the default value for max-message-bytes.
const DefaultMaxMessageBytes = 10 * 1024 * 1024 // 10M

// AtomicityLevel represents the atomicity level of a changefeed.
type AtomicityLevel string

const (
	// unknowTxnAtomicity is the default atomicity level, which is invalid and will
	// be set to a valid value when initializing sink in processor.
	unknowTxnAtomicity AtomicityLevel = ""

	// noneTxnAtomicity means atomicity of transactions is not guaranteed
	noneTxnAtomicity AtomicityLevel = "none"

	// tableTxnAtomicity means atomicity of single table transactions is guaranteed.
	tableTxnAtomicity AtomicityLevel = "table"

	// globalTxnAtomicity means atomicity of cross table transactions is guaranteed, which
	// is currently not supported by TiCDC.
	// globalTxnAtomicity AtomicityLevel = "global"

	defaultMqTxnAtomicity    AtomicityLevel = noneTxnAtomicity
	defaultMysqlTxnAtomicity AtomicityLevel = noneTxnAtomicity
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
	DispatchRules      []*DispatchRule   `toml:"dispatchers" json:"dispatchers"`
	Protocol           string            `toml:"protocol" json:"protocol"`
	ColumnSelectors    []*ColumnSelector `toml:"column-selectors" json:"column-selectors"`
	SchemaRegistry     string            `toml:"schema-registry" json:"schema-registry"`
	TxnAtomicity       AtomicityLevel    `toml:"transaction-atomicity" json:"transaction-atomicity"`
	EncoderConcurrency int               `toml:"encoder-concurrency" json:"encoder-concurrency"`
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

	if s.EncoderConcurrency < 0 {
		return cerror.ErrSinkInvalidConfig.GenWithStack(
			"encoder-concurrency should greater than 0, but got %d", s.EncoderConcurrency)
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
	case unknowTxnAtomicity:
		// Set default value according to scheme.
		if IsMqScheme(sinkURI.Scheme) {
			s.TxnAtomicity = defaultMqTxnAtomicity
		} else {
			s.TxnAtomicity = defaultMysqlTxnAtomicity
		}
	case noneTxnAtomicity:
		s.TxnAtomicity = noneTxnAtomicity
	case tableTxnAtomicity:
		// MqSink only support `noneTxnAtomicity`.
		if IsMqScheme(sinkURI.Scheme) {
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
				fmt.Sprintf("protocol is specified in both sink URI and config file"+
					"the value in sink URI will be used"+
					"protocol in sink URI:%s, protocol in config file:%s",
					protocolFromURI, s.Protocol))
		}
		s.Protocol = protocolFromURI
	}

	// validate that protocol is compatible with the scheme
	if IsMqScheme(sinkURI.Scheme) {
		var protocol Protocol
		err := protocol.FromString(s.Protocol)
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

// IsMqScheme returns true if the scheme belong to mq schema.
func IsMqScheme(scheme string) bool {
	return scheme == "kafka" || scheme == "kafka+ssl" ||
		scheme == "pulsar" || scheme == "pulsar+ssl"
}
