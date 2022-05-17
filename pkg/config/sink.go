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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// DefaultMaxMessageBytes sets the default value for max-message-bytes
const DefaultMaxMessageBytes = 10 * 1024 * 1024 // 10M

// ForceEnableOldValueProtocols specifies which protocols need to be forced to enable old value.
var ForceEnableOldValueProtocols = []string{
	ProtocolCanal.String(),
	ProtocolCanalJSON.String(),
	ProtocolMaxwell.String(),
}

// SinkConfig represents sink config for a changefeed
type SinkConfig struct {
	DispatchRules   []*DispatchRule   `toml:"dispatchers" json:"dispatchers"`
	Protocol        string            `toml:"protocol" json:"protocol"`
	ColumnSelectors []*ColumnSelector `toml:"column-selectors" json:"column-selectors"`
}

// DispatchRule represents partition rule for a table
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

func (s *SinkConfig) validate(enableOldValue bool) error {
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

	return nil
}
