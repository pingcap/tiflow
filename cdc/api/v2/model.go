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

package v2

import (
	"time"

	tidbModel "github.com/pingcap/tidb/parser/model"
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
)

// Tso contains timestamp get from PD
type Tso struct {
	Timestamp int64 `json:"timestamp"`
	LogicTime int64 `json:"logic-time"`
}

// Tables contains IneligibleTables and EligibleTables
type Tables struct {
	IneligibleTables []model.TableName `json:"ineligible-tables,omitempty"`
	EligibleTables   []model.TableName `json:"eligible-tables,omitempty"`
}

// VerifyTableConfig use to verify tables.
// Only use by Open API v2.
type VerifyTableConfig struct {
	PDAddrs       []string `json:"pd-addrs"`
	CAPath        string   `toml:"ca-path" json:"ca-path"`
	CertPath      string   `toml:"cert-path" json:"cert-path"`
	KeyPath       string   `toml:"key-path" json:"key-path"`
	CertAllowedCN []string `toml:"cert-allowed-cn" json:"cert-allowed-cn"`

	ReplicaConfig *ReplicaConfig `json:"replica-config"`
	StartTs       uint64         `json:"start-ts"`
}

func getDefaultVerifyTableConfig() *VerifyTableConfig {
	return &VerifyTableConfig{
		ReplicaConfig: GetDefaultReplicaConfig(),
	}
}

// ChangefeedConfig use by create changefeed api
type ChangefeedConfig struct {
	Namespace string `json:"namespace"`
	ID        string `json:"changefeed-id"`
	StartTs   uint64 `json:"start-ts"`
	TargetTs  uint64 `json:"target-ts"`
	SinkURI   string `json:"sink-uri"`

	ReplicaConfig *ReplicaConfig `json:"replica-config"`

	SyncPointEnabled  bool          `json:"sync-point-enabled"`
	SyncPointInterval time.Duration `json:"sync-point-interval"`

	PDAddrs       []string `json:"pd-addrs"`
	CAPath        string   `toml:"ca-path" json:"ca-path"`
	CertPath      string   `toml:"cert-path" json:"cert-path"`
	KeyPath       string   `toml:"key-path" json:"key-path"`
	CertAllowedCN []string `toml:"cert-allowed-cn" json:"cert-allowed-cn"`
}

// ReplicaConfig is a duplicate of  config.ReplicaConfig
type ReplicaConfig struct {
	CaseSensitive         bool              `json:"case-sensitive"`
	EnableOldValue        bool              `json:"enable-old-value"`
	ForceReplicate        bool              `json:"force-replicate"`
	IgnoreIneligibleTable bool              `json:"ignore-ineligible-table"`
	CheckGCSafePoint      bool              `json:"check-gc-safe-point"`
	Filter                *FilterConfig     `json:"filter"`
	Sink                  *SinkConfig       `json:"sink"`
	Consistent            *ConsistentConfig `json:"consistent"`
}

// ToInternalReplicaConfig coverts *v2.ReplicaConfig into *config.ReplicaConfig
func (c *ReplicaConfig) ToInternalReplicaConfig() *config.ReplicaConfig {
	res := config.GetDefaultReplicaConfig()
	res.CaseSensitive = c.CaseSensitive
	res.EnableOldValue = c.EnableOldValue
	res.ForceReplicate = c.ForceReplicate
	res.CheckGCSafePoint = c.CheckGCSafePoint

	if c.Filter != nil {
		res.Filter = &config.FilterConfig{
			Rules:                 c.Filter.Rules,
			MySQLReplicationRules: c.Filter.MySQLReplicationRules,
			IgnoreTxnStartTs:      c.Filter.IgnoreTxnStartTs,
			DDLAllowlist:          c.Filter.DDLAllowlist,
		}
	}

	if c.Consistent != nil {
		res.Consistent = &config.ConsistentConfig{
			Level:             c.Consistent.Level,
			MaxLogSize:        c.Consistent.MaxLogSize,
			FlushIntervalInMs: c.Consistent.FlushIntervalInMs,
			Storage:           c.Consistent.Storage,
		}
	}
	if c.Sink != nil {
		var dispatchRules []*config.DispatchRule
		for _, rule := range c.Sink.DispatchRules {
			dispatchRules = append(dispatchRules, &config.DispatchRule{
				Matcher:        rule.Matcher,
				DispatcherRule: "",
				PartitionRule:  rule.PartitionRule,
				TopicRule:      rule.TopicRule,
			})
		}
		var columnSelectors []*config.ColumnSelector
		for _, selector := range c.Sink.ColumnSelectors {
			columnSelectors = append(columnSelectors, &config.ColumnSelector{
				Matcher: selector.Matcher,
				Columns: selector.Columns,
			})
		}
		res.Sink = &config.SinkConfig{
			DispatchRules:   dispatchRules,
			Protocol:        c.Sink.Protocol,
			ColumnSelectors: columnSelectors,
			SchemaRegistry:  c.Sink.SchemaRegistry,
		}
	}
	return res
}

// ToInternalReplicaConfig coverts *config.ReplicaConfig into *v2.ReplicaConfig
func ToAPIReplicaConfig(c *config.ReplicaConfig) *ReplicaConfig {
	res := &ReplicaConfig{
		CaseSensitive:         c.CaseSensitive,
		EnableOldValue:        c.EnableOldValue,
		ForceReplicate:        c.ForceReplicate,
		IgnoreIneligibleTable: false,
		CheckGCSafePoint:      c.CheckGCSafePoint,
	}

	if c.Filter != nil {
		res.Filter = &FilterConfig{
			MySQLReplicationRules: c.Filter.MySQLReplicationRules,
			Rules:                 c.Filter.Rules,
			IgnoreTxnStartTs:      c.Filter.IgnoreTxnStartTs,
			DDLAllowlist:          c.Filter.DDLAllowlist,
		}
	}
	if c.Sink != nil {
		var dispatchRules []*DispatchRule
		for _, rule := range c.Sink.DispatchRules {
			dispatchRules = append(dispatchRules, &DispatchRule{
				Matcher:       rule.Matcher,
				PartitionRule: rule.PartitionRule,
				TopicRule:     rule.TopicRule,
			})
		}
		var columnSelectors []*ColumnSelector
		for _, selector := range c.Sink.ColumnSelectors {
			columnSelectors = append(columnSelectors, &ColumnSelector{
				Matcher: selector.Matcher,
				Columns: selector.Columns,
			})
		}
		res.Sink = &SinkConfig{
			Protocol:        c.Sink.Protocol,
			SchemaRegistry:  c.Sink.SchemaRegistry,
			DispatchRules:   dispatchRules,
			ColumnSelectors: columnSelectors,
		}
	}
	if c.Consistent != nil {
		res.Consistent = &ConsistentConfig{
			Level:             c.Consistent.Level,
			MaxLogSize:        c.Consistent.MaxLogSize,
			FlushIntervalInMs: c.Consistent.FlushIntervalInMs,
			Storage:           c.Consistent.Storage,
		}
	}
	return res
}

// GetDefaultReplicaConfig returns a default ReplicaConfig
func GetDefaultReplicaConfig() *ReplicaConfig {
	return &ReplicaConfig{
		CaseSensitive:    true,
		EnableOldValue:   true,
		CheckGCSafePoint: true,
		Filter: &FilterConfig{
			Rules: []string{"*.*"},
		},
		Sink: &SinkConfig{},
		Consistent: &ConsistentConfig{
			Level:             "none",
			MaxLogSize:        64,
			FlushIntervalInMs: 1000,
			Storage:           "",
		},
	}
}

// FilterConfig represents filter config for a changefeed
// This is a duplicate of config.FilterConfig
type FilterConfig struct {
	*filter.MySQLReplicationRules
	Rules            []string               `json:"rules"`
	IgnoreTxnStartTs []uint64               `json:"ignore-txn-start-ts"`
	DDLAllowlist     []tidbModel.ActionType `json:"ddl-allow-list,omitempty"`
}

// SinkConfig represents sink config for a changefeed
// This is a duplicate of config.SinkConfig
type SinkConfig struct {
	Protocol        string            `json:"protocol"`
	SchemaRegistry  string            `json:"schema-registry"`
	DispatchRules   []*DispatchRule   `json:"dispatchers"`
	ColumnSelectors []*ColumnSelector `json:"column-selectors"`
}

// DispatchRule represents partition rule for a table
// This is a duplicate of config.DispatchRule
type DispatchRule struct {
	Matcher       []string `toml:"matcher" json:"matcher"`
	PartitionRule string   `toml:"partition" json:"partition"`
	TopicRule     string   `toml:"topic" json:"topic"`
}

// ColumnSelector represents a column selector for a table.
// This is a duplicate of config.ColumnSelector
type ColumnSelector struct {
	Matcher []string `toml:"matcher" json:"matcher"`
	Columns []string `toml:"columns" json:"columns"`
}

// ConsistentConfig represents replication consistency config for a changefeed
// This is a duplicate of config.ConsistentConfig
type ConsistentConfig struct {
	Level             string `toml:"level" json:"level"`
	MaxLogSize        int64  `toml:"max-log-size" json:"max-log-size"`
	FlushIntervalInMs int64  `toml:"flush-interval" json:"flush-interval"`
	Storage           string `toml:"storage" json:"storage"`
}

// EtcdData contains key/value pair of etcd data
type EtcdData struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// ResolveLockReq contains request parameter to resolve lock
type ResolveLockReq struct {
	RegionID uint64 `json:"region-id,omitempty"`
	Ts       uint64 `json:"ts,omitempty"`
}
