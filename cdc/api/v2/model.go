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

	"github.com/pingcap/tiflow/cdc/model"
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
		ReplicaConfig: getDefaultReplicaConfig(),
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
	CaseSensitive         bool              `toml:"case-sensitive" json:"case-sensitive"`
	EnableOldValue        bool              `toml:"enable-old-value" json:"enable-old-value"`
	ForceReplicate        bool              `toml:"force-replicate" json:"force-replicate"`
	IgnoreIneligibleTable bool              `toml:"ignore-ineligible-table" json:"ignore-ineligible-table"`
	CheckGCSafePoint      bool              `toml:"check-gc-safe-point" json:"check-gc-safe-point"`
	Filter                *FilterConfig     `toml:"filter" json:"filter"`
	Sink                  *SinkConfig       `toml:"sink" json:"sink"`
	Consistent            *ConsistentConfig `toml:"consistent" json:"consistent"`
}

// getDefaultReplicaConfig returns a default ReplicaConfig
func getDefaultReplicaConfig() *ReplicaConfig {
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
	Rules            []string `toml:"rules" json:"rules"`
	IgnoreTxnStartTs []uint64 `toml:"ignore-txn-start-ts" json:"ignore-txn-start-ts"`
	DDLAllowlist     []byte   `toml:"ddl-allow-list" json:"ddl-allow-list,omitempty"`
}

// SinkConfig represents sink config for a changefeed
// This is a duplicate of config.SinkConfig
type SinkConfig struct {
	Protocol        string            `toml:"protocol" json:"protocol"`
	SchemaRegistry  string            `toml:"schema-registry" json:"schema-registry"`
	TimeZone        string            `toml:"time-zone" json:"time-zone"`
	DispatchRules   []*DispatchRule   `toml:"dispatchers" json:"dispatchers"`
	ColumnSelectors []*ColumnSelector `toml:"column-selectors" json:"column-selectors"`
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
