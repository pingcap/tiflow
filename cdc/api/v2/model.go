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
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	tidbModel "github.com/pingcap/tidb/parser/model"
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
)

// Tso contains timestamp get from PD
type Tso struct {
	Timestamp int64 `json:"timestamp"`
	LogicTime int64 `json:"logic_time"`
}

// Tables contains IneligibleTables and EligibleTables
type Tables struct {
	IneligibleTables []TableName `json:"ineligible_tables,omitempty"`
	EligibleTables   []TableName `json:"eligible_tables,omitempty"`
}

// TableName contains table information
type TableName struct {
	Schema      string `json:"database_name"`
	Table       string `json:"table_name"`
	TableID     int64  `json:"table_id" `
	IsPartition bool   `json:"is_partition"`
}

// VerifyTableConfig use to verify tables.
// Only use by Open API v2.
type VerifyTableConfig struct {
	PDConfig
	ReplicaConfig *ReplicaConfig `json:"replica_config"`
	StartTs       uint64         `json:"start_ts"`
}

func getDefaultVerifyTableConfig() *VerifyTableConfig {
	return &VerifyTableConfig{
		ReplicaConfig: GetDefaultReplicaConfig(),
	}
}

// ResumeChangefeedConfig is used by resume changefeed api
type ResumeChangefeedConfig struct {
	PDConfig
	OverwriteCheckpointTs uint64 `json:"overwrite_checkpoint_ts"`
}

// PDConfig is a configuration used to connect to pd
type PDConfig struct {
	PDAddrs       []string `json:"pd_addrs"`
	CAPath        string   `json:"ca_path"`
	CertPath      string   `json:"cert_path"`
	KeyPath       string   `json:"key_path"`
	CertAllowedCN []string `json:"cert_allowed_cn"`
}

// ChangefeedConfig use by create changefeed api
type ChangefeedConfig struct {
	Namespace string `json:"namespace"`
	ID        string `json:"changefeed_id"`
	StartTs   uint64 `json:"start_ts"`
	TargetTs  uint64 `json:"target_ts"`
	SinkURI   string `json:"sink_uri"`
	Engine    string `json:"engine"`

	ReplicaConfig *ReplicaConfig `json:"replica_config"`

	SyncPointEnabled  bool          `json:"sync_point_enabled"`
	SyncPointInterval time.Duration `json:"sync_point_interval"`
	PDConfig
}

// ReplicaConfig is a duplicate of  config.ReplicaConfig
type ReplicaConfig struct {
	CaseSensitive         bool              `json:"case_sensitive"`
	EnableOldValue        bool              `json:"enable_old_value"`
	ForceReplicate        bool              `json:"force_replicate"`
	IgnoreIneligibleTable bool              `json:"ignore_ineligible_table"`
	CheckGCSafePoint      bool              `json:"check_gc_safe_point"`
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

// ToAPIReplicaConfig coverts *config.ReplicaConfig into *v2.ReplicaConfig
func ToAPIReplicaConfig(c *config.ReplicaConfig) *ReplicaConfig {
	cloned := c.Clone()
	res := &ReplicaConfig{
		CaseSensitive:         cloned.CaseSensitive,
		EnableOldValue:        cloned.EnableOldValue,
		ForceReplicate:        cloned.ForceReplicate,
		IgnoreIneligibleTable: false,
		CheckGCSafePoint:      cloned.CheckGCSafePoint,
	}

	if cloned.Filter != nil {
		res.Filter = &FilterConfig{
			MySQLReplicationRules: cloned.Filter.MySQLReplicationRules,
			Rules:                 cloned.Filter.Rules,
			IgnoreTxnStartTs:      cloned.Filter.IgnoreTxnStartTs,
			DDLAllowlist:          cloned.Filter.DDLAllowlist,
		}
	}
	if cloned.Sink != nil {
		var dispatchRules []*DispatchRule
		for _, rule := range cloned.Sink.DispatchRules {
			dispatchRules = append(dispatchRules, &DispatchRule{
				Matcher:       rule.Matcher,
				PartitionRule: rule.PartitionRule,
				TopicRule:     rule.TopicRule,
			})
		}
		var columnSelectors []*ColumnSelector
		for _, selector := range cloned.Sink.ColumnSelectors {
			columnSelectors = append(columnSelectors, &ColumnSelector{
				Matcher: selector.Matcher,
				Columns: selector.Columns,
			})
		}
		res.Sink = &SinkConfig{
			Protocol:        cloned.Sink.Protocol,
			SchemaRegistry:  cloned.Sink.SchemaRegistry,
			DispatchRules:   dispatchRules,
			ColumnSelectors: columnSelectors,
		}
	}
	if cloned.Consistent != nil {
		res.Consistent = &ConsistentConfig{
			Level:             cloned.Consistent.Level,
			MaxLogSize:        cloned.Consistent.MaxLogSize,
			FlushIntervalInMs: cloned.Consistent.FlushIntervalInMs,
			Storage:           cloned.Consistent.Storage,
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
	IgnoreTxnStartTs []uint64               `json:"ignore_txn_start_ts"`
	DDLAllowlist     []tidbModel.ActionType `json:"ddl_allow_list,omitempty"`
}

// SinkConfig represents sink config for a changefeed
// This is a duplicate of config.SinkConfig
type SinkConfig struct {
	Protocol        string            `json:"protocol"`
	SchemaRegistry  string            `json:"schema_registry"`
	DispatchRules   []*DispatchRule   `json:"dispatchers"`
	ColumnSelectors []*ColumnSelector `json:"column_selectors"`
}

// DispatchRule represents partition rule for a table
// This is a duplicate of config.DispatchRule
type DispatchRule struct {
	Matcher       []string `json:"matcher"`
	PartitionRule string   `json:"partition"`
	TopicRule     string   `json:"topic"`
}

// ColumnSelector represents a column selector for a table.
// This is a duplicate of config.ColumnSelector
type ColumnSelector struct {
	Matcher []string `json:"matcher"`
	Columns []string `json:"columns"`
}

// ConsistentConfig represents replication consistency config for a changefeed
// This is a duplicate of config.ConsistentConfig
type ConsistentConfig struct {
	Level             string `json:"level"`
	MaxLogSize        int64  `json:"max_log_size"`
	FlushIntervalInMs int64  `json:"flush_interval"`
	Storage           string `json:"storage"`
}

// EtcdData contains key/value pair of etcd data
type EtcdData struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// ResolveLockReq contains request parameter to resolve lock
type ResolveLockReq struct {
	RegionID      uint64   `json:"region_id,omitempty"`
	Ts            uint64   `json:"ts,omitempty"`
	PDAddrs       []string `json:"pd_addrs"`
	CAPath        string   `json:"ca_path"`
	CertPath      string   `json:"cert_path"`
	KeyPath       string   `json:"key_path"`
	CertAllowedCN []string `json:"cert_allowed_cn"`
}

// ChangeFeedInfo describes the detail of a ChangeFeed
type ChangeFeedInfo struct {
	UpstreamID uint64    `json:"upstream_id,omitempty"`
	Namespace  string    `json:"namespace,omitempty"`
	ID         string    `json:"id,omitempty"`
	SinkURI    string    `json:"sink_uri,omitempty"`
	CreateTime time.Time `json:"create_time"`
	// Start sync at this commit ts if `StartTs` is specify or using the CreateTime of changefeed.
	StartTs uint64 `json:"start_ts,omitempty"`
	// The ChangeFeed will exits until sync to timestamp TargetTs
	TargetTs uint64 `json:"target_ts,omitempty"`
	// used for admin job notification, trigger watch event in capture
	AdminJobType      model.AdminJobType `json:"admin_job_type,omitempty"`
	Engine            string             `json:"engine,omitempty"`
	Config            *ReplicaConfig     `json:"config,omitempty"`
	State             model.FeedState    `json:"state,omitempty"`
	Error             *RunningError      `json:"error,omitempty"`
	SyncPointEnabled  bool               `json:"sync_point_enabled,omitempty"`
	SyncPointInterval time.Duration      `json:"sync_point_interval,omitempty"`
	CreatorVersion    string             `json:"creator_version,omitempty"`
}

// RunningError represents some running error from cdc components, such as processor.
type RunningError struct {
	Addr    string `json:"addr"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// toCredential generates a security.Credential from a PDConfig
func (cfg *PDConfig) toCredential() *security.Credential {
	credential := &security.Credential{
		CAPath:   cfg.CAPath,
		CertPath: cfg.CertPath,
		KeyPath:  cfg.KeyPath,
	}
	if len(cfg.CertAllowedCN) != 0 {
		credential.CertAllowedCN = cfg.CertAllowedCN
	}
	return credential
}

// Marshal returns the json marshal format of a ChangeFeedInfo
func (info *ChangeFeedInfo) Marshal() (string, error) {
	data, err := json.Marshal(info)
	return string(data), cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// Clone returns a cloned ChangeFeedInfo
func (info *ChangeFeedInfo) Clone() (*ChangeFeedInfo, error) {
	s, err := info.Marshal()
	if err != nil {
		return nil, err
	}
	cloned := new(ChangeFeedInfo)
	err = cloned.Unmarshal([]byte(s))
	return cloned, err
}

// Unmarshal unmarshals into *ChangeFeedInfo from json marshal byte slice
func (info *ChangeFeedInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, &info)
	if err != nil {
		return errors.Annotatef(
			cerror.WrapError(cerror.ErrUnmarshalFailed, err), "Unmarshal data: %v", data)
	}
	return nil
}

// UpstreamConfig contains info to connect to pd
type UpstreamConfig struct {
	ID uint64 `json:"id"`
	PDConfig
}
