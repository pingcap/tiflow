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
)

// Tso contains timestamp get from PD
type Tso struct {
	Timestamp int64 `json:"timestamp,omitempty"`
	LogicTime int64 `json:"logic_time,omitempty"`
}

// Tables contains IneligibleTables and EligibleTables
type Tables struct {
	IneligibleTables []TableName `json:"ineligible_tables,omitempty"`
	EligibleTables   []TableName `json:"eligible_tables,omitempty"`
}

// TableName contains table information
type TableName struct {
	Schema      string `json:"database_name,omitempty"`
	Table       string `json:"table_name,omitempty"`
	TableID     int64  `json:"table_id,omitempty"`
	IsPartition bool   `json:"is_partition,omitempty"`
}

// VerifyTableConfig use to verify tables.
// Only use by Open API v2.
type VerifyTableConfig struct {
	PDConfig
	ReplicaConfig *ReplicaConfig `json:"replica_config,omitempty"`
	StartTs       uint64         `json:"start_ts,omitempty"`
}

func getDefaultVerifyTableConfig() *VerifyTableConfig {
	return &VerifyTableConfig{
		ReplicaConfig: GetDefaultReplicaConfig(),
	}
}

// ResumeChangefeedConfig is used by resume changefeed api
type ResumeChangefeedConfig struct {
	PDConfig
	OverwriteCheckpointTs uint64 `json:"overwrite_checkpoint_ts,omitempty"`
}

// PDConfig is a configuration used to connect to pd
type PDConfig struct {
	PDAddrs       []string `json:"pd_addrs,omitempty"`
	CAPath        string   `json:"ca_path,omitempty"`
	CertPath      string   `json:"cert_path,omitempty"`
	KeyPath       string   `json:"key_path,omitempty"`
	CertAllowedCN []string `json:"cert_allowed_cn,omitempty"`
}

// ChangefeedConfig use by create changefeed api
type ChangefeedConfig struct {
	Namespace         string         `json:"namespace,omitempty"`
	ID                string         `json:"changefeed_id,omitempty"`
	StartTs           uint64         `json:"start_ts,omitempty"`
	TargetTs          uint64         `json:"target_ts,omitempty"`
	SinkURI           string         `json:"sink_uri,omitempty"`
	Engine            string         `json:"engine,omitempty"`
	ReplicaConfig     *ReplicaConfig `json:"replica_config,omitempty"`
	SyncPointEnabled  bool           `json:"sync_point_enabled,omitempty"`
	SyncPointInterval time.Duration  `json:"sync_point_interval,omitempty"`
	PDConfig
}

// ReplicaConfig is a duplicate of  config.ReplicaConfig
type ReplicaConfig struct {
	CaseSensitive         bool              `json:"case_sensitive,omitempty"`
	EnableOldValue        bool              `json:"enable_old_value,omitempty"`
	ForceReplicate        bool              `json:"force_replicate,omitempty"`
	IgnoreIneligibleTable bool              `json:"ignore_ineligible_table,omitempty"`
	CheckGCSafePoint      bool              `json:"check_gc_safe_point,omitempty"`
	Filter                *FilterConfig     `json:"filter,omitempty"`
	Sink                  *SinkConfig       `json:"sink,omitempty"`
	Consistent            *ConsistentConfig `json:"consistent,omitempty"`
}

// ToInternalReplicaConfig coverts *v2.ReplicaConfig into *config.ReplicaConfig
func (c *ReplicaConfig) ToInternalReplicaConfig() *config.ReplicaConfig {
	res := config.GetDefaultReplicaConfig()
	res.CaseSensitive = c.CaseSensitive
	res.EnableOldValue = c.EnableOldValue
	res.ForceReplicate = c.ForceReplicate
	res.CheckGCSafePoint = c.CheckGCSafePoint

	if c.Filter != nil {
		var mySQLReplicationRules *filter.MySQLReplicationRules
		if c.Filter.MySQLReplicationRules != nil {
			mySQLReplicationRules = &filter.MySQLReplicationRules{}
			mySQLReplicationRules.DoDBs = c.Filter.DoDBs
			mySQLReplicationRules.IgnoreDBs = c.Filter.IgnoreDBs
			if c.Filter.MySQLReplicationRules.DoTables != nil {
				for _, tbl := range c.Filter.MySQLReplicationRules.DoTables {
					mySQLReplicationRules.DoTables = append(mySQLReplicationRules.DoTables,
						&filter.Table{
							Schema: tbl.Schema,
							Name:   tbl.Name,
						})
				}
				for _, tbl := range c.Filter.MySQLReplicationRules.IgnoreTables {
					mySQLReplicationRules.IgnoreTables = append(mySQLReplicationRules.IgnoreTables,
						&filter.Table{
							Schema: tbl.Schema,
							Name:   tbl.Name,
						})
				}
			}
		}
		res.Filter = &config.FilterConfig{
			Rules:                 c.Filter.Rules,
			MySQLReplicationRules: mySQLReplicationRules,
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
			TxnAtomicity:    config.AtomicityLevel(c.Sink.TxnAtomicity),
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
		var mySQLReplicationRules *MySQLReplicationRules
		if c.Filter.MySQLReplicationRules != nil {
			mySQLReplicationRules = &MySQLReplicationRules{}
			mySQLReplicationRules.DoDBs = c.Filter.DoDBs
			mySQLReplicationRules.IgnoreDBs = c.Filter.IgnoreDBs
			if c.Filter.MySQLReplicationRules.DoTables != nil {
				for _, tbl := range c.Filter.MySQLReplicationRules.DoTables {
					mySQLReplicationRules.DoTables = append(mySQLReplicationRules.DoTables,
						&Table{
							Schema: tbl.Schema,
							Name:   tbl.Name,
						})
				}
				for _, tbl := range c.Filter.MySQLReplicationRules.IgnoreTables {
					mySQLReplicationRules.IgnoreTables = append(mySQLReplicationRules.IgnoreTables,
						&Table{
							Schema: tbl.Schema,
							Name:   tbl.Name,
						})
				}
			}
		}
		res.Filter = &FilterConfig{
			MySQLReplicationRules: mySQLReplicationRules,
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
			TxnAtomicity:    string(cloned.Sink.TxnAtomicity),
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
	*MySQLReplicationRules
	Rules            []string               `json:"rules,omitempty"`
	IgnoreTxnStartTs []uint64               `json:"ignore_txn_start_ts,omitempty"`
	DDLAllowlist     []tidbModel.ActionType `json:"ddl_allow_list,omitempty"`
}

// MySQLReplicationRules is a set of rules based on MySQL's replication tableFilter.
type MySQLReplicationRules struct {
	// DoTables is an allowlist of tables.
	DoTables []*Table `json:"do_tables,omitempty"`
	// DoDBs is an allowlist of schemas.
	DoDBs []string `json:"do_dbs,omitempty"`

	// IgnoreTables is a blocklist of tables.
	IgnoreTables []*Table `json:"ignore_tables,omitempty"`
	// IgnoreDBs is a blocklist of schemas.
	IgnoreDBs []string `json:"ignore_dbs,omitempty"`
}

// Table represents a qualified table name.
type Table struct {
	// Schema is the name of the schema (database) containing this table.
	Schema string `json:"database_name,omitempty"`
	// Name is the unqualified table name.
	Name string `json:"table_name,omitempty"`
}

// SinkConfig represents sink config for a changefeed
// This is a duplicate of config.SinkConfig
type SinkConfig struct {
	Protocol        string            `json:"protocol,omitempty"`
	SchemaRegistry  string            `json:"schema_registry,omitempty"`
	DispatchRules   []*DispatchRule   `json:"dispatchers,omitempty,omitempty"`
	ColumnSelectors []*ColumnSelector `json:"column_selectors,omitempty"`
	TxnAtomicity    string            `json:"transaction_atomicity"`
}

// DispatchRule represents partition rule for a table
// This is a duplicate of config.DispatchRule
type DispatchRule struct {
	Matcher       []string `json:"matcher,omitempty"`
	PartitionRule string   `json:"partition,omitempty"`
	TopicRule     string   `json:"topic,omitempty"`
}

// ColumnSelector represents a column selector for a table.
// This is a duplicate of config.ColumnSelector
type ColumnSelector struct {
	Matcher []string `json:"matcher,omitempty"`
	Columns []string `json:"columns,omitempty"`
}

// ConsistentConfig represents replication consistency config for a changefeed
// This is a duplicate of config.ConsistentConfig
type ConsistentConfig struct {
	Level             string `json:"level,omitempty"`
	MaxLogSize        int64  `json:"max_log_size,omitempty"`
	FlushIntervalInMs int64  `json:"flush_interval,omitempty"`
	Storage           string `json:"storage,omitempty"`
}

// EtcdData contains key/value pair of etcd data
type EtcdData struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// ResolveLockReq contains request parameter to resolve lock
type ResolveLockReq struct {
	RegionID uint64 `json:"region_id,omitempty,omitempty"`
	Ts       uint64 `json:"ts,omitempty,omitempty"`
	PDConfig
}

// ChangeFeedInfo describes the detail of a ChangeFeed
type ChangeFeedInfo struct {
	UpstreamID uint64    `json:"upstream_id,omitempty"`
	Namespace  string    `json:"namespace,omitempty"`
	ID         string    `json:"id,omitempty,omitempty"`
	SinkURI    string    `json:"sink_uri,omitempty"`
	CreateTime time.Time `json:"create_time,omitempty"`
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
	Addr    string `json:"addr,omitempty"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
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
	ID uint64 `json:"id,omitempty"`
	PDConfig
}
