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
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
)

// EmptyResponse return empty {} to http client
type EmptyResponse struct{}

// LogLevelReq log level request
type LogLevelReq struct {
	Level string `json:"log_level"`
}

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
	TableID     int64  `json:"table_id"`
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
	PDAddrs       []string `json:"pd_addrs,omitempty"`
	CAPath        string   `json:"ca_path"`
	CertPath      string   `json:"cert_path"`
	KeyPath       string   `json:"key_path"`
	CertAllowedCN []string `json:"cert_allowed_cn,omitempty"`
}

// ChangefeedConfig use by create changefeed api
type ChangefeedConfig struct {
	Namespace     string         `json:"namespace"`
	ID            string         `json:"changefeed_id"`
	StartTs       uint64         `json:"start_ts"`
	TargetTs      uint64         `json:"target_ts"`
	SinkURI       string         `json:"sink_uri"`
	Engine        string         `json:"engine"`
	ReplicaConfig *ReplicaConfig `json:"replica_config"`
	PDConfig
}

// ReplicaConfig is a duplicate of  config.ReplicaConfig
type ReplicaConfig struct {
	MemoryQuota           uint64                     `json:"memory_quota"`
	CaseSensitive         bool                       `json:"case_sensitive"`
	EnableOldValue        bool                       `json:"enable_old_value"`
	ForceReplicate        bool                       `json:"force_replicate"`
	IgnoreIneligibleTable bool                       `json:"ignore_ineligible_table"`
	CheckGCSafePoint      bool                       `json:"check_gc_safe_point"`
	EnableSyncPoint       bool                       `json:"enable_sync_point"`
	BDRMode               bool                       `json:"bdr_mode"`
	SyncPointInterval     time.Duration              `json:"sync_point_interval"`
	SyncPointRetention    time.Duration              `json:"sync_point_retention"`
	Filter                *FilterConfig              `json:"filter"`
	Mounter               *MounterConfig             `json:"mounter"`
	Sink                  *SinkConfig                `json:"sink"`
	Consistent            *ConsistentConfig          `json:"consistent"`
	Scheduler             *ChangefeedSchedulerConfig `json:"scheduler"`
}

// ToInternalReplicaConfig coverts *v2.ReplicaConfig into *config.ReplicaConfig
func (c *ReplicaConfig) ToInternalReplicaConfig() *config.ReplicaConfig {
	res := config.GetDefaultReplicaConfig()
	res.MemoryQuota = c.MemoryQuota
	res.CaseSensitive = c.CaseSensitive
	res.EnableOldValue = c.EnableOldValue
	res.ForceReplicate = c.ForceReplicate
	res.CheckGCSafePoint = c.CheckGCSafePoint
	res.EnableSyncPoint = c.EnableSyncPoint
	res.SyncPointInterval = c.SyncPointInterval
	res.SyncPointRetention = c.SyncPointRetention
	res.BDRMode = c.BDRMode

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
			}
			if c.Filter.MySQLReplicationRules.IgnoreTables != nil {
				for _, tbl := range c.Filter.MySQLReplicationRules.IgnoreTables {
					mySQLReplicationRules.IgnoreTables = append(mySQLReplicationRules.IgnoreTables,
						&filter.Table{
							Schema: tbl.Schema,
							Name:   tbl.Name,
						})
				}
			}
		}
		var efs []*config.EventFilterRule
		if len(c.Filter.EventFilters) != 0 {
			efs = make([]*config.EventFilterRule, len(c.Filter.EventFilters))
			for i, ef := range c.Filter.EventFilters {
				efs[i] = ef.ToInternalEventFilterRule()
			}
		}
		res.Filter = &config.FilterConfig{
			Rules:                 c.Filter.Rules,
			MySQLReplicationRules: mySQLReplicationRules,
			IgnoreTxnStartTs:      c.Filter.IgnoreTxnStartTs,
			EventFilters:          efs,
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
		var csvConfig *config.CSVConfig
		if c.Sink.CSVConfig != nil {
			csvConfig = &config.CSVConfig{
				Delimiter:       c.Sink.CSVConfig.Delimiter,
				Quote:           c.Sink.CSVConfig.Quote,
				NullString:      c.Sink.CSVConfig.NullString,
				IncludeCommitTs: c.Sink.CSVConfig.IncludeCommitTs,
			}
		}

		res.Sink = &config.SinkConfig{
			DispatchRules:            dispatchRules,
			Protocol:                 c.Sink.Protocol,
			CSVConfig:                csvConfig,
			TxnAtomicity:             config.AtomicityLevel(c.Sink.TxnAtomicity),
			ColumnSelectors:          columnSelectors,
			SchemaRegistry:           c.Sink.SchemaRegistry,
			EncoderConcurrency:       c.Sink.EncoderConcurrency,
			Terminator:               c.Sink.Terminator,
			DateSeparator:            c.Sink.DateSeparator,
			EnablePartitionSeparator: c.Sink.EnablePartitionSeparator,
		}
	}
	if c.Mounter != nil {
		res.Mounter = &config.MounterConfig{
			WorkerNum: c.Mounter.WorkerNum,
		}
	}
	if c.Scheduler != nil {
		res.Scheduler = &config.ChangefeedSchedulerConfig{
			RegionPerSpan: c.Scheduler.RegionPerSpan,
		}
	}
	return res
}

// ToAPIReplicaConfig coverts *config.ReplicaConfig into *v2.ReplicaConfig
func ToAPIReplicaConfig(c *config.ReplicaConfig) *ReplicaConfig {
	cloned := c.Clone()
	res := &ReplicaConfig{
		MemoryQuota:           cloned.MemoryQuota,
		CaseSensitive:         cloned.CaseSensitive,
		EnableOldValue:        cloned.EnableOldValue,
		ForceReplicate:        cloned.ForceReplicate,
		IgnoreIneligibleTable: false,
		CheckGCSafePoint:      cloned.CheckGCSafePoint,
		EnableSyncPoint:       cloned.EnableSyncPoint,
		SyncPointInterval:     cloned.SyncPointInterval,
		SyncPointRetention:    cloned.SyncPointRetention,
		BDRMode:               cloned.BDRMode,
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
			}
			if c.Filter.MySQLReplicationRules.IgnoreTables != nil {
				for _, tbl := range c.Filter.MySQLReplicationRules.IgnoreTables {
					mySQLReplicationRules.IgnoreTables = append(mySQLReplicationRules.IgnoreTables,
						&Table{
							Schema: tbl.Schema,
							Name:   tbl.Name,
						})
				}
			}
		}

		var efs []EventFilterRule
		if len(c.Filter.EventFilters) != 0 {
			efs = make([]EventFilterRule, len(c.Filter.EventFilters))
			for i, ef := range c.Filter.EventFilters {
				efs[i] = ToAPIEventFilterRule(ef)
			}
		}

		res.Filter = &FilterConfig{
			MySQLReplicationRules: mySQLReplicationRules,
			Rules:                 cloned.Filter.Rules,
			IgnoreTxnStartTs:      cloned.Filter.IgnoreTxnStartTs,
			EventFilters:          efs,
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
		var csvConfig *CSVConfig
		if cloned.Sink.CSVConfig != nil {
			csvConfig = &CSVConfig{
				Delimiter:       cloned.Sink.CSVConfig.Delimiter,
				Quote:           cloned.Sink.CSVConfig.Quote,
				NullString:      cloned.Sink.CSVConfig.NullString,
				IncludeCommitTs: cloned.Sink.CSVConfig.IncludeCommitTs,
			}
		}

		res.Sink = &SinkConfig{
			Protocol:                 cloned.Sink.Protocol,
			SchemaRegistry:           cloned.Sink.SchemaRegistry,
			DispatchRules:            dispatchRules,
			CSVConfig:                csvConfig,
			ColumnSelectors:          columnSelectors,
			TxnAtomicity:             string(cloned.Sink.TxnAtomicity),
			EncoderConcurrency:       cloned.Sink.EncoderConcurrency,
			Terminator:               cloned.Sink.Terminator,
			DateSeparator:            cloned.Sink.DateSeparator,
			EnablePartitionSeparator: cloned.Sink.EnablePartitionSeparator,
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
	if cloned.Mounter != nil {
		res.Mounter = &MounterConfig{
			WorkerNum: cloned.Mounter.WorkerNum,
		}
	}
	if cloned.Scheduler != nil {
		res.Scheduler = &ChangefeedSchedulerConfig{
			RegionPerSpan: cloned.Scheduler.RegionPerSpan,
		}
	}
	return res
}

// GetDefaultReplicaConfig returns a default ReplicaConfig
func GetDefaultReplicaConfig() *ReplicaConfig {
	return &ReplicaConfig{
		CaseSensitive:      true,
		EnableOldValue:     true,
		CheckGCSafePoint:   true,
		EnableSyncPoint:    false,
		SyncPointInterval:  10 * time.Second,
		SyncPointRetention: 24 * time.Hour,
		Filter: &FilterConfig{
			Rules: []string{"*.*"},
		},
		Sink: &SinkConfig{},
		Consistent: &ConsistentConfig{
			Level:             "none",
			MaxLogSize:        64,
			FlushIntervalInMs: config.MinFlushIntervalInMs,
			Storage:           "",
		},
	}
}

// FilterConfig represents filter config for a changefeed
// This is a duplicate of config.FilterConfig
type FilterConfig struct {
	*MySQLReplicationRules
	Rules            []string          `json:"rules,omitempty"`
	IgnoreTxnStartTs []uint64          `json:"ignore_txn_start_ts,omitempty"`
	EventFilters     []EventFilterRule `json:"event_filters"`
}

// MounterConfig represents mounter config for a changefeed
type MounterConfig struct {
	WorkerNum int `json:"worker_num"`
}

// EventFilterRule is used by sql event filter and expression filter
type EventFilterRule struct {
	Matcher     []string `json:"matcher"`
	IgnoreEvent []string `json:"ignore_event"`
	// regular expression
	IgnoreSQL []string `toml:"ignore_sql" json:"ignore_sql"`
	// sql expression
	IgnoreInsertValueExpr    string `json:"ignore_insert_value_expr"`
	IgnoreUpdateNewValueExpr string `json:"ignore_update_new_value_expr"`
	IgnoreUpdateOldValueExpr string `json:"ignore_update_old_value_expr"`
	IgnoreDeleteValueExpr    string `json:"ignore_delete_value_expr"`
}

// ToInternalEventFilterRule converts EventFilterRule to *config.EventFilterRule
func (e EventFilterRule) ToInternalEventFilterRule() *config.EventFilterRule {
	res := &config.EventFilterRule{
		Matcher:                  e.Matcher,
		IgnoreSQL:                e.IgnoreSQL,
		IgnoreInsertValueExpr:    e.IgnoreInsertValueExpr,
		IgnoreUpdateNewValueExpr: e.IgnoreUpdateNewValueExpr,
		IgnoreUpdateOldValueExpr: e.IgnoreUpdateOldValueExpr,
		IgnoreDeleteValueExpr:    e.IgnoreDeleteValueExpr,
	}
	if len(e.IgnoreEvent) != 0 {
		res.IgnoreEvent = make([]bf.EventType, len(e.IgnoreEvent))
		for i, et := range e.IgnoreEvent {
			res.IgnoreEvent[i] = bf.EventType(et)
		}
	}
	return res
}

// ToAPIEventFilterRule converts *config.EventFilterRule to API EventFilterRule
func ToAPIEventFilterRule(er *config.EventFilterRule) EventFilterRule {
	res := EventFilterRule{
		IgnoreInsertValueExpr:    er.IgnoreInsertValueExpr,
		IgnoreUpdateNewValueExpr: er.IgnoreUpdateNewValueExpr,
		IgnoreUpdateOldValueExpr: er.IgnoreUpdateOldValueExpr,
		IgnoreDeleteValueExpr:    er.IgnoreDeleteValueExpr,
	}
	if len(er.Matcher) != 0 {
		res.Matcher = make([]string, len(er.Matcher))
		copy(res.Matcher, er.Matcher)
	}
	if len(er.IgnoreSQL) != 0 {
		res.IgnoreSQL = make([]string, len(er.IgnoreSQL))
		copy(res.IgnoreSQL, er.IgnoreSQL)
	}
	if len(er.IgnoreEvent) != 0 {
		res.IgnoreEvent = make([]string, len(er.IgnoreEvent))
		for i, et := range er.IgnoreEvent {
			res.IgnoreEvent[i] = string(et)
		}
	}
	return res
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
	Schema string `json:"database_name"`
	// Name is the unqualified table name.
	Name string `json:"table_name"`
}

// SinkConfig represents sink config for a changefeed
// This is a duplicate of config.SinkConfig
type SinkConfig struct {
	Protocol                 string            `json:"protocol"`
	SchemaRegistry           string            `json:"schema_registry"`
	CSVConfig                *CSVConfig        `json:"csv"`
	DispatchRules            []*DispatchRule   `json:"dispatchers,omitempty"`
	ColumnSelectors          []*ColumnSelector `json:"column_selectors"`
	TxnAtomicity             string            `json:"transaction_atomicity"`
	EncoderConcurrency       int               `json:"encoder_concurrency"`
	Terminator               string            `json:"terminator"`
	DateSeparator            string            `json:"date_separator"`
	EnablePartitionSeparator bool              `json:"enable_partition_separator"`
}

// CSVConfig denotes the csv config
// This is the same as config.CSVConfig
type CSVConfig struct {
	Delimiter       string `json:"delimiter"`
	Quote           string `json:"quote"`
	NullString      string `json:"null"`
	IncludeCommitTs bool   `json:"include_commit_ts"`
}

// DispatchRule represents partition rule for a table
// This is a duplicate of config.DispatchRule
type DispatchRule struct {
	Matcher       []string `json:"matcher,omitempty"`
	PartitionRule string   `json:"partition"`
	TopicRule     string   `json:"topic"`
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
	Level             string `json:"level"`
	MaxLogSize        int64  `json:"max_log_size"`
	FlushIntervalInMs int64  `json:"flush_interval"`
	Storage           string `json:"storage"`
}

// ChangefeedSchedulerConfig is per changefeed scheduler settings.
// This is a duplicate of config.ChangefeedSchedulerConfig
type ChangefeedSchedulerConfig struct {
	// RegionPerSpan the number of regions in a span, must be greater than 1000.
	// Set 0 to disable span replication.
	RegionPerSpan int `toml:"region_per_span" json:"region_per_span"`
}

// EtcdData contains key/value pair of etcd data
type EtcdData struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// ResolveLockReq contains request parameter to resolve lock
type ResolveLockReq struct {
	RegionID uint64 `json:"region_id,omitempty"`
	Ts       uint64 `json:"ts,omitempty"`
	PDConfig
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
	AdminJobType   model.AdminJobType `json:"admin_job_type,omitempty"`
	Engine         string             `json:"engine,omitempty"`
	Config         *ReplicaConfig     `json:"config,omitempty"`
	State          model.FeedState    `json:"state,omitempty"`
	Error          *RunningError      `json:"error,omitempty"`
	CreatorVersion string             `json:"creator_version,omitempty"`
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
	credential.CertAllowedCN = make([]string, len(cfg.CertAllowedCN))
	copy(credential.CertAllowedCN, cfg.CertAllowedCN)
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
