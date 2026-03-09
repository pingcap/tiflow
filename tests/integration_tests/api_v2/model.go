// Copyright 2023 PingCAP, Inc.
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

package main

import (
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
)

const timeFormat = `"2006-01-02 15:04:05.000"`

// JSONTime used to wrap time into json format
type JSONTime time.Time

// MarshalJSON used to specify the time format
func (t JSONTime) MarshalJSON() ([]byte, error) {
	stamp := time.Time(t).Format(timeFormat)
	return []byte(stamp), nil
}

// UnmarshalJSON is used to parse time.Time from bytes. The builtin json.Unmarshal function cannot unmarshal
// a date string formatted as "2006-01-02 15:04:05.000", so we must implement a customized unmarshal function.
func (t *JSONTime) UnmarshalJSON(data []byte) error {
	tm, err := time.Parse(timeFormat, string(data))
	if err != nil {
		return err
	}

	*t = JSONTime(tm)
	return nil
}

// EmptyResponse return empty {} to http client
type EmptyResponse struct{}

// LogLevelReq log level request
type LogLevelReq struct {
	Level string `json:"log_level"`
}

// ListResponse is the response for all List APIs
type ListResponse[T any] struct {
	Total int `json:"total"`
	Items []T `json:"items"`
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

// ChangefeedCommonInfo holds some common usage information of a changefeed
type ChangefeedCommonInfo struct {
	UpstreamID     uint64        `json:"upstream_id"`
	Namespace      string        `json:"namespace"`
	ID             string        `json:"id"`
	FeedState      string        `json:"state"`
	CheckpointTSO  uint64        `json:"checkpoint_tso"`
	CheckpointTime JSONTime      `json:"checkpoint_time"`
	RunningError   *RunningError `json:"error"`
}

// ChangefeedConfig use by create changefeed api
type ChangefeedConfig struct {
	Namespace     string         `json:"namespace"`
	ID            string         `json:"changefeed_id"`
	StartTs       uint64         `json:"start_ts"`
	TargetTs      uint64         `json:"target_ts"`
	SinkURI       string         `json:"sink_uri"`
	ReplicaConfig *ReplicaConfig `json:"replica_config"`
	PDConfig
}

// ProcessorCommonInfo holds the common info of a processor
type ProcessorCommonInfo struct {
	Namespace    string `json:"namespace"`
	ChangeFeedID string `json:"changefeed_id"`
	CaptureID    string `json:"capture_id"`
}

// JSONDuration used to wrap duration into json format
type JSONDuration struct {
	duration time.Duration
}

// MarshalJSON marshal duration to string
func (d JSONDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.duration.Nanoseconds())
}

// UnmarshalJSON unmarshal json value to wrapped duration
func (d *JSONDuration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}

// ReplicaConfig is a duplicate of  config.ReplicaConfig
type ReplicaConfig struct {
	MemoryQuota           uint64 `json:"memory_quota"`
	CaseSensitive         bool   `json:"case_sensitive"`
	ForceReplicate        bool   `json:"force_replicate"`
	IgnoreIneligibleTable bool   `json:"ignore_ineligible_table"`
	CheckGCSafePoint      bool   `json:"check_gc_safe_point"`
	EnableSyncPoint       *bool  `json:"enable_sync_point,omitempty"`
	BDRMode               *bool  `json:"bdr_mode,omitempty"`

	SyncPointInterval  *JSONDuration `json:"sync_point_interval,omitempty" swaggertype:"string"`
	SyncPointRetention *JSONDuration `json:"sync_point_retention,omitempty" swaggertype:"string"`

	Filter     *FilterConfig              `json:"filter"`
	Mounter    *MounterConfig             `json:"mounter"`
	Sink       *SinkConfig                `json:"sink"`
	Consistent *ConsistentConfig          `json:"consistent,omitempty"`
	Scheduler  *ChangefeedSchedulerConfig `json:"scheduler"`
	Integrity  *IntegrityConfig           `json:"integrity"`
}

// FilterConfig represents filter config for a changefeed
// This is a duplicate of config.FilterConfig
type FilterConfig struct {
	Rules            []string          `json:"rules,omitempty"`
	IgnoreTxnStartTs []uint64          `json:"ignore_txn_start_ts,omitempty"`
	EventFilters     []EventFilterRule `json:"event_filters,omitempty"`
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
	Protocol                    string              `json:"protocol,omitempty"`
	SchemaRegistry              string              `json:"schema_registry,omitempty"`
	CSVConfig                   *CSVConfig          `json:"csv,omitempty"`
	DispatchRules               []*DispatchRule     `json:"dispatchers,omitempty"`
	ColumnSelectors             []*ColumnSelector   `json:"column_selectors,omitempty"`
	TxnAtomicity                string              `json:"transaction_atomicity"`
	EncoderConcurrency          *int                `json:"encoder_concurrency,omitempty"`
	Terminator                  string              `json:"terminator"`
	DateSeparator               string              `json:"date_separator,omitempty"`
	EnablePartitionSeparator    *bool               `json:"enable_partition_separator,omitempty"`
	ContentCompatible           *bool               `json:"content_compatible"`
	SendBootstrapIntervalInSec  *int64              `json:"send_bootstrap_interval_in_sec,omitempty"`
	SendBootstrapInMsgCount     *int32              `json:"send_bootstrap_in_msg_count,omitempty"`
	SendBootstrapToAllPartition *bool               `json:"send_bootstrap_to_all_partition,omitempty"`
	DebeziumDisableSchema       *bool               `json:"debezium_disable_schema,omitempty"`
	DebeziumConfig              *DebeziumConfig     `json:"debezium,omitempty"`
	OpenProtocolConfig          *OpenProtocolConfig `json:"open,omitempty"`
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
	IndexName     string   `json:"index,omitempty"`
	Columns       []string `json:"columns,omitempty"`
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
	Level                 string `json:"level"`
	MaxLogSize            int64  `json:"max_log_size"`
	FlushIntervalInMs     int64  `json:"flush_interval"`
	MetaFlushIntervalInMs int64  `json:"meta_flush_interval"`
	EncoderWorkerNum      int    `json:"encoding_worker_num"`
	FlushWorkerNum        int    `json:"flush_worker_num"`
	Storage               string `json:"storage"`
	UseFileBackend        bool   `json:"use_file_backend"`
}

// ChangefeedSchedulerConfig is per changefeed scheduler settings.
// This is a duplicate of config.ChangefeedSchedulerConfig
type ChangefeedSchedulerConfig struct {
	// EnableTableAcrossNodes set true to split one table to multiple spans and
	// distribute to multiple TiCDC nodes.
	EnableTableAcrossNodes bool `toml:"enable_table_across_nodes" json:"enable_table_across_nodes"`
	// RegionThreshold is the region count threshold of splitting a table.
	RegionThreshold int `toml:"region_threshold" json:"region_threshold"`
	// WriteKeyThreshold is the written keys threshold of splitting a table.
	WriteKeyThreshold int `toml:"write_key_threshold" json:"write_key_threshold"`
}

// IntegrityConfig is the config for integrity check
// This is a duplicate of config.IntegrityConfig
type IntegrityConfig struct {
	IntegrityCheckLevel   string `json:"integrity_check_level"`
	CorruptionHandleLevel string `json:"corruption_handle_level"`
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
	AdminJobType   int            `json:"admin_job_type,omitempty"`
	Config         *ReplicaConfig `json:"config,omitempty"`
	State          string         `json:"state,omitempty"`
	Error          *RunningError  `json:"error,omitempty"`
	CreatorVersion string         `json:"creator_version,omitempty"`

	ResolvedTs     uint64              `json:"resolved_ts"`
	CheckpointTs   uint64              `json:"checkpoint_ts"`
	CheckpointTime JSONTime            `json:"checkpoint_time"`
	TaskStatus     []CaptureTaskStatus `json:"task_status,omitempty"`
}

// CaptureTaskStatus holds TaskStatus of a capture
type CaptureTaskStatus struct {
	CaptureID string `json:"capture_id"`
	// Table list, containing tables that processor should process
	Tables    []int64                    `json:"table_ids"`
	Operation map[uint64]*TableOperation `json:"table_operations"`
}

// TableOperation records the current information of a table migration
type TableOperation struct {
	Delete bool   `json:"delete"`
	Flag   uint64 `json:"flag,omitempty"`
	// if the operation is a delete operation, BoundaryTs is checkpoint ts
	// if the operation is an add operation, BoundaryTs is start ts
	BoundaryTs uint64 `json:"boundary_ts"`
	Status     uint64 `json:"status,omitempty"`
}

// RunningError represents some running error from cdc components,
// such as processor.
type RunningError struct {
	Addr    string `json:"addr"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ProcessorDetail holds the detail info of a processor
type ProcessorDetail struct {
	// All table ids that this processor are replicating.
	Tables []int64 `json:"table_ids"`
}

// Liveness is the liveness status of a capture.
// Liveness can only be changed from alive to stopping, and no way back.
type Liveness int32

// ServerStatus holds some common information of a server
type ServerStatus struct {
	Version   string   `json:"version"`
	GitHash   string   `json:"git_hash"`
	ID        string   `json:"id"`
	ClusterID string   `json:"cluster_id"`
	Pid       int      `json:"pid"`
	IsOwner   bool     `json:"is_owner"`
	Liveness  Liveness `json:"liveness"`
}

// Capture holds common information of a capture in cdc
type Capture struct {
	ID            string `json:"id"`
	IsOwner       bool   `json:"is_owner"`
	AdvertiseAddr string `json:"address"`
	ClusterID     string `json:"cluster_id"`
}

// OpenProtocolConfig represents the configurations for open protocol encoding
type OpenProtocolConfig struct {
	OutputOldValue bool `json:"output_old_value"`
}

// DebeziumConfig represents the configurations for debezium protocol encoding
type DebeziumConfig struct {
	OutputOldValue bool `json:"output_old_value"`
}
