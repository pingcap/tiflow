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
	"github.com/pingcap/tiflow/pkg/integrity"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
)

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

// ChangefeedCommonInfo holds some common usage information of a changefeed
type ChangefeedCommonInfo struct {
	UpstreamID     uint64              `json:"upstream_id"`
	Namespace      string              `json:"namespace"`
	ID             string              `json:"id"`
	FeedState      model.FeedState     `json:"state"`
	CheckpointTSO  uint64              `json:"checkpoint_tso"`
	CheckpointTime model.JSONTime      `json:"checkpoint_time"`
	RunningError   *model.RunningError `json:"error"`
}

// MarshalJSON marshal changefeed common info to json
// we need to set feed state to normal if it is uninitialized and pending to warning
// to hide the detail of uninitialized and pending state from user
func (c ChangefeedCommonInfo) MarshalJSON() ([]byte, error) {
	// alias the original type to prevent recursive call of MarshalJSON
	type Alias ChangefeedCommonInfo

	if c.FeedState == model.StateUnInitialized {
		c.FeedState = model.StateNormal
	}
	if c.FeedState == model.StatePending {
		c.FeedState = model.StateWarning
	}

	return json.Marshal(struct {
		Alias
	}{
		Alias: Alias(c),
	})
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
	EnableOldValue        bool   `json:"enable_old_value"`
	ForceReplicate        bool   `json:"force_replicate"`
	IgnoreIneligibleTable bool   `json:"ignore_ineligible_table"`
	CheckGCSafePoint      bool   `json:"check_gc_safe_point"`
	EnableSyncPoint       bool   `json:"enable_sync_point"`
	BDRMode               bool   `json:"bdr_mode"`

	SyncPointInterval  *JSONDuration `json:"sync_point_interval" swaggertype:"string"`
	SyncPointRetention *JSONDuration `json:"sync_point_retention" swaggertype:"string"`

	Filter                       *FilterConfig              `json:"filter"`
	Mounter                      *MounterConfig             `json:"mounter"`
	Sink                         *SinkConfig                `json:"sink"`
	Consistent                   *ConsistentConfig          `json:"consistent,omitempty"`
	Scheduler                    *ChangefeedSchedulerConfig `json:"scheduler"`
	Integrity                    *IntegrityConfig           `json:"integrity"`
	ChangefeedErrorStuckDuration *JSONDuration              `json:"changefeed_error_stuck_duration,omitempty"`
	SQLMode                      string                     `json:"sql_mode,omitempty"`
}

// ToInternalReplicaConfig coverts *v2.ReplicaConfig into *config.ReplicaConfig
func (c *ReplicaConfig) ToInternalReplicaConfig() *config.ReplicaConfig {
	return c.toInternalReplicaConfigWithOriginConfig(config.GetDefaultReplicaConfig())
}

// ToInternalReplicaConfigWithOriginConfig coverts *v2.ReplicaConfig into *config.ReplicaConfig
func (c *ReplicaConfig) toInternalReplicaConfigWithOriginConfig(
	res *config.ReplicaConfig,
) *config.ReplicaConfig {
	res.MemoryQuota = c.MemoryQuota
	res.CaseSensitive = c.CaseSensitive
	res.EnableOldValue = c.EnableOldValue
	res.ForceReplicate = c.ForceReplicate
	res.CheckGCSafePoint = c.CheckGCSafePoint
	res.EnableSyncPoint = c.EnableSyncPoint
	res.SQLMode = c.SQLMode
	if c.SyncPointInterval != nil {
		res.SyncPointInterval = c.SyncPointInterval.duration
	}
	if c.SyncPointRetention != nil {
		res.SyncPointRetention = c.SyncPointRetention.duration
	}
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
			Level:                 c.Consistent.Level,
			MaxLogSize:            c.Consistent.MaxLogSize,
			FlushIntervalInMs:     c.Consistent.FlushIntervalInMs,
			MetaFlushIntervalInMs: c.Consistent.MetaFlushIntervalInMs,
			EncodingWorkerNum:     c.Consistent.EncodingWorkerNum,
			FlushWorkerNum:        c.Consistent.FlushWorkerNum,
			Storage:               c.Consistent.Storage,
			UseFileBackend:        c.Consistent.UseFileBackend,
			Compression:           c.Consistent.Compression,
			FlushConcurrency:      c.Consistent.FlushConcurrency,
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
				Delimiter:            c.Sink.CSVConfig.Delimiter,
				Quote:                c.Sink.CSVConfig.Quote,
				NullString:           c.Sink.CSVConfig.NullString,
				IncludeCommitTs:      c.Sink.CSVConfig.IncludeCommitTs,
				BinaryEncodingMethod: c.Sink.CSVConfig.BinaryEncodingMethod,
			}
		}
		var kafkaConfig *config.KafkaConfig
		if c.Sink.KafkaConfig != nil {
			var codeConfig *config.CodecConfig
			if c.Sink.KafkaConfig.CodecConfig != nil {
				oldConfig := c.Sink.KafkaConfig.CodecConfig
				codeConfig = &config.CodecConfig{
					EnableTiDBExtension:            oldConfig.EnableTiDBExtension,
					MaxBatchSize:                   oldConfig.MaxBatchSize,
					AvroEnableWatermark:            oldConfig.AvroEnableWatermark,
					AvroDecimalHandlingMode:        oldConfig.AvroDecimalHandlingMode,
					AvroBigintUnsignedHandlingMode: oldConfig.AvroBigintUnsignedHandlingMode,
				}
			}

			var largeMessageHandle *config.LargeMessageHandleConfig
			if c.Sink.KafkaConfig.LargeMessageHandle != nil {
				oldConfig := c.Sink.KafkaConfig.LargeMessageHandle
				largeMessageHandle = &config.LargeMessageHandleConfig{
					LargeMessageHandleOption: oldConfig.LargeMessageHandleOption,
				}
			}

			kafkaConfig = &config.KafkaConfig{
				PartitionNum:                 c.Sink.KafkaConfig.PartitionNum,
				ReplicationFactor:            c.Sink.KafkaConfig.ReplicationFactor,
				KafkaVersion:                 c.Sink.KafkaConfig.KafkaVersion,
				MaxMessageBytes:              c.Sink.KafkaConfig.MaxMessageBytes,
				Compression:                  c.Sink.KafkaConfig.Compression,
				KafkaClientID:                c.Sink.KafkaConfig.KafkaClientID,
				AutoCreateTopic:              c.Sink.KafkaConfig.AutoCreateTopic,
				DialTimeout:                  c.Sink.KafkaConfig.DialTimeout,
				WriteTimeout:                 c.Sink.KafkaConfig.WriteTimeout,
				ReadTimeout:                  c.Sink.KafkaConfig.ReadTimeout,
				RequiredAcks:                 c.Sink.KafkaConfig.RequiredAcks,
				SASLUser:                     c.Sink.KafkaConfig.SASLUser,
				SASLPassword:                 c.Sink.KafkaConfig.SASLPassword,
				SASLMechanism:                c.Sink.KafkaConfig.SASLMechanism,
				SASLGssAPIAuthType:           c.Sink.KafkaConfig.SASLGssAPIAuthType,
				SASLGssAPIKeytabPath:         c.Sink.KafkaConfig.SASLGssAPIKeytabPath,
				SASLGssAPIKerberosConfigPath: c.Sink.KafkaConfig.SASLGssAPIKerberosConfigPath,
				SASLGssAPIServiceName:        c.Sink.KafkaConfig.SASLGssAPIServiceName,
				SASLGssAPIUser:               c.Sink.KafkaConfig.SASLGssAPIUser,
				SASLGssAPIPassword:           c.Sink.KafkaConfig.SASLGssAPIPassword,
				SASLGssAPIRealm:              c.Sink.KafkaConfig.SASLGssAPIRealm,
				SASLGssAPIDisablePafxfast:    c.Sink.KafkaConfig.SASLGssAPIDisablePafxfast,
				SASLOAuthClientID:            c.Sink.KafkaConfig.SASLOAuthClientID,
				SASLOAuthClientSecret:        c.Sink.KafkaConfig.SASLOAuthClientSecret,
				SASLOAuthTokenURL:            c.Sink.KafkaConfig.SASLOAuthTokenURL,
				SASLOAuthScopes:              c.Sink.KafkaConfig.SASLOAuthScopes,
				SASLOAuthGrantType:           c.Sink.KafkaConfig.SASLOAuthGrantType,
				SASLOAuthAudience:            c.Sink.KafkaConfig.SASLOAuthAudience,
				EnableTLS:                    c.Sink.KafkaConfig.EnableTLS,
				CA:                           c.Sink.KafkaConfig.CA,
				Cert:                         c.Sink.KafkaConfig.Cert,
				Key:                          c.Sink.KafkaConfig.Key,
				InsecureSkipVerify:           c.Sink.KafkaConfig.InsecureSkipVerify,
				CodecConfig:                  codeConfig,
				LargeMessageHandle:           largeMessageHandle,
			}
		}
		var mysqlConfig *config.MySQLConfig
		if c.Sink.MySQLConfig != nil {
			mysqlConfig = &config.MySQLConfig{
				WorkerCount:                  c.Sink.MySQLConfig.WorkerCount,
				MaxTxnRow:                    c.Sink.MySQLConfig.MaxTxnRow,
				MaxMultiUpdateRowSize:        c.Sink.MySQLConfig.MaxMultiUpdateRowSize,
				MaxMultiUpdateRowCount:       c.Sink.MySQLConfig.MaxMultiUpdateRowCount,
				TiDBTxnMode:                  c.Sink.MySQLConfig.TiDBTxnMode,
				SSLCa:                        c.Sink.MySQLConfig.SSLCa,
				SSLCert:                      c.Sink.MySQLConfig.SSLCert,
				SSLKey:                       c.Sink.MySQLConfig.SSLKey,
				TimeZone:                     c.Sink.MySQLConfig.TimeZone,
				WriteTimeout:                 c.Sink.MySQLConfig.WriteTimeout,
				ReadTimeout:                  c.Sink.MySQLConfig.ReadTimeout,
				Timeout:                      c.Sink.MySQLConfig.Timeout,
				EnableBatchDML:               c.Sink.MySQLConfig.EnableBatchDML,
				EnableMultiStatement:         c.Sink.MySQLConfig.EnableMultiStatement,
				EnableCachePreparedStatement: c.Sink.MySQLConfig.EnableCachePreparedStatement,
			}
		}
		var cloudStorageConfig *config.CloudStorageConfig
		if c.Sink.CloudStorageConfig != nil {
			cloudStorageConfig = &config.CloudStorageConfig{
				WorkerCount:         c.Sink.CloudStorageConfig.WorkerCount,
				FlushInterval:       c.Sink.CloudStorageConfig.FlushInterval,
				FileSize:            c.Sink.CloudStorageConfig.FileSize,
				OutputColumnID:      c.Sink.CloudStorageConfig.OutputColumnID,
				FileExpirationDays:  c.Sink.CloudStorageConfig.FileExpirationDays,
				FileCleanupCronSpec: c.Sink.CloudStorageConfig.FileCleanupCronSpec,
				FlushConcurrency:    c.Sink.CloudStorageConfig.FlushConcurrency,
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
			FileIndexWidth:           c.Sink.FileIndexWidth,
			EnableKafkaSinkV2:        c.Sink.EnableKafkaSinkV2,
			OnlyOutputUpdatedColumns: c.Sink.OnlyOutputUpdatedColumns,
			ContentCompatible:        c.Sink.ContentCompatible,
			KafkaConfig:              kafkaConfig,
			MySQLConfig:              mysqlConfig,
			CloudStorageConfig:       cloudStorageConfig,
			SafeMode:                 c.Sink.SafeMode,
		}
		if c.Sink.AdvanceTimeoutInSec != nil {
			res.Sink.AdvanceTimeoutInSec = util.AddressOf(*c.Sink.AdvanceTimeoutInSec)
		}
	}
	if c.Mounter != nil {
		res.Mounter = &config.MounterConfig{
			WorkerNum: c.Mounter.WorkerNum,
		}
	}
	if c.Scheduler != nil {
		res.Scheduler = &config.ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: c.Scheduler.EnableTableAcrossNodes,
			RegionThreshold:        c.Scheduler.RegionThreshold,
			WriteKeyThreshold:      c.Scheduler.WriteKeyThreshold,
		}
	}
	if c.Integrity != nil {
		res.Integrity = &integrity.Config{
			IntegrityCheckLevel:   c.Integrity.IntegrityCheckLevel,
			CorruptionHandleLevel: c.Integrity.CorruptionHandleLevel,
		}
	}
	if c.ChangefeedErrorStuckDuration != nil {
		res.ChangefeedErrorStuckDuration = &c.ChangefeedErrorStuckDuration.duration
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
		SyncPointInterval:     &JSONDuration{cloned.SyncPointInterval},
		SyncPointRetention:    &JSONDuration{cloned.SyncPointRetention},
		BDRMode:               cloned.BDRMode,
		SQLMode:               cloned.SQLMode,
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
				Delimiter:            cloned.Sink.CSVConfig.Delimiter,
				Quote:                cloned.Sink.CSVConfig.Quote,
				NullString:           cloned.Sink.CSVConfig.NullString,
				IncludeCommitTs:      cloned.Sink.CSVConfig.IncludeCommitTs,
				BinaryEncodingMethod: cloned.Sink.CSVConfig.BinaryEncodingMethod,
			}
		}
		var kafkaConfig *KafkaConfig
		if cloned.Sink.KafkaConfig != nil {
			var codeConfig *CodecConfig
			if cloned.Sink.KafkaConfig.CodecConfig != nil {
				oldConfig := cloned.Sink.KafkaConfig.CodecConfig
				codeConfig = &CodecConfig{
					EnableTiDBExtension:            oldConfig.EnableTiDBExtension,
					MaxBatchSize:                   oldConfig.MaxBatchSize,
					AvroEnableWatermark:            oldConfig.AvroEnableWatermark,
					AvroDecimalHandlingMode:        oldConfig.AvroDecimalHandlingMode,
					AvroBigintUnsignedHandlingMode: oldConfig.AvroBigintUnsignedHandlingMode,
				}
			}

			var largeMessageHandle *LargeMessageHandleConfig
			if cloned.Sink.KafkaConfig.LargeMessageHandle != nil {
				oldConfig := cloned.Sink.KafkaConfig.LargeMessageHandle
				largeMessageHandle = &LargeMessageHandleConfig{
					LargeMessageHandleOption: oldConfig.LargeMessageHandleOption,
				}
			}

			kafkaConfig = &KafkaConfig{
				PartitionNum:                 cloned.Sink.KafkaConfig.PartitionNum,
				ReplicationFactor:            cloned.Sink.KafkaConfig.ReplicationFactor,
				KafkaVersion:                 cloned.Sink.KafkaConfig.KafkaVersion,
				MaxMessageBytes:              cloned.Sink.KafkaConfig.MaxMessageBytes,
				Compression:                  cloned.Sink.KafkaConfig.Compression,
				KafkaClientID:                cloned.Sink.KafkaConfig.KafkaClientID,
				AutoCreateTopic:              cloned.Sink.KafkaConfig.AutoCreateTopic,
				DialTimeout:                  cloned.Sink.KafkaConfig.DialTimeout,
				WriteTimeout:                 cloned.Sink.KafkaConfig.WriteTimeout,
				ReadTimeout:                  cloned.Sink.KafkaConfig.ReadTimeout,
				RequiredAcks:                 cloned.Sink.KafkaConfig.RequiredAcks,
				SASLUser:                     cloned.Sink.KafkaConfig.SASLUser,
				SASLPassword:                 cloned.Sink.KafkaConfig.SASLPassword,
				SASLMechanism:                cloned.Sink.KafkaConfig.SASLMechanism,
				SASLGssAPIAuthType:           cloned.Sink.KafkaConfig.SASLGssAPIAuthType,
				SASLGssAPIKeytabPath:         cloned.Sink.KafkaConfig.SASLGssAPIKeytabPath,
				SASLGssAPIKerberosConfigPath: cloned.Sink.KafkaConfig.SASLGssAPIKerberosConfigPath,
				SASLGssAPIServiceName:        cloned.Sink.KafkaConfig.SASLGssAPIServiceName,
				SASLGssAPIUser:               cloned.Sink.KafkaConfig.SASLGssAPIUser,
				SASLGssAPIPassword:           cloned.Sink.KafkaConfig.SASLGssAPIPassword,
				SASLGssAPIRealm:              cloned.Sink.KafkaConfig.SASLGssAPIRealm,
				SASLGssAPIDisablePafxfast:    cloned.Sink.KafkaConfig.SASLGssAPIDisablePafxfast,
				SASLOAuthClientID:            cloned.Sink.KafkaConfig.SASLOAuthClientID,
				SASLOAuthClientSecret:        cloned.Sink.KafkaConfig.SASLOAuthClientSecret,
				SASLOAuthTokenURL:            cloned.Sink.KafkaConfig.SASLOAuthTokenURL,
				SASLOAuthScopes:              cloned.Sink.KafkaConfig.SASLOAuthScopes,
				SASLOAuthGrantType:           cloned.Sink.KafkaConfig.SASLOAuthGrantType,
				SASLOAuthAudience:            cloned.Sink.KafkaConfig.SASLOAuthAudience,
				EnableTLS:                    cloned.Sink.KafkaConfig.EnableTLS,
				CA:                           cloned.Sink.KafkaConfig.CA,
				Cert:                         cloned.Sink.KafkaConfig.Cert,
				Key:                          cloned.Sink.KafkaConfig.Key,
				InsecureSkipVerify:           cloned.Sink.KafkaConfig.InsecureSkipVerify,
				CodecConfig:                  codeConfig,
				LargeMessageHandle:           largeMessageHandle,
			}
		}
		var mysqlConfig *MySQLConfig
		if cloned.Sink.MySQLConfig != nil {
			mysqlConfig = &MySQLConfig{
				WorkerCount:                  cloned.Sink.MySQLConfig.WorkerCount,
				MaxTxnRow:                    cloned.Sink.MySQLConfig.MaxTxnRow,
				MaxMultiUpdateRowSize:        cloned.Sink.MySQLConfig.MaxMultiUpdateRowSize,
				MaxMultiUpdateRowCount:       cloned.Sink.MySQLConfig.MaxMultiUpdateRowCount,
				TiDBTxnMode:                  cloned.Sink.MySQLConfig.TiDBTxnMode,
				SSLCa:                        cloned.Sink.MySQLConfig.SSLCa,
				SSLCert:                      cloned.Sink.MySQLConfig.SSLCert,
				SSLKey:                       cloned.Sink.MySQLConfig.SSLKey,
				TimeZone:                     cloned.Sink.MySQLConfig.TimeZone,
				WriteTimeout:                 cloned.Sink.MySQLConfig.WriteTimeout,
				ReadTimeout:                  cloned.Sink.MySQLConfig.ReadTimeout,
				Timeout:                      cloned.Sink.MySQLConfig.Timeout,
				EnableBatchDML:               cloned.Sink.MySQLConfig.EnableBatchDML,
				EnableMultiStatement:         cloned.Sink.MySQLConfig.EnableMultiStatement,
				EnableCachePreparedStatement: cloned.Sink.MySQLConfig.EnableCachePreparedStatement,
			}
		}
		var cloudStorageConfig *CloudStorageConfig
		if cloned.Sink.CloudStorageConfig != nil {
			cloudStorageConfig = &CloudStorageConfig{
				WorkerCount:         cloned.Sink.CloudStorageConfig.WorkerCount,
				FlushInterval:       cloned.Sink.CloudStorageConfig.FlushInterval,
				FileSize:            cloned.Sink.CloudStorageConfig.FileSize,
				OutputColumnID:      cloned.Sink.CloudStorageConfig.OutputColumnID,
				FileExpirationDays:  cloned.Sink.CloudStorageConfig.FileExpirationDays,
				FileCleanupCronSpec: cloned.Sink.CloudStorageConfig.FileCleanupCronSpec,
				FlushConcurrency:    cloned.Sink.CloudStorageConfig.FlushConcurrency,
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
			FileIndexWidth:           cloned.Sink.FileIndexWidth,
			EnableKafkaSinkV2:        cloned.Sink.EnableKafkaSinkV2,
			OnlyOutputUpdatedColumns: cloned.Sink.OnlyOutputUpdatedColumns,
			ContentCompatible:        cloned.Sink.ContentCompatible,
			KafkaConfig:              kafkaConfig,
			MySQLConfig:              mysqlConfig,
			CloudStorageConfig:       cloudStorageConfig,
			SafeMode:                 cloned.Sink.SafeMode,
		}
		if cloned.Sink.AdvanceTimeoutInSec != nil {
			res.Sink.AdvanceTimeoutInSec = util.AddressOf(*cloned.Sink.AdvanceTimeoutInSec)
		}
	}
	if cloned.Consistent != nil {
		res.Consistent = &ConsistentConfig{
			Level:                 cloned.Consistent.Level,
			MaxLogSize:            cloned.Consistent.MaxLogSize,
			FlushIntervalInMs:     cloned.Consistent.FlushIntervalInMs,
			MetaFlushIntervalInMs: cloned.Consistent.MetaFlushIntervalInMs,
			EncodingWorkerNum:     c.Consistent.EncodingWorkerNum,
			FlushWorkerNum:        c.Consistent.FlushWorkerNum,
			Storage:               cloned.Consistent.Storage,
			UseFileBackend:        cloned.Consistent.UseFileBackend,
			Compression:           cloned.Consistent.Compression,
			FlushConcurrency:      cloned.Consistent.FlushConcurrency,
		}
	}
	if cloned.Mounter != nil {
		res.Mounter = &MounterConfig{
			WorkerNum: cloned.Mounter.WorkerNum,
		}
	}
	if cloned.Scheduler != nil {
		res.Scheduler = &ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: cloned.Scheduler.EnableTableAcrossNodes,
			RegionThreshold:        cloned.Scheduler.RegionThreshold,
			WriteKeyThreshold:      cloned.Scheduler.WriteKeyThreshold,
		}
	}

	if cloned.Integrity != nil {
		res.Integrity = &IntegrityConfig{
			IntegrityCheckLevel:   cloned.Integrity.IntegrityCheckLevel,
			CorruptionHandleLevel: cloned.Integrity.CorruptionHandleLevel,
		}
	}
	if cloned.ChangefeedErrorStuckDuration != nil {
		res.ChangefeedErrorStuckDuration = &JSONDuration{*cloned.ChangefeedErrorStuckDuration}
	}
	return res
}

// GetDefaultReplicaConfig returns a default ReplicaConfig
func GetDefaultReplicaConfig() *ReplicaConfig {
	return ToAPIReplicaConfig(config.GetDefaultReplicaConfig())
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
	Protocol                 string              `json:"protocol"`
	SchemaRegistry           string              `json:"schema_registry"`
	CSVConfig                *CSVConfig          `json:"csv"`
	DispatchRules            []*DispatchRule     `json:"dispatchers,omitempty"`
	ColumnSelectors          []*ColumnSelector   `json:"column_selectors"`
	TxnAtomicity             string              `json:"transaction_atomicity"`
	EncoderConcurrency       int                 `json:"encoder_concurrency"`
	Terminator               string              `json:"terminator"`
	DateSeparator            string              `json:"date_separator"`
	EnablePartitionSeparator bool                `json:"enable_partition_separator"`
	FileIndexWidth           int                 `json:"file_index_digit"`
	EnableKafkaSinkV2        bool                `json:"enable_kafka_sink_v2"`
	OnlyOutputUpdatedColumns *bool               `json:"only_output_updated_columns"`
	SafeMode                 *bool               `json:"safe_mode,omitempty"`
	ContentCompatible        *bool               `json:"content_compatible"`
	KafkaConfig              *KafkaConfig        `json:"kafka_config,omitempty"`
	MySQLConfig              *MySQLConfig        `json:"mysql_config,omitempty"`
	CloudStorageConfig       *CloudStorageConfig `json:"cloud_storage_config,omitempty"`
	AdvanceTimeoutInSec      *uint               `json:"advance_timeout,omitempty"`
}

// CSVConfig denotes the csv config
// This is the same as config.CSVConfig
type CSVConfig struct {
	Delimiter            string `json:"delimiter"`
	Quote                string `json:"quote"`
	NullString           string `json:"null"`
	IncludeCommitTs      bool   `json:"include_commit_ts"`
	BinaryEncodingMethod string `json:"binary_encoding_method"`
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
	Level                 string `json:"level,omitempty"`
	MaxLogSize            int64  `json:"max_log_size"`
	FlushIntervalInMs     int64  `json:"flush_interval"`
	MetaFlushIntervalInMs int64  `json:"meta_flush_interval"`
	EncodingWorkerNum     int    `json:"encoding_worker_num"`
	FlushWorkerNum        int    `json:"flush_worker_num"`
	Storage               string `json:"storage,omitempty"`
	UseFileBackend        bool   `json:"use_file_backend"`
	Compression           string `json:"compression,omitempty"`
	FlushConcurrency      int    `json:"flush_concurrency,omitempty"`
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
// This is a duplicate of Integrity.Config
type IntegrityConfig struct {
	IntegrityCheckLevel   string `json:"integrity_check_level"`
	CorruptionHandleLevel string `json:"corruption_handle_level"`
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
	Config         *ReplicaConfig     `json:"config,omitempty"`
	State          model.FeedState    `json:"state,omitempty"`
	Error          *RunningError      `json:"error,omitempty"`
	CreatorVersion string             `json:"creator_version,omitempty"`

	ResolvedTs     uint64                    `json:"resolved_ts"`
	CheckpointTs   uint64                    `json:"checkpoint_ts"`
	CheckpointTime model.JSONTime            `json:"checkpoint_time"`
	TaskStatus     []model.CaptureTaskStatus `json:"task_status,omitempty"`
}

// RunningError represents some running error from cdc components,
// such as processor.
type RunningError struct {
	Time    *time.Time `json:"time,omitempty"`
	Addr    string     `json:"addr"`
	Code    string     `json:"code"`
	Message string     `json:"message"`
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

// CodecConfig represents a MQ codec configuration
type CodecConfig struct {
	EnableTiDBExtension            *bool   `json:"enable_tidb_extension,omitempty"`
	MaxBatchSize                   *int    `json:"max_batch_size,omitempty"`
	AvroEnableWatermark            *bool   `json:"avro_enable_watermark"`
	AvroDecimalHandlingMode        *string `json:"avro_decimal_handling_mode,omitempty"`
	AvroBigintUnsignedHandlingMode *string `json:"avro_bigint_unsigned_handling_mode,omitempty"`
}

// KafkaConfig represents a kafka sink configuration
type KafkaConfig struct {
	PartitionNum                 *int32       `json:"partition_num,omitempty"`
	ReplicationFactor            *int16       `json:"replication_factor,omitempty"`
	KafkaVersion                 *string      `json:"kafka_version,omitempty"`
	MaxMessageBytes              *int         `json:"max_message_bytes,omitempty"`
	Compression                  *string      `json:"compression,omitempty"`
	KafkaClientID                *string      `json:"kafka_client_id,omitempty"`
	AutoCreateTopic              *bool        `json:"auto_create_topic,omitempty"`
	DialTimeout                  *string      `json:"dial_timeout,omitempty"`
	WriteTimeout                 *string      `json:"write_timeout,omitempty"`
	ReadTimeout                  *string      `json:"read_timeout,omitempty"`
	RequiredAcks                 *int         `json:"required_acks,omitempty"`
	SASLUser                     *string      `json:"sasl_user,omitempty"`
	SASLPassword                 *string      `json:"sasl_password,omitempty"`
	SASLMechanism                *string      `json:"sasl_mechanism,omitempty"`
	SASLGssAPIAuthType           *string      `json:"sasl_gssapi_auth_type,omitempty"`
	SASLGssAPIKeytabPath         *string      `json:"sasl_gssapi_keytab_path,omitempty"`
	SASLGssAPIKerberosConfigPath *string      `json:"sasl_gssapi_kerberos_config_path,omitempty"`
	SASLGssAPIServiceName        *string      `json:"sasl_gssapi_service_name,omitempty"`
	SASLGssAPIUser               *string      `json:"sasl_gssapi_user,omitempty"`
	SASLGssAPIPassword           *string      `json:"sasl_gssapi_password,omitempty"`
	SASLGssAPIRealm              *string      `json:"sasl_gssapi_realm,omitempty"`
	SASLGssAPIDisablePafxfast    *bool        `json:"sasl_gssapi_disable_pafxfast,omitempty"`
	SASLOAuthClientID            *string      `json:"sasl_oauth_client_id,omitempty"`
	SASLOAuthClientSecret        *string      `json:"sasl_oauth_client_secret,omitempty"`
	SASLOAuthTokenURL            *string      `json:"sasl_oauth_token_url,omitempty"`
	SASLOAuthScopes              []string     `json:"sasl_oauth_scopes,omitempty"`
	SASLOAuthGrantType           *string      `json:"sasl_oauth_grant_type,omitempty"`
	SASLOAuthAudience            *string      `json:"sasl_oauth_audience,omitempty"`
	EnableTLS                    *bool        `json:"enable_tls,omitempty"`
	CA                           *string      `json:"ca,omitempty"`
	Cert                         *string      `json:"cert,omitempty"`
	Key                          *string      `json:"key,omitempty"`
	InsecureSkipVerify           *bool        `json:"insecure_skip_verify,omitempty"`
	CodecConfig                  *CodecConfig `json:"codec_config,omitempty"`

	LargeMessageHandle *LargeMessageHandleConfig `json:"large_message_handle,omitempty"`
}

// MySQLConfig represents a MySQL sink configuration
type MySQLConfig struct {
	WorkerCount                  *int    `json:"worker_count,omitempty"`
	MaxTxnRow                    *int    `json:"max_txn_row,omitempty"`
	MaxMultiUpdateRowSize        *int    `json:"max_multi_update_row_size,omitempty"`
	MaxMultiUpdateRowCount       *int    `json:"max_multi_update_row_count,omitempty"`
	TiDBTxnMode                  *string `json:"tidb_txn_mode,omitempty"`
	SSLCa                        *string `json:"ssl_ca,omitempty"`
	SSLCert                      *string `json:"ssl_cert,omitempty"`
	SSLKey                       *string `json:"ssl_key,omitempty"`
	TimeZone                     *string `json:"time_zone,omitempty"`
	WriteTimeout                 *string `json:"write_timeout,omitempty"`
	ReadTimeout                  *string `json:"read_timeout,omitempty"`
	Timeout                      *string `json:"timeout,omitempty"`
	EnableBatchDML               *bool   `json:"enable_batch_dml,omitempty"`
	EnableMultiStatement         *bool   `json:"enable_multi_statement,omitempty"`
	EnableCachePreparedStatement *bool   `json:"enable_cache_prepared_statement,omitempty"`
}

// CloudStorageConfig represents a cloud storage sink configuration
type CloudStorageConfig struct {
	WorkerCount         *int    `json:"worker_count,omitempty"`
	FlushInterval       *string `json:"flush_interval,omitempty"`
	FileSize            *int    `json:"file_size,omitempty"`
	OutputColumnID      *bool   `json:"output_column_id,omitempty"`
	FileExpirationDays  *int    `json:"file_expiration_days,omitempty"`
	FileCleanupCronSpec *string `json:"file_cleanup_cron_spec,omitempty"`
	FlushConcurrency    *int    `json:"flush_concurrency,omitempty"`
}

// ChangefeedStatus holds common information of a changefeed in cdc
type ChangefeedStatus struct {
	State        string        `json:"state,omitempty"`
	ResolvedTs   uint64        `json:"resolved_ts"`
	CheckpointTs uint64        `json:"checkpoint_ts"`
	LastError    *RunningError `json:"last_error,omitempty"`
	LastWarning  *RunningError `json:"last_warning,omitempty"`
}

// LargeMessageHandleConfig denotes the large message handling config
// This is the same as config.LargeMessageHandleConfig
type LargeMessageHandleConfig struct {
	LargeMessageHandleOption string `json:"large_message_handle_option"`
}
