// Copyright 2021 PingCAP, Inc.
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
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/pkg/config/outdated"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/integrity"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

const (
	// minSyncPointInterval is the minimum of SyncPointInterval can be set.
	minSyncPointInterval = time.Second * 30
	// minSyncPointRetention is the minimum of SyncPointRetention can be set.
	minSyncPointRetention           = time.Hour * 1
	minChangeFeedErrorStuckDuration = time.Minute * 30
	// The default SQL Mode of TiDB: "ONLY_FULL_GROUP_BY,
	// STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,
	// NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
	// Note: The SQL Mode of TiDB is not the same as ORACLE.
	// If you want to use the same SQL Mode as ORACLE, you need to add "ORACLE" to the SQL Mode.
	defaultSQLMode = mysql.DefaultSQLMode
)

var defaultReplicaConfig = &ReplicaConfig{
	MemoryQuota:        DefaultChangefeedMemoryQuota,
	CaseSensitive:      false,
	EnableOldValue:     true,
	CheckGCSafePoint:   true,
	EnableSyncPoint:    false,
	SyncPointInterval:  time.Minute * 10,
	SyncPointRetention: time.Hour * 24,
	Filter: &FilterConfig{
		Rules: []string{"*.*"},
	},
	Mounter: &MounterConfig{
		WorkerNum: 16,
	},
	Sink: &SinkConfig{
		CSVConfig: &CSVConfig{
			Quote:                string(DoubleQuoteChar),
			Delimiter:            Comma,
			NullString:           NULL,
			BinaryEncodingMethod: BinaryEncodingBase64,
		},
		EncoderConcurrency:       16,
		Terminator:               CRLF,
		DateSeparator:            DateSeparatorDay.String(),
		EnablePartitionSeparator: true,
		EnableKafkaSinkV2:        false,
		TiDBSourceID:             1,
		AdvanceTimeoutInSec:      util.AddressOf(DefaultAdvanceTimeoutInSec),
	},
	Consistent: &ConsistentConfig{
		Level:                 "none",
		MaxLogSize:            redo.DefaultMaxLogSize,
		FlushIntervalInMs:     redo.DefaultFlushIntervalInMs,
		MetaFlushIntervalInMs: redo.DefaultMetaFlushIntervalInMs,
		EncodingWorkerNum:     redo.DefaultEncodingWorkerNum,
		FlushWorkerNum:        redo.DefaultFlushWorkerNum,
		Storage:               "",
		UseFileBackend:        false,
		Compression:           "",
	},
	Scheduler: &ChangefeedSchedulerConfig{
		EnableTableAcrossNodes: false,
		RegionThreshold:        100_000,
		WriteKeyThreshold:      0,
	},
	Integrity: &integrity.Config{
		IntegrityCheckLevel:   integrity.CheckLevelNone,
		CorruptionHandleLevel: integrity.CorruptionHandleLevelWarn,
	},
	ChangefeedErrorStuckDuration: util.AddressOf(time.Minute * 30),
	SQLMode:                      defaultSQLMode,
}

// GetDefaultReplicaConfig returns the default replica config.
func GetDefaultReplicaConfig() *ReplicaConfig {
	return defaultReplicaConfig.Clone()
}

// Duration wrap time.Duration to override UnmarshalText func
type Duration struct {
	time.Duration
}

// UnmarshalText unmarshal byte to duration
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// ReplicaConfig represents some addition replication config for a changefeed
type ReplicaConfig replicaConfig

type replicaConfig struct {
	MemoryQuota      uint64 `toml:"memory-quota" json:"memory-quota"`
	CaseSensitive    bool   `toml:"case-sensitive" json:"case-sensitive"`
	EnableOldValue   bool   `toml:"enable-old-value" json:"enable-old-value"`
	ForceReplicate   bool   `toml:"force-replicate" json:"force-replicate"`
	CheckGCSafePoint bool   `toml:"check-gc-safe-point" json:"check-gc-safe-point"`
	EnableSyncPoint  bool   `toml:"enable-sync-point" json:"enable-sync-point"`
	// BDR(Bidirectional Replication) is a feature that allows users to
	// replicate data of same tables from TiDB-1 to TiDB-2 and vice versa.
	// This feature is only available for TiDB.
	BDRMode            bool              `toml:"bdr-mode" json:"bdr-mode"`
	SyncPointInterval  time.Duration     `toml:"sync-point-interval" json:"sync-point-interval"`
	SyncPointRetention time.Duration     `toml:"sync-point-retention" json:"sync-point-retention"`
	Filter             *FilterConfig     `toml:"filter" json:"filter"`
	Mounter            *MounterConfig    `toml:"mounter" json:"mounter"`
	Sink               *SinkConfig       `toml:"sink" json:"sink"`
	Consistent         *ConsistentConfig `toml:"consistent" json:"consistent"`
	// Scheduler is the configuration for scheduler.
	Scheduler                    *ChangefeedSchedulerConfig `toml:"scheduler" json:"scheduler"`
	Integrity                    *integrity.Config          `toml:"integrity" json:"integrity"`
	ChangefeedErrorStuckDuration *time.Duration             `toml:"changefeed-error-stuck-duration" json:"changefeed-error-stuck-duration,omitempty"`
	SQLMode                      string                     `toml:"sql-mode" json:"sql-mode"`
}

// Marshal returns the json marshal format of a ReplicationConfig
func (c *ReplicaConfig) Marshal() (string, error) {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrEncodeFailed, errors.Annotatef(err, "Unmarshal data: %v", c))
	}
	return string(cfg), nil
}

// UnmarshalJSON unmarshals into *ReplicationConfig from json marshal byte slice
func (c *ReplicaConfig) UnmarshalJSON(data []byte) error {
	// The purpose of casting ReplicaConfig to replicaConfig is to avoid recursive calls UnmarshalJSON,
	// resulting in stack overflow
	r := (*replicaConfig)(c)
	err := json.Unmarshal(data, &r)
	if err != nil {
		return cerror.WrapError(cerror.ErrDecodeFailed, err)
	}
	v1 := outdated.ReplicaConfigV1{}
	err = v1.Unmarshal(data)
	if err != nil {
		return cerror.WrapError(cerror.ErrDecodeFailed, err)
	}
	r.fillFromV1(&v1)
	return nil
}

// Clone clones a replication
func (c *ReplicaConfig) Clone() *ReplicaConfig {
	str, err := c.Marshal()
	if err != nil {
		log.Panic("failed to marshal replica config",
			zap.Error(cerror.WrapError(cerror.ErrDecodeFailed, err)))
	}
	clone := new(ReplicaConfig)
	err = clone.UnmarshalJSON([]byte(str))
	if err != nil {
		log.Panic("failed to unmarshal replica config",
			zap.Error(cerror.WrapError(cerror.ErrDecodeFailed, err)))
	}
	return clone
}

func (c *replicaConfig) fillFromV1(v1 *outdated.ReplicaConfigV1) {
	if v1 == nil || v1.Sink == nil {
		return
	}
	for _, dispatch := range v1.Sink.DispatchRules {
		c.Sink.DispatchRules = append(c.Sink.DispatchRules, &DispatchRule{
			Matcher:        []string{fmt.Sprintf("%s.%s", dispatch.Schema, dispatch.Name)},
			DispatcherRule: dispatch.Rule,
		})
	}
}

// ValidateAndAdjust verifies and adjusts the replica configuration.
func (c *ReplicaConfig) ValidateAndAdjust(sinkURI *url.URL) error { // check sink uri
	if c.Sink != nil {
		err := c.Sink.validateAndAdjust(sinkURI)
		if err != nil {
			return err
		}

		err = c.AdjustEnableOldValueAndVerifyForceReplicate(sinkURI)
		if err != nil {
			return err
		}
	}

	if c.Consistent != nil {
		err := c.Consistent.ValidateAndAdjust()
		if err != nil {
			return err
		}
	}

	// check sync point config
	if c.EnableSyncPoint {
		if c.SyncPointInterval < minSyncPointInterval {
			return cerror.ErrInvalidReplicaConfig.
				FastGenByArgs(
					fmt.Sprintf("The SyncPointInterval:%s must be larger than %s",
						c.SyncPointInterval.String(),
						minSyncPointInterval.String()))
		}
		if c.SyncPointRetention < minSyncPointRetention {
			return cerror.ErrInvalidReplicaConfig.
				FastGenByArgs(
					fmt.Sprintf("The SyncPointRetention:%s must be larger than %s",
						c.SyncPointRetention.String(),
						minSyncPointRetention.String()))
		}
	}
	if c.MemoryQuota == uint64(0) {
		c.FixMemoryQuota()
	}
	if c.Scheduler == nil {
		c.FixScheduler(false)
	} else {
		err := c.Scheduler.Validate()
		if err != nil {
			return err
		}
	}
	// TODO: Remove the hack once span replication is compatible with all sinks.
	if !isSinkCompatibleWithSpanReplication(sinkURI) {
		c.Scheduler.EnableTableAcrossNodes = false
	}

	if c.Integrity != nil {
		switch strings.ToLower(sinkURI.Scheme) {
		case sink.KafkaScheme, sink.KafkaSSLScheme:
		default:
			if c.Integrity.Enabled() {
				log.Warn("integrity checksum only support kafka sink now, disable integrity")
				c.Integrity.IntegrityCheckLevel = integrity.CheckLevelNone
			}
		}

		if err := c.Integrity.Validate(); err != nil {
			return err
		}
	}

	if c.ChangefeedErrorStuckDuration != nil &&
		*c.ChangefeedErrorStuckDuration < minChangeFeedErrorStuckDuration {
		return cerror.ErrInvalidReplicaConfig.
			FastGenByArgs(
				fmt.Sprintf("The ChangefeedErrorStuckDuration:%f must be larger than %f Seconds",
					c.ChangefeedErrorStuckDuration.Seconds(),
					minChangeFeedErrorStuckDuration.Seconds()))
	}

	return nil
}

// FixScheduler adjusts scheduler to default value
func (c *ReplicaConfig) FixScheduler(inheritV66 bool) {
	if c.Scheduler == nil {
		c.Scheduler = defaultReplicaConfig.Clone().Scheduler
		return
	}
	if inheritV66 && c.Scheduler.RegionPerSpan != 0 {
		c.Scheduler.EnableTableAcrossNodes = true
		c.Scheduler.RegionThreshold = c.Scheduler.RegionPerSpan
		c.Scheduler.RegionPerSpan = 0
	}
}

// FixMemoryQuota adjusts memory quota to default value
func (c *ReplicaConfig) FixMemoryQuota() {
	c.MemoryQuota = DefaultChangefeedMemoryQuota
}

// isSinkCompatibleWithSpanReplication returns true if the sink uri is
// compatible with span replication.
func isSinkCompatibleWithSpanReplication(u *url.URL) bool {
	return u != nil &&
		(strings.Contains(u.Scheme, "kafka") || strings.Contains(u.Scheme, "blackhole"))
}

// AdjustEnableOldValue adjust the old value configuration by the sink scheme and encoding protocol
func (c *ReplicaConfig) AdjustEnableOldValue(scheme, protocol string) {
	if sink.IsMySQLCompatibleScheme(scheme) {
		return
	}

	if c.EnableOldValue {
		_, ok := ForceDisableOldValueProtocols[protocol]
		if ok {
			log.Warn("Attempting to replicate with old value enabled, but the specified protocol must disable old value. "+
				"CDC will disable old value and continue.", zap.String("protocol", protocol))
			c.EnableOldValue = false
		}
		return
	}

	_, ok := ForceEnableOldValueProtocols[protocol]
	if ok {
		log.Warn("Attempting to replicate with old value disabled, but the specified protocol must enable old value. "+
			"CDC will enable old value and continue.", zap.String("protocol", protocol))
		c.EnableOldValue = true
	}
}

// AdjustEnableOldValueAndVerifyForceReplicate adjust the old value configuration by the sink scheme and encoding protocol
// and then verify the force replicate.
func (c *ReplicaConfig) AdjustEnableOldValueAndVerifyForceReplicate(sinkURI *url.URL) error {
	scheme := strings.ToLower(sinkURI.Scheme)
	protocol := sinkURI.Query().Get(ProtocolKey)
	if protocol != "" {
		c.Sink.Protocol = protocol
	}
	c.AdjustEnableOldValue(scheme, c.Sink.Protocol)

	if !c.ForceReplicate {
		return nil
	}

	// MySQL Sink require the old value feature must be enabled to allow delete event send to downstream.
	if sink.IsMySQLCompatibleScheme(scheme) {
		if !c.EnableOldValue {
			log.Error("force replicate, old value feature is disabled for the changefeed using mysql sink")
			return cerror.ErrIncompatibleConfig.GenWithStackByArgs()
		}
	}

	return nil
}

// MaskSensitiveData masks sensitive data in ReplicaConfig
func (c *ReplicaConfig) MaskSensitiveData() {
	if c.Sink != nil {
		c.Sink.MaskSensitiveData()
	}
	if c.Consistent != nil {
		c.Consistent.MaskSensitiveData()
	}
}
