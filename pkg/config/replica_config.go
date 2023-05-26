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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config/outdated"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
<<<<<<< HEAD
=======
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/util"
>>>>>>> c601a1adb6 (pkg/config(ticdc): hide fields that are not required for specific protocols (#8836))
	"go.uber.org/zap"
)

const (
	// minSyncPointInterval is the minimum of SyncPointInterval can be set.
	minSyncPointInterval = time.Second * 30
	// minSyncPointRetention is the minimum of SyncPointRetention can be set.
	minSyncPointRetention = time.Hour * 1
)

var defaultReplicaConfig = &ReplicaConfig{
	MemoryQuota:        DefaultChangefeedMemoryQuota,
	CaseSensitive:      true,
	EnableOldValue:     true,
	CheckGCSafePoint:   true,
	EnableSyncPoint:    util.AddressOf(false),
	SyncPointInterval:  util.AddressOf(10 * time.Minute),
	SyncPointRetention: util.AddressOf(24 * time.Hour),
	BDRMode:            util.AddressOf(false),
	Filter: &FilterConfig{
		Rules: []string{"*.*"},
	},
	Mounter: &MounterConfig{
		WorkerNum: 16,
	},
	Sink: &SinkConfig{
		CSVConfig: &CSVConfig{
			Quote:      string(DoubleQuoteChar),
			Delimiter:  Comma,
			NullString: NULL,
		},
<<<<<<< HEAD
		EncoderConcurrency:       16,
		Terminator:               CRLF,
		DateSeparator:            DateSeparatorDay.String(),
		EnablePartitionSeparator: true,
=======
		EncoderConcurrency:       util.AddressOf(16),
		Terminator:               util.AddressOf(CRLF),
		DateSeparator:            util.AddressOf(DateSeparatorNone.String()),
		EnablePartitionSeparator: util.AddressOf(true),
		EnableKafkaSinkV2:        util.AddressOf(false),
		OnlyOutputUpdatedColumns: util.AddressOf(false),
>>>>>>> c601a1adb6 (pkg/config(ticdc): hide fields that are not required for specific protocols (#8836))
		TiDBSourceID:             1,
	},
	Consistent: &ConsistentConfig{
		Level:             "none",
		MaxLogSize:        redo.DefaultMaxLogSize,
		FlushIntervalInMs: redo.DefaultFlushIntervalInMs,
		Storage:           "",
		UseFileBackend:    false,
	},
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
<<<<<<< HEAD
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
=======
	// EnableSyncPoint is only available when the downstream is a Database.
	EnableSyncPoint *bool `toml:"enable-sync-point" json:"enable-sync-point,omitempty"`
	// IgnoreIneligibleTable is used to store the user's config when creating a changefeed.
	// not used in the changefeed's lifecycle.
	IgnoreIneligibleTable bool `toml:"ignore-ineligible-table" json:"ignore-ineligible-table"`

	// BDR(Bidirectional Replication) is a feature that allows users to
	// replicate data of same tables from TiDB-1 to TiDB-2 and vice versa.
	// This feature is only available for TiDB.
	BDRMode *bool `toml:"bdr-mode" json:"bdr-mode,omitempty"`
	// SyncPointInterval is only available when the downstream is DB.
	SyncPointInterval *time.Duration `toml:"sync-point-interval" json:"sync-point-interval,omitempty"`
	// SyncPointRetention is only available when the downstream is DB.
	SyncPointRetention *time.Duration `toml:"sync-point-retention" json:"sync-point-retention,omitempty"`
	Filter             *FilterConfig  `toml:"filter" json:"filter"`
	Mounter            *MounterConfig `toml:"mounter" json:"mounter"`
	Sink               *SinkConfig    `toml:"sink" json:"sink"`
	// Consistent is only available for DB downstream with redo feature enabled.
	Consistent *ConsistentConfig `toml:"consistent" json:"consistent,omitempty"`
	// Scheduler is the configuration for scheduler.
	Scheduler *ChangefeedSchedulerConfig `toml:"scheduler" json:"scheduler"`
	// Integrity is only available when the downstream is MQ.
	Integrity *integrity.Config `toml:"integrity" json:"integrity"`
>>>>>>> c601a1adb6 (pkg/config(ticdc): hide fields that are not required for specific protocols (#8836))
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
func (c *ReplicaConfig) ValidateAndAdjust(sinkURI *url.URL) error {
	// check sink uri
	if c.Sink != nil {
		err := c.Sink.validateAndAdjust(sinkURI, c.EnableOldValue)
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
	if util.GetOrZero(c.EnableSyncPoint) {
		if c.SyncPointInterval != nil &&
			*c.SyncPointInterval < minSyncPointInterval {
			return cerror.ErrInvalidReplicaConfig.
				FastGenByArgs(
					fmt.Sprintf("The SyncPointInterval:%s must be larger than %s",
						c.SyncPointInterval.String(),
						minSyncPointInterval.String()))
		}
		if c.SyncPointRetention != nil &&
			*c.SyncPointRetention < minSyncPointRetention {
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

	return nil
}

// FixMemoryQuota adjusts memory quota to default value
func (c *ReplicaConfig) FixMemoryQuota() {
	c.MemoryQuota = DefaultChangefeedMemoryQuota
}

// GetSinkURIAndAdjustConfigWithSinkURI parses sinkURI as a URI and adjust config with sinkURI.
func GetSinkURIAndAdjustConfigWithSinkURI(
	sinkURIStr string,
	config *ReplicaConfig,
) (*url.URL, error) {
	// parse sinkURI as a URI
	sinkURI, err := url.Parse(sinkURIStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	if err := config.ValidateAndAdjust(sinkURI); err != nil {
		return nil, err
	}

	return sinkURI, nil
}
