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

	"github.com/pingcap/tiflow/pkg/config/outdated"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

var defaultReplicaConfig = &ReplicaConfig{
	CaseSensitive:    true,
	EnableOldValue:   true,
	CheckGCSafePoint: true,
	Filter: &FilterConfig{
		Rules: []string{"*.*"},
	},
	Mounter: &MounterConfig{
		WorkerNum: 16,
	},
	Sink: &SinkConfig{
		EncoderConcurrency: 16,
	},
	Cyclic: &CyclicConfig{
		Enable: false,
	},
	Consistent: &ConsistentConfig{
		Level:             "none",
		MaxLogSize:        64,
		FlushIntervalInMs: DefaultFlushIntervalInMs,
		Storage:           "",
	},
}

// ReplicaConfig represents some addition replication config for a changefeed
type ReplicaConfig replicaConfig

type replicaConfig struct {
	CaseSensitive    bool              `toml:"case-sensitive" json:"case-sensitive"`
	EnableOldValue   bool              `toml:"enable-old-value" json:"enable-old-value"`
	ForceReplicate   bool              `toml:"force-replicate" json:"force-replicate"`
	CheckGCSafePoint bool              `toml:"check-gc-safe-point" json:"check-gc-safe-point"`
	Filter           *FilterConfig     `toml:"filter" json:"filter"`
	Mounter          *MounterConfig    `toml:"mounter" json:"mounter"`
	Sink             *SinkConfig       `toml:"sink" json:"sink"`
	Cyclic           *CyclicConfig     `toml:"cyclic-replication" json:"cyclic-replication"`
	Consistent       *ConsistentConfig `toml:"consistent" json:"consistent"`
}

// Marshal returns the json marshal format of a ReplicationConfig
func (c *ReplicaConfig) Marshal() (string, error) {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrEncodeFailed, errors.Annotatef(err, "Unmarshal data: %v", c))
	}
	return string(cfg), nil
}

// Unmarshal unmarshals into *ReplicationConfig from json marshal byte slice
func (c *ReplicaConfig) Unmarshal(data []byte) error {
	return c.UnmarshalJSON(data)
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
	err = clone.Unmarshal([]byte(str))
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
	return nil
}

// GetDefaultReplicaConfig returns the default replica config.
func GetDefaultReplicaConfig() *ReplicaConfig {
	return defaultReplicaConfig.Clone()
}
