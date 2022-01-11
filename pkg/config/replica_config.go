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
	Sink: &SinkConfig{},
	Cyclic: &CyclicConfig{
		Enable: false,
	},
	Scheduler: &SchedulerConfig{
		Tp:          "table-number",
		PollingTime: -1,
	},
	Consistent: &ConsistentConfig{
		Level:             "none",
		MaxLogSize:        64,
		FlushIntervalInMs: 1000,
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
	Scheduler        *SchedulerConfig  `toml:"scheduler" json:"scheduler"`
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
			Matcher:    []string{fmt.Sprintf("%s.%s", dispatch.Schema, dispatch.Name)},
			Dispatcher: dispatch.Rule,
		})
	}
}

// Validate verifies that each parameter is valid.
func (c *ReplicaConfig) Validate() error {
	if c.Sink != nil {
		err := c.Sink.validate(c.EnableOldValue)
		if err != nil {
			return err
		}
	}
	return nil
}

// [TODO] After merging cli/openapi parameter check code, pls make this function as member function of ReplicaConfig
// @return: string, warning info if nessesary
//			error
func ValidateDispatcherRule(sinkURI string, sinkCfg *SinkConfig, enableOldValue bool) (string, error) {
	if sinkCfg == nil {
		return "", nil
	}
	sinkURIParsed, err := url.Parse(sinkURI)
	if err != nil {
		return "", cerror.ErrSinkURIInvalid.Wrap(errors.Annotatef(err, "sink-uri:%s", sinkURI))
	}
	for _, rules := range sinkCfg.DispatchRules {
		scheme := strings.ToLower(sinkURIParsed.Scheme)
		dispatcher := strings.ToLower(rules.Dispatcher)
		if scheme == "mysql" || scheme == "tidb" {
			if dispatcher != "table" && dispatcher != "casuality" {
				return "", cerror.ErrDispatchRuleUnsupported.GenWithStackByArgs(fmt.Sprintf("dispatcher:%s for scheme:%s", dispatcher, scheme))
			}
		} else {
			if dispatcher == "casuality" {
				return "", cerror.ErrDispatchRuleUnsupported.GenWithStackByArgs(fmt.Sprintf("unsupported dispatcher:%s for scheme:%s", dispatcher, scheme))
			}
			if (dispatcher == "rowid" || dispatcher == "index-value") && enableOldValue {
				return fmt.Sprintf("[WARN] This index-value or rowid distribution mode "+
					"does not guarantee row-level orderliness when "+
					"switching on the old value, so please use caution! dispatch-rules: %#v", rules), nil
			}
		}
	}
	return "", nil
}

// [TODO] move all related replica parameters check here to avoid duplicate code in openapi and cli

// GetDefaultReplicaConfig returns the default replica config.
func GetDefaultReplicaConfig() *ReplicaConfig {
	return defaultReplicaConfig.Clone()
}
