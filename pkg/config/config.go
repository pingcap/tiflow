// Copyright 2020 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var defaultReplicaConfig = &ReplicaConfig{
	CaseSensitive: true,
	Filter:        &FilterConfig{},
	Mounter: &MounterConfig{
		WorkerNum: 16,
	},
	Sink: &SinkConfig{},
	Cyclic: &CyclicConfig{
		Enable: false,
	},
}

// ReplicaConfig represents some addition replication config for a changefeed
type ReplicaConfig struct {
	CaseSensitive bool           `toml:"case-sensitive" json:"case-sensitive"`
	Filter        *FilterConfig  `toml:"filter" json:"filter"`
	Mounter       *MounterConfig `toml:"mounter" json:"mounter"`
	Sink          *SinkConfig    `toml:"sink" json:"sink"`
	Cyclic        *CyclicConfig  `toml:"cyclic-replication" json:"cyclic-replication"`
}

// Marshal returns the json marshal format of a ReplicationConfig
func (c *ReplicaConfig) Marshal() (string, error) {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "", errors.Annotatef(err, "Unmarshal data: %v", c)
	}
	return string(cfg), nil
}

// Unmarshal unmarshals into *ReplicationConfig from json marshal byte slice
func (c *ReplicaConfig) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}

// Clone clones a replication
func (c *ReplicaConfig) Clone() *ReplicaConfig {
	str, err := c.Marshal()
	if err != nil {
		log.Fatal("failed to marshal replica config", zap.Error(err))
	}
	clone := new(ReplicaConfig)
	err = clone.Unmarshal([]byte(str))
	if err != nil {
		log.Fatal("failed to marshal replica config", zap.Error(err))
	}
	return clone
}

// GetDefaultReplicaConfig returns the default replica config
func GetDefaultReplicaConfig() *ReplicaConfig {
	return defaultReplicaConfig.Clone()
}
