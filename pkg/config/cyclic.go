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
)

// CyclicConfig represents config used for cyclic replication
type CyclicConfig struct {
	Enable          bool     `toml:"enable" json:"enable"`
	ReplicaID       uint64   `toml:"replica-id" json:"replica-id"`
	FilterReplicaID []uint64 `toml:"filter-replica-ids" json:"filter-replica-ids"`
	IDBuckets       int      `toml:"id-buckets" json:"id-buckets"`
	SyncDDL         bool     `toml:"sync-ddl" json:"sync-ddl"`
}

// IsEnabled returns whether cyclic replication is enabled or not.
func (c *CyclicConfig) IsEnabled() bool {
	return c != nil && c.Enable
}

// Marshal returns the json marshal format of a CyclicConfig
func (c *CyclicConfig) Marshal() (string, error) {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "", errors.Annotatef(err, "Unmarshal data: %v", c)
	}
	return string(cfg), nil
}

// Unmarshal unmarshals into *CyclicConfig from json marshal byte slice
func (c *CyclicConfig) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}
