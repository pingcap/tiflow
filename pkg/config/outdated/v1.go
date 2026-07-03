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

package outdated

import (
	"encoding/json"

	"github.com/pingcap/tidb/pkg/util/filter"
)

// ReplicaConfigV1 represents some incompatible config with current Config
type ReplicaConfigV1 struct {
	Sink *struct {
		DispatchRules []*struct {
			filter.Table
			Rule string `toml:"rule" json:"rule"`
		} `toml:"dispatch-rules" json:"dispatch-rules"`
	} `toml:"sink" json:"sink"`
}

// Unmarshal unmarshals into *ReplicaConfigV1 from json marshal byte slice
func (c *ReplicaConfigV1) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}
