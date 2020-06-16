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

package model

import (
	"encoding/json"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

// SortEngine is the sorter engine
type SortEngine string

// sort engines
const (
	SortInMemory SortEngine = "memory"
	SortInFile   SortEngine = "file"
)

// FeedState represents the running state of a changefeed
type FeedState string

// All FeedStates
const (
	StateNormal FeedState = "normal"
	StateFailed FeedState = "failed"
)

// ChangeFeedInfo describes the detail of a ChangeFeed
type ChangeFeedInfo struct {
	SinkURI    string            `json:"sink-uri"`
	Opts       map[string]string `json:"opts"`
	CreateTime time.Time         `json:"create-time"`
	// Start sync at this commit ts if `StartTs` is specify or using the CreateTime of changefeed.
	StartTs uint64 `json:"start-ts"`
	// The ChangeFeed will exits until sync to timestamp TargetTs
	TargetTs uint64 `json:"target-ts"`
	// used for admin job notification, trigger watch event in capture
	AdminJobType AdminJobType `json:"admin-job-type"`
	Engine       SortEngine   `json:"sort-engine"`
	SortDir      string       `json:"sort-dir"`

	Config *config.ReplicaConfig `json:"config"`
	State  FeedState             `json:"state"`
}

// GetStartTs returns StartTs if it's  specified or using the CreateTime of changefeed.
func (info *ChangeFeedInfo) GetStartTs() uint64 {
	if info.StartTs > 0 {
		return info.StartTs
	}

	return oracle.EncodeTSO(info.CreateTime.Unix() * 1000)
}

// GetCheckpointTs returns CheckpointTs if it's specified in ChangeFeedStatus, otherwise StartTs is returned.
func (info *ChangeFeedInfo) GetCheckpointTs(status *ChangeFeedStatus) uint64 {
	if status != nil {
		return status.CheckpointTs
	}
	return info.GetStartTs()
}

// GetTargetTs returns TargetTs if it's specified, otherwise MaxUint64 is returned.
func (info *ChangeFeedInfo) GetTargetTs() uint64 {
	if info.TargetTs > 0 {
		return info.TargetTs
	}
	return uint64(math.MaxUint64)
}

// Marshal returns the json marshal format of a ChangeFeedInfo
func (info *ChangeFeedInfo) Marshal() (string, error) {
	data, err := json.Marshal(info)
	return string(data), errors.Trace(err)
}

// Unmarshal unmarshals into *ChangeFeedInfo from json marshal byte slice
func (info *ChangeFeedInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, &info)
	if err != nil {
		return errors.Annotatef(err, "Unmarshal data: %v", data)
	}
	// TODO(neil) find a better way to let sink know cyclic is enabled.
	if info.Config != nil && info.Config.Cyclic.IsEnabled() {
		cyclicCfg, err := info.Config.Cyclic.Marshal()
		if err != nil {
			return errors.Annotatef(err, "Marshal data: %v", data)
		}
		info.Opts[filter.OptCyclicConfig] = string(cyclicCfg)
	}
	return nil
}

// VerifyAndFix verifies changefeed info and may fillin some fields.
// If a must field is not provided, return an error.
// If some necessary filed is missing but can use a default value, fillin it.
func (info *ChangeFeedInfo) VerifyAndFix() error {
	defaultConfig := config.GetDefaultReplicaConfig()
	if info.Engine == "" {
		info.Engine = SortInMemory
	}
	if info.Config.Filter == nil {
		info.Config.Filter = defaultConfig.Filter
	}
	if info.Config.Mounter == nil {
		info.Config.Mounter = defaultConfig.Mounter
	}
	if info.Config.Sink == nil {
		info.Config.Sink = defaultConfig.Sink
	}
	if info.Config.Cyclic == nil {
		info.Config.Cyclic = defaultConfig.Cyclic
	}
	if info.Config.Scheduler == nil {
		info.Config.Scheduler = defaultConfig.Scheduler
	}
	return nil
}
