// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/tidb/store/tikv/oracle"
)

// ChangeFeedDetail describes the detail of a ChangeFeed
type ChangeFeedDetail struct {
	SinkURI    string            `json:"sink-uri"`
	Opts       map[string]string `json:"opts"`
	CreateTime time.Time         `json:"create-time"`
	// Start sync at this commit ts if `StartTs` is specify or using the CreateTime of changefeed.
	StartTs uint64 `json:"start-ts"`
	// The ChangeFeed will exits until sync to timestamp TargetTs
	TargetTs uint64          `json:"target-ts"`
	Info     *ChangeFeedInfo `json:"-"`
}

// GetStartTs returns StartTs if it's  specified or using the CreateTime of changefeed.
func (detail *ChangeFeedDetail) GetStartTs() uint64 {
	if detail.StartTs > 0 {
		return detail.StartTs
	}

	return oracle.EncodeTSO(detail.CreateTime.Unix() * 1000)
}

// GetTargetTs returns TargetTs if it's specified, otherwise MaxUint64 is returned.
func (detail *ChangeFeedDetail) GetTargetTs() uint64 {
	if detail.TargetTs > 0 {
		return detail.TargetTs
	}
	return uint64(math.MaxUint64)
}

// GetCheckpointTs returns the checkpoint ts of changefeed.
func (detail *ChangeFeedDetail) GetCheckpointTs() uint64 {
	if detail.Info != nil {
		return detail.Info.CheckpointTs
	}

	return detail.GetStartTs()
}

// Marshal returns the json marshal format of a ChangeFeedDetail
func (detail *ChangeFeedDetail) Marshal() (string, error) {
	data, err := json.Marshal(detail)
	return string(data), errors.Trace(err)
}

// Unmarshal unmarshals into *ChangeFeedDetail from json marshal byte slice
func (detail *ChangeFeedDetail) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, &detail)
	return errors.Annotatef(err, "Unmarshal data: %v", data)
}
