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

package cdc

import (
	"encoding/json"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-cdc/cdc/roles"
)

// SubChangeFeedInfo records the process information of a capture
type SubChangeFeedInfo struct {
	// The maximum event CommitTS that has been synchronized. This is updated by corresponding processor.
	CheckPointTS uint64 `json:"checkpoint-ts"`
	// The event that satisfies CommitTS <= ResolvedTS can be synchronized. This is updated by corresponding processor.
	ResolvedTS uint64 `json:"resolved-ts"`
	// Table information list, containing tables that processor should process, updated by ownrer, processor is read only.
	TableInfos []*roles.ProcessTableInfo `json:"table-infos"`
}

func (scfi *SubChangeFeedInfo) String() string {
	data, err := json.Marshal(scfi)
	if err != nil {
		log.Error("fail to marshal ChangeFeedDetail to json", zap.Error(err))
	}
	return string(data)
}
