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

package roles

type ProcessTableInfo struct {
	ID      uint64 `json:"id"`
	StartTS uint64 `json:"start-ts"`
}

// Owner is used to process etcd information for a capture with owner role
type Owner interface {
	// TableSchedule updates table infos stored in SubChangeFeed in etcd
	TableSchedule(changeFeedID string, schedule map[string][]*ProcessTableInfo) error
}
