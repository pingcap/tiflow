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

package replication

import "github.com/pingcap/ticdc/cdc/model"

// changefeed is part of the replication model that implements the control logic of a changefeed
type changefeed struct {
	tableTasks map[int]*tableTask
}

type tableTask struct {
	CheckpointTs uint64
	ResolvedTs   uint64
	Capture      model.CaptureID
}

