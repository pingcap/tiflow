// Copyright 2022 PingCAP, Inc.
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

package statusutil

import (
	"fmt"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
)

// WorkerStatusMessage contains necessary fileds of a worker status message
type WorkerStatusMessage struct {
	Worker      frameModel.WorkerID      `json:"worker"`
	MasterEpoch frameModel.Epoch         `json:"master-epoch"`
	Status      *frameModel.WorkerStatus `json:"status"`
}

// WorkerStatusTopic returns the p2p topic for worker status subscription of a
// given master.
func WorkerStatusTopic(masterID frameModel.MasterID) string {
	return fmt.Sprintf("worker-status-%s", masterID)
}
