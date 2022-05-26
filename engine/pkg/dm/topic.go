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

package dm

import (
	"encoding/json"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

// Defines topics here
const (
	OperateTask p2p.Topic = "OperateTask"
	QueryStatus p2p.Topic = "QueryStatus"
	StopWorker  p2p.Topic = "StopWorker"
)

// OperateTaskMessage is operate task message
type OperateTaskMessage struct {
	TaskID string
	Stage  metadata.TaskStage
}

// StopWorkerMessage is stop worker message
type StopWorkerMessage struct {
	Task string
}

// QueryStatusRequest is query status request
type QueryStatusRequest struct {
	Task string
}

// QueryStatusResponse is query status response
type QueryStatusResponse struct {
	ErrorMsg string
	Unit     libModel.WorkerType
	Stage    metadata.TaskStage
	Status   json.RawMessage
}
