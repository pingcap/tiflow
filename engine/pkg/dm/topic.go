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
	"fmt"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

// Message use for asynchronous message.
// It use json format to transfer, all the fields should be public.
type Message interface{}

// MessageWithID use for synchronous request/response message.
type MessageWithID struct {
	ID      uint64
	Message Message
}

// Request alias to Message
type Request Message

// Response alias to Message
type Response Message

// OperateTaskMessageTopic is topic constructor for operate task message
func OperateTaskMessageTopic(masterID libModel.MasterID, taskID string) p2p.Topic {
	return fmt.Sprintf("operate-task-message-%s-%s", masterID, taskID)
}

// OperateTaskMessage is operate task message
type OperateTaskMessage struct {
	TaskID string
	Stage  metadata.TaskStage
}

func QueryStatusRequestTopic(masterID libModel.MasterID, taskID string) p2p.Topic {
	return fmt.Sprintf("query-status-request-%s-%s", masterID, taskID)
}

func QueryStatusResponseTopic(workerID libModel.WorkerID, taskID string) p2p.Topic {
	return fmt.Sprintf("query-status-response-%s-%s", workerID, taskID)
}

type QueryStatusRequest struct {
	Task string
}

type QueryStatusResponse struct {
	ErrorMsg   string
	TaskStatus runtime.TaskStatus
}
