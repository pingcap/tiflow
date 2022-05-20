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

package dmtask

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
)

var defaultResponseTimeOut = time.Second * 2

// SendHandle defines an interface that supports SendMessage
type SendHandle interface {
	SendMessage(ctx context.Context, topic string, message interface{}, nonblocking bool) error
}

// MessageAgent hold by TaskWorker, it manage all interactions with DMJobmaster
type MessageAgent struct {
	messagePair *dmpkg.MessagePair
	sendHandle  SendHandle
	id          libModel.WorkerID
	taskID      string
}

// NewMessageAgent creates a new MessageAgent instance
func NewMessageAgent(sender SendHandle) *MessageAgent {
	messageAgent := &MessageAgent{
		messagePair: dmpkg.NewMessagePair(),
		sendHandle:  sender,
	}
	return messageAgent
}

// QueryStatusResponse delegates to send query-status response with p2p messaging system
func (agent *MessageAgent) QueryStatusResponse(ctx context.Context, messageID uint64, taskStatus runtime.TaskStatus, errMsg string) error {
	topic := dmpkg.QueryStatusRequestTopic(agent.id, agent.taskID)
	response := &dmpkg.QueryStatusResponse{
		ErrorMsg:   errMsg,
		TaskStatus: taskStatus,
	}

	ctx, cancel := context.WithTimeout(ctx, defaultResponseTimeOut)
	defer cancel()
	return agent.messagePair.SendResponse(ctx, topic, messageID, response, agent.sendHandle)
}
