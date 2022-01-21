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

package pipeline

import "github.com/pingcap/tiflow/pkg/context"

// NodeContext adds two functions to `context.Context` and is created by pipeline
type NodeContext interface {
	context.Context

	// Message returns the message sent by the previous node
	Message() Message
	// SendToNextNode sends the message to the next node
	SendToNextNode(msg Message)
}

type nodeContext struct {
	context.Context
	msg      Message
	outputCh chan Message
}

func newNodeContext(ctx context.Context, msg Message, outputCh chan Message) NodeContext {
	return &nodeContext{
		Context:  ctx,
		msg:      msg,
		outputCh: outputCh,
	}
}

func (ctx *nodeContext) Message() Message {
	return ctx.msg
}

func (ctx *nodeContext) SendToNextNode(msg Message) {
	// The header channel should never be blocked
	ctx.outputCh <- msg
}

type messageContext struct {
	NodeContext
	message Message
}

func withMessage(ctx NodeContext, msg Message) NodeContext {
	return messageContext{
		NodeContext: ctx,
		message:     msg,
	}
}

func (ctx messageContext) Message() Message {
	return ctx.message
}
