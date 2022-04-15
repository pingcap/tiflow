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

import (
	"github.com/pingcap/tiflow/pkg/context"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
)

// NodeContext adds two functions to `context.Context` and is created by pipeline
type NodeContext interface {
	context.Context

	// Message returns the message sent by the previous node
	Message() pmessage.Message
	// SendToNextNode sends the message to the next node
	SendToNextNode(msg pmessage.Message)
}

type nodeContext struct {
	context.Context
	msg      pmessage.Message
	outputCh chan<- pmessage.Message
}

// NewNodeContext returns a new NodeContext.
func NewNodeContext(
	ctx context.Context, msg pmessage.Message, outputCh chan<- pmessage.Message,
) NodeContext {
	return &nodeContext{
		Context:  ctx,
		msg:      msg,
		outputCh: outputCh,
	}
}

// MockNodeContext4Test creates a node context with a message and an output channel for tests.
func MockNodeContext4Test(
	ctx context.Context, msg pmessage.Message, outputCh chan pmessage.Message,
) NodeContext {
	return NewNodeContext(ctx, msg, outputCh)
}

func (ctx *nodeContext) Message() pmessage.Message {
	return ctx.msg
}

func (ctx *nodeContext) SendToNextNode(msg pmessage.Message) {
	// The header channel should never be blocked
	ctx.outputCh <- msg
}
