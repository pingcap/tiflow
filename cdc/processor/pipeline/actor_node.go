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

package pipeline

import (
	"context"

	"github.com/pingcap/errors"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
)

// ActorNode is an async message process node, it fetches and handle table message non-blocking
// if processing is blocked, the message will be cached and wait next run
type ActorNode struct {
	messageStash     *pmessage.Message
	parentNode       AsyncMessageHolder
	messageProcessor AsyncMessageProcessor
}

// NewActorNode create a new ActorNode
func NewActorNode(
	parentNode AsyncMessageHolder, messageProcessor AsyncMessageProcessor,
) *ActorNode {
	return &ActorNode{
		parentNode:       parentNode,
		messageProcessor: messageProcessor,
	}
}

// TryRun get message from parentNode and handle it util there is no more message to come
// or message handling is blocking
//
// only one message will be cached
func (n *ActorNode) TryRun(ctx context.Context) error {
	processedCount := 0
	for {
		// batch?
		if n.messageStash == nil {
			n.messageStash = n.parentNode.TryGetDataMessage()
		}
		if n.messageStash == nil {
			return nil
		}
		ok, err := n.messageProcessor.TryHandleDataMessage(ctx, *n.messageStash)
		// process message failed, stop table actor
		if err != nil {
			return errors.Trace(err)
		}

		// node is blocked
		if !ok {
			return nil
		}

		n.messageStash = nil
		processedCount++
		// processed too many messages may consume more than 1 second,
		// return here to allow actor system poll other tables, and avoid dead loop
		if processedCount >= defaultOutputChannelSize {
			return nil
		}
	}
}

// AsyncMessageProcessor is an interface to handle message non-blocking
type AsyncMessageProcessor interface {
	TryHandleDataMessage(ctx context.Context, msg pmessage.Message) (bool, error)
}

// AsyncMessageHolder is an interface to get message non-blocking
type AsyncMessageHolder interface {
	TryGetDataMessage() *pmessage.Message
}

type asyncMessageProcessorFunc func(ctx context.Context, msg pmessage.Message) (bool, error)

func (fn asyncMessageProcessorFunc) TryHandleDataMessage(
	ctx context.Context, msg pmessage.Message,
) (bool, error) {
	return fn(ctx, msg)
}

type asyncMessageHolderFunc func() *pmessage.Message

func (fn asyncMessageHolderFunc) TryGetDataMessage() *pmessage.Message {
	return fn()
}
