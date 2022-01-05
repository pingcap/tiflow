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
	"github.com/pingcap/tiflow/pkg/pipeline"
)

// ActorNode is an async data process node, it fetches and process table event non-blocking
// if processing is blocked, the data will be cached and wait next run
type ActorNode struct {
	eventStash    *pipeline.Message
	parentNode    AsyncDataHolder
	dataProcessor AsyncDataProcessor
}

// NewActorNode create a new ActorNode
func NewActorNode(parentNode AsyncDataHolder, dataProcessor AsyncDataProcessor) *ActorNode {
	return &ActorNode{
		parentNode:    parentNode,
		dataProcessor: dataProcessor,
	}
}

// TryRun get event and process it util no data is available or data processing is blocked
// only one event will be cached
func (n *ActorNode) TryRun(ctx context.Context) error {
	for {
		// batch?
		if n.eventStash == nil {
			n.eventStash = n.parentNode.TryGetProcessedMessage()
		}
		if n.eventStash == nil {
			return nil
		}
		ok, err := n.dataProcessor.TryHandleDataMessage(ctx, *n.eventStash)
		// process message failed, stop table actor
		if err != nil {
			return errors.Trace(err)
		}

		if ok {
			n.eventStash = nil
		} else {
			return nil
		}
	}
}

// AsyncDataProcessor is an interface to handle event non-blocking
type AsyncDataProcessor interface {
	TryHandleDataMessage(ctx context.Context, msg pipeline.Message) (bool, error)
}

// AsyncDataHolder is an interface to get event non-blocking
type AsyncDataHolder interface {
	TryGetProcessedMessage() *pipeline.Message
}

type AsyncDataProcessorFunc func(ctx context.Context, msg pipeline.Message) (bool, error)

func (fn AsyncDataProcessorFunc) TryHandleDataMessage(ctx context.Context, msg pipeline.Message) (bool, error) {
	return fn(ctx, msg)
}

type AsyncDataHolderFunc func() *pipeline.Message

func (fn AsyncDataHolderFunc) TryGetProcessedMessage() *pipeline.Message {
	return fn()
}
