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

package framework

import (
	"context"

	"github.com/pingcap/log"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/zap"
)

const defaultMessageRouterBufferSize = 4

type messageWrapper struct {
	topic p2p.Topic
	msg   p2p.MessageValue
}

// MessageRouter is a SPSC(single producer, single consumer) work model, since
// the message frequency is not high, we use a simple channel for message transit.
type MessageRouter struct {
	workerID frameModel.WorkerID
	buffer   chan messageWrapper
	pool     workerpool.AsyncPool
	errCh    chan error
	routeFn  func(topic p2p.Topic, msg p2p.MessageValue) error
}

// NewMessageRouter creates a new MessageRouter
func NewMessageRouter(
	workerID frameModel.WorkerID,
	pool workerpool.AsyncPool,
	bufferSize int,
	routeFn func(topic p2p.Topic, msg p2p.MessageValue) error,
) *MessageRouter {
	return &MessageRouter{
		workerID: workerID,
		pool:     pool,
		buffer:   make(chan messageWrapper, bufferSize),
		errCh:    make(chan error, 1),
		routeFn:  routeFn,
	}
}

// Tick should be called periodically, it receives message from buffer and route it
func (r *MessageRouter) Tick(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case msg := <-r.buffer:
		return r.routeMessage(ctx, msg)
	default:
	}
	return nil
}

// AppendMessage always appends a new message into buffer, if the message buffer
// is full, it will evicits the oldest message
func (r *MessageRouter) AppendMessage(topic p2p.Topic, msg p2p.MessageValue) {
	msgWrap := messageWrapper{
		topic: topic,
		msg:   msg,
	}
	for {
		select {
		case r.buffer <- msgWrap:
			return
		default:
			select {
			case dropMsg := <-r.buffer:
				log.Warn("drop message because of buffer is full",
					zap.String("topic", dropMsg.topic), zap.Any("message", dropMsg.msg))
			default:
			}
		}
	}
}

func (r *MessageRouter) onError(err error) {
	select {
	case r.errCh <- err:
	default:
		log.Warn("error is dropped because errCh is full", zap.Error(err))
	}
}

func (r *MessageRouter) routeMessage(ctx context.Context, msg messageWrapper) error {
	err := r.pool.Go(ctx, func() {
		err := r.routeFn(msg.topic, msg.msg)
		if err != nil {
			r.onError(err)
		}
	})
	return errors.Trace(err)
}
