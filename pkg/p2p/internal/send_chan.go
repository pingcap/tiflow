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

package internal

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	proto "github.com/pingcap/tiflow/proto/p2p"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
)

type SendChan struct {
	mu      sync.Mutex
	buf     []*proto.MessageEntry
	sendIdx int64
	recvIdx int64

	ready chan struct{}

	nextSeq *atomic.Int64

	cap int64
}

func NewSendChan(cap int64, nextSeq *atomic.Int64) *SendChan {
	return &SendChan{
		buf:     make([]*proto.MessageEntry, cap),
		ready:   make(chan struct{}, 1),
		nextSeq: nextSeq,
		cap:     cap,
	}
}

func (c *SendChan) SendSync(ctx context.Context, topic string, value []byte) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}

		if c.SendAsync(topic, value) {
			return nil
		}
	}
}

func (c *SendChan) SendAsync(topic string, value []byte) (ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sendIdx-c.recvIdx > c.cap {
		log.Panic("unreachable",
			zap.Int64("sendIdx", c.sendIdx),
			zap.Int64("recvIndex", c.recvIdx))
	}

	if c.sendIdx == c.recvIdx {
		return false
	}

	c.buf[c.sendIdx] = &proto.MessageEntry{
		Topic:    topic,
		Content:  value,
		Sequence: c.getSeq(),
	}
	c.sendIdx++

	select {
	case c.ready <- struct{}{}:
	default:
	}

	return true
}

func (c *SendChan) Receive(ctx context.Context, tick <-chan time.Time) (*proto.MessageEntry, bool, error) {
	select {
	case <-ctx.Done():
		return nil, false, errors.Trace(ctx.Err())
	default:
	}

	for {
		entry := c.doReceive()
		if entry != nil {
			return entry, true, nil
		}

		select {
		case <-ctx.Done():
			return nil, false, errors.Trace(ctx.Err())
		case <-tick:
			return nil, false, nil
		case <-c.ready:
		}
	}
}

func (c *SendChan) doReceive() *proto.MessageEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sendIdx < c.recvIdx {
		log.Panic("unreachable",
			zap.Int64("sendIdx", c.sendIdx),
			zap.Int64("recvIndex", c.recvIdx))
	}

	if c.sendIdx == c.recvIdx {
		return nil
	}

	var ret *proto.MessageEntry
	ret, c.buf[c.recvIdx] = c.buf[c.recvIdx], nil
	return ret
}

func (c *SendChan) getSeq() int64 {
	return c.nextSeq.Inc()
}
