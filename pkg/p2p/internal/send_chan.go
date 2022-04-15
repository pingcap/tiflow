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

package internal

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	proto "github.com/pingcap/tiflow/proto/p2p"
	"go.uber.org/zap"
)

// SendChan is a specialized channel used to implement
// the asynchronous interface of the MessageClient.
// SendChan is a MPSC channel.
type SendChan struct {
	mu      sync.Mutex
	buf     []*proto.MessageEntry
	sendIdx int64
	recvIdx int64

	// hasNewMsg is a buffered channel that the sending goroutine(s)
	// use to notify the receiver end.
	hasNewMsg chan struct{}

	// notifyChan is an unbuffered channel that
	// SendSync writes to in order to determine whether the
	// consumer is ready.
	notifyChan chan struct{}

	cap int64
}

// NewSendChan returns a new SendChan.
func NewSendChan(cap int64) *SendChan {
	return &SendChan{
		buf:        make([]*proto.MessageEntry, cap),
		hasNewMsg:  make(chan struct{}, 1),
		notifyChan: make(chan struct{}),
		cap:        cap,
	}
}

// SendSync sends a message synchronously.
func (c *SendChan) SendSync(
	ctx context.Context,
	topic string,
	value []byte,
	closeCh <-chan struct{},
	nextSeq func() int64,
) (int64, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if ok, seq := c.SendAsync(topic, value, nextSeq); ok {
			return seq, nil
		}

		select {
		case <-ctx.Done():
			return 0, errors.Trace(ctx.Err())
		case <-closeCh:
			return 0, cerror.ErrPeerMessageClientClosed.GenWithStackByArgs()
		case c.notifyChan <- struct{}{}:
		case <-ticker.C:
		}
	}
}

// SendAsync tries to send a message. If the message is accepted, nextSeq will be called
// once, and the returned value will be used as the Sequence number of the message.
func (c *SendChan) SendAsync(topic string, value []byte, nextSeq func() int64) (ok bool, seq int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sendIdx-c.recvIdx > c.cap {
		log.Panic("unreachable",
			zap.Int64("sendIdx", c.sendIdx),
			zap.Int64("recvIndex", c.recvIdx))
	}

	if c.sendIdx-c.recvIdx == c.cap {
		return false, 0
	}

	seq = nextSeq()
	c.buf[c.sendIdx%c.cap] = &proto.MessageEntry{
		Topic:    topic,
		Content:  value,
		Sequence: seq,
	}
	c.sendIdx++

	select {
	case c.hasNewMsg <- struct{}{}:
	default:
	}

	return true, seq
}

// Receive receives one message from the channel.
// If a message is received from `tick`, the function will return
// (nil, false, nil).
func (c *SendChan) Receive(ctx context.Context, tick <-chan time.Time) (*proto.MessageEntry, bool, error) {
	select {
	case <-ctx.Done():
		return nil, false, errors.Trace(ctx.Err())
	default:
	}

	for {
		entry := c.doReceive()
		if entry != nil {
			select {
			case <-c.notifyChan:
			default:
			}
			return entry, true, nil
		}

		select {
		case <-ctx.Done():
			return nil, false, errors.Trace(ctx.Err())
		case <-tick:
			return nil, false, nil
		case <-c.hasNewMsg:
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
	ret, c.buf[c.recvIdx%c.cap] = c.buf[c.recvIdx%c.cap], nil
	c.recvIdx++

	return ret
}
