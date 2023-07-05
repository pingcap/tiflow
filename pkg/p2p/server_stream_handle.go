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

package p2p

import (
	"context"
	"sync"

	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/proto/p2p"
	"go.uber.org/atomic"
)

// streamHandle is used to provide the server a non-blocking handle of a stream.
// It is needed because grpc-go does not provide a stream easily cancellable.
// Since Go is not friendly with the receiver closing a channel, we need this
// auxiliary data structure to make the stream cancellation more graceful.
type streamHandle struct {
	// mu protects sendCh. We should take the shared lock
	// when sending and the exclusive lock when closing it.
	// We consider Read to be anything except closing the channel,
	// and close is Write in this definition.
	// This definition provides an elegant solution for lock protecting Go channels.
	mu     sync.RWMutex
	sendCh chan<- p2p.SendMessageResponse

	isClosed atomic.Bool
	closeCh  chan struct{}

	// read-only
	streamMeta *p2p.StreamMeta
}

// newStreamHandle returns a new streamHandle.
func newStreamHandle(meta *p2p.StreamMeta, sendCh chan<- p2p.SendMessageResponse) *streamHandle {
	return &streamHandle{
		sendCh:     sendCh,
		closeCh:    make(chan struct{}),
		streamMeta: meta,
	}
}

// Send sends a message to the stream.
// If called after Close, an error will be returned.
func (s *streamHandle) Send(ctx context.Context, response p2p.SendMessageResponse) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// We need to read from isClosed while holding the lock.
	// Otherwise, we risk the sendCh being closed after we have checked
	// isClosed to be false. If this happens, we will try to send
	// to a closed channel, which will panic.
	if s.isClosed.Load() {
		return errors.ErrPeerMessageInternalSenderClosed.GenWithStackByArgs()
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case s.sendCh <- response:
	case <-s.closeCh:
		return errors.ErrPeerMessageInternalSenderClosed.GenWithStackByArgs()
	}

	return nil
}

// Close closes the stream handle.
// We should not call Send after Close.
func (s *streamHandle) Close() {
	if s.isClosed.Swap(true) {
		// already closed
		return
	}
	// Must close `s.closeCh` while not holding `s.mu`.
	close(s.closeCh)

	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.sendCh)
}

// GetStreamMeta returns the metadata associated with the stream.
func (s *streamHandle) GetStreamMeta() *p2p.StreamMeta {
	return s.streamMeta
}
