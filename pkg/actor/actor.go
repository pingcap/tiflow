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

package actor

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/tiflow/pkg/actor/message"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
)

var errMailboxFull = cerrors.ErrMailboxFull.FastGenByArgs()

// ID is ID for actors.
type ID uint64

// Actor is a universal primitive of concurrent computation.
// See more https://en.wikipedia.org/wiki/Actor_model
type Actor interface {
	// Poll handles messages that are sent to actor's mailbox.
	//
	// The ctx is only for cancellation, and an actor must be aware of
	// the cancellation.
	//
	// If it returns true, then the actor will be rescheduled and polled later.
	// If it returns false, then the actor will be removed from Router and
	// polled if there are still messages in its mailbox.
	// Once it returns false, it must always return false.
	//
	// We choose message to have a concrete type instead of an interface to save
	// memory allocation.
	Poll(ctx context.Context, msgs []message.Message) (running bool)

	// OnClose is called after Poll returns false,
	// or actor system is stopping and all message has been received.
	// An actor should release its resources during OnClose.
	//
	// OnClose must be idempotent and nonblocking.
	OnClose()
}

// Mailbox sends messages to an actor.
// Mailbox is threadsafe.
type Mailbox interface {
	ID() ID
	// Send non-blocking send a message to its actor.
	// Returns ErrMailboxFull when it's full.
	// Returns ErrActorStopped when its actor is closed.
	Send(msg message.Message) error
	// SendB sends a message to its actor, blocks when it's full.
	// Returns ErrActorStopped when its actor is closed.
	// Retruns context.Canceled or context.DeadlineExceeded
	// when context is canceled or deadline exceeded.
	SendB(ctx context.Context, msg message.Message) error

	// Receive a message.
	// It must be nonblocking and should only be called by System.
	Receive() (message.Message, bool)
	// Return the length of a mailbox.
	// It should only be called by System.
	len() int
	// Close stops receiving messages.
	// It must be idempotent and nonblocking.
	close()
}

// NewMailbox creates a fixed capacity mailbox.
// The minimum capacity is 1.
func NewMailbox(id ID, cap int) Mailbox {
	if cap <= 0 {
		cap = 1
	}
	return &mailbox{
		id:      id,
		msgCh:   make(chan message.Message, cap),
		closeCh: make(chan struct{}),
		state:   mailboxStateRunning,
	}
}

var _ Mailbox = (*mailbox)(nil)

const (
	mailboxStateRunning uint64 = 0
	mailboxStateClosed  uint64 = 1
)

type mailbox struct {
	state   uint64
	closeCh chan struct{}

	id    ID
	msgCh chan message.Message
}

func (m *mailbox) ID() ID {
	return m.id
}

func (m *mailbox) Send(msg message.Message) error {
	if atomic.LoadUint64(&m.state) == mailboxStateClosed {
		return errActorStopped
	}
	select {
	case m.msgCh <- msg:
		return nil
	default:
		return errMailboxFull
	}
}

func (m *mailbox) SendB(ctx context.Context, msg message.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.closeCh:
		return errActorStopped
	case m.msgCh <- msg:
		return nil
	}
}

func (m *mailbox) Receive() (message.Message, bool) {
	select {
	case msg, ok := <-m.msgCh:
		return msg, ok
	default:
	}
	return message.Message{}, false
}

func (m *mailbox) len() int {
	return len(m.msgCh)
}

func (m *mailbox) close() {
	if atomic.CompareAndSwapUint64(&m.state, mailboxStateRunning, mailboxStateClosed) {
		close(m.closeCh)
	}
}
