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
	"github.com/pingcap/tiflow/pkg/errors"
)

var errMailboxFull = errors.ErrMailboxFull.FastGenByArgs()

// ID is ID for actors.
type ID uint64

// Actor is a universal primitive of concurrent computation.
// See more https://en.wikipedia.org/wiki/Actor_model
type Actor[T any] interface {
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
	Poll(ctx context.Context, msgs []message.Message[T]) (running bool)

	// OnClose is called after Poll returns false,
	// or actor system is stopping and all message has been received.
	// An actor should release its resources during OnClose.
	//
	// OnClose must be idempotent and nonblocking.
	OnClose()
}

// Mailbox sends messages to an actor.
// Mailbox is thread-safe.
type Mailbox[T any] interface {
	ID() ID
	// Send non-blocking send a message to its actor.
	// Returns ErrMailboxFull when it's full.
	// Returns ErrActorStopped when its actor is closed.
	Send(msg message.Message[T]) error
	// SendB sends a message to its actor, blocks when it's full.
	// Returns ErrActorStopped when its actor is closed.
	// Returns context.Canceled or context.DeadlineExceeded
	// when context is canceled or deadline exceeded.
	SendB(ctx context.Context, msg message.Message[T]) error

	// Receive a message.
	// It must be nonblocking and should only be called by System.
	Receive() (message.Message[T], bool)
	// Return the length of a mailbox.
	// It should only be called by System.
	len() int
	// Close stops receiving messages.
	// It must be idempotent and nonblocking.
	close()
}

// NewMailbox creates a fixed capacity mailbox.
// The minimum capacity is 1.
func NewMailbox[T any](id ID, cap int) Mailbox[T] {
	if cap <= 0 {
		cap = 1
	}
	return &mailbox[T]{
		id:      id,
		msgCh:   make(chan message.Message[T], cap),
		closeCh: make(chan struct{}),
		state:   mailboxStateRunning,
	}
}

var _ Mailbox[any] = (*mailbox[any])(nil)

const (
	mailboxStateRunning uint64 = 0
	mailboxStateClosed  uint64 = 1
)

type mailbox[T any] struct {
	state   uint64
	closeCh chan struct{}

	id    ID
	msgCh chan message.Message[T]
}

func (m *mailbox[T]) ID() ID {
	return m.id
}

func (m *mailbox[T]) Send(msg message.Message[T]) error {
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

func (m *mailbox[T]) SendB(ctx context.Context, msg message.Message[T]) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.closeCh:
		return errActorStopped
	case m.msgCh <- msg:
		return nil
	}
}

func (m *mailbox[T]) Receive() (message.Message[T], bool) {
	select {
	case msg, ok := <-m.msgCh:
		return msg, ok
	default:
	}
	return message.Message[T]{}, false
}

//nolint:unused
func (m *mailbox[T]) len() int {
	return len(m.msgCh)
}

//nolint:unused
func (m *mailbox[T]) close() {
	if atomic.CompareAndSwapUint64(&m.state, mailboxStateRunning, mailboxStateClosed) {
		close(m.closeCh)
	}
}
