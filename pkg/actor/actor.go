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

	"github.com/pingcap/ticdc/pkg/actor/message"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
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
}

// Mailbox sends messages to an actor.
// Mailbox is threadsafe.
type Mailbox interface {
	ID() ID
	// Send a message to its actor.
	// It's a non-blocking send, returns ErrMailboxFull when it's full.
	Send(msg message.Message) error
	// SendB sends a message to its actor, blocks when it's full.
	// It may return context.Canceled or context.DeadlineExceeded.
	SendB(ctx context.Context, msg message.Message) error

	// Try to receive a message.
	// It must be nonblocking and should only be called by System.
	tryReceive() (message.Message, bool)
	// Return the length of a mailbox.
	// It should only be called by System.
	len() int
}

// NewMailbox creates a fixed capacity mailbox.
func NewMailbox(id ID, cap int) Mailbox {
	return &mailbox{
		id: id,
		ch: make(chan message.Message, cap),
	}
}

var _ Mailbox = (*mailbox)(nil)

type mailbox struct {
	id ID
	ch chan message.Message
}

func (m *mailbox) ID() ID {
	return m.id
}

func (m *mailbox) Send(msg message.Message) error {
	select {
	case m.ch <- msg:
		return nil
	default:
		return errMailboxFull
	}
}

func (m *mailbox) SendB(ctx context.Context, msg message.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.ch <- msg:
		return nil
	}
}

func (m *mailbox) tryReceive() (message.Message, bool) {
	select {
	case msg, ok := <-m.ch:
		return msg, ok
	default:
	}
	return message.Message{}, false
}

func (m *mailbox) len() int {
	return len(m.ch)
}
