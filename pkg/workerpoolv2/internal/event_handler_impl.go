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
	"github.com/pingcap/tiflow/pkg/container/queue"
	"github.com/pingcap/tiflow/pkg/syncutil"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"
)

type Poller interface {
	Poll() (hasNext bool)
	SetNotifyFunc(fn NotifyFunc)
}

// A NotifyFunc is used to notify the pool that a new event has come in.
type NotifyFunc = func(p Poller, shouldExit bool)

// ErrEventHandlerCanceled indicates that a given event handler has been canceled.
var ErrEventHandlerCanceled = errors.New("event handler has been canceled")

type cancelType = int32

const (
	cancelTypeNone = cancelType(iota + 1)
	cancelTypeImmediate
	cancelTypeGraceful
)

// EventHandlerImpl implements EventHandler.
type EventHandlerImpl[T any] struct {
	cond *syncutil.Cond
	mu   sync.Mutex
	q    *queue.ChunkQueue[T]

	cancelFlag atomic.Int32
	errorFlag  bool // should only be accessed from the Poll thread
	rcu        *syncutil.RCU

	errCh chan error
	rl    *rate.Limiter

	unregisterOnce sync.Once

	notify NotifyFunc
	fn     func(T) error

	maxQueueLen int
	batchSize   int
}

func NewEventHandlerImpl[T any](
	fn func(T) error, maxQueueLen int, batchSize int,
) *EventHandlerImpl[T] {
	ret := &EventHandlerImpl[T]{
		q:           queue.NewChunkQueue[T](),
		cancelFlag:  *atomic.NewInt32(cancelTypeNone),
		rcu:         syncutil.NewRCU(),
		errCh:       make(chan error, 1),
		rl:          rate.NewLimiter(rate.Every(100*time.Millisecond), 1),
		fn:          fn,
		maxQueueLen: maxQueueLen,
		batchSize:   batchSize,
	}
	ret.cond = syncutil.NewCond(&ret.mu)
	return ret
}

func (h *EventHandlerImpl[T]) AddEvent(ctx context.Context, event T) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	for h.q.Len() >= h.maxQueueLen && h.cancelFlag.Load() != cancelTypeNone {
		err := h.cond.WaitWithContext(ctx)
		return err
	}

	if h.cancelFlag.Load() != cancelTypeNone {
		return errors.Trace(ErrEventHandlerCanceled)
	}

	h.q.Push(event)

	if h.shouldNotify() && h.notify != nil {
		h.notify(h, false)
	}
	return nil
}

func (h *EventHandlerImpl[T]) Unregister() {
	h.unregisterOnce.Do(h.doUnregister)
}

func (h *EventHandlerImpl[T]) doUnregister() {
	h.mu.Lock()
	success := h.cancelFlag.CAS(cancelTypeNone, cancelTypeImmediate)
	h.mu.Unlock()

	if !success {
		return
	}

	h.cond.Broadcast()

	h.rcu.Synchronize()
	h.notify(h, true)
}

func (h *EventHandlerImpl[T]) GracefulUnregister(ctx context.Context, timeout time.Duration) error {
	// TODO implement me
	panic("implement me")
}

func (h *EventHandlerImpl[T]) ErrCh() <-chan error {
	return h.errCh
}

func (h *EventHandlerImpl[T]) SetNotifyFunc(fn NotifyFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.notify = fn
}

func (h *EventHandlerImpl[T]) Poll() (hasNext bool) {
	defer func() {
		h.rcu.Quiesce()
	}()

	if h.errorFlag {
		return
	}

	h.mu.Lock()

	if h.q.Empty() {
		h.mu.Unlock()
		return
	}
	events := h.getEventBatch()
	remaining := h.q.Len()
	h.mu.Unlock()

	for _, event := range events {
		if h.cancelFlag.Load() != cancelTypeNone {
			return
		}

		err := h.safeProcessEvent(event)
		if err != nil {
			// Set errorFlag so that no more events will be processed.
			h.errorFlag = true
			h.exitOnError(err)
			return
		}
	}

	hasNext = remaining > 0
	return
}

func (h *EventHandlerImpl[T]) safeProcessEvent(val T) (retErr error) {
	defer func() {
		if v := recover(); v != nil {
			retErr = errors.Errorf("panicked: %s", v)
		}
	}()

	return errors.Trace(h.fn(val))
}

func (h *EventHandlerImpl[T]) getEventBatch() []T {
	batchSize := h.batchSize
	if batchSize > h.q.Len() {
		batchSize = h.q.Len()
	}

	events, ok := h.q.PopMany(batchSize)
	if !ok {
		panic("unreachable")
	}
	return events
}

func (h *EventHandlerImpl[T]) shouldNotify() bool {
	// Note that this method is always called with h.mu locked.
	if h.q.Len() >= h.maxQueueLen {
		// We must notify if the queue is full.
		return true
	}

	return h.rl.Allow()
}

func (h *EventHandlerImpl[T]) exitOnError(errIn error) {
	// Note that the following logic must be run in a separate goroutine
	// other than Poll, or otherwise we risk deadlocking concurrent Unregister calls.
	go func() {
		h.unregisterOnce.Do(h.doUnregister)

		select {
		case h.errCh <- errors.Trace(errIn):
		default:
			log.Panic("only one error is possible")
		}
		return
	}()
}
