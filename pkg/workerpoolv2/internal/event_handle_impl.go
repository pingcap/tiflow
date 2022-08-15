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
	poll() (hasNext bool)
	setNotifyFunc(fn NotifyFunc)
}

// A NotifyFunc is used to notify the pool that a new event has come in.
type NotifyFunc = func(p Poller, shouldExit bool)

var (
	// ErrEventHandleCanceled indicates that a given event handler has been canceled.
	ErrEventHandleCanceled = errors.New("event handle has been canceled")

	// ErrEventHandleAlreadyCanceled indicates that the event handler had already been canceled
	// when GracefulUnregister was called.
	ErrEventHandleAlreadyCanceled = errors.New("event handle had already been canceled " +
		"when GracefulUnregister was called")
)

type cancelType = int32

const (
	cancelTypeNone = cancelType(iota + 1)
	cancelTypeImmediate
	cancelTypeGraceful
)

// EventHandleImpl implements EventHandler.
type EventHandleImpl[T any] struct {
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
	onExit func(error)

	maxQueueLen int
	batchSize   int
}

func NewEventHandlerImpl[T any](
	fn func(T) error, maxQueueLen int, batchSize int,
) *EventHandleImpl[T] {
	ret := &EventHandleImpl[T]{
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

func (h *EventHandleImpl[T]) AddEvent(ctx context.Context, event T) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	for h.q.Len() >= h.maxQueueLen && h.cancelFlag.Load() == cancelTypeNone {
		err := h.cond.WaitWithContext(ctx)
		if err != nil {
			return err
		}
	}

	if h.cancelFlag.Load() != cancelTypeNone {
		return errors.Trace(ErrEventHandleCanceled)
	}

	h.q.Push(event)

	if h.shouldNotify() && h.notify != nil {
		h.notify(h, false)
	}
	return nil
}

func (h *EventHandleImpl[T]) Unregister() {
	h.unregisterOnce.Do(h.doUnregister)
}

func (h *EventHandleImpl[T]) doUnregister() {
	h.mu.Lock()
	// From this point on, no new event will be added by AddEvent.
	success := h.cancelFlag.CAS(cancelTypeNone, cancelTypeImmediate)
	h.mu.Unlock()

	if !success {
		panic("unreachable")
	}

	// Wake up possible blocked producers.
	h.cond.Broadcast()

	// Linearization point. No event will be processed hereafter.
	h.rcu.Synchronize()
	h.notify(h, true)
}

func (h *EventHandleImpl[T]) doGracefulUnregister(ctx context.Context) error {
	h.mu.Lock()
	// From this point on, no new event will be added by AddEvent.
	success := h.cancelFlag.CAS(cancelTypeNone, cancelTypeGraceful)
	h.mu.Unlock()

	if !success {
		panic("unreachable")
	}

	// Wake up possible blocked producers.
	h.cond.Broadcast()

	h.mu.Lock()
	for !h.q.Empty() && !h.errorFlag {
		err := h.cond.WaitWithContext(ctx)
		if err != nil {
			h.mu.Unlock()
			return err
		}
	}
	// Must unlock here for Synchronize() to work.
	h.mu.Unlock()

	// Still need to synchronize because poll
	// maybe processing the last batch of events.
	h.rcu.Synchronize() // Linearization point.
	h.notify(h, true)

	if h.errorFlag {
		// An error flag was set, indicating that an error occurred
		// before the linearization point of the GracefulUnregister operation.
		return errors.Trace(ErrEventHandleAlreadyCanceled)
	}

	return nil
}

func (h *EventHandleImpl[T]) GracefulUnregister(ctx context.Context) error {
	var (
		hasRun atomic.Bool
		err    error
	)
	h.unregisterOnce.Do(func() {
		defer hasRun.Store(true)
		err = h.doGracefulUnregister(ctx)
	})

	if !hasRun.Load() {
		return errors.Trace(ErrEventHandleAlreadyCanceled)
	}
	return err
}

func (h *EventHandleImpl[T]) ErrCh() <-chan error {
	return h.errCh
}

func (h *EventHandleImpl[T]) OnExit(fn func(error)) {
	h.onExit = fn
}

func (h *EventHandleImpl[T]) setNotifyFunc(fn NotifyFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.notify = fn
}

func (h *EventHandleImpl[T]) poll() (hasNext bool) {
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

	h.cond.Broadcast()

	for _, event := range events {
		if h.cancelFlag.Load() == cancelTypeImmediate {
			return
		}

		err := h.safeProcessEvent(event)
		if err != nil {
			// Set errorFlag so that no more events will be processed.
			h.errorFlag = true
			if h.onExit != nil {
				h.onExit(err)
			}
			h.exitOnError(err)
			return
		}
	}

	hasNext = remaining > 0
	return
}

func (h *EventHandleImpl[T]) safeProcessEvent(val T) (retErr error) {
	defer func() {
		if v := recover(); v != nil {
			retErr = errors.Errorf("panicked: %s", v)
		}
	}()

	return errors.Trace(h.fn(val))
}

func (h *EventHandleImpl[T]) getEventBatch() []T {
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

func (h *EventHandleImpl[T]) shouldNotify() bool {
	// Note that this method is always called with h.mu locked.
	if h.q.Len() >= h.maxQueueLen {
		// We must notify if the queue is full.
		return true
	}

	return h.rl.Allow()
}

func (h *EventHandleImpl[T]) exitOnError(errIn error) {
	// Note that the following logic must be run in a separate goroutine
	// other than poll, or otherwise we risk deadlocking concurrent Unregister calls.
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
