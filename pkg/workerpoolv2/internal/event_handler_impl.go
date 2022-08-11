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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/logutil"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/container/queue"
	"github.com/pingcap/tiflow/pkg/syncutil"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"
)

type poller interface {
	Poll()
}

// A NotifyFunc is used to notify the pool that a new event has come in.
type NotifyFunc = func(poller)

// ErrEventHandlerCanceled indicates that a given event handler has been canceled.
var ErrEventHandlerCanceled = errors.New("event handler has been canceled")

// EventHandlerImpl implements EventHandler.
type EventHandlerImpl[T any] struct {
	cond     *syncutil.Cond
	mu       sync.Mutex
	q        queue.ChunkQueue[T]
	canceled bool

	errCh     chan error
	pollCount atomic.Int64
	rl        rate.Limiter
	notify    NotifyFunc

	fn func(T) error

	maxQueueLen int
	batchSize   int
}

func NewEventHandlerImpl[T any](fn func(T) error, maxQueueLen int, batchSize int) {

}

func (h *EventHandlerImpl[T]) AddEvent(ctx context.Context, event T) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.canceled {
		return errors.Trace(ErrEventHandlerCanceled)
	}

	for h.q.Len() >= h.maxQueueLen {
		err := h.cond.WaitWithContext(ctx)
		return err
	}

	h.q.Push(event)

	if h.shouldNotify() && h.notify != nil {
		h.notify(h)
	}
	return nil
}

func (h *EventHandlerImpl[T]) Unregister() {
	//TODO implement me
	panic("implement me")
}

func (h *EventHandlerImpl[T]) GracefulUnregister(ctx context.Context, timeout time.Duration) error {
	//TODO implement me
	panic("implement me")
}

func (h *EventHandlerImpl[T]) ErrCh() <-chan error {
	//TODO implement me
	panic("implement me")
}

func (h *EventHandlerImpl[T]) SetNotifyFunc(fn NotifyFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.notify = fn
}

func (h *EventHandlerImpl[T]) Poll() {
	h.mu.Lock()

	if h.q.Empty() {
		h.mu.Unlock()
		return
	}

	events := h.getEventBatch()
	h.mu.Unlock()

	for _, event := range events {
		err := h.fn(event)
		if err != nil {
			select {
			case h.errCh <- errors.Trace(err):
			default:
				log.Info("workerpool error ignored", logutil.ShortError(err))
			}
			return
		}
	}
}

func (h *EventHandlerImpl[T]) getEventBatch() []T {
	batchSize := h.batchSize
	if batchSize > h.q.Len() {
		batchSize = h.q.Len()
	}

	events, _ := h.q.PopMany(h.batchSize)
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
