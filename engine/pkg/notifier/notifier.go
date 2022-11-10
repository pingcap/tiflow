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

package notifier

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiflow/engine/pkg/containers"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
)

type receiverID = int64

// Notifier is the sending endpoint of an event
// notification mechanism. It broadcasts a stream of
// events to a number of receivers.
type Notifier[T any] struct {
	receivers sync.Map // receiverID -> *Receiver[T]
	nextID    atomic.Int64

	// queue is unbounded.
	queue *containers.SliceQueue[T]

	closed        atomic.Bool
	closeCh       chan struct{}
	synchronizeCh chan struct{}

	wg sync.WaitGroup
}

// Receiver is the receiving endpoint of a single-producer-multiple-consumer
// notification mechanism.
type Receiver[T any] struct {
	// C is a channel to read the events from.
	// Note that it is part of the public interface of this package.
	C chan T

	id receiverID

	closeOnce sync.Once

	// closed MUST be closed before closing `C`.
	closed chan struct{}

	notifier *Notifier[T]
}

// Close closes the receiver
func (r *Receiver[T]) Close() {
	r.closeOnce.Do(
		func() {
			close(r.closed)
			// Waits for the synchronization barrier, which
			// means that run() has finished the last iteration,
			// and since we have set `closed` to true, the `C` channel,
			// will not be written to anymore. So it is safe to close it now.
			<-r.notifier.synchronizeCh
			close(r.C)
			r.notifier.receivers.Delete(r.id)
		})
}

// NewNotifier creates a new Notifier.
func NewNotifier[T any]() *Notifier[T] {
	ret := &Notifier[T]{
		receivers:     sync.Map{},
		queue:         containers.NewSliceQueue[T](),
		closeCh:       make(chan struct{}),
		synchronizeCh: make(chan struct{}),
	}

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		ret.run()
	}()
	return ret
}

// NewReceiver creates a new Receiver associated with
// the given Notifier.
func (n *Notifier[T]) NewReceiver() *Receiver[T] {
	ch := make(chan T, 16)
	receiver := &Receiver[T]{
		id:       n.nextID.Add(1),
		C:        ch,
		closed:   make(chan struct{}),
		notifier: n,
	}

	n.receivers.Store(receiver.id, receiver)
	return receiver
}

// Notify sends a new notification event.
func (n *Notifier[T]) Notify(event T) {
	n.queue.Push(event)
}

// Close closes the notifier.
func (n *Notifier[T]) Close() {
	if n.closed.Swap(true) {
		// Ensures idempotency of closing once.
		return
	}

	close(n.closeCh)
	n.wg.Wait()

	n.receivers.Range(func(_, value any) bool {
		receiver := value.(*Receiver[T])
		receiver.Close()
		return true
	})
}

// Flush flushes all pending notifications.
// Note that for Flush to work as expected, a
// quiescent period is required, i.e. you should
// not send more events until Flush returns.
func (n *Notifier[T]) Flush(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-n.closeCh:
			return nil
		case <-n.synchronizeCh:
			// Checks the queue size after each iteration
			// of run().
		}

		if n.queue.Size() == 0 {
			return nil
		}
	}
}

func (n *Notifier[T]) run() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	defer func() {
		close(n.synchronizeCh)
	}()

	for {
		select {
		case <-n.closeCh:
			return
		case n.synchronizeCh <- struct{}{}:
			// no-op here. Just a synchronization barrier.
		case <-n.queue.C:
		Inner:
			for {
				event, ok := n.queue.Pop()
				if !ok {
					break Inner
				}

				// TODO In the current implementation, congestion
				// in once receiver will prevent all other receivers
				// from receiving events.
				n.receivers.Range(func(_, value any) bool {
					receiver := value.(*Receiver[T])

					select {
					case <-n.closeCh:
						return false
					case <-receiver.closed:
						// Receiver has been closed.
					case receiver.C <- event:
						// send the event to the receiver.
					}
					return true
				})

				select {
				case <-n.closeCh:
					return
				default:
				}
			}
		}
	}
}
