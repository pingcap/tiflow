// Copyright 2020 PingCAP, Inc.
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

package workerpool

import (
	"context"
	"time"
)

// WorkerPool runs a number of Goroutines that process the submitted events.
// Each EventHandle is bound to a fixed worker thread to prevent data races.
type WorkerPool interface {
	// RegisterEvent returns a handle that can be used to trigger the execution of `f`.
	// `f` will be called with a context that is a child of the context with which Run is called.
	// TODO more reasonable usage of contexts, potentially involving context merging.
	RegisterEvent(f func(ctx context.Context, event interface{}) error) EventHandle

	// Run runs the WorkerPool.
	// Internally several Goroutines are spawned.
	Run(ctx context.Context) error
}

// EventHandle is a handle for a registered event.
// Since events are executed asynchronously, errors should be collected from ErrCh().
// EventHandles SHOULD NOT be assumed to be thread safe.
type EventHandle interface {
	// AddEvent adds an `event` object to the internal queue, so that the `f` used to register the handle can be called.
	// Note: events are always processed in the order they are added.
	// Unregistering the EventHandle MAY CAUSE EVENT LOSSES. But for an event lost, any event after it is guaranteed to be lost too.
	// Cancelling `ctx` here will cancel the on-going or next execution of the event.
	AddEvent(ctx context.Context, event interface{}) error

	// AddEvents is like AddEvent but retrieves a slice instead of an object.
	AddEvents(ctx context.Context, events []interface{}) error

	// SetTimer is used to provide a function that is periodic called, as long as the EventHandle has not been unregistered.
	// The current implementation uses as the base clock source a ticker whose interval is the const workerPoolDefaultClockSourceInterval.
	// DO NOT set an interval less than workerPoolDefaultClockSourceInterval.
	// Cancelling `ctx` here will cancel the on-going or next execution of `f`.
	SetTimer(ctx context.Context, interval time.Duration, f func(ctx context.Context) error) EventHandle

	// Unregister removes the EventHandle from the WorkerPool.
	// Note: Unregister WILL block until the operation has taken effect, i.e. the handler will not be executed after
	// Unregister returns. Unregister WILL NOT attempt to wait for pending events to complete, which means the last few events can be lost.
	Unregister()

	// GracefulUnregister removes the EventHandle after
	// all pending events have been processed.
	GracefulUnregister(ctx context.Context, timeout time.Duration) error

	// ErrCh returns a channel that outputs the first non-nil result of events submitted to this EventHandle.
	// Note that a non-nil result of an event cancels the EventHandle, so there is at most one error.
	ErrCh() <-chan error

	// OnExit is used to provide a function that will be called when the handle exits abnormally.
	OnExit(f func(err error)) EventHandle
}
