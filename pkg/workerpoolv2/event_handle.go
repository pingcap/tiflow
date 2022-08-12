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

package workerpoolv2

import (
	"context"
	"github.com/pingcap/tiflow/pkg/workerpoolv2/internal"
	"time"
)

// EventHandle is a handle for a registered callback in the workerpool.
// Since events are executed asynchronously, errors should be collected from ErrCh().
// EventHandles SHOULD NOT be assumed to be thread safe.
type EventHandle[T any] interface {
	// AddEvent adds an `event` object to the internal queue, so that the callback for the handle is called.
	// Note:
	// (1) Events are always processed in the order they are added.
	// (2) Unregistering the EventHandle MAY CAUSE EVENT LOSSES.
	//     But for an event lost, any event after it is guaranteed to be lost too.
	// (3) Cancelling `ctx` here will only cancel this function call, rather than the callback's execution.
	AddEvent(ctx context.Context, event T) error

	// Unregister removes the EventHandle from the WorkerPool.
	// Note:
	// (1) Unregister will block until the operation has taken effect,
	//     i.e. the handler will not be executed after Unregister returns.
	// (2) Unregister WILL NOT attempt to wait for pending events to complete,
	//     which means the last few events can be lost.
	Unregister()

	// GracefulUnregister removes the EventHandle after
	// all pending events have been processed.
	GracefulUnregister(ctx context.Context, timeout time.Duration) error

	// ErrCh returns a channel that outputs the first non-nil error of events submitted to this EventHandle.
	// Note that a non-nil result of an event cancels the EventHandle, so there is at most one error.
	ErrCh() <-chan error

	internal.Poller
}
