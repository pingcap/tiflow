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

package context

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// CustomErrCanceler represents a function used to cancel a CustomCancelContext
type CustomErrCanceler = func(err error)

// WithCustomErrCancel produces a context.Context that can be canceled with a custom error
func WithCustomErrCancel(ctx context.Context) (context.Context, CustomErrCanceler) {
	ret := &CustomCancelContext{
		inner:    ctx,
		cancelCh: make(chan error, 1),
	}

	cancelF := func(err error) {
		if atomic.SwapInt32(&ret.isCanceled, 1) == 1 {
			return
		}
		ret.cancelCh <- err
		close(ret.cancelCh)
	}

	return ret, cancelF
}

// CustomCancelContext is a cancelable context that returns a custom error.
type CustomCancelContext struct {
	mu     sync.Mutex
	doneCh chan struct{}

	errRWLock sync.RWMutex
	err       error

	inner      context.Context
	cancelCh   chan error
	isCanceled int32
}

// Deadline implements context.Context
func (c *CustomCancelContext) Deadline() (deadline time.Time, ok bool) {
	return c.inner.Deadline()
}

// Done implements context.Context
func (c *CustomCancelContext) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	// c.doneCh and its associated Goroutine is created lazily on first access
	if c.doneCh == nil {
		doneCh := make(chan struct{})
		go func() {
			var err error
			select {
			case err = <-c.cancelCh:
				break
			case <-c.inner.Done():
				err = c.inner.Err()
			}

			c.errRWLock.Lock()
			defer c.errRWLock.Unlock()
			c.err = err
			// Note that doneCh must be closed before releasing `errRWLock`
			close(doneCh)
		}()
		c.doneCh = doneCh
	}

	return c.doneCh
}

// Err implements context.Context
func (c *CustomCancelContext) Err() error {
	c.errRWLock.RLock()
	defer c.errRWLock.RUnlock()

	return c.err
}

// Value implements context.Context
func (c *CustomCancelContext) Value(key interface{}) interface{} {
	return c.inner.Value(key)
}
