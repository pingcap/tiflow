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

package errctx

import (
	"context"
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// ErrCenter is used to receive errors and provide
// ways to detect the error(s).
type ErrCenter struct {
	rwm      sync.RWMutex
	firstErr error
	children map[*errCtx]struct{}
}

// NewErrCenter creates a new ErrCenter.
func NewErrCenter() *ErrCenter {
	return &ErrCenter{}
}

func (c *ErrCenter) removeChild(child *errCtx) {
	c.rwm.Lock()
	defer c.rwm.Unlock()

	delete(c.children, child)
}

// OnError receivers an error, if the error center has received one, drops the
// new error and records a warning log. Otherwise the error will be recorded and
// doneCh will be closed to use as notification.
func (c *ErrCenter) OnError(err error) {
	if err == nil {
		return
	}

	c.rwm.Lock()
	defer c.rwm.Unlock()

	if c.firstErr != nil {
		// OnError is no-op after the first call with
		// a non-nil error.
		log.Warn("More than one error is received",
			zap.Error(err))
		return
	}
	c.firstErr = err
	for child := range c.children {
		child.doCancel(c.firstErr)
	}
	c.children = nil
}

// CheckError retusn the recorded error
func (c *ErrCenter) CheckError() error {
	c.rwm.RLock()
	defer c.rwm.RUnlock()

	return c.firstErr
}

// WithCancelOnFirstError creates an error context which will cancel the context when the first error is received.
func (c *ErrCenter) WithCancelOnFirstError(ctx context.Context) (context.Context, context.CancelFunc) {
	ec := newErrCtx(ctx)

	c.rwm.Lock()
	defer c.rwm.Unlock()

	if c.firstErr != nil {
		// First error is received, cancel the context directly.
		ec.doCancel(c.firstErr)
	} else {
		if c.children == nil {
			c.children = make(map[*errCtx]struct{})
		}
		c.children[ec] = struct{}{}
	}
	return ec, func() {
		ec.doCancel(context.Canceled)
		c.removeChild(ec)
	}
}
