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
	"time"
)

type errCtx struct {
	ctx context.Context

	mu     sync.Mutex
	cancel context.CancelFunc
	err    error
}

func newErrCtx(parent context.Context) *errCtx {
	ctx, cancel := context.WithCancel(parent)
	return &errCtx{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (c *errCtx) doCancel(err error) {
	if err == nil {
		panic("errctx: internal error: missing cancel error")
	}

	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return // already canceled
	}
	c.err = err
	c.cancel()
	c.mu.Unlock()
}

func (c *errCtx) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c *errCtx) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *errCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	return c.ctx.Err()
}

func (c *errCtx) Value(key any) any {
	return c.ctx.Value(key)
}
