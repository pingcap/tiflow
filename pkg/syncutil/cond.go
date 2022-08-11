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

package syncutil

import (
	"context"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/errors"
)

// Cond is like to regular sync.Cond with enhancement with respective to
// cancellability.
type Cond struct {
	L  sync.Locker
	ch unsafe.Pointer
}

// NewCond creates a new Cond.
func NewCond(l sync.Locker) *Cond {
	ch := make(chan struct{})
	return &Cond{
		L:  l,
		ch: unsafe.Pointer(&ch),
	}
}

// Wait waits on the condition variable.
func (c *Cond) Wait() {
	ch := c.getCh()
	c.L.Unlock()
	<-ch
	c.L.Lock()
}

// WaitWithContext waits on the condition variable until the context is canceled
// or until Broadcast is called.
// The lock is NOT re-locked if ctx is canceled.
func (c *Cond) WaitWithContext(ctx context.Context) error {
	ch := c.getCh()
	c.L.Unlock()
	select {
	case <-ch:
		c.L.Lock()
		return nil
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	}
}

func (c *Cond) getCh() <-chan struct{} {
	ptr := atomic.LoadPointer(&c.ch)
	return *((*chan struct{})(ptr))
}

// Broadcast wakes up all the waiters.
func (c *Cond) Broadcast() {
	ch := make(chan struct{})
	old := atomic.SwapPointer(&c.ch, unsafe.Pointer(&ch))
	close(*(*chan struct{})(old))
}
