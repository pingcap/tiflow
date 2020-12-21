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
	"sync"
	"sync/atomic"
	"time"
)

type mergedContext struct {
	doneChMu    sync.Mutex
	hasDoneCh   int32
	doneCh      chan struct{}
	isCancelled int32
	cancelCh    chan struct{}

	ctx1 context.Context
	ctx2 context.Context
}

// MergeContexts merges two contexts.
// In case of conflicting errors or values, ctx1 takes priority.
func MergeContexts(ctx1, ctx2 context.Context) (context.Context, context.CancelFunc) {
	cancelCh := make(chan struct{})
	ret := &mergedContext{
		ctx1:     ctx1,
		ctx2:     ctx2,
		cancelCh: cancelCh,
	}

	cancelFunc := func() {
		if atomic.SwapInt32(&ret.isCancelled, 1) == 0 {
			close(cancelCh)
		}
	}
	return ret, cancelFunc
}

func (m *mergedContext) Deadline() (deadline time.Time, ok bool) {
	ddl1, ok1 := m.ctx1.Deadline()
	ddl2, ok2 := m.ctx2.Deadline()

	if ok1 && ok2 {
		if ddl2.After(ddl1) {
			return ddl1, true
		}
		return ddl2, true
	}

	if ok1 {
		return ddl1, true
	}

	return ddl2, ok2
}

func (m *mergedContext) Done() <-chan struct{} {
	// Using an atomic operation to check if the channel has already been created.
	// The makes sure that all calls to Done, except for the first few, are block-free.
	if atomic.LoadInt32(&m.hasDoneCh) == 1 {
		// fast path
		return m.doneCh
	}

	// slow path using mutex
	m.doneChMu.Lock()
	defer m.doneChMu.Unlock()

	// Several goroutines could have attempted to lock the mutex,
	// only the first one should create the channel.
	// So we check if m.hasDoneCh is still 0.
	if atomic.LoadInt32(&m.hasDoneCh) == 0 {
		m.doneCh = make(chan struct{})
		atomic.StoreInt32(&m.hasDoneCh, 1)

		go func() {
			select {
			case <-m.ctx1.Done():
			case <-m.ctx2.Done():
			case <-m.cancelCh:
			}
			close(m.doneCh)
		}()
	}

	return m.doneCh
}

func (m *mergedContext) Err() error {
	if m.ctx1.Err() != nil {
		return m.ctx1.Err()
	}
	return m.ctx2.Err()
}

func (m *mergedContext) Value(key interface{}) interface{} {
	if v1 := m.ctx1.Value(key); v1 != nil {
		return v1
	}
	return m.ctx2.Value(key)
}
