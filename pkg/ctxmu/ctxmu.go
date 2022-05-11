// Copyright 2012-2020, Hǎi-Liàng “Hal” Wáng
// Copyright 2022 PingCAP, Inc.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or at
// https://developers.google.com/open-source/licenses/bsd
//
// original code: https://h12.io/article/go-pattern-context-aware-lock

package ctxmu

import "context"

// CtxMutex implements a context aware lock
type CtxMutex struct {
	ch chan struct{}
}

// New creates a new CtxMutex
func New() *CtxMutex {
	return &CtxMutex{
		ch: make(chan struct{}, 1),
	}
}

// Lock acquires a lock, it can be canceled by context
func (mu *CtxMutex) Lock(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case mu.ch <- struct{}{}:
		return true
	}
}

// Unlock releases the acquired lock
func (mu *CtxMutex) Unlock() {
	<-mu.ch
}

// Locked checks whether the lock is hold
func (mu *CtxMutex) Locked() bool {
	return len(mu.ch) > 0 // locked or not
}
