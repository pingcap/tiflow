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
	errMu  sync.RWMutex
	errVal error

	doneCh chan struct{}
}

// NewErrCenter creates a new ErrCenter.
func NewErrCenter() *ErrCenter {
	return &ErrCenter{
		doneCh: make(chan struct{}),
	}
}

// OnError receivers an error, if the error center has received one, drops the
// new error and records a warning log. Otherwise the error will be recorded and
// doneCh will be closed to use as notification.
func (c *ErrCenter) OnError(err error) {
	c.errMu.Lock()
	defer c.errMu.Unlock()

	if err == nil {
		return
	}
	if c.errVal != nil {
		// OnError is no-op after the first call with
		// a non-nil error.
		log.L().Warn("More than one error is received",
			zap.Error(err))
		return
	}
	c.errVal = err

	close(c.doneCh)
}

// CheckError retusn the recorded error
func (c *ErrCenter) CheckError() error {
	c.errMu.RLock()
	defer c.errMu.RUnlock()

	return c.errVal
}

// WithCancelOnFirstError creates an error context which supports is cancelled on error
func (c *ErrCenter) WithCancelOnFirstError(ctx context.Context) context.Context {
	return newErrCtx(ctx, c)
}
