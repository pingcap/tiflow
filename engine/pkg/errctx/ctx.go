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
)

type errCtx struct {
	context.Context
	center *ErrCenter

	once   sync.Once
	doneCh <-chan struct{}
}

func newErrCtx(parent context.Context, center *ErrCenter) *errCtx {
	return &errCtx{
		Context: parent,
		center:  center,
	}
}

func (c *errCtx) Done() <-chan struct{} {
	c.once.Do(func() {
		doneCh := make(chan struct{})

		go func() {
			select {
			case <-c.center.doneCh:
			case <-c.Context.Done():
			}

			close(doneCh)
		}()

		c.doneCh = doneCh
	})
	return c.doneCh
}

func (c *errCtx) Err() error {
	if err := c.center.CheckError(); err != nil {
		return err
	}

	return c.Context.Err()
}
