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

package util

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
)

// WaitSomething waits for something done with `true`, it retrys for nRetry times at most
func WaitSomething(nRetry int, waitTime time.Duration, fn func() bool) bool {
	for i := 0; i < nRetry-1; i++ {
		if fn() {
			return true
		}

		time.Sleep(waitTime)
	}
	return fn()
}

// HandleErr receives error from an error channel, until the context is Done
func HandleErr(ctx context.Context, errCh <-chan error, errFn func(error)) {
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errCh:
			errFn(err)
		}
	}
}

// HandleErrWithErrGroup creates a `errgroup.Group` and calls `HandleErr` within the error group
func HandleErrWithErrGroup(ctx context.Context, errCh <-chan error, errFn func(error)) *errgroup.Group {
	errg, cctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		HandleErr(cctx, errCh, errFn)
		return nil
	})
	return errg
}

// Must panics if err is not nil.
func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
