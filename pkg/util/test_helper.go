// Copyright 2019 PingCAP, Inc.
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
	"sync"
	"time"
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

// RecvErrorUntilContextDone receives error from an error channel, until the context is Done
func RecvErrorUntilContextDone(ctx context.Context, wg sync.WaitGroup, errCh <-chan error, errFn func(e error)) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errCh:
				errFn(err)
			}
		}
	}()
}
