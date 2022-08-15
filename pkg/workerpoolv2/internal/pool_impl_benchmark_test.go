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

package internal

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/atomic"
)

func BenchmarkPool(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := NewPoolImpl(1)
	go func() {
		_ = pool.Run(ctx)
	}()

	done := make(chan struct{})
	var lastN atomic.Int64
	handle := NewEventHandlerImpl(func(n int64) error {
		if n != lastN.Add(1)-1 && n > 0 {
			panic("unreachable")
		}
		if n == int64(b.N-1) {
			close(done)
		}
		return nil
	}, 1024, 1024)
	pool.RegisterHandle(handle)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = handle.AddEvent(ctx, int64(i))
	}

	<-done
}

func BenchmarkPoolV1(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := workerpool.NewDefaultWorkerPool(1)
	go func() {
		_ = pool.Run(ctx)
	}()

	done := make(chan struct{})
	var lastN atomic.Int64
	handle := pool.RegisterEvent(func(ctx context.Context, ev interface{}) error {
		n := ev.(int64)
		if n != lastN.Add(1)-1 && n > 0 {
			panic("unreachable")
		}
		if n == int64(b.N-1) {
			close(done)
		}
		return nil
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = handle.AddEvent(ctx, int64(i))
	}

	<-done
}

const (
	concurrency = 16
)

func BenchmarkPoolContention(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := NewPoolImpl(1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		_ = pool.Run(ctx)
	}()

	var (
		done    []chan struct{}
		handles []*EventHandleImpl[int64]
	)
	for i := 0; i < concurrency; i++ {
		i := i
		done = append(done, make(chan struct{}))
		var lastN atomic.Int64
		handle := NewEventHandlerImpl(func(n int64) error {
			if n != lastN.Add(1)-1 && n > 0 {
				panic("unreachable")
			}
			if n == int64(b.N-1) {
				close(done[i])
			}
			return nil
		}, 1024, 128)
		pool.RegisterHandle(handle)
		handles = append(handles, handle)
	}

	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < b.N; j++ {
				_ = handles[i].AddEvent(ctx, int64(j))
			}

			<-done[i]
		}()
	}

	wg.Wait()
}

func BenchmarkPoolContentionV1(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := workerpool.NewDefaultWorkerPool(1)
	go func() {
		_ = pool.Run(ctx)
	}()

	var (
		done    []chan struct{}
		handles []workerpool.EventHandle
	)
	for i := 0; i < concurrency; i++ {
		i := i
		done = append(done, make(chan struct{}))
		var lastN atomic.Int64
		handle := pool.RegisterEvent(func(ctx context.Context, ev interface{}) error {
			n := ev.(int64)
			if n != lastN.Add(1)-1 && n > 0 {
				panic("unreachable")
			}
			if n == int64(b.N-1) {
				close(done[i])
			}
			return nil
		})
		handles = append(handles, handle)
	}

	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < b.N; j++ {
				_ = handles[i].AddEvent(ctx, int64(j))
			}

			<-done[i]
		}()
	}

	wg.Wait()
}
