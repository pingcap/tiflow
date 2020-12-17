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
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"

	"github.com/pingcap/errors"
	"golang.org/x/sync/errgroup"
)

type defaultAsyncPoolImpl struct {
	workers      []*asyncWorker
	nextWorkerID int32
}

// NewDefaultAsyncPool creates a new AsyncPool that uses the default implementation
func NewDefaultAsyncPool() AsyncPool {
	return newDefaultAsyncPoolImpl(runtime.NumCPU() * 2)
}

func newDefaultAsyncPoolImpl(numWorkers int) *defaultAsyncPoolImpl {
	workers := make([]*asyncWorker, numWorkers)
	for i := range workers {
		workers[i] = newAsyncWorker()
	}

	return &defaultAsyncPoolImpl{
		workers: workers,
	}
}

func (p *defaultAsyncPoolImpl) Go(ctx context.Context, f func()) error {
	task := &asyncTask{f: f}
	worker := p.workers[int(atomic.AddInt32(&p.nextWorkerID, 1))%len(p.workers)]

	worker.chLock.RLock()
	defer worker.chLock.RUnlock()

	if atomic.LoadInt32(&worker.isClosed) == 1 {
		return nil
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case worker.inputCh <- task:
	}

	return nil
}

func (p *defaultAsyncPoolImpl) Run(ctx context.Context) error {
	errg := &errgroup.Group{}

	subctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, worker := range p.workers {
		workerFinal := worker
		errg.Go(func() error {
			return workerFinal.run(subctx)
		})
	}

	errg.Go(func() error {
		<-ctx.Done()

		log.Info("AsyncPool Cancelled")
		for _, worker := range p.workers {
			worker.close()
		}

		return nil
	})

	return errors.Trace(errg.Wait())
}

type asyncTask struct {
	f func()
}

type asyncWorker struct {
	inputCh  chan *asyncTask
	isClosed int32
	chLock   sync.RWMutex
}

func newAsyncWorker() *asyncWorker {
	return &asyncWorker{inputCh: make(chan *asyncTask, 12800)}
}

func (w *asyncWorker) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case task := <-w.inputCh:
			if task == nil {
				return nil
			}

			task.f()
		}
	}
}

func (w *asyncWorker) close() {
	if atomic.SwapInt32(&w.isClosed, 1) == 1 {
		return
	}

	w.chLock.Lock()
	defer w.chLock.Unlock()

	close(w.inputCh)
}
