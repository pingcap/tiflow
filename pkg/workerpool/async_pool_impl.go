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

	"github.com/pingcap/errors"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"golang.org/x/sync/errgroup"
)

const (
	backoffBaseDelayInMs = 1
	maxTries             = 25
)

type defaultAsyncPoolImpl struct {
	workers      []*asyncWorker
	nextWorkerID int32
	isRunning    int32
	runningLock  sync.RWMutex
}

// NewDefaultAsyncPool creates a new AsyncPool that uses the default implementation
func NewDefaultAsyncPool(numWorkers int) AsyncPool {
	return newDefaultAsyncPoolImpl(numWorkers)
}

func newDefaultAsyncPoolImpl(numWorkers int) *defaultAsyncPoolImpl {
	workers := make([]*asyncWorker, numWorkers)

	return &defaultAsyncPoolImpl{
		workers: workers,
	}
}

func (p *defaultAsyncPoolImpl) Go(ctx context.Context, f func()) error {
	if p.doGo(ctx, f) == nil {
		return nil
	}

	err := retry.Do(ctx, func() error {
		return errors.Trace(p.doGo(ctx, f))
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs), retry.WithMaxTries(maxTries), retry.WithIsRetryableErr(isRetryable))
	return errors.Trace(err)
}

func isRetryable(err error) bool {
	return cerrors.IsRetryableError(err) && cerrors.ErrAsyncPoolExited.Equal(err)
}

func (p *defaultAsyncPoolImpl) doGo(ctx context.Context, f func()) error {
	p.runningLock.RLock()
	defer p.runningLock.RUnlock()

	if atomic.LoadInt32(&p.isRunning) == 0 {
		return cerrors.ErrAsyncPoolExited.GenWithStackByArgs()
	}

	task := &asyncTask{f: f}
	worker := p.workers[int(atomic.AddInt32(&p.nextWorkerID, 1))%len(p.workers)]

	worker.chLock.RLock()
	defer worker.chLock.RUnlock()

	if atomic.LoadInt32(&worker.isClosed) == 1 {
		return cerrors.ErrAsyncPoolExited.GenWithStackByArgs()
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case worker.inputCh <- task:
	}

	return nil
}

func (p *defaultAsyncPoolImpl) Run(ctx context.Context) error {
	p.prepare()
	errg := errgroup.Group{}

	p.runningLock.Lock()
	atomic.StoreInt32(&p.isRunning, 1)
	p.runningLock.Unlock()

	defer func() {
		p.runningLock.Lock()
		atomic.StoreInt32(&p.isRunning, 0)
		p.runningLock.Unlock()
	}()

	errCh := make(chan error, len(p.workers))
	defer close(errCh)

	for _, worker := range p.workers {
		workerFinal := worker
		errg.Go(func() error {
			err := workerFinal.run()
			if err != nil && cerrors.ErrAsyncPoolExited.NotEqual(errors.Cause(err)) {
				errCh <- err
			}
			return nil
		})
	}

	errg.Go(func() error {
		var err error
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case err = <-errCh:
		}

		for _, worker := range p.workers {
			worker.close()
		}

		return err
	})

	return errors.Trace(errg.Wait())
}

func (p *defaultAsyncPoolImpl) prepare() {
	for i := range p.workers {
		p.workers[i] = newAsyncWorker()
	}
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

func (w *asyncWorker) run() error {
	for {
		task := <-w.inputCh
		if task == nil {
			return cerrors.ErrAsyncPoolExited.GenWithStackByArgs()
		}
		task.f()
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
