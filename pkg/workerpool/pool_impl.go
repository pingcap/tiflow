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

	cerrors "github.com/pingcap/ticdc/pkg/errors"

	"github.com/pingcap/errors"
	"golang.org/x/sync/errgroup"
)

type defaultPoolImpl struct {
	hasher        Hasher
	workers       []*worker
	mu            sync.Mutex
	nextHandlerID int64
}

func newDefaultPoolImpl(hasher Hasher, numWorkers int) *defaultPoolImpl {
	workers := make([]*worker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = newWorker()
	}
	return &defaultPoolImpl{
		hasher:  hasher,
		workers: workers,
	}
}

func (p *defaultPoolImpl) Run(ctx context.Context) error {
	errg, ctx := errgroup.WithContext(ctx)

	for _, worker := range p.workers {
		workerFinal := worker
		errg.Go(func() error {
			err := workerFinal.run(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			return nil
		})
	}

	return errg.Wait()
}

func (p *defaultPoolImpl) RegisterEvent(f func(ctx context.Context, event interface{}) error) EventHandle {
	p.mu.Lock()
	defer p.mu.Unlock()

	handler := &defaultEventHandler{
		f:     f,
		errCh: make(chan error, 1),
		id:    p.nextHandlerID,
	}
	p.nextHandlerID++

	workerID := p.hasher.Hash(handler) % int64(len(p.workers))
	p.workers[workerID].addHandle(handler)
	handler.worker = p.workers[workerID]

	return handler
}

type defaultEventHandler struct {
	f           func(ctx context.Context, event interface{}) error
	isCancelled int32
	errCh       chan error
	worker      *worker
	id          int64

	hasTimer      int32
	lastTimer     time.Time
	timerInterval time.Duration
	timerHandler  func(ctx context.Context) error
}

func (h *defaultEventHandler) AddEvent(ctx context.Context, event interface{}) error {
	if atomic.LoadInt32(&h.isCancelled) == 1 {
		return cerrors.ErrWorkerPoolHandleCancelled.GenWithStackByArgs()
	}

	task := &task{
		handle: h,
		f: func(ctx context.Context) error {
			return h.f(ctx, event)
		},
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case h.worker.taskCh <- task:
	}
	return nil
}

func (h *defaultEventHandler) SetTimer(interval time.Duration, f func(ctx context.Context) error) {
	atomic.StoreInt32(&h.hasTimer, 0)
	h.worker.handleCancelCh <- struct{}{}

	h.timerInterval = interval
	h.timerHandler = f
	atomic.StoreInt32(&h.hasTimer, 1)
}

func (h *defaultEventHandler) Unregister() {
	if !atomic.CompareAndSwapInt32(&h.isCancelled, 0, 1) {
		// already cancelled
		return
	}

	h.worker.handleCancelCh <- struct{}{}
	h.worker.removeHandle(h)
}

func (h *defaultEventHandler) ErrCh() <-chan error {
	return h.errCh
}

func (h *defaultEventHandler) HashCode() int64 {
	return h.id
}

func (h *defaultEventHandler) cancelWithErr(err error) {
	if atomic.SwapInt32(&h.isCancelled, 1) == 1 {
		// Already cancelled
		return
	}
	h.errCh <- err
	close(h.errCh)
}

func (h *defaultEventHandler) durationSinceLastTimer() time.Duration {
	return time.Since(h.lastTimer)
}

func (h *defaultEventHandler) doTimer(ctx context.Context) error {
	if atomic.LoadInt32(&h.hasTimer) == 0 {
		return nil
	}

	if h.durationSinceLastTimer() < h.timerInterval {
		return nil
	}

	if h.timerHandler == nil {
		return nil
	}

	err := h.timerHandler(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	h.lastTimer = time.Now()

	return nil
}

type task struct {
	handle *defaultEventHandler
	f      func(ctx context.Context) error
}

type worker struct {
	taskCh         chan *task
	handles        map[*defaultEventHandler]struct{}
	handleRWLock   sync.RWMutex
	handleCancelCh chan struct{}
}

func newWorker() *worker {
	return &worker{
		taskCh:         make(chan *task, 128000),
		handles:        make(map[*defaultEventHandler]struct{}),
		handleCancelCh: make(chan struct{}), // this channel must be unbuffered, i.e. blocking
	}
}

func (w *worker) run(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case task := <-w.taskCh:
			if task == nil {
				return cerrors.ErrWorkerPoolEmptyTask.GenWithStackByArgs()
			}

			if atomic.LoadInt32(&task.handle.isCancelled) == 1 {
				// ignored cancelled handle
				continue
			}

			err := task.f(ctx)
			if err != nil {
				task.handle.cancelWithErr(err)
			}
		case <-ticker.C:
			w.handleRWLock.RLock()
			for handle := range w.handles {
				err := handle.doTimer(ctx)
				if err != nil {
					if atomic.LoadInt32(&handle.isCancelled) == 1 {
						// ignored cancelled handle
						continue
					}
					handle.cancelWithErr(err)
				}
			}
			w.handleRWLock.RUnlock()
		case <-w.handleCancelCh:
		}
	}
}

func (w *worker) addHandle(handle *defaultEventHandler) {
	w.handleRWLock.Lock()
	defer w.handleRWLock.Unlock()

	w.handles[handle] = struct{}{}
}

func (w *worker) removeHandle(handle *defaultEventHandler) {
	w.handleRWLock.Lock()
	defer w.handleRWLock.Unlock()

	delete(w.handles, handle)
}
