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
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/notify"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	workerPoolDefaultClockSourceInterval = time.Millisecond * 100
)

type defaultPoolImpl struct {
	hasher        Hasher
	workers       []*worker
	mu            sync.Mutex
	nextHandlerID int64
}

// NewDefaultWorkerPool creates a new WorkerPool that uses the default implementation
func NewDefaultWorkerPool() WorkerPool {
	return newDefaultPoolImpl(&defaultHasher{}, runtime.NumCPU())
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

	handler := &defaultEventHandle{
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

type defaultEventHandle struct {
	f           func(ctx context.Context, event interface{}) error
	isCancelled int32
	errCh       chan error
	worker      *worker
	id          int64

	hasTimer      int32
	lastTimer     time.Time
	timerInterval time.Duration
	timerHandler  func(ctx context.Context) error

	hasErrorHandler int32
	errorHandler    func(err error)
}

func (h *defaultEventHandle) AddEvent(ctx context.Context, event interface{}) error {
	if atomic.LoadInt32(&h.isCancelled) == 1 {
		return cerrors.ErrWorkerPoolHandleCancelled.GenWithStackByArgs()
	}

	failpoint.Inject("addEventDelayPoint", func() {})

	task := &task{
		handle: h,
		f: func(ctx1 context.Context) error {
			return h.f(MergeContexts(ctx, ctx1), event)
		},
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case h.worker.taskCh <- task:
	}
	return nil
}

func (h *defaultEventHandle) SetTimer(interval time.Duration, f func(ctx context.Context) error) EventHandle {
	atomic.StoreInt32(&h.hasTimer, 0)
	h.worker.synchronize()

	h.timerInterval = interval
	h.timerHandler = f
	atomic.StoreInt32(&h.hasTimer, 1)

	return h
}

func (h *defaultEventHandle) Unregister() {
	if !atomic.CompareAndSwapInt32(&h.isCancelled, 0, 1) {
		// already cancelled
		return
	}

	failpoint.Inject("unregisterDelayPoint", func() {})

	h.worker.synchronize()

	h.doCancel(cerrors.ErrWorkerPoolHandleCancelled.GenWithStackByArgs())
}

func (h *defaultEventHandle) doCancel(err error) {
	h.worker.removeHandle(h)

	if atomic.LoadInt32(&h.hasErrorHandler) == 1 {
		h.errorHandler(err)
	}

	h.errCh <- err
	close(h.errCh)
}

func (h *defaultEventHandle) ErrCh() <-chan error {
	return h.errCh
}

func (h *defaultEventHandle) OnExit(f func(err error)) EventHandle {
	atomic.StoreInt32(&h.hasErrorHandler, 0)
	h.worker.synchronize()
	h.errorHandler = f
	atomic.StoreInt32(&h.hasErrorHandler, 1)
	return h
}

func (h *defaultEventHandle) HashCode() int64 {
	return h.id
}

func (h *defaultEventHandle) cancelWithErr(err error) {
	if !atomic.CompareAndSwapInt32(&h.isCancelled, 0, 1) {
		// already cancelled
		return
	}

	h.doCancel(err)
}

func (h *defaultEventHandle) durationSinceLastTimer() time.Duration {
	return time.Since(h.lastTimer)
}

func (h *defaultEventHandle) doTimer(ctx context.Context) error {
	if atomic.LoadInt32(&h.hasTimer) == 0 {
		return nil
	}

	if h.durationSinceLastTimer() < h.timerInterval {
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
	handle *defaultEventHandle
	f      func(ctx context.Context) error
}

type worker struct {
	taskCh         chan *task
	handles        map[*defaultEventHandle]struct{}
	handleRWLock   sync.RWMutex
	handleCancelCh chan struct{}
	isRunning      int32

	stopNotifier notify.Notifier
}

func newWorker() *worker {
	return &worker{
		taskCh:         make(chan *task, 128000),
		handles:        make(map[*defaultEventHandle]struct{}),
		handleCancelCh: make(chan struct{}), // this channel must be unbuffered, i.e. blocking
	}
}

func (w *worker) run(ctx context.Context) error {
	ticker := time.NewTicker(workerPoolDefaultClockSourceInterval)
	atomic.StoreInt32(&w.isRunning, 1)
	defer func() {
		ticker.Stop()
		atomic.StoreInt32(&w.isRunning, 0)
		w.stopNotifier.Notify()
	}()

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
			var handleErrs []struct {
				h *defaultEventHandle
				e error
			}

			w.handleRWLock.RLock()
			for handle := range w.handles {
				if atomic.LoadInt32(&handle.isCancelled) == 1 {
					// ignored cancelled handle
					continue
				}
				err := handle.doTimer(ctx)
				if err != nil {
					handleErrs = append(handleErrs, struct {
						h *defaultEventHandle
						e error
					}{handle, err})
				}
			}
			w.handleRWLock.RUnlock()

			for _, handleErr := range handleErrs {
				handleErr.h.cancelWithErr(handleErr.e)
			}
		case <-w.handleCancelCh:
		}
	}
}

func (w *worker) synchronize() {
	if atomic.LoadInt32(&w.isRunning) == 0 {
		return
	}

	receiver, err := w.stopNotifier.NewReceiver(time.Millisecond * 100)
	if err != nil {
		if cerrors.ErrOperateOnClosedNotifier.Equal(errors.Cause(err)) {
			return
		}
		log.Panic("unexpected error", zap.Error(err))
	}
	defer receiver.Stop()

	startTime := time.Now()
	for {
		workerHasFinishedLoop := false
		select {
		case w.handleCancelCh <- struct{}{}:
			workerHasFinishedLoop = true
		case <-receiver.C:
		}
		if workerHasFinishedLoop || atomic.LoadInt32(&w.isRunning) == 0 {
			break
		}

		if time.Since(startTime) > time.Second*10 {
			panic("synchronize taking too long")
		}
	}
}

func (w *worker) addHandle(handle *defaultEventHandle) {
	w.handleRWLock.Lock()
	defer w.handleRWLock.Unlock()

	w.handles[handle] = struct{}{}
}

func (w *worker) removeHandle(handle *defaultEventHandle) {
	w.handleRWLock.Lock()
	defer w.handleRWLock.Unlock()

	delete(w.handles, handle)
}
