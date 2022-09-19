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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/notify"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	workerPoolDefaultClockSourceInterval = time.Millisecond * 100
)

type defaultPoolImpl struct {
	// assume the hasher to be the trivial hasher for now
	hasher Hasher
	// do not resize this slice after creating the pool
	workers []*worker
	// used to generate handler IDs, must be accessed atomically
	nextHandlerID int64
}

// NewDefaultWorkerPool creates a new WorkerPool that uses the default implementation
func NewDefaultWorkerPool(numWorkers int) WorkerPool {
	return newDefaultPoolImpl(&defaultHasher{}, numWorkers)
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
	handler := &defaultEventHandle{
		f:     f,
		errCh: make(chan error, 1),
		id:    atomic.AddInt64(&p.nextHandlerID, 1) - 1,
	}

	workerID := p.hasher.Hash(handler) % int64(len(p.workers))
	p.workers[workerID].addHandle(handler)
	handler.worker = p.workers[workerID]

	return handler
}

type handleStatus = int32

const (
	handleRunning = handleStatus(iota)
	handleCancelling
	handleCancelled
)

type defaultEventHandle struct {
	// the function to be run each time the event is triggered
	f func(ctx context.Context, event interface{}) error
	// must be accessed atomically
	status handleStatus
	// channel for the error returned by f
	errCh chan error
	// the worker that the handle is associated with
	worker *worker
	// identifier for this handle. No significant usage for now.
	// Might be used to support consistent hashing in the future,
	// so that the pool can be resized efficiently.
	id int64

	// whether there is a valid timer handler, must be accessed atomically
	hasTimer int32
	// the time when timer was triggered the last time
	lastTimer time.Time
	// minimum interval between two timer calls
	timerInterval time.Duration
	// the handler for the timer
	timerHandler func(ctx context.Context) error

	// whether this is a valid errorHandler, must be accessed atomically
	hasErrorHandler int32
	// the error handler, called when the handle meets an error (which is returned by f)
	errorHandler func(err error)
}

func (h *defaultEventHandle) AddEvent(ctx context.Context, event interface{}) error {
	status := atomic.LoadInt32(&h.status)
	if status != handleRunning {
		return cerrors.ErrWorkerPoolHandleCancelled.GenWithStackByArgs()
	}

	failpoint.Inject("addEventDelayPoint", func() {})

	task := task{
		handle: h,
		f: func(ctx1 context.Context) error {
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

func (h *defaultEventHandle) AddEvents(ctx context.Context, events []interface{}) error {
	status := atomic.LoadInt32(&h.status)
	if status != handleRunning {
		return cerrors.ErrWorkerPoolHandleCancelled.GenWithStackByArgs()
	}

	failpoint.Inject("addEventDelayPoint", func() {})

	task := task{
		handle: h,
		f: func(ctx1 context.Context) error {
			for _, event := range events {
				err := h.f(ctx, event)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case h.worker.taskCh <- task:
	}
	return nil
}

func (h *defaultEventHandle) SetTimer(ctx context.Context, interval time.Duration, f func(ctx context.Context) error) EventHandle {
	// mark the timer handler function as invalid
	atomic.StoreInt32(&h.hasTimer, 0)
	// wait for `hasTimer` to take effect, otherwise we might have a data race, if there was a previous handler.
	h.worker.synchronize()

	h.timerInterval = interval
	h.timerHandler = func(ctx1 context.Context) error {
		return f(ctx)
	}
	// mark the timer handler function as valid
	atomic.StoreInt32(&h.hasTimer, 1)

	return h
}

func (h *defaultEventHandle) Unregister() {
	if !atomic.CompareAndSwapInt32(&h.status, handleRunning, handleCancelled) {
		// call synchronize so that the returning of Unregister cannot race
		// with the calling of the errorHandler, if an error is already being processed.
		h.worker.synchronize()
		// already cancelled
		return
	}

	failpoint.Inject("unregisterDelayPoint", func() {})

	// call synchronize so that all function executions related to this handle will be
	// linearized BEFORE Unregister.
	h.worker.synchronize()

	h.doCancel(cerrors.ErrWorkerPoolHandleCancelled.GenWithStackByArgs())
}

func (h *defaultEventHandle) GracefulUnregister(ctx context.Context, timeout time.Duration) error {
	if !atomic.CompareAndSwapInt32(&h.status, handleRunning, handleCancelling) {
		// already cancelling or cancelled
		return nil
	}

	defer func() {
		if !atomic.CompareAndSwapInt32(&h.status, handleCancelling, handleCancelled) {
			// already cancelled
			return
		}

		// call synchronize so that all function executions related to this handle will be
		// linearized BEFORE Unregister.
		h.worker.synchronize()
		h.doCancel(cerrors.ErrWorkerPoolHandleCancelled.GenWithStackByArgs())
	}()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	doneCh := make(chan struct{})
	select {
	case <-ctx.Done():
		return cerrors.ErrWorkerPoolGracefulUnregisterTimedOut.GenWithStackByArgs()
	case h.worker.taskCh <- task{
		handle: h,
		doneCh: doneCh,
	}:
	}

	select {
	case <-ctx.Done():
		return cerrors.ErrWorkerPoolGracefulUnregisterTimedOut.GenWithStackByArgs()
	case <-doneCh:
	}

	return nil
}

// callers of doCancel need to check h.isCancelled first.
// DO NOT call doCancel multiple times on the same handle.
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
	if !atomic.CompareAndSwapInt32(&h.status, handleRunning, handleCancelled) {
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

	doneCh chan struct{} // only used in implementing GracefulUnregister
}

type worker struct {
	taskCh       chan task
	handles      map[*defaultEventHandle]struct{}
	handleRWLock sync.RWMutex
	// A message is passed to handleCancelCh when we need to wait for the
	// current execution of handler to finish. Should be BLOCKING.
	handleCancelCh chan struct{}
	// must be accessed atomically
	isRunning int32
	// notifies exits of run()
	stopNotifier notify.Notifier

	slowSynchronizeThreshold time.Duration
	slowSynchronizeLimiter   *rate.Limiter
}

func newWorker() *worker {
	return &worker{
		taskCh:         make(chan task, 128),
		handles:        make(map[*defaultEventHandle]struct{}),
		handleCancelCh: make(chan struct{}), // this channel must be unbuffered, i.e. blocking

		slowSynchronizeThreshold: 10 * time.Second,
		slowSynchronizeLimiter:   rate.NewLimiter(rate.Every(time.Second*5), 1),
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
			if atomic.LoadInt32(&task.handle.status) == handleCancelled {
				// ignored cancelled handle
				continue
			}

			if task.doneCh != nil {
				close(task.doneCh)
				if task.f != nil {
					log.L().DPanic("unexpected message handler func in cancellation task", zap.Stack("stack"))
				}
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
				if atomic.LoadInt32(&handle.status) == handleCancelled {
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

			// cancelWithErr must be called out side of the loop above,
			// to avoid deadlock.
			for _, handleErr := range handleErrs {
				handleErr.h.cancelWithErr(handleErr.e)
			}
		case <-w.handleCancelCh:
		}
	}
}

// synchronize waits for the worker to loop at least once, or to exit.
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

		if time.Since(startTime) > w.slowSynchronizeThreshold &&
			w.slowSynchronizeLimiter.Allow() {
			// likely the workerpool has deadlocked, or there is a bug
			// in the event handlers.
			logWarn("synchronize is taking too long, report a bug",
				zap.Duration("duration", time.Since(startTime)),
				zap.Stack("stacktrace"))
		}
	}
}

// A delegate to log.Warn. It exists only for testing.
var logWarn = log.Warn

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
