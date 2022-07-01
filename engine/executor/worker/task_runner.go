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

package worker

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/executor/worker/internal"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
)

// Re-export types for public use
type (
	// Runnable alias internal.Runnable
	Runnable = internal.Runnable
	// RunnableID alias internal.RunnableID
	RunnableID = internal.RunnableID
	// Workloader alias internal.Workloader
	Workloader = internal.Workloader
)

// TaskRunner receives RunnableContainer in a FIFO way, and runs them in
// independent background goroutines.
type TaskRunner struct {
	inQueue chan *internal.RunnableContainer
	tasks   sync.Map
	wg      sync.WaitGroup

	cancelMu sync.RWMutex
	canceled bool

	taskCount atomic.Int64

	clock clock.Clock

	taskStopNotifier *notifier.Notifier[RunnableID]
}

type taskEntry struct {
	*internal.RunnableContainer
	cancel context.CancelFunc
}

// NewTaskRunner creates a new TaskRunner instance
func NewTaskRunner(inQueueSize int, initConcurrency int) *TaskRunner {
	return &TaskRunner{
		inQueue:          make(chan *internal.RunnableContainer, inQueueSize),
		clock:            clock.New(),
		taskStopNotifier: notifier.NewNotifier[RunnableID](),
	}
}

// AddTask enqueues a naked task, and AddTask will wrap the task with internal.WrapRunnable.
// Deprecated. TODO Will be removed once two-phase task dispatching is enabled.
func (r *TaskRunner) AddTask(task Runnable) error {
	wrappedTask := internal.WrapRunnable(task, r.clock.Mono())
	select {
	case r.inQueue <- wrappedTask:
		return nil
	default:
	}

	return cerrors.ErrRuntimeIncomingQueueFull.GenWithStackByArgs()
}

// addWrappedTask enqueues a task already wrapped by internal.WrapRunnable.
// NOTE: internal.RunnableContainer contains the submit-time for the task.
func (r *TaskRunner) addWrappedTask(task *internal.RunnableContainer) error {
	select {
	case r.inQueue <- task:
		return nil
	default:
	}

	return cerrors.ErrRuntimeIncomingQueueFull.GenWithStackByArgs()
}

// Run runs forever until context is canceled or task queue is closed.
// It receives new added task and call onNewTask with task
func (r *TaskRunner) Run(ctx context.Context) error {
	defer r.cancelAll()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case task := <-r.inQueue:
			if task == nil {
				return cerrors.ErrRuntimeIsClosed.GenWithStackByArgs()
			}
			if err := r.onNewTask(task); err != nil {
				log.L().Warn("Failed to launch task",
					zap.String("id", task.ID()),
					zap.Error(err))
			}
		}
	}
}

// Workload returns total workload of task runner
func (r *TaskRunner) Workload() (ret model.RescUnit) {
	r.tasks.Range(func(key, value interface{}) bool {
		container := value.(*taskEntry).RunnableContainer
		if container.Status() != internal.TaskRunning {
			// Skip tasks that are not currently running
			return true
		}
		workloader, ok := container.Runnable.(Workloader)
		if !ok {
			return true
		}
		workload := workloader.Workload()
		ret += workload
		return true
	})
	return
}

// WorkerCount returns the number of currently running workers.
func (r *TaskRunner) WorkerCount() int64 {
	return r.taskCount.Load()
}

func (r *TaskRunner) cancelAll() {
	r.cancelMu.Lock()
	if r.canceled {
		return
	}
	r.canceled = true

	r.tasks.Range(func(key, value interface{}) bool {
		id := key.(RunnableID)
		t := value.(*taskEntry)
		t.cancel()
		log.L().Info("Cancelling task", zap.String("id", id))
		return true
	})
	r.cancelMu.Unlock()

	r.taskStopNotifier.Close()
	r.wg.Wait()
}

func (r *TaskRunner) onNewTask(task *internal.RunnableContainer) (ret error) {
	defer func() {
		if r := recover(); r != nil {
			ret = errors.Trace(errors.Errorf("panic: %v", r))
		}
	}()

	taskCtx, cancel := context.WithCancel(context.Background())
	t := &taskEntry{
		RunnableContainer: task,
		cancel:            cancel,
	}

	rctx := newRuntimeCtx(taskCtx, task.Info())

	r.cancelMu.RLock()
	defer r.cancelMu.RUnlock()

	if r.canceled {
		return cerrors.ErrRuntimeClosed.GenWithStackByArgs()
	}

	_, exists := r.tasks.LoadOrStore(task.ID(), t)
	if exists {
		log.L().Warn("Duplicate Task ID", zap.String("id", task.ID()))
		return cerrors.ErrRuntimeDuplicateTaskID.GenWithStackByArgs(task.ID())
	}

	r.launchTask(rctx, t)

	return nil
}

func (r *TaskRunner) launchTask(rctx *RuntimeContext, entry *taskEntry) {
	r.wg.Add(1)
	r.taskCount.Inc()

	go func() {
		defer r.wg.Done()
		defer r.taskCount.Dec()

		var err error
		defer func() {
			log.L().Info("Task Closed",
				zap.String("id", entry.ID()),
				zap.Error(err),
				zap.Int64("runtime-task-count", r.taskCount.Load()))
			entry.OnStopped()
			r.taskStopNotifier.Notify(entry.ID())
			if _, ok := r.tasks.LoadAndDelete(entry.ID()); !ok {
				log.L().Panic("Task does not exist", zap.String("id", entry.ID()))
			}
		}()

		entry.OnLaunched()
		log.L().Info("Launching task",
			zap.String("id", entry.ID()),
			zap.Int64("runtime-task-count", r.taskCount.Load()))

		err = entry.Run(rctx)
		log.L().Info("Task stopped", zap.String("id", entry.ID()), zap.Error(err))
	}()
}

// TaskCount returns current task count
func (r *TaskRunner) TaskCount() int64 {
	return r.taskCount.Load()
}

// TaskStopReceiver returns a *notifier.Notifier to notify when task is stopped.
func (r *TaskRunner) TaskStopReceiver() *notifier.Receiver[RunnableID] {
	return r.taskStopNotifier.NewReceiver()
}
