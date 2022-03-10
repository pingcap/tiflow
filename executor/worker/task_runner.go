package worker

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hanfei1991/microcosm/executor/worker/internal"
	"github.com/hanfei1991/microcosm/model"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
)

type TaskRunner struct {
	inQueue       chan Runnable
	initQuotaSema *semaphore.Weighted
	tasks         sync.Map
	wg            sync.WaitGroup

	cancelMu sync.RWMutex
	canceled bool

	taskCount atomic.Int64
}

const (
	defaultTaskWeight         = 1
	defaultPollInterval       = 50 * time.Millisecond
	defaultInitQueuingTimeout = 10 * time.Second
)

type taskEntry struct {
	*internal.RunnableContainer
	cancel context.CancelFunc
}

func (e *taskEntry) EventLoop(ctx context.Context) error {
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if err := e.Poll(ctx); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func NewTaskRunner(inQueueSize int, initConcurrency int) *TaskRunner {
	return &TaskRunner{
		inQueue:       make(chan Runnable, inQueueSize),
		initQuotaSema: semaphore.NewWeighted(int64(initConcurrency)),
	}
}

func (r *TaskRunner) AddTask(task Runnable) error {
	select {
	case r.inQueue <- task:
		return nil
	default:
	}

	return status.Error(codes.ResourceExhausted, "task runner in-queue is full")
}

func (r *TaskRunner) Run(ctx context.Context) error {
	defer r.cancelAll()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case task := <-r.inQueue:
			if task == nil {
				return derror.ErrRuntimeIsClosed.GenWithStackByArgs()
			}
			if err := r.onNewTask(ctx, task); err != nil {
				log.L().Warn("Failed to launch task",
					zap.String("id", task.ID()),
					zap.Error(err))
			}
		}
	}
}

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

	r.wg.Wait()
}

func (r *TaskRunner) onNewTask(ctx context.Context, task Runnable) (ret error) {
	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, defaultInitQueuingTimeout)
	defer cancelTimeout()

	err := r.initQuotaSema.Acquire(timeoutCtx, defaultTaskWeight)
	if err != nil {
		return derror.ErrRuntimeInitQueuingTimeOut.Wrap(err).GenWithStackByArgs()
	}

	defer func() {
		if r := recover(); r != nil {
			ret = errors.Trace(errors.Errorf("panic: %v", r))
		}
		if ret != nil {
			r.initQuotaSema.Release(defaultTaskWeight)
		}
	}()

	taskCtx, cancel := context.WithCancel(context.Background())
	t := &taskEntry{
		RunnableContainer: internal.WrapRunnable(task),
		cancel:            cancel,
	}

	r.cancelMu.RLock()
	defer r.cancelMu.RUnlock()

	if r.canceled {
		return derror.ErrRuntimeClosed.GenWithStackByArgs()
	}

	_, exists := r.tasks.LoadOrStore(task.ID(), t)
	if exists {
		log.L().Warn("Duplicate Task ID", zap.String("id", task.ID()))
		return derror.ErrRuntimeDuplicateTaskID.GenWithStackByArgs(task.ID())
	}

	r.taskCount.Inc()
	runInit := func(initCtx context.Context) (ret error) {
		defer func() {
			if r := recover(); r != nil {
				ret = errors.Trace(errors.Errorf("panic: %v", r))
			}
			r.initQuotaSema.Release(defaultTaskWeight)
		}()

		if err := t.Init(initCtx); err != nil {
			return errors.Trace(err)
		}
		t.OnInitialized()
		return nil
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer r.taskCount.Dec()

		defer func() {
			err := t.Close(ctx)
			log.L().Info("Task Closed",
				zap.String("id", t.ID()),
				zap.Error(err),
				zap.Int64("runtime-task-count", r.taskCount.Load()))
			t.OnStopped()

			if _, ok := r.tasks.LoadAndDelete(t.ID()); !ok {
				log.L().Panic("Task does not exist", zap.String("id", t.ID()))
			}
		}()

		if err := runInit(taskCtx); err != nil {
			log.L().Warn("Task init returned error", zap.String("id", t.ID()), zap.Error(err))
			return
		}

		log.L().Info("Task initialized",
			zap.String("id", t.ID()),
			zap.Int64("runtime-task-count", r.taskCount.Load()))

		err := t.EventLoop(ctx)
		log.L().Info("Task stopped", zap.String("id", t.ID()), zap.Error(err))
	}()

	return nil
}
