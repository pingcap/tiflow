package worker

import (
	"context"
	"sync"
	"time"

	"github.com/edwingeng/deque"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/executor/worker/internal"
	"github.com/hanfei1991/microcosm/model"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
)

// Re-export types for public use
type (
	Runnable   = internal.Runnable
	RunnableID = internal.RunnableID
	Workloader = internal.Workloader
	Closer     = internal.Closer
)

type Scheduler struct {
	sync.Mutex

	ctx            context.Context
	queue          deque.Deque
	onTaskFinished func(*internal.RunnableContainer, error)
}

const emptyRestDuration = 50 * time.Millisecond

func (s *Scheduler) AddTask(task Runnable) {
	s.Lock()
	s.queue.PushBack(task)
	s.Unlock()
}

func (s *Scheduler) Run(conn int) {
	var wg sync.WaitGroup

	for i := 0; i < conn; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runImpl()
		}()
	}

	wg.Wait()
}

func (s *Scheduler) runImpl() {
	ticker := time.NewTicker(emptyRestDuration)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		s.Lock()
		if s.queue.Empty() {
			s.Unlock()
			continue
		}
		task := s.queue.PopFront().(*internal.RunnableContainer)
		s.Unlock()
		if err := task.Poll(s.ctx); err != nil {
			s.onTaskFinished(task, err)
			continue
		}

		s.Lock()
		s.queue.PushBack(task)
		s.Unlock()
	}
}

func NewRuntime(ctx context.Context, capacity int64) *Runtime {
	rt := &Runtime{
		closingTasks: make(chan *internal.RunnableContainer, capacity),
		initingTasks: make(chan *internal.RunnableContainer, capacity),
		scheduler:    Scheduler{ctx: ctx, queue: deque.NewDeque()},
		capacity:     capacity,
	}
	rt.scheduler.onTaskFinished = rt.onTaskFinished
	return rt
}

type Runtime struct {
	taskList sync.Map // stores *internal.RunnableContainer
	// We should abstract a schedule interface to implement different
	// schedule algorithm. For now, we assume every task consume similar
	// poll time, so we expect go scheduler can produce a fair result.
	scheduler    Scheduler
	closingTasks chan *internal.RunnableContainer
	initingTasks chan *internal.RunnableContainer

	capacity int64

	// taskNumMu protects taskNum from concurrent writes
	// Reading from it does not require locking.
	taskNumMu sync.Mutex
	taskNum   atomic.Int64
}

func (r *Runtime) onTaskFinished(task *internal.RunnableContainer, err error) {
	log.L().Warn("Task has finished",
		zap.Any("worker-id", task.ID()),
		zap.Error(err))
	task.OnStopped()

	select {
	case r.closingTasks <- task:
	default:
		log.L().Panic("closingTasks is blocked unexpectedly",
			zap.Int64("capacity", r.capacity),
			zap.Int64("task-num", r.taskNum.Load()),
			zap.Int("chan-size", len(r.closingTasks)))
	}
}

func (r *Runtime) closeTask(ctx context.Context) {
	for {
		var (
			task *internal.RunnableContainer
			ok   bool
		)
		select {
		case <-ctx.Done():
			return
		case task, ok = <-r.closingTasks:
		}

		if !ok {
			return
		}
		// TODO error handling
		_ = task.Close(ctx)
		r.taskList.Delete(task.ID())

		r.taskNumMu.Lock()
		newTaskNum := r.taskNum.Sub(1)
		r.taskNumMu.Unlock()

		if newTaskNum < 0 {
			log.L().Panic("negative taskNum", zap.Int64("task-num", newTaskNum))
		}
	}
}

func (r *Runtime) initTask(ctx context.Context) {
	for {
		var (
			task *internal.RunnableContainer
			ok   bool
		)

		select {
		case <-ctx.Done():
			return
		case task, ok = <-r.initingTasks:
		}

		if !ok {
			return
		}

		if err := task.Init(ctx); err != nil {
			r.onTaskFinished(task, err)
		} else {
			task.OnInitialized()
			r.scheduler.AddTask(task)
		}
	}
}

func (r *Runtime) Start(ctx context.Context, conn int) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		r.closeTask(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		r.initTask(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		r.scheduler.Run(conn)
	}()

	wg.Wait()
}

func (r *Runtime) SubmitTask(task Runnable) error {
	ok := func() bool {
		// We use lock-protected writes instead of a lock-free algorithm
		// to make the code more readable. Plus, SubmitTask should not be
		// a bottleneck in the system, so there is no need to optimize
		// for latency or throughput.

		r.taskNumMu.Lock()
		defer r.taskNumMu.Unlock()

		taskNum := r.taskNum.Load()
		if taskNum >= r.capacity {
			return false
		}
		r.taskNum.Add(1)
		return true
	}()

	if !ok {
		return derror.ErrRuntimeReachedCapacity.GenWithStackByArgs(r.capacity)
	}

	wrappedtask := internal.WrapRunnable(task)
	r.taskList.Store(task.ID(), wrappedtask)

	select {
	case r.initingTasks <- wrappedtask:
	default:
		log.L().Panic("initingTasks is blocked unexpectedly",
			zap.Int64("capacity", r.capacity),
			zap.Int64("task-num", r.taskNum.Load()),
			zap.Int("chan-size", len(r.initingTasks)))
	}
	return nil
}

func (r *Runtime) Workload() model.RescUnit {
	ret := model.RescUnit(0)
	r.taskList.Range(func(_, value interface{}) bool {
		container := value.(*internal.RunnableContainer)
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
	return ret
}

// TaskCount returns running task currently
func (r *Runtime) TaskCount() int64 {
	return r.taskNum.Load()
}
