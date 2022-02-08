package worker

import (
	"context"
	"sync"
	"time"

	"github.com/edwingeng/deque"
	"github.com/hanfei1991/microcosm/model"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type Scheduler struct {
	sync.Mutex

	ctx              context.Context
	queue            deque.Deque
	onWorkerFinished func(Runnable, error)
}

type Closer interface {
	Close(ctx context.Context) error
}

type Workloader interface {
	Workload() model.RescUnit
}

type RunnableID = string

type Runnable interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	ID() RunnableID

	Closer
}

const emptyRestDuration = 50 * time.Millisecond

func (s *Scheduler) AddWorker(worker Runnable) {
	s.Lock()
	s.queue.PushBack(worker)
	s.Unlock()
}

func (s *Scheduler) Run(conn int) {
	for i := 0; i < conn; i++ {
		go s.runImpl()
	}
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
		worker := s.queue.PopFront().(Runnable)
		s.Unlock()
		if err := worker.Poll(s.ctx); err != nil {
			s.onWorkerFinished(worker, err)
			continue
		}
		// TODO: calculate workload

		s.Lock()
		s.queue.PushBack(worker)
		s.Unlock()
	}
}

func NewRuntime(ctx context.Context) *Runtime {
	rt := &Runtime{
		ctx:           ctx,
		closingWorker: make(chan Runnable, 1024),
		initingWorker: make(chan Runnable, 1024),
		scheduler:     Scheduler{ctx: ctx, queue: deque.NewDeque()},
	}
	rt.scheduler.onWorkerFinished = rt.onWorkerFinish
	return rt
}

type Runtime struct {
	workerList sync.Map // map[lib.WorkerID]lib.Worker
	// We should abstract a schedule interface to implement different
	// schedule algorithm. For now, we assume every worker consume similar
	// poll time, so we expect go scheduler can produce a fair result.
	scheduler     Scheduler
	closingWorker chan Runnable
	initingWorker chan Runnable
	ctx           context.Context
}

func (r *Runtime) onWorkerFinish(worker Runnable, err error) {
	log.L().Warn("Worker has finished",
		zap.Any("worker-id", worker.ID()),
		zap.Error(err))
	r.closingWorker <- worker
}

func (r *Runtime) closeWorker() {
	for worker := range r.closingWorker {
		// TODO context and error handling
		_ = worker.Close(context.Background())
		r.workerList.Delete(worker.ID())
	}
}

func (r *Runtime) initWorker() {
	for worker := range r.initingWorker {
		if err := worker.Init(r.ctx); err != nil {
			r.onWorkerFinish(worker, err)
		} else {
			r.scheduler.AddWorker(worker)
		}
	}
}

func (r *Runtime) Start(conn int) {
	go r.closeWorker()
	go r.initWorker()
	r.scheduler.Run(conn)
}

func (r *Runtime) AddWorker(worker Runnable) {
	r.workerList.Store(worker.ID(), worker)
	r.initingWorker <- worker
}

func (r *Runtime) Workload() model.RescUnit {
	ret := model.RescUnit(0)
	r.workerList.Range(func(_, value interface{}) bool {
		workloader, ok := value.(Workloader)
		if !ok {
			return true
		}
		workload := workloader.Workload()
		ret += workload
		return true
	})
	return ret
}
