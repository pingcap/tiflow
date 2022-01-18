package worker

import (
	"context"
	"sync"
	"time"

	"github.com/edwingeng/deque"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
)

type Scheduler struct {
	sync.Mutex

	ctx              context.Context
	queue            deque.Deque
	onWorkerFinished func(lib.Worker, error)
}

const emptyRestDuration time.Duration = 50 * time.Millisecond

func (s *Scheduler) AddWorker(worker lib.Worker) {
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
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		s.Lock()
		if s.queue.Empty() {
			s.Unlock()
			time.Sleep(emptyRestDuration)
			continue
		}
		worker := s.queue.PopFront().(lib.Worker)
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
		closingWorker: make(chan lib.Worker, 1024),
		initingWorker: make(chan lib.Worker, 1024),
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
	closingWorker chan lib.Worker
	initingWorker chan lib.Worker
	ctx           context.Context
}

func (r *Runtime) onWorkerFinish(worker lib.Worker, err error) {
	r.closingWorker <- worker
}

func (r *Runtime) closeWorker() {
	for worker := range r.closingWorker {
		worker.Close()
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

func (r *Runtime) AddWorker(worker lib.Worker) {
	r.workerList.Store(worker.ID(), worker)
	r.initingWorker <- worker
}

func (r *Runtime) Workload() model.RescUnit {
	ret := model.RescUnit(0)
	r.workerList.Range(func(_, value interface{}) bool {
		worker := value.(lib.Worker)
		ret += worker.Workload()
		return true
	})
	return ret
}
