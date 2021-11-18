package runtime

import (
	"context"
	"log"

	"github.com/hanfei1991/microcosom/pkg/workerpool"
	"sync"
)

type queue struct {
	sync.Mutex
	tasks []*taskContainer
}

func (q *queue) pop() *taskContainer {
	q.Lock()
	defer q.Unlock()
	if len(q.tasks) == 0 {
		return nil
	}
	task := q.tasks[0]
	q.tasks = q.tasks[1:]
	return task
}

func (q *queue) push(t *taskContainer) {
	q.Lock()
	defer q.Unlock()
	q.tasks = append(q.tasks, t)
}

type Runtime struct {
	ctx *taskContext
	q   queue
}

func (s *Runtime) getWaker(task *taskContainer) func() {
	return func() {
		// you can't wake or it is already been waked.
		if !task.tryAwake() {
			return
		}
		task.setRunnable()
		s.q.push(task)
	}
}

func (s *Runtime) ShowStats(sec int) {
	for tid, stats := range s.ctx.stats {
		log.Printf("tid %d qps %d avgLag %d ms", tid, stats.recordCnt/sec, stats.totalLag.Milliseconds()/int64(stats.recordCnt))
	}
}

func (s *Runtime) Run(ctx context.Context) {
	//log.Printf("scheduler running")
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		t := s.q.pop()
		if t == nil {
			// idle
			continue
		}
		status := t.Poll()
		if status == Blocked {
			if t.tryBlock() {
				//log.Printf("task %d blocked success", t.id)
				continue
			}
		}
		t.setRunnable()
		s.q.push(t)
	}
}

func NewRuntime() *Runtime {
	ctx := &taskContext{
		ioPool: workerpool.NewDefaultAsyncPool(20),
	}
	s := &Runtime{ctx: ctx}
	return s
}
