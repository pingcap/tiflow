package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/test"
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
	testCtx   *test.Context
	tasksLock sync.Mutex
	tasks     map[model.ID]*taskContainer
	q         queue
	wg        sync.WaitGroup
}

func (s *Runtime) Stop(tasks []int64) error {
	s.tasksLock.Lock()
	defer s.tasksLock.Unlock()
	var retErr error
	for _, id := range tasks {
		if task, ok := s.tasks[model.ID(id)]; ok {
			err := task.Stop()
			if err != nil {
				retErr = err
			}
			delete(s.tasks, task.id)
		}
	}
	return retErr
}

func (s *Runtime) Run(ctx context.Context, cur int) {
	s.wg.Add(cur)
	for i := 0; i < cur; i++ {
		go s.runImpl(ctx)
	}
	s.wg.Wait()
}

func (s *Runtime) runImpl(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		t := s.q.pop()
		if t == nil {
			// idle, sleep for sometime to avoid busy loop
			// TODO: find better wake up mechanism way if needed
			time.Sleep(time.Millisecond * 50)
			continue
		}
		status := t.Poll()
		if status == Blocked {
			if t.tryBlock() {
				continue
			}
			// the status is waking
		} else if status == Stop {
			continue
		}
		t.setRunnable()
		s.q.push(t)
	}
}

func NewRuntime(ctx *test.Context) *Runtime {
	s := &Runtime{
		testCtx: ctx,
	}
	return s
}
