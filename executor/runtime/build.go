package runtime

import (
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

func (s *Runtime) newTaskContainer(task *model.Task) *taskContainer {
	t := &taskContainer{
		cfg: task,
		id:  task.ID,
		ctx: new(TaskContext),
	}
	t.ctx.TestCtx = s.testCtx
	t.ctx.Wake = func() {
		// you can't wake or it is already been waked.
		if !t.tryAwake() {
			return
		}
		t.setRunnable()
		s.q.push(t)
	}
	return t
}

func (s *Runtime) connectTasks(sender, receiver *taskContainer) {
	ch := &Channel{
		innerChan: make(chan *Record, 1024),
		sendCtx:   sender.ctx,
		recvCtx:   receiver.ctx,
	}
	sender.outputs = append(sender.outputs, ch)
	receiver.inputs = append(receiver.inputs, ch)
}

type opBuilder interface {
	Build(model.Operator) (Operator, bool, error)
}

var OpBuilders map[model.OperatorType]opBuilder

func InitOpBuilders() {
	OpBuilders = make(map[model.OperatorType]opBuilder)
}

func (s *Runtime) SubmitTasks(tasks []*model.Task) error {
	taskSet := make(map[model.ID]*taskContainer)
	taskToRun := make([]*taskContainer, 0)
	for _, t := range tasks {
		task := s.newTaskContainer(t)
		log.L().Logger.Info("config", zap.ByteString("op", t.Op))
		builder, ok := OpBuilders[t.OpTp]
		if !ok {
			return errors.ErrExecutorUnknownOperator.FastGenByArgs(t.OpTp)
		}
		op, runnable, err := builder.Build(t.Op)
		if err != nil {
			return err
		}
		task.op = op
		if runnable {
			task.setRunnable()
			taskToRun = append(taskToRun, task)
		} else {
			task.tryBlock()
		}
		taskSet[task.id] = task
	}

	log.L().Logger.Info("begin to connect tasks")

	for _, t := range taskSet {
		for _, tid := range t.cfg.Outputs {
			dst, ok := taskSet[tid]
			if ok {
				s.connectTasks(t, dst)
			} else {
				return errors.ErrTaskNotFound.GenWithStackByArgs(tid)
			}
		}
	}

	if s.tasks == nil {
		s.tasks = make(map[model.ID]*taskContainer)
	}
	for _, t := range taskSet {
		err := t.prepare()
		if err != nil {
			return err
		}
		s.tasks[t.id] = t
	}

	log.L().Logger.Info("begin to push")
	// add to queue, begin to run.
	for _, t := range taskToRun {
		s.q.push(t)
	}
	return nil
}
