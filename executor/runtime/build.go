package runtime

import (
	"encoding/json"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
)

func (s *Runtime) newTaskContainer(task *model.Task) *taskContainer {
	t := &taskContainer{
		cfg: task,
		id:  task.ID,
		ctx: new(taskContext),
	}
	t.ctx.testCtx = s.testCtx
	t.ctx.wake = func() {
		// you can't wake or it is already been waked.
		if !t.tryAwake() {
			return
		}
		t.setRunnable()
		s.q.push(t)
	}
	return t
}

func newSyncOp(cfg *model.HashOp) operator {
	return &opSyncer{}
}

func newReadTableOp(cfg *model.TableReaderOp) operator {
	return &opReceive{
		flowID: cfg.FlowID,
		addr:   cfg.Addr,
		data:   make(chan *Record, 1024),
		errCh:  make(chan error, 10),
	}
}

func newSinkOp(cfg *model.TableSinkOp) operator {
	return &opSink{
		writer: fileWriter{
			filePath: cfg.File,
			tid:      cfg.TableID,
		},
	}
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

func (s *Runtime) SubmitTasks(tasks []*model.Task) error {
	taskSet := make(map[model.TaskID]*taskContainer)
	taskToRun := make([]*taskContainer, 0)
	for _, t := range tasks {
		task := s.newTaskContainer(t)
		log.L().Logger.Info("config", zap.ByteString("op", t.Op))
		switch t.OpTp {
		case model.TableReaderType:
			op := &model.TableReaderOp{}
			err := json.Unmarshal(t.Op, op)
			if err != nil {
				return err
			}
			task.op = newReadTableOp(op)
			task.setRunnable()
			taskToRun = append(taskToRun, task)
		case model.HashType:
			op := &model.HashOp{}
			err := json.Unmarshal(t.Op, op)
			if err != nil {
				return err
			}
			task.op = newSyncOp(op)
			task.tryBlock()
		case model.TableSinkType:
			op := &model.TableSinkOp{}
			err := json.Unmarshal(t.Op, op)
			if err != nil {
				return err
			}
			task.op = newSinkOp(op)
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

	for _, t := range taskSet {
		err := t.prepare()
		if err != nil {
			return err
		}
	}

	log.L().Logger.Info("begin to push")
	// add to queue, begin to run.
	for _, t := range taskToRun {
		s.q.push(t)
	}
	return nil
}
