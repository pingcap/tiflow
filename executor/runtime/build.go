package runtime

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hanfei1991/microcosom/model"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
)

func newTaskContainer(task *model.Task, ctx *taskContext) *taskContainer {
	t := &taskContainer{
		cfg: task,
		id:  task.ID,
		ctx: ctx,
	}
	return t
}

func newHashOp(cfg *model.HashOp) operator {
	return &opHash{}
}

func newReadTableOp(cfg *model.TableReaderOp) operator {
	return &opReceive{
		addr:     cfg.Addr,
		data:     make(chan *Record, 1024),
		tableCnt: cfg.TableNum,
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
		sendWaker: s.getWaker(sender),
		recvWaker: s.getWaker(receiver),
	}
	sender.output = append(sender.output, ch)
	receiver.inputs = append(receiver.inputs, ch)
}

func (s *Runtime) SubmitTasks(tasks []*model.Task) error {
	taskSet := make(map[model.TaskID]*taskContainer)
	for _, t := range tasks {
		task := newTaskContainer(t, s.ctx)
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
		case model.HashType:
			op := &model.HashOp{}
			err := json.Unmarshal(t.Op, op)
			if err != nil {
				return err
			}
			task.op = newHashOp(op)
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
				return errors.New(fmt.Sprintf("cannot find task %d", tid))
			}
		}
		err := t.prepare()
		if err != nil {
			return err
		}
	}

	log.L().Logger.Info("begin to push")
	// add to queue, begin to run.
	for _, t := range taskSet {
		if t.status == int32(Runnable) {
			s.q.push(t)
		}
	}
	return nil
}
