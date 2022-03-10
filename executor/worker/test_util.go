package worker

import (
	"context"
	"sync"

	"github.com/hanfei1991/microcosm/model"
	"github.com/pingcap/errors"
	"go.uber.org/atomic"
)

type dummyWorker struct {
	id RunnableID

	needQuit atomic.Bool

	blockMu   sync.Mutex
	blockCond *sync.Cond
	blocked   bool
}

func newDummyWorker(id RunnableID) *dummyWorker {
	ret := &dummyWorker{
		id: id,
	}
	ret.blockCond = sync.NewCond(&ret.blockMu)
	return ret
}

func (d *dummyWorker) Init(ctx context.Context) error {
	d.blockMu.Lock()
	for d.blocked {
		d.blockCond.Wait()
	}
	d.blockMu.Unlock()

	return nil
}

func (d *dummyWorker) Poll(ctx context.Context) error {
	if d.needQuit.Load() {
		return errors.New("worker is finished")
	}
	return nil
}

func (d *dummyWorker) ID() RunnableID {
	return d.id
}

func (d *dummyWorker) Workload() model.RescUnit {
	return model.RescUnit(1)
}

func (d *dummyWorker) Close(ctx context.Context) error {
	return nil
}

func (d *dummyWorker) SetFinished() {
	d.needQuit.Store(true)
}

func (d *dummyWorker) BlockInit() {
	d.blockMu.Lock()
	defer d.blockMu.Unlock()

	d.blocked = true
}

func (d *dummyWorker) UnblockInit() {
	d.blockMu.Lock()
	defer d.blockMu.Unlock()

	d.blocked = false
	d.blockCond.Broadcast()
}
