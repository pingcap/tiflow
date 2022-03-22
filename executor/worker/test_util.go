package worker

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/atomic"

	"github.com/hanfei1991/microcosm/model"
)

type dummyWorker struct {
	id RunnableID

	needQuit atomic.Bool

	blockMu   sync.Mutex
	blockCond *sync.Cond
	blocked   bool

	submitTime atomic.Time
}

func newDummyWorker(id RunnableID) *dummyWorker {
	ret := &dummyWorker{
		id:         id,
		submitTime: *atomic.NewTime(time.Time{}),
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

	rctx, ok := ToRuntimeCtx(ctx)
	if !ok {
		log.L().Panic("A RuntimeContext is expected to be used in unit tests")
	}
	d.submitTime.Store(rctx.SubmitTime())

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

func (d *dummyWorker) SubmitTime() time.Time {
	return d.submitTime.Load()
}
