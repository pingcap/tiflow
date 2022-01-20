package fake

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
)

var _ lib.Worker = (*dummyWorker)(nil)

type (
	Worker      = dummyWorker
	dummyWorker struct {
		*lib.BaseWorker

		init   bool
		closed int32
		tick   int64
	}
)

func (d *dummyWorker) InitImpl(ctx context.Context) error {
	if !d.init {
		d.init = true
		return nil
	}
	return errors.New("repeated init")
}

func (d *dummyWorker) Tick(ctx context.Context) error {
	if !d.init {
		return errors.New("not yet init")
	}

	if atomic.LoadInt32(&d.closed) == 1 {
		return nil
	}
	d.tick++
	return nil
}

func (d *dummyWorker) Status() (lib.WorkerStatus, error) {
	if d.init {
		return lib.WorkerStatus{Code: lib.WorkerStatusNormal, Ext: d.tick}, nil
	}
	return lib.WorkerStatus{Code: lib.WorkerStatusCreated}, nil
}

func (d *dummyWorker) Workload() model.RescUnit {
	return model.RescUnit(10)
}

func (d *dummyWorker) OnMasterFailover(_ lib.MasterFailoverReason) error {
	return nil
}

func (d *dummyWorker) CloseImpl() {
	atomic.StoreInt32(&d.closed, 1)
}

func NewDummyWorker(ctx *dcontext.Context, id lib.WorkerID, masterID lib.MasterID) lib.Worker {
	ret := &dummyWorker{}
	dependencies := ctx.Dependencies
	ret.BaseWorker = lib.NewBaseWorker(
		ret,
		dependencies.MessageHandlerManager,
		dependencies.MessageRouter,
		dependencies.MetaKVClient,
		id,
		masterID)

	return ret
}
