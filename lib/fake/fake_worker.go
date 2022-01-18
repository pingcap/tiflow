package fake

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
)

var _ lib.WorkerImpl = &dummyWorkerImpl{}

type dummyWorkerImpl struct {
	init   bool
	closed int32
	tick   int64
}

func (d *dummyWorkerImpl) InitImpl(ctx context.Context) error {
	if !d.init {
		d.init = true
		return nil
	}
	return errors.New("repeated init")
}

func (d *dummyWorkerImpl) Tick(ctx context.Context) error {
	if !d.init {
		return errors.New("not yet init")
	}

	if atomic.LoadInt32(&d.closed) == 1 {
		return nil
	}
	d.tick++
	return nil
}

func (d *dummyWorkerImpl) Status() (lib.WorkerStatus, error) {
	if d.init {
		return lib.WorkerStatus{Code: lib.WorkerStatusNormal, Ext: d.tick}, nil
	}
	return lib.WorkerStatus{Code: lib.WorkerStatusCreated}, nil
}

func (d *dummyWorkerImpl) Workload() (model.RescUnit, error) {
	return model.RescUnit(10), nil
}

func (d *dummyWorkerImpl) OnMasterFailover(_ lib.MasterFailoverReason) error {
	return nil
}

func (d *dummyWorkerImpl) CloseImpl() {
	atomic.StoreInt32(&d.closed, 1)
}

func NewDummyWorkerImpl(ctx dcontext.Context) lib.WorkerImpl {
	return &dummyWorkerImpl{}
}
