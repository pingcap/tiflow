package worker_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/fake"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
)

func TestBasicFunc(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rt := worker.NewRuntime(ctx)
	rt.Start(10)
	workerNum := 1000
	for i := 0; i < workerNum; i++ {
		workerImpl := fake.NewDummyWorkerImpl(dcontext.Context{})
		worker := &dummyWorker{
			id:   lib.WorkerID("executor" + strconv.Itoa(i)),
			impl: workerImpl,
		}
		rt.AddWorker(worker)
	}
	time.Sleep(time.Second)
	if rtwl := rt.Workload(); int(rtwl) != workerNum {
		t.Error("not equal", rtwl, workerNum)
	}
	cancel()
}

type dummyWorker struct {
	id   lib.WorkerID
	impl lib.WorkerImpl
}

func (d *dummyWorker) Init(ctx context.Context) error {
	return d.impl.InitImpl(ctx)
}

func (d *dummyWorker) Poll(ctx context.Context) error {
	return d.impl.Tick(ctx)
}

func (d *dummyWorker) ID() lib.WorkerID {
	return d.id
}

func (d *dummyWorker) Workload() model.RescUnit {
	return model.RescUnit(1)
}

func (d *dummyWorker) Close() {
	d.impl.CloseImpl()
}
