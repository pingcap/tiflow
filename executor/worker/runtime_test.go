package worker_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
)

func TestBasicFunc(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rt := worker.NewRuntime(ctx)
	rt.Start(10)
	workerNum := 1000

	for i := 0; i < workerNum; i++ {
		id := lib.WorkerID("executor" + strconv.Itoa(i))
		rt.AddWorker(&dummyWorker{
			id: id,
		})
	}
	time.Sleep(time.Second)
	if rtwl := rt.Workload(); int(rtwl) != workerNum {
		t.Error("not equal", rtwl, workerNum)
	}
	cancel()
}

type dummyWorker struct {
	id lib.WorkerID
}

func (d *dummyWorker) Init(ctx context.Context) error {
	return nil
}

func (d *dummyWorker) Poll(ctx context.Context) error {
	return nil
}

func (d *dummyWorker) WorkerID() lib.WorkerID {
	return d.id
}

func (d *dummyWorker) Workload() model.RescUnit {
	return model.RescUnit(1)
}

func (d *dummyWorker) Close() {
}
