package example

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/lib"
)

var _ lib.Worker = &exampleWorker{}

var tickKey = "tick_count"

type exampleWorker struct {
	*lib.DefaultBaseWorker

	work struct {
		mu        sync.Mutex
		tickCount int
		finished  bool
	}
	wg sync.WaitGroup
}

func (w *exampleWorker) run() {
	defer w.wg.Done()

	time.Sleep(time.Second)
	w.work.mu.Lock()
	count := w.work.tickCount
	w.work.mu.Unlock()
	// nolint:errcheck
	_, _ = w.DefaultBaseWorker.MetaKVClient().Put(
		context.TODO(), tickKey, strconv.Itoa(count))

	w.work.mu.Lock()
	w.work.finished = true
	w.work.mu.Unlock()
}

func (w *exampleWorker) InitImpl(ctx context.Context) error {
	log.L().Info("InitImpl")
	w.wg.Add(1)
	go w.run()
	return nil
}

func (w *exampleWorker) Tick(ctx context.Context) error {
	log.L().Info("Tick")
	w.work.mu.Lock()
	w.work.tickCount++
	w.work.mu.Unlock()
	return nil
}

func (w *exampleWorker) Status() lib.WorkerStatus {
	log.L().Info("Status")
	code := lib.WorkerStatusNormal
	w.work.mu.Lock()
	finished := w.work.finished
	w.work.mu.Unlock()

	if finished {
		code = lib.WorkerStatusFinished
	}
	return lib.WorkerStatus{Code: code}
}

func (w *exampleWorker) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("OnMasterFailover")
	return nil
}

func (w *exampleWorker) CloseImpl(ctx context.Context) error {
	log.L().Info("CloseImpl")
	w.wg.Wait()
	return nil
}
