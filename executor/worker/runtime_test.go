package worker

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBasicFunc(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rt := NewRuntime(ctx, 65535)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		rt.Start(ctx, 10)
	}()

	workerNum := 1000
	for i := 0; i < workerNum; i++ {
		id := "executor" + strconv.Itoa(i)
		err := rt.SubmitTask(&dummyWorker{
			id: id,
		})
		require.NoError(t, err)
	}
	time.Sleep(time.Second)
	if rtwl := rt.Workload(); int(rtwl) != workerNum {
		t.Error("not equal", rtwl, workerNum)
	}
	cancel()

	wg.Wait()
}

func TestWorkerFinished(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := NewRuntime(ctx, 65535)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		rt.Start(ctx, 20)
	}()

	workerNum := 1000

	var workers []*dummyWorker
	for i := 0; i < workerNum; i++ {
		id := "executor" + strconv.Itoa(i)
		worker := &dummyWorker{
			id: id,
		}
		workers = append(workers, worker)
		err := rt.SubmitTask(worker)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		return rt.taskNum.Load() == int64(workerNum)
	}, 1*time.Second, 100*time.Millisecond)

	cancel()
	wg.Wait()
}
