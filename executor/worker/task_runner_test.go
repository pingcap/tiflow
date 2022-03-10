package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	workerNum = 100
)

func TestTaskRunnerBasics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tr := NewTaskRunner(workerNum+1, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tr.Run(ctx)
		require.Error(t, err)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	var workers []*dummyWorker
	for i := 0; i < workerNum; i++ {
		worker := &dummyWorker{
			id: fmt.Sprintf("worker-%d", i),
		}
		workers = append(workers, worker)
		err := tr.AddTask(worker)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		t.Logf("taskNum %d", tr.Workload())
		return tr.Workload() == workerNum
	}, 1*time.Second, 10*time.Millisecond)

	for _, worker := range workers {
		worker.SetFinished()
	}

	require.Eventually(t, func() bool {
		return tr.Workload() == 0
	}, 1*time.Second, 100*time.Millisecond)

	cancel()
}

func TestTaskRunnerInitBlocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tr := NewTaskRunner(10, 10)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tr.Run(ctx)
		require.Error(t, err)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	var workers []*dummyWorker
	for i := 0; i < 21; i++ {
		worker := newDummyWorker(fmt.Sprintf("worker-%d", i))
		worker.BlockInit()
		workers = append(workers, worker)

		require.Eventually(t, func() bool {
			err := tr.AddTask(worker)
			return err == nil
		}, 100*time.Millisecond, 1*time.Millisecond)
	}

	worker := newDummyWorker("my-worker")
	err := tr.AddTask(worker)
	require.Error(t, err)
	require.Regexp(t, ".*ResourceExhausted.*", err.Error())

	for _, worker := range workers {
		worker.UnblockInit()
	}

	require.Eventually(t, func() bool {
		t.Logf("taskNum %d", tr.Workload())
		return tr.Workload() == 21
	}, 1*time.Second, 10*time.Millisecond)

	cancel()
}
