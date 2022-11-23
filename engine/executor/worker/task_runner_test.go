// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/framework/taskutil"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

const (
	workerNum = 100
)

// TaskRunner must implement WrappedTaskAdder for TaskCommitter to work.
var _ WrappedTaskAdder = &TaskRunner{}

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
		require.Regexp(t, "context canceled", err.Error())
	}()

	var workers []*dummyWorker
	for i := 0; i < workerNum; i++ {
		worker := &dummyWorker{
			id: fmt.Sprintf("worker-%d", i),
		}
		workers = append(workers, worker)
		err := tr.AddTask(taskutil.WrapWorker(worker))
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		t.Logf("taskNum %d", tr.WorkerCount())
		return tr.WorkerCount() == workerNum
	}, 1*time.Second, 10*time.Millisecond)

	for _, worker := range workers {
		worker.SetFinished()
	}

	require.Eventually(t, func() bool {
		return tr.WorkerCount() == 0
	}, 1*time.Second, 100*time.Millisecond)

	cancel()
	wg.Wait()
}

func TestTaskRunnerSubmitTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tr := NewTaskRunner(10, 10)

	mockClock := clock.NewMock()
	tr.clock = mockClock
	submitTime := time.Unix(0, 1)
	mockClock.Set(submitTime)

	// We call AddTask before calling Run to make sure that the submitTime
	// is recorded during the execution of the AddTask call.
	worker := newDummyWorker("my-worker")
	err := tr.AddTask(taskutil.WrapWorker(worker))
	require.NoError(t, err)

	// Advance the internal clock
	mockClock.Add(time.Hour)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tr.Run(ctx)
		require.Error(t, err)
		require.Regexp(t, "context canceled", err.Error())
	}()

	require.Eventually(t, func() bool {
		return worker.SubmitTime() == clock.ToMono(submitTime)
	}, 1*time.Second, 10*time.Millisecond)

	cancel()
	wg.Wait()
}

func TestTaskStopReceiver(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tr := NewTaskRunner(workerNum+1, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tr.Run(ctx)
		require.Error(t, err)
		require.Regexp(t, "context canceled", err.Error())
	}()

	var (
		workers []*dummyWorker
		running sync.Map
	)
	for i := 0; i < workerNum; i++ {
		worker := &dummyWorker{
			id: fmt.Sprintf("worker-%d", i),
		}
		running.Store(worker.id, struct{}{})
		workers = append(workers, worker)
		err := tr.AddTask(taskutil.WrapWorker(worker))
		require.NoError(t, err)
	}

	receiver := tr.TaskStopReceiver()
	stopped := atomic.NewInt32(0)
	wg.Add(1)
	go func() {
		defer func() {
			receiver.Close()
			wg.Done()
		}()
		for id := range receiver.C {
			_, ok := running.LoadAndDelete(id)
			require.True(t, ok, "worker %s has already stopped", id)
			if stopped.Inc() == workerNum {
				break
			}
		}
	}()

	for _, worker := range workers {
		worker.SetFinished()
	}

	require.Eventually(t, func() bool {
		return stopped.Load() == workerNum
	}, time.Second, 100*time.Millisecond)

	cancel()
	wg.Wait()
}
