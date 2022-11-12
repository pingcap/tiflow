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

package eventloop

import (
	"context"
	"sync"
	"testing"
	"time"

	runtime "github.com/pingcap/tiflow/engine/executor/worker"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type toyTaskStatus = int32

const (
	toyTaskUninit = toyTaskStatus(iota + 1)
	toyTaskRunning
	toyTaskClosing
	toyTaskClosed
	toyTaskCanceled
)

type toyTask struct {
	mock.Mock

	t      *testing.T
	status atomic.Int32

	expectForcefulExit bool
	injectedErrCh      chan error
}

func newToyTask(t *testing.T, willExitForcefully bool) *toyTask {
	return &toyTask{
		t:                  t,
		status:             *atomic.NewInt32(toyTaskUninit),
		expectForcefulExit: willExitForcefully,
		injectedErrCh:      make(chan error, 1),
	}
}

func (t *toyTask) Init(ctx context.Context) error {
	require.True(t.t, t.status.CAS(toyTaskUninit, toyTaskRunning))

	args := t.Called(ctx)
	return args.Error(0)
}

func (t *toyTask) Poll(ctx context.Context) error {
	require.Equal(t.t, toyTaskRunning, t.status.Load())

	select {
	case err := <-t.injectedErrCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (t *toyTask) Stop(ctx context.Context) error {
	require.True(t.t, t.status.CAS(toyTaskRunning, toyTaskCanceled))
	args := t.Called(ctx)
	return args.Error(0)
}

func (t *toyTask) NotifyExit(ctx context.Context, errIn error) error {
	if !errors.Is(errIn, errors.ErrWorkerCancel) {
		require.True(t.t, t.status.CAS(toyTaskRunning, toyTaskClosing))
	}

	args := t.Called(ctx, errIn)
	return args.Error(0)
}

func (t *toyTask) Close(ctx context.Context) error {
	if !t.expectForcefulExit {
		require.True(t.t, t.status.CAS(toyTaskClosing, toyTaskClosed))
	} else {
		require.True(t.t, t.status.CAS(toyTaskRunning, toyTaskClosed))
	}

	args := t.Called(ctx)
	return args.Error(0)
}

func (t *toyTask) ID() runtime.RunnableID {
	return "toy"
}

func TestRunnerNormalPath(t *testing.T) {
	t.Parallel()

	task := newToyTask(t, false)
	runner := NewRunner(task)

	errIn := errors.New("injected error")

	task.On("Init", mock.Anything).Return(nil).Once()
	task.On("NotifyExit", mock.Anything, errIn).Return(nil).Once()
	task.On("Close", mock.Anything).Return(nil).Once()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := runner.Run(context.Background())
		require.Error(t, err)
		require.Regexp(t, "injected error", err)
	}()

	require.Eventually(t, func() bool {
		return task.status.Load() == toyTaskRunning
	}, 1*time.Second, 10*time.Millisecond)

	task.injectedErrCh <- errIn

	require.Eventually(t, func() bool {
		return task.status.Load() == toyTaskClosed
	}, 1*time.Second, 10*time.Millisecond)

	wg.Wait()
	task.AssertExpectations(t)
}

func TestRunnerForcefulExit(t *testing.T) {
	t.Parallel()

	task := newToyTask(t, true)
	runner := NewRunner(task)

	errIn := errors.ErrWorkerSuicide.GenWithStackByArgs()

	task.On("Init", mock.Anything).Return(nil).Once()
	task.On("Close", mock.Anything).Return(nil).Once()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := runner.Run(context.Background())
		require.Error(t, err)
		require.Regexp(t, "ErrWorkerSuicide", err)
	}()

	require.Eventually(t, func() bool {
		return task.status.Load() == toyTaskRunning
	}, 1*time.Second, 10*time.Millisecond)

	task.injectedErrCh <- errIn

	require.Eventually(t, func() bool {
		return task.status.Load() == toyTaskClosed
	}, 1*time.Second, 10*time.Millisecond)

	wg.Wait()
	task.AssertExpectations(t)
}

func TestRunnerContextCanceled(t *testing.T) {
	t.Parallel()

	task := newToyTask(t, true)
	runner := NewRunner(task)

	task.On("Init", mock.Anything).Return(nil).Once()
	task.On("Close", mock.Anything).Return(nil).Once()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := runner.Run(ctx)
	require.Error(t, err)
	require.Regexp(t, "context canceled", err)
}

func TestRunnerStopByCancel(t *testing.T) {
	t.Parallel()

	task := newToyTask(t, true)
	runner := NewRunner(task)
	errIn := errors.ErrWorkerCancel.GenWithStackByArgs()

	task.On("Init", mock.Anything).Return(nil).Once()
	task.On("NotifyExit", mock.Anything, errIn).Return(nil).Once()
	task.On("Stop", mock.Anything).Return(nil).Once()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := runner.Run(context.Background())
		require.Error(t, err)
		require.Regexp(t, "worker is canceled", err)
	}()

	require.Eventually(t, func() bool {
		return task.status.Load() == toyTaskRunning
	}, 1*time.Second, 10*time.Millisecond)

	// Inject canceled worker and check runner.Stop is called
	task.injectedErrCh <- errIn
	require.Eventually(t, func() bool {
		return task.status.Load() == toyTaskCanceled
	}, 1*time.Second, 10*time.Millisecond)

	wg.Wait()
	task.AssertExpectations(t)
}
