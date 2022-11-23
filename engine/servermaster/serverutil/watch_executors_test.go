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

package serverutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type mockExecutorWatcher struct {
	mock.Mock
}

func newMockExecutorWatcher() *mockExecutorWatcher {
	return &mockExecutorWatcher{}
}

func (w *mockExecutorWatcher) WatchExecutors(ctx context.Context) (
	snap map[model.ExecutorID]string,
	stream *notifier.Receiver[model.ExecutorStatusChange],
	err error,
) {
	args := w.Called(ctx)
	return args.Get(0).(map[model.ExecutorID]string),
		args.Get(1).(*notifier.Receiver[model.ExecutorStatusChange]),
		args.Error(2)
}

type mockExecutorInfoUser struct {
	mock.Mock
}

func newMockExecutorInfoUser() *mockExecutorInfoUser {
	return &mockExecutorInfoUser{}
}

func (u *mockExecutorInfoUser) UpdateExecutorList(executors map[model.ExecutorID]string) error {
	args := u.Called(executors)
	return args.Error(0)
}

func (u *mockExecutorInfoUser) AddExecutor(executorID model.ExecutorID, addr string) error {
	args := u.Called(executorID, addr)
	return args.Error(0)
}

func (u *mockExecutorInfoUser) RemoveExecutor(executorID model.ExecutorID) error {
	args := u.Called(executorID)
	return args.Error(0)
}

func TestWatchExecutors(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	watcher := newMockExecutorWatcher()
	user := newMockExecutorInfoUser()

	snap := map[model.ExecutorID]string{
		"executor-1": "127.0.0.1:1111",
		"executor-2": "127.0.0.1:2222",
	}

	evNotifier := notifier.NewNotifier[model.ExecutorStatusChange]()
	defer evNotifier.Close()

	watcher.On("WatchExecutors", mock.Anything).
		Return(snap, evNotifier.NewReceiver(), nil).Times(1)
	user.On("UpdateExecutorList", snap).Return(nil).Times(1)

	events := []model.ExecutorStatusChange{
		{
			Tp:   model.EventExecutorOnline,
			ID:   "executor-3",
			Addr: "127.0.0.1:3333",
		},
		{
			Tp:   model.EventExecutorOffline,
			ID:   "executor-2",
			Addr: "127.0.0.1:2222",
		},
		{
			Tp:   model.EventExecutorOffline,
			ID:   "executor-3",
			Addr: "127.0.0.1:3333",
		},
	}

	var eventCount atomic.Int64
	for _, event := range events {
		if event.Tp == model.EventExecutorOnline {
			user.On("AddExecutor", event.ID, event.Addr).
				Return(nil).Times(1).
				Run(func(_ mock.Arguments) {
					eventCount.Add(1)
				})
		} else if event.Tp == model.EventExecutorOffline {
			user.On("RemoveExecutor", event.ID).
				Return(nil).Times(1).
				Run(func(_ mock.Arguments) {
					eventCount.Add(1)
				})
		} else {
			require.FailNow(t, "unexpected event type")
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := WatchExecutors(ctx, watcher, user)
		require.ErrorIs(t, err, context.Canceled)
	}()

	for _, event := range events {
		evNotifier.Notify(event)
	}

	require.Eventually(t, func() bool {
		return eventCount.Load() == int64(len(events))
	}, 1*time.Second, 10*time.Millisecond)

	cancel()
	wg.Wait()
}

func TestWatchExecutorFailed(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	watcher := newMockExecutorWatcher()
	user := newMockExecutorInfoUser()

	evNotifier := notifier.NewNotifier[model.ExecutorStatusChange]()
	defer evNotifier.Close()

	watcher.On("WatchExecutors", mock.Anything).
		Return(map[model.ExecutorID]string(nil),
			(*notifier.Receiver[model.ExecutorStatusChange])(nil),
			errors.New("test error"),
		).Times(1)

	err := WatchExecutors(ctx, watcher, user)
	require.ErrorContains(t, err, "test error")
}

func TestCloseWatchExecutors(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	watcher := newMockExecutorWatcher()
	user := newMockExecutorInfoUser()

	evNotifier := notifier.NewNotifier[model.ExecutorStatusChange]()

	snap := map[model.ExecutorID]string{}
	watcher.On("WatchExecutors", mock.Anything).
		Return(snap, evNotifier.NewReceiver(), nil).Times(1)
	user.On("UpdateExecutorList", snap).Return(nil).Times(1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := WatchExecutors(ctx, watcher, user)
		require.ErrorIs(t, err, errors.ErrExecutorWatcherClosed)
	}()

	evNotifier.Close()
	wg.Wait()
}
