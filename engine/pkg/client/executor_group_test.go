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

package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

type executorClientFactoryStub struct {
	ctrl    *gomock.Controller
	clients map[string]*MockExecutorClient
}

func newExecutorClientFactoryStub(ctrl *gomock.Controller) *executorClientFactoryStub {
	return &executorClientFactoryStub{
		ctrl:    ctrl,
		clients: make(map[string]*MockExecutorClient),
	}
}

func (f *executorClientFactoryStub) NewExecutorClient(addr string) (ExecutorClient, error) {
	client := NewMockExecutorClient(f.ctrl)
	return client, nil
}

func TestExecutorGroup(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	factory := newExecutorClientFactoryStub(ctrl)
	group := newExecutorGroupWithClientFactory(nil, factory)

	_, exists := group.GetExecutorClient("executor-1")
	require.False(t, exists)

	err := group.AddExecutor("executor-1", "test-addr:1234")
	require.NoError(t, err)
	cli, exists := group.GetExecutorClient("executor-1")
	require.True(t, exists)
	require.NotNil(t, cli)

	err = group.AddExecutor("executor-1", "test-addr:1234")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrExecutorAlreadyExists))

	cli.(*MockExecutorClient).EXPECT().Close().Times(1)
	err = group.RemoveExecutor("executor-1")
	require.NoError(t, err)
	_, exists = group.GetExecutorClient("executor-1")
	require.False(t, exists)

	err = group.RemoveExecutor("executor-1")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrExecutorNotFound))

	err = group.UpdateExecutorList(map[model.ExecutorID]string{
		"executor-1": "test-addr:1234",
		"executor-2": "test-addr:2345",
	})
	require.NoError(t, err)
	cli2, exists := group.GetExecutorClient("executor-2")
	require.True(t, exists)
	require.NotNil(t, cli)

	cli2.(*MockExecutorClient).EXPECT().Close().Times(1)
	err = group.UpdateExecutorList(map[model.ExecutorID]string{
		"executor-1": "test-addr:1234",
		"executor-3": "test-addr:3456",
	})
	require.NoError(t, err)
	_, exists = group.GetExecutorClient("executor-2")
	require.False(t, exists)

	_, exists = group.GetExecutorClient("executor-3")
	require.True(t, exists)
}

func TestGetExecutorBlocked(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	factory := newExecutorClientFactoryStub(ctrl)
	group := newExecutorGroupWithClientFactory(nil, factory)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		cli, err := group.GetExecutorClientB(ctx, "executor-1")
		require.NoError(t, err)
		require.NotNil(t, cli)
	}()

	time.Sleep(10 * time.Millisecond)
	err := group.AddExecutor("executor-1", "test-addr:1234")
	require.NoError(t, err)

	wg.Wait()
}

func TestGetExecutorTombstone(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	factory := newExecutorClientFactoryStub(ctrl)
	group := newExecutorGroupWithClientFactory(nil, factory)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := group.AddExecutor("executor-1", "test-addr:1234")
	require.NoError(t, err)
	err = group.AddExecutor("executor-2", "test-addr-2:1234")
	require.NoError(t, err)

	cli, ok := group.GetExecutorClient("executor-1")
	require.True(t, ok)
	cli.(*MockExecutorClient).EXPECT().Close().Times(1)

	cli, ok = group.GetExecutorClient("executor-2")
	require.True(t, ok)
	cli.(*MockExecutorClient).EXPECT().Close().Times(1)

	err = group.RemoveExecutor("executor-1")
	require.NoError(t, err)
	err = group.RemoveExecutor("executor-2")
	require.NoError(t, err)

	startTime := time.Now()
	_, err = group.GetExecutorClientB(ctx, "executor-1")
	require.Error(t, err)
	_, err = group.GetExecutorClientB(ctx, "executor-2")
	require.Error(t, err)
	require.Less(t, time.Since(startTime), 100*time.Millisecond)
}

func TestExecutorTombstoneCleaned(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	factory := newExecutorClientFactoryStub(ctrl)
	group := newExecutorGroupWithClientFactory(nil, factory)
	group.tombstoneKeepTime = time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := group.AddExecutor("executor-1", "test-addr:1234")
	require.NoError(t, err)
	err = group.AddExecutor("executor-2", "test-addr-2:1234")
	require.NoError(t, err)

	cli, ok := group.GetExecutorClient("executor-1")
	require.True(t, ok)
	cli.(*MockExecutorClient).EXPECT().Close().Times(1)

	cli, ok = group.GetExecutorClient("executor-2")
	require.True(t, ok)
	cli.(*MockExecutorClient).EXPECT().Close().Times(1)

	err = group.RemoveExecutor("executor-1")
	require.NoError(t, err)
	err = group.RemoveExecutor("executor-2")
	require.NoError(t, err)

	// wait for the tombstones to be cleaned up
	time.Sleep(100 * time.Millisecond)

	_, err = group.GetExecutorClientB(ctx, "executor-1")
	require.True(t, errors.Is(err, errors.ErrExecutorNotFound))
}

func TestGetExecutorBCanceled(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	factory := newExecutorClientFactoryStub(ctrl)
	group := newExecutorGroupWithClientFactory(nil, factory)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := group.GetExecutorClientB(ctx, "executor-1")
		require.Error(t, err)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	wg.Wait()
}
