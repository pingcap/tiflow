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

package capture

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/cdc/model"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	mock_processor "github.com/pingcap/tiflow/cdc/processor/mock"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestReset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// init etcd mocker
	clientURL, etcdServer, err := etcd.SetupEmbedEtcd(t.TempDir())
	require.Nil(t, err)
	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL.String()},
		Context:     ctx,
		LogConfig:   &logConfig,
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	client := etcd.NewCDCEtcdClient(ctx, etcdCli)
	// Close the client before the test function exits to prevent possible
	// ctx leaks.
	// Ref: https://github.com/grpc/grpc-go/blob/master/stream.go#L229
	defer client.Close()

	cp := NewCapture4Test(nil)
	cp.EtcdClient = &client

	// simulate network isolation scenarios
	etcdServer.Close()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err = cp.reset(ctx)
		require.Regexp(t, ".*context canceled.*", err)
		wg.Done()
	}()
	time.Sleep(100 * time.Millisecond)
	info, err := cp.Info()
	require.Nil(t, err)
	require.NotNil(t, info)
	cancel()
	wg.Wait()
}

func TestInfo(t *testing.T) {
	cp := NewCapture4Test(nil)
	cp.info = nil
	require.NotPanics(t, func() { cp.Info() })
}

func TestDrainImmediately(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mm := mock_processor.NewMockManager(ctrl)
	cp := &captureImpl{processorManager: mm}
	require.Equal(t, model.LivenessCaptureAlive, cp.Liveness())

	// Drain completes immediately.
	mm.EXPECT().
		QueryTableCount(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, tableCh chan int, done chan<- error) {
			tableCh <- 0
			close(done)
		})
	done := cp.Drain(ctx)
	select {
	case <-done:
		require.Equal(t, model.LivenessCaptureStopping, cp.Liveness())
	case <-time.After(time.Second):
		require.Fail(t, "timeout")
	}
}

func TestDrainWaitsTables(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mm := mock_processor.NewMockManager(ctrl)
	cp := &captureImpl{processorManager: mm}
	require.Equal(t, model.LivenessCaptureAlive, cp.Liveness())

	// Drain waits for moving out all tables.
	calls := 0
	t2 := mm.EXPECT().
		QueryTableCount(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, tableCh chan int, done chan<- error) {
			tableCh <- 2
			calls = 1
			close(done)
		})
	t1 := mm.EXPECT().
		QueryTableCount(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, tableCh chan int, done chan<- error) {
			tableCh <- 1
			calls = 2
			close(done)
		}).After(t2)
	mm.EXPECT().
		QueryTableCount(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, tableCh chan int, done chan<- error) {
			tableCh <- 0
			calls = 3
			close(done)
		}).After(t1)
	done := cp.Drain(ctx)
	select {
	case <-done:
		require.Equal(t, model.LivenessCaptureStopping, cp.Liveness())
		require.EqualValues(t, 3, calls)
	case <-time.After(3 * time.Second):
		require.Fail(t, "timeout")
	}
}

func TestDrainWaitsOwnerResign(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	mm := mock_processor.NewMockManager(ctrl)
	cp := &captureImpl{processorManager: mm, owner: mo}
	require.Equal(t, model.LivenessCaptureAlive, cp.Liveness())

	ownerStopCh := make(chan struct{}, 1)
	mo.EXPECT().AsyncStop().Do(func() {
		select {
		case ownerStopCh <- struct{}{}:
		default:
		}
	}).AnyTimes()
	mm.EXPECT().
		QueryTableCount(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, tableCh chan int, done chan<- error) {
			tableCh <- 0
			close(done)
		})

	done := cp.Drain(ctx)

	// Must wait owner resign by wait for async close.
	select {
	case <-ownerStopCh:
		// Simulate owner has resigned.
		require.Equal(t, model.LivenessCaptureAlive, cp.Liveness())
		cp.setOwner(nil)
	case <-time.After(3 * time.Second):
		require.Fail(t, "timeout")
	case <-done:
		require.Fail(t, "unexpected")
	}

	select {
	case <-time.After(3 * time.Second):
		require.Fail(t, "timeout")
	case <-done:
		require.Equal(t, model.LivenessCaptureStopping, cp.Liveness())
	}
}
