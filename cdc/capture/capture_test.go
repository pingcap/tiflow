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
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/etcd"
	mock_etcd "github.com/pingcap/tiflow/pkg/etcd/mock"
	"github.com/prometheus/client_golang/prometheus"
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

	client, err := etcd.NewCDCEtcdClient(ctx, etcdCli, etcd.DefaultCDCClusterID)
	require.Nil(t, err)
	// Close the client before the test function exits to prevent possible
	// ctx leaks.
	// Ref: https://github.com/grpc/grpc-go/blob/master/stream.go#L229
	defer client.Close()

	cp := NewCapture4Test(nil)
	cp.EtcdClient = client

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

type mockEtcdClient struct {
	etcd.CDCEtcdClient
	clientv3.Lease
	called chan struct{}
}

func (m *mockEtcdClient) GetEtcdClient() *etcd.Client {
	cli := &clientv3.Client{Lease: m}
	return etcd.Wrap(cli, map[string]prometheus.Counter{})
}

func (m *mockEtcdClient) Grant(_ context.Context, _ int64) (*clientv3.LeaseGrantResponse, error) {
	select {
	case m.called <- struct{}{}:
	default:
	}
	return nil, context.DeadlineExceeded
}

func TestRetryInternalContextDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	called := make(chan struct{}, 2)
	cp := NewCapture4Test(nil)
	// In the current implementation, the first RPC is grant.
	// the mock client always retry DeadlineExceeded for the RPC.
	cp.EtcdClient = &mockEtcdClient{called: called}

	errCh := make(chan error, 1)
	go func() {
		errCh <- cp.Run(ctx)
	}()
	time.Sleep(100 * time.Millisecond)
	// Waiting for Grant to be called.
	<-called
	time.Sleep(100 * time.Millisecond)
	// Make sure it retrys
	<-called

	// Do not retry context canceled.
	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout")
	}
}

func TestInfo(t *testing.T) {
	cp := NewCapture4Test(nil)
	cp.info = nil
	require.NotPanics(t, func() { cp.Info() })
}

func TestDrainCaptureBySignal(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mm := mock_processor.NewMockManager(ctrl)
	me := mock_etcd.NewMockCDCEtcdClient(ctrl)
	cp := &captureImpl{
		info: &model.CaptureInfo{
			ID:            "capture-for-test",
			AdvertiseAddr: "127.0.0.1", Version: "test",
		},
		processorManager: mm,
		config:           config.GetDefaultServerConfig(),
		EtcdClient:       me,
	}
	require.Equal(t, model.LivenessCaptureAlive, cp.Liveness())

	done := cp.Drain()
	select {
	case <-done:
		require.Equal(t, model.LivenessCaptureStopping, cp.Liveness())
	case <-time.After(time.Second):
		require.Fail(t, "timeout")
	}
}

func TestDrainWaitsOwnerResign(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mo := mock_owner.NewMockOwner(ctrl)
	mm := mock_processor.NewMockManager(ctrl)
	me := mock_etcd.NewMockCDCEtcdClient(ctrl)
	cp := &captureImpl{
		EtcdClient: me,
		info: &model.CaptureInfo{
			ID:            "capture-for-test",
			AdvertiseAddr: "127.0.0.1", Version: "test",
		},
		processorManager: mm,
		owner:            mo,
		config:           config.GetDefaultServerConfig(),
	}
	require.Equal(t, model.LivenessCaptureAlive, cp.Liveness())

	mo.EXPECT().AsyncStop().Do(func() {}).AnyTimes()

	done := cp.Drain()
	select {
	case <-time.After(3 * time.Second):
		require.Fail(t, "timeout")
	case <-done:
		require.Equal(t, model.LivenessCaptureStopping, cp.Liveness())
	}
}

type mockElection struct {
	campaignRequestCh chan struct{}
	campaignGrantCh   chan struct{}

	campaignFlag, resignFlag bool
}

func (e *mockElection) campaign(ctx context.Context, key string) error {
	e.campaignRequestCh <- struct{}{}
	<-e.campaignGrantCh
	e.campaignFlag = true
	return nil
}

func (e *mockElection) resign(ctx context.Context) error {
	e.resignFlag = true
	return nil
}

func TestCampaignLiveness(t *testing.T) {
	t.Parallel()

	me := &mockElection{
		campaignRequestCh: make(chan struct{}, 1),
		campaignGrantCh:   make(chan struct{}, 1),
	}
	cp := &captureImpl{
		config:   config.GetDefaultServerConfig(),
		info:     &model.CaptureInfo{ID: "test"},
		election: me,
	}
	ctx := cdcContext.NewContext4Test(context.Background(), true)

	cp.liveness.Store(model.LivenessCaptureStopping)
	err := cp.campaignOwner(ctx)
	require.Nil(t, err)
	require.False(t, me.campaignFlag)

	// Force set alive.
	cp.liveness = model.LivenessCaptureAlive
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Grant campaign
		g := <-me.campaignRequestCh
		// Set liveness to stopping
		cp.liveness.Store(model.LivenessCaptureStopping)
		me.campaignGrantCh <- g
	}()
	err = cp.campaignOwner(ctx)
	require.Nil(t, err)
	require.True(t, me.campaignFlag)
	require.True(t, me.resignFlag)

	wg.Wait()
}
