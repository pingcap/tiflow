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
