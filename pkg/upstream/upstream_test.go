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

package upstream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestUpstreamShouldClose(t *testing.T) {
	t.Parallel()

	up := &Upstream{}
	up.isDefaultUpstream = false
	mockClock := clock.NewMock()
	require.False(t, up.shouldClose())
	up.clock = mockClock
	up.idleTime = mockClock.Now().Add(-2 * maxIdleDuration)
	require.True(t, up.shouldClose())
	up.isDefaultUpstream = true
	require.False(t, up.shouldClose())
}

func TestUpstreamError(t *testing.T) {
	t.Parallel()

	up := &Upstream{}
	err := errors.New("test")
	up.err.Store(err)
	require.Equal(t, err, up.Error())
	up.err.Store(nil)
	require.Nil(t, up.Error())
}

func TestUpstreamIsNormal(t *testing.T) {
	t.Parallel()

	up := &Upstream{}
	up.status = uninit
	require.False(t, up.IsNormal())
	up.status = normal
	require.True(t, up.IsNormal())
	up.err.Store(errors.New("test"))
	require.False(t, up.IsNormal())
}

func TestTrySetIdleTime(t *testing.T) {
	t.Parallel()

	up := newUpstream(nil, nil)
	require.Equal(t, uninit, up.status)
	up.clock = clock.New()
	up.trySetIdleTime()
	require.False(t, up.idleTime.IsZero())
	idleTime := up.idleTime
	up.trySetIdleTime()
	require.Equal(t, idleTime, up.idleTime)
	up.resetIdleTime()
	require.True(t, up.idleTime.IsZero())
	up.resetIdleTime()
	require.True(t, up.idleTime.IsZero())
}

func TestRegisterTopo(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientURL, etcdServer, err := etcd.SetupEmbedEtcd(t.TempDir())
	defer etcdServer.Close()

	require.NoError(t, err)
	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)

	rawEtcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL.String()},
		Context:     ctx,
		LogConfig:   &logConfig,
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer rawEtcdCli.Close()
	etcdCli := etcd.Wrap(rawEtcdCli, make(map[string]prometheus.Counter))
	up := &Upstream{
		cancel:  func() {},
		etcdCli: etcdCli,
		wg:      &sync.WaitGroup{},
	}

	info := &model.CaptureInfo{
		AdvertiseAddr: "localhost:8300",
		Version:       "test.1.0",
	}
	err = up.registerTopologyInfo(ctx, CaptureTopologyCfg{
		CaptureInfo: info,
		GCServiceID: "clusterID",
		SessionTTL:  2,
	})
	require.NoError(t, err)

	resp, err := etcdCli.Get(ctx, "/topology/ticdc/clusterID/localhost:8300")
	require.NoError(t, err)

	infoData, err := info.Marshal()
	require.NoError(t, err)
	require.Equal(t, infoData, resp.Kvs[0].Value)

	up.etcdCli = nil
	up.Close()
	require.Eventually(t, func() bool {
		resp, err := etcdCli.Get(ctx, "/topology/ticdc/clusterID/localhost:8300")
		require.NoError(t, err)
		return len(resp.Kvs) == 0
	}, time.Second*5, time.Millisecond*100)
}
