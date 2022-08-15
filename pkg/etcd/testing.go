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

package etcd

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

// Tester is for ut tests
type Tester struct {
	dir       string
	etcd      *embed.Etcd
	ClientURL *url.URL
	client    *CDCEtcdClientImpl
	ctx       context.Context
	cancel    context.CancelFunc
	errg      *errgroup.Group
}

// SetUpTest setup etcd tester
func (s *Tester) SetUpTest(t *testing.T) {
	var err error
	s.dir = t.TempDir()
	s.ClientURL, s.etcd, err = SetupEmbedEtcd(s.dir)
	require.Nil(t, err)
	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.ClientURL.String()},
		DialTimeout: 3 * time.Second,
		LogConfig:   &logConfig,
	})
	require.NoError(t, err)

	s.client, err = NewCDCEtcdClient(context.TODO(), client, DefaultCDCClusterID)
	require.Nil(t, err)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.errg = util.HandleErrWithErrGroup(s.ctx, s.etcd.Err(), func(e error) { t.Log(e) })
}

// TearDownTest teardown etcd
func (s *Tester) TearDownTest(t *testing.T) {
	s.etcd.Close()
	s.cancel()
logEtcdError:
	for {
		select {
		case err, ok := <-s.etcd.Err():
			if !ok {
				break logEtcdError
			}
			t.Logf("etcd server error: %v", err)
		default:
			break logEtcdError
		}
	}
	_ = s.client.Close() //nolint:errcheck
}
