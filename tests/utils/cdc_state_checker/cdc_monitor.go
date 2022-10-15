// Copyright 2020 PingCAP, Inc.
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

package main

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/migrate"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

type cdcMonitor struct {
	etcdCli    *etcd.Client
	etcdWorker *orchestrator.EtcdWorker
	reactor    *cdcMonitReactor
}

func newCDCMonitor(ctx context.Context, pd string, credential *security.Credential) (*cdcMonitor, error) {
	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	grpcCredential, err := credential.ToGRPCDialOption()
	if err != nil {
		return nil, errors.Trace(err)
	}

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{pd},
		TLS:         nil,
		Context:     ctx,
		LogConfig:   &logConfig,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpcCredential,
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		},
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	wrappedCli := etcd.Wrap(etcdCli, map[string]prometheus.Counter{})
	reactor := &cdcMonitReactor{}
	initState := newCDCReactorState()
	cli, err := etcd.NewCDCEtcdClient(ctx, etcdCli, "default")
	if err != nil {
		return nil, errors.Trace(err)
	}
	etcdWorker, err := orchestrator.NewEtcdWorker(cli,
		etcd.BaseKey(etcd.DefaultCDCClusterID), reactor, initState,
		&migrate.NoOpMigrator{})
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret := &cdcMonitor{
		etcdCli:    wrappedCli,
		etcdWorker: etcdWorker,
		reactor:    reactor,
	}

	return ret, nil
}

func (m *cdcMonitor) run(ctx context.Context) error {
	log.Debug("start running cdcMonitor")
	err := m.etcdWorker.Run(ctx, nil, 200*time.Millisecond, "127.0.0.1")
	log.Error("etcdWorker exited: test-case-failed", zap.Error(err))
	log.Info("CDC state", zap.Reflect("state", m.reactor.state))
	return err
}
