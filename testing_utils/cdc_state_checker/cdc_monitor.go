package main

import (
	"context"
	"github.com/pingcap/log"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

type cdcMonitor struct {
	etcdCli    *etcd.Client
	etcdWorker *orchestrator.EtcdWorker
	reactor    *CDCMonitReactor
}

func newCDCMonitor(ctx context.Context, pd string) (*cdcMonitor, error) {
	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{pd},
		TLS:         nil,
		Context:     ctx,
		LogConfig:   &logConfig,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithInsecure(),
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
	reactor := &CDCMonitReactor{}
	initState := newCDCReactorState()
	etcdWorker, err := orchestrator.NewEtcdWorker(wrappedCli, kv.EtcdKeyBase, reactor, initState)
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
	err := m.etcdWorker.Run(ctx, 200 * time.Millisecond)
	log.Error("etcdWorker exited", zap.Error(err))
	log.Info("CDC state", zap.Reflect("state", m.reactor.state))
	return err
}
