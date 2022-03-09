package jobmaster

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/jobmaster/benchmark"
	"github.com/hanfei1991/microcosm/jobmaster/system"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/config"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/test"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

func RegisterBuilder() {
	runtime.OpBuilders[model.JobMasterType] = &jobMasterBuilder{}
}

type jobMasterBuilder struct{}

func getEtcdMetaKV(ctx context.Context, clients *client.Manager) (metadata.MetaKV, error) {
	resp, err := clients.MasterClient().QueryMetaStore(
		ctx,
		&pb.QueryMetaStoreRequest{Tp: pb.StoreType_ServiceDiscovery},
		5*time.Second,
	)
	if err != nil {
		return nil, err
	}
	log.L().Info("update service discovery metastore", zap.String("addr", resp.Address))

	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:        strings.Split(resp.GetAddress(), ","),
		Context:          ctx,
		LogConfig:        &logConfig,
		DialTimeout:      config.ServerMasterEtcdDialTimeout,
		AutoSyncInterval: config.ServerMasterEtcdSyncInterval,
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
		return nil, err
	}
	return metadata.NewMetaEtcd(etcdCli), nil
}

func (b *jobMasterBuilder) Build(op model.Operator) (runtime.Operator, bool, error) {
	cfg := &model.JobMaster{}
	err := json.Unmarshal(op, cfg)
	if err != nil {
		return nil, false, err
	}
	var jobMaster system.JobMaster
	clients := client.NewClientManager()
	err = clients.AddMasterClient(context.Background(), cfg.MasterAddrs)
	if err != nil {
		return nil, false, err
	}
	var metaKV metadata.MetaKV
	if !test.GetGlobalTestFlag() {
		metaKV, err = getEtcdMetaKV(context.Background(), clients)
		if err != nil {
			return nil, false, err
		}
	}
	switch cfg.Tp {
	case model.Benchmark:
		jobMaster, err = benchmark.BuildBenchmarkJobMaster(
			string(cfg.Config), cfg.ID, clients)
	default:
		return nil, false, errors.ErrExecutorUnknownOperator.FastGenByArgs(cfg.Tp)
	}
	if err != nil {
		return nil, false, err
	}
	return &jobMasterAgent{
		metaKV: metaKV,
		master: jobMaster,
	}, true, nil
}
