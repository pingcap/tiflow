package cdc

import (
	"context"
	"time"

	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/capture"
	"github.com/pingcap/ticdc/cdc/kv"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/etcd"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

type ClusterTester struct {
	c *check.C

	embedEtcd  *embed.Etcd
	etcdClient *kv.CDCEtcdClient

	captures []*capture.Capture
}

func NewClusterTester(c *check.C, captureNum int) *ClusterTester {
	captures := make([]*capture.Capture, 0, captureNum)
	for i := 0; i < captureNum; i++ {
		captures = append(captures, capture.NewCapture())
	}
	return &ClusterTester{c: c, captures: captures}
}

func (t *ClusterTester) Run(ctx context.Context) error {
	errg, ctx := errgroup.WithContext(ctx)
	t.runEtcd(ctx)
	for _, capture := range t.captures {
		errg.Go(func() error {
			return t.runCapture(ctx, capture)
		})
	}
	return errg.Wait()
}

func (t *ClusterTester) runCapture(ctx context.Context, capture *capture.Capture) error {
	captureInfo := capture.Info()
	cdcCtx := cdcContext.NewContext(ctx, &cdcContext.GlobalVars{
		PDClient:    nil,
		KVStorage:   nil,
		CaptureInfo: &captureInfo,
		EtcdClient:  t.etcdClient,
	})
	err := capture.Run(cdcCtx)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (t *ClusterTester) runEtcd(ctx context.Context) {
	dir := t.c.MkDir()
	clientURL, embedEtcd, err := etcd.SetupEmbedEtcd(dir)
	t.c.Assert(err, check.IsNil)
	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL.String()},
		DialTimeout: 3 * time.Second,
		LogConfig:   &logConfig,
	})
	t.c.Assert(err, check.IsNil)
	etcdClient := kv.NewCDCEtcdClient(ctx, client)
	t.embedEtcd = embedEtcd
	t.etcdClient = &etcdClient
}

func (t *ClusterTester) applyDDLJob(job *timodel.Job) {}

func (t *ClusterTester) SetClusterBarrierTs(ts model.Ts) {}

func (t *ClusterTester) CreateTable(tableID model.TableID, tableName model.TableName, startTs model.Ts) {
}

func (t *ClusterTester) CheckConsistency() {}
