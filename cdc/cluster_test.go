package cdc

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	tablepipeline "github.com/pingcap/ticdc/cdc/processor/pipeline"

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

func (t *ClusterTester) SetClusterBarrierTs(ts model.Ts) {}

func (t *ClusterTester) CreateTable(tableID model.TableID, tableName model.TableName, startTs model.Ts) {
}

func (t *ClusterTester) CheckConsistency(consistencyTs model.Ts) {}

type mockUpstream struct {
	clusterBarrierTs model.Ts

	tableReceivedTsMap map[model.TableID]*[]model.Ts
}

func (u *mockUpstream) setClusterBarrierTs(ts model.Ts) {}

func (u *mockUpstream) applyDDLJob(job *timodel.Job) {}

func (u *mockUpstream) ddlPuller() {}

func (u *mockUpstream) createTablePipeline(ctx context.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error) {
	outputCh := make(chan model.Ts, 128)
	ctx, cancel := context.WithCancel(ctx)
	tablePipeline := &mockTablePipeline{
		tableID:          tableID,
		replicaInfo:      replicaInfo,
		resolvedTs:       replicaInfo.StartTs,
		checkpointTs:     replicaInfo.StartTs,
		barrierTs:        replicaInfo.StartTs,
		targetTs:         math.MaxUint64,
		clusterBarrierTs: &u.clusterBarrierTs,
		status:           tablepipeline.TableStatusInitializing,
		outputCh:         outputCh,
		cancel:           cancel,
	}
	tablePipeline.wg.Add(1)
	var receivedTsArrPtr *[]model.Ts
	var exist bool
	if receivedTsArrPtr, exist = u.tableReceivedTsMap[tableID]; !exist {
		receivedTsArr := make([]model.Ts, 0, 128)
		receivedTsArrPtr = &receivedTsArr
		u.tableReceivedTsMap[tableID] = receivedTsArrPtr
	}

	go tablePipeline.run(ctx)
	go func() {
		for ts := range outputCh {
			*receivedTsArrPtr = append(*receivedTsArrPtr, ts)
		}
	}()
	return tablePipeline, nil
}

func (u *mockUpstream) checkConsistency(consistencyTs model.Ts) {
}

type mockTablePipeline struct {
	tableID          model.TableID
	replicaInfo      *model.TableReplicaInfo
	resolvedTs       model.Ts
	checkpointTs     model.Ts
	barrierTs        model.Ts
	targetTs         model.Ts
	clusterBarrierTs *model.Ts
	status           tablepipeline.TableStatus

	cancel   context.CancelFunc
	wg       sync.WaitGroup
	outputCh chan model.Ts
}

func (m *mockTablePipeline) run(ctx context.Context) {
	defer m.wg.Done()
	defer close(m.outputCh)
	defer m.status.Store(tablepipeline.TableStatusStopped)
	m.status.Store(tablepipeline.TableStatusRunning)
	for {
		if m.CheckpointTs() == atomic.LoadUint64(&m.targetTs) {
			return
		}
		nextResolvedTs := m.calcResolvedTs()
		nextCheckpointTs := m.CheckpointTs() + 1
		if nextCheckpointTs > nextResolvedTs {
			nextCheckpointTs = nextResolvedTs
		}
		select {
		case <-ctx.Done():
			return
		case m.outputCh <- nextCheckpointTs:
		}
		atomic.StoreUint64(&m.resolvedTs, nextResolvedTs)
		atomic.StoreUint64(&m.checkpointTs, nextCheckpointTs)
		time.Sleep(time.Duration(rand.Int63n(10)) * time.Millisecond)
	}
}

func (m *mockTablePipeline) calcResolvedTs() model.Ts {
	resolvedTs := m.ResolvedTs()
	resolvedTs += model.Ts(rand.Int63n(2))
	barrierTs := atomic.LoadUint64(&m.barrierTs)
	if resolvedTs > barrierTs {
		resolvedTs = barrierTs
	}
	targetTs := atomic.LoadUint64(&m.targetTs)
	if resolvedTs > targetTs {
		resolvedTs = targetTs
	}
	clusterBarrierTs := atomic.LoadUint64(m.clusterBarrierTs)
	if resolvedTs > clusterBarrierTs {
		resolvedTs = clusterBarrierTs
	}
	return resolvedTs
}

func (m *mockTablePipeline) ID() (tableID, markTableID int64) {
	return m.tableID, m.replicaInfo.MarkTableID
}

func (m *mockTablePipeline) Name() string {
	return fmt.Sprintf("mock-tabke-%d-%d", m.tableID, m.replicaInfo.MarkTableID)
}

func (m *mockTablePipeline) ResolvedTs() model.Ts {
	return atomic.LoadUint64(&m.resolvedTs)
}

func (m *mockTablePipeline) CheckpointTs() model.Ts {
	return atomic.LoadUint64(&m.checkpointTs)
}

func (m *mockTablePipeline) UpdateBarrierTs(ts model.Ts) {
	atomic.StoreUint64(&m.barrierTs, ts)
}

func (m *mockTablePipeline) AsyncStop(targetTs model.Ts) {
	atomic.StoreUint64(&m.targetTs, targetTs)
}

func (m *mockTablePipeline) Workload() model.WorkloadInfo {
	return model.WorkloadInfo{Workload: 1}
}

func (m *mockTablePipeline) Status() tablepipeline.TableStatus {
	return m.status.Load()
}

func (m *mockTablePipeline) Cancel() {
	m.cancel()
}

func (m *mockTablePipeline) Wait() {
	m.wg.Wait()
}
