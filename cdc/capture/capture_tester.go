// Copyright 2021 PingCAP, Inc.
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
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/owner"
	"github.com/pingcap/ticdc/cdc/processor"
	tablepipeline "github.com/pingcap/ticdc/cdc/processor/pipeline"
	"github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/version"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

type Tester struct {
	c *check.C

	embedEtcd    *embed.Etcd
	etcdClient   *kv.CDCEtcdClient
	pdClient     pd.Client
	kvStorage    tidbkv.Storage
	schemaHelper *entry.SchemaTestHelper

	ddlPuller         *mockDDLPuller
	captureCreatingCh chan *Capture
	wg                sync.WaitGroup
}

func NewTester(ctx context.Context, c *check.C, captureNum int) *Tester {
	log.Info("LEOPPRO111 1")
	cluster := &Tester{
		c:                 c,
		captureCreatingCh: make(chan *Capture),
		schemaHelper:      entry.NewSchemaTestHelper(c),
		pdClient:          &mockPDClient{},
	}
	cluster.ddlPuller = newMockDDLPuller(c, cluster.schemaHelper)
	cluster.runEtcd(ctx)
	cluster.wg.Add(1)
	go func() {
		defer cluster.wg.Done()
		cluster.run(ctx)
	}()
	log.Info("LEOPPRO111 2")
	cluster.ScaleOut(captureNum)
	log.Info("LEOPPRO111 3")
	return cluster
}

func (t *Tester) ScaleOut(captureNum int) {
	for i := 0; i < captureNum; i++ {
		capture := newCapture4Test(t.pdClient, t.kvStorage, t.etcdClient,
			func() *processor.Manager {
				return processor.NewManager4Test(
					func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error) {
						return createTablePipeline(ctx, tableID, replicaInfo), nil
					})
			}, func() *owner.Owner {
				return owner.NewOwner4Test(func(ctx cdcContext.Context, startTs uint64) (owner.DDLPuller, error) {
					return t.ddlPuller, nil
				}, func(ctx cdcContext.Context) (owner.AsyncSink, error) {
					return &mockAsyncSink{}, nil
				})
			})
		t.captureCreatingCh <- capture
	}
}

func (t *Tester) ScaleIn(captureNum int, force bool) {
	// todo
}

func (t *Tester) CreateChangefeed(ctx context.Context, name string, startTs uint64, targetTs uint64, rules ...string) {
	info := new(model.ChangeFeedInfo)
	info.StartTs = startTs
	info.TargetTs = targetTs
	info.Config = config.GetDefaultReplicaConfig()
	info.CreatorVersion = version.ReleaseVersion
	info.Config.Filter.Rules = rules
	err := t.etcdClient.CreateChangefeedInfo(ctx, info, name)
	t.c.Assert(err, check.IsNil)
}

func (t *Tester) CurrentVersion() model.Ts{
	ver,err:=t.schemaHelper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	t.c.Assert(err, check.IsNil)
	return ver.Ver
}

func (t *Tester) run(ctx context.Context) {
	errg, ctx := errgroup.WithContext(ctx)
MainLoop:
	for {
		var cap *Capture
		select {
		case <-ctx.Done():
			break MainLoop
		case cap = <-t.captureCreatingCh:
			errg.Go(func() error {
				return cap.Run(ctx)
			})
		}
	}
	t.c.Assert(errg.Wait(), check.IsNil)
}

func (t *Tester) runEtcd(ctx context.Context) {
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

func (t *Tester) ApplyDDLJob(query string) {
	t.ddlPuller.applyDDLJob(query)
}

func (t *Tester) CheckpointTs() map[model.TableID]model.Ts {
	panic("unimplemented")
}

type mockAsyncSink struct {
}

func (m *mockAsyncSink) Initialize(ctx cdcContext.Context, tableInfo []*model.SimpleTableInfo) error {
	log.Info("mock async sink Initialize", zap.Any("tableInfo", tableInfo))
	return nil
}

func (m *mockAsyncSink) EmitCheckpointTs(ctx cdcContext.Context, ts uint64) {
	log.Info("mock async sink EmitCheckpointTs", zap.Any("ts", ts))
}

func (m *mockAsyncSink) EmitDDLEvent(ctx cdcContext.Context, ddl *model.DDLEvent) (bool, error) {
	log.Info("mock async sink EmitDDLEvent", zap.Any("ddl", ddl))
	return true, nil
}

func (m *mockAsyncSink) SinkSyncpoint(ctx cdcContext.Context, checkpointTs uint64) error {
	log.Info("mock async sink SinkSyncpoint", zap.Any("checkpointTs", checkpointTs))
	return nil
}

func (m *mockAsyncSink) Close() error {
	log.Info("mock async sink Close")
	return nil
}

type mockPDClient struct {
	pd.Client
}

func (m *mockPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	log.Info("mock pd client update service gc safe point", zap.String("serviceID", serviceID), zap.Int64("ttl", ttl), zap.Uint64("safePoint", safePoint))
	return safePoint, nil
}

func (m *mockPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	return oracle.GetPhysical(time.Now()), 0, nil
}

type mockDDLPuller struct {
	c *check.C

	resolvedTs model.Ts
	ddlQueue   []*timodel.Job
	ddlQueueMu sync.Mutex

	ddlQueryQueue chan string
	schemaHelper  *entry.SchemaTestHelper
}

func newMockDDLPuller(c *check.C, schemaHelper *entry.SchemaTestHelper) *mockDDLPuller {
	return &mockDDLPuller{
		c:             c,
		schemaHelper:  schemaHelper,
		ddlQueryQueue: make(chan string, 16),
	}
}

func (m *mockDDLPuller) applyDDLJob(query string) {
	m.ddlQueryQueue <- query
}

func (m *mockDDLPuller) Run(ctx cdcContext.Context) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return nil
		case query := <-m.ddlQueryQueue:
			job := m.schemaHelper.DDL2Job(query)
			m.ddlQueueMu.Lock()
			m.ddlQueue = append(m.ddlQueue, job)
			m.resolvedTs = job.BinlogInfo.FinishedTS
			m.ddlQueueMu.Unlock()
		case <-ticker.C:
			ver, err := m.schemaHelper.Storage().CurrentVersion(oracle.GlobalTxnScope)
			if err != nil {
				return errors.Trace(err)
			}
			m.ddlQueueMu.Lock()
			log.Info("resolvedts ddl",zap.Uint64("re",ver.Ver))
			m.resolvedTs = ver.Ver
			m.ddlQueueMu.Unlock()
		}
	}
}

func (m *mockDDLPuller) FrontDDL() (uint64, *timodel.Job) {
	m.ddlQueueMu.Lock()
	defer m.ddlQueueMu.Unlock()
	if len(m.ddlQueue) == 0 {
		return atomic.LoadUint64(&m.resolvedTs), nil
	}
	return m.ddlQueue[0].BinlogInfo.FinishedTS, m.ddlQueue[0]
}

func (m *mockDDLPuller) PopFrontDDL() (uint64, *timodel.Job) {
	m.ddlQueueMu.Lock()
	defer m.ddlQueueMu.Unlock()
	if len(m.ddlQueue) == 0 {
		return atomic.LoadUint64(&m.resolvedTs), nil
	}
	job := m.ddlQueue[0]
	m.ddlQueue = m.ddlQueue[1:]
	return job.BinlogInfo.FinishedTS, job
}

func (m *mockDDLPuller) Close() {
	// do nothing
}

func createTablePipeline(ctx context.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) tablepipeline.TablePipeline {
	log.Info("create mock table pipeline", zap.Int64("tableID", tableID), zap.Reflect("replicaInfo", replicaInfo))
	ctx, cancel := context.WithCancel(ctx)
	tablePipeline := &mockTablePipeline{
		tableID:      tableID,
		replicaInfo:  replicaInfo,
		resolvedTs:   replicaInfo.StartTs,
		checkpointTs: replicaInfo.StartTs,
		barrierTs:    replicaInfo.StartTs,
		targetTs:     math.MaxUint64,
		status:       tablepipeline.TableStatusInitializing,
		cancel:       cancel,
	}

	tablePipeline.wg.Add(1)
	go tablePipeline.run(ctx)
	return tablePipeline
}

type mockTablePipeline struct {
	tableID      model.TableID
	replicaInfo  *model.TableReplicaInfo
	resolvedTs   model.Ts
	checkpointTs model.Ts
	barrierTs    model.Ts
	targetTs     model.Ts
	status       tablepipeline.TableStatus

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (m *mockTablePipeline) run(ctx context.Context) {
	defer m.wg.Done()
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
		log.Info("table", zap.Int64("tableID", m.tableID), zap.Uint64("checkpointTs", nextCheckpointTs))
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
