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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/owner"
	"github.com/pingcap/ticdc/cdc/processor"
	tablepipeline "github.com/pingcap/ticdc/cdc/processor/pipeline"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/etcd"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

type CaptureTester struct {
	c *check.C

	embedEtcd  *embed.Etcd
	etcdClient *kv.CDCEtcdClient

	upstream          *mockUpstream
	captureCreatingCh chan *Capture
}

func NewCaptureTester(c *check.C, captureNum int) *CaptureTester {
	cluster := &CaptureTester{
		c:                 c,
		upstream:          newMockUpstream(c),
		captureCreatingCh: make(chan *Capture),
	}
	cluster.ScaleOut(captureNum)
	return cluster
}

func (t *CaptureTester) ScaleOut(captureNum int) {
	for i := 0; i < captureNum; i++ {
		cap := newCapture4Test(
			func(leaseID clientv3.LeaseID) *processor.Manager {
				return processor.NewManager4Test(leaseID,
					func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error) {
						return t.upstream.createTablePipeline(ctx, tableID, replicaInfo), nil
					})
			}, func(leaseID clientv3.LeaseID) *owner.Owner {
				return owner.NewOwner4Test(leaseID, func(ctx cdcContext.Context, startTs uint64) owner.DDLPuller {
					return t.upstream.ddlPuller
				}, func(ctx cdcContext.Context) (owner.AsyncSink, error) {
					return &mockAsyncSink{}, nil
				})
			})
		t.captureCreatingCh <- cap
	}
}

func (t *CaptureTester) ScaleIn(captureNum int) {
	// todo
}

func (t *CaptureTester) Run(ctx context.Context) error {
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		t.runEtcd(ctx)
		return nil
	})
	errg.Go(func() error {
		for {
			var cap *Capture
			select {
			case <-ctx.Done():
				return ctx.Err()
			case cap = <-t.captureCreatingCh:
			}
			errg.Go(func() error {
				return t.runCapture(ctx, cap)
			})
		}
	})
	return errg.Wait()
}

func (t *CaptureTester) runCapture(ctx context.Context, capture *Capture) error {
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

func (t *CaptureTester) runEtcd(ctx context.Context) {
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

func (t *CaptureTester) UpdateDDLPullerResolvedTs(ts model.Ts) {
	t.upstream.ddlPuller.updateDDLPullerResolvedTs(ts)
}

func (t *CaptureTester) ApplyDDLJob(job *timodel.Job) {
	t.upstream.ddlPuller.applyDDLJob(job)
}

func (t *CaptureTester) CheckpointTs() map[model.TableID]model.Ts {
	return t.upstream.checkpointTs()
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

type mockUpstream struct {
	c *check.C

	ddlPuller          *mockDDLPuller
	tableReceivedTsMap map[model.TableID]*[]model.Ts
}

func newMockUpstream(c *check.C) *mockUpstream {
	return &mockUpstream{
		c:                  c,
		tableReceivedTsMap: make(map[model.TableID]*[]model.Ts),
		ddlPuller:          &mockDDLPuller{c: c},
	}
}

func (u *mockUpstream) checkpointTs() map[model.TableID]model.Ts {
	// todo
	panic("unimplemented")
}

type mockDDLPuller struct {
	c          *check.C
	resolvedTs model.Ts
	ddlQueue   []*timodel.Job
	ddlQueueMu sync.Mutex
}

func (m *mockDDLPuller) updateDDLPullerResolvedTs(ts model.Ts) {
	atomic.StoreUint64(&m.resolvedTs, ts)
}

func (m *mockDDLPuller) applyDDLJob(job *timodel.Job) {
	m.ddlQueueMu.Lock()
	defer m.ddlQueueMu.Unlock()
	finishedTs := job.BinlogInfo.FinishedTS
	for {
		resolvedTs := atomic.LoadUint64(&m.resolvedTs)
		if resolvedTs > finishedTs {
			m.c.Fatalf("the ddl resolved TS is greater than finishedTs")
		}
		if atomic.CompareAndSwapUint64(&m.resolvedTs, resolvedTs, finishedTs) {
			break
		}
	}
	m.ddlQueue = append(m.ddlQueue, job)
}

func (m *mockDDLPuller) Run(ctx cdcContext.Context) error {
	// do nothing
	return nil
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

func (u *mockUpstream) createTablePipeline(ctx context.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) tablepipeline.TablePipeline {
	outputCh := make(chan model.Ts, 128)
	ctx, cancel := context.WithCancel(ctx)
	tablePipeline := &mockTablePipeline{
		tableID:      tableID,
		replicaInfo:  replicaInfo,
		resolvedTs:   replicaInfo.StartTs,
		checkpointTs: replicaInfo.StartTs,
		barrierTs:    replicaInfo.StartTs,
		targetTs:     math.MaxUint64,
		status:       tablepipeline.TableStatusInitializing,
		outputCh:     outputCh,
		cancel:       cancel,
	}
	var receivedTsArrPtr *[]model.Ts
	var exist bool
	if receivedTsArrPtr, exist = u.tableReceivedTsMap[tableID]; !exist {
		receivedTsArr := make([]model.Ts, 0, 128)
		receivedTsArrPtr = &receivedTsArr
		u.tableReceivedTsMap[tableID] = receivedTsArrPtr
	}

	tablePipeline.wg.Add(2)
	go tablePipeline.run(ctx)
	go func() {
		defer tablePipeline.wg.Done()
		for ts := range outputCh {
			*receivedTsArrPtr = append(*receivedTsArrPtr, ts)
		}
	}()
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

type clusterSuite struct {
}

var _ = check.Suite(&clusterSuite{})

func (s *clusterSuite) TestClusters(c *check.C) {
	tester := NewCaptureTester(c, 3)
	tester.UpdateDDLPullerResolvedTs(10)
	tester.ApplyDDLJob(&timodel.Job{
		ID:       3,
		State:    timodel.JobStateSynced,
		SchemaID: 1,
		Type:     timodel.ActionCreateSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1, DBInfo: &timodel.DBInfo{
			ID:    1,
			Name:  timodel.NewCIStr("test"),
			State: timodel.StatePublic,
		}, FinishedTS: 20},
		Query: "create database test",
	})
	tester.UpdateDDLPullerResolvedTs(25)
	tester.ApplyDDLJob(&timodel.Job{
		ID:       4,
		State:    timodel.JobStateSynced,
		SchemaID: 1,
		Type:     timodel.ActionCreateTable,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1, DBInfo: &timodel.DBInfo{
			ID:    1,
			Name:  timodel.NewCIStr("test"),
			State: timodel.StatePublic,
		}, TableInfo: &timodel.TableInfo{
			ID:    2,
			Name:  timodel.NewCIStr("test"),
			State: timodel.StatePublic,
			Columns: []*timodel.ColumnInfo{{
				ID:        1,
				Name:      timodel.NewCIStr("A"),
				Offset:    0,
				FieldType: *types.NewFieldType(mysql.TypeLonglong),
				State:     timodel.StatePublic,
			}},
			Indices: []*timodel.IndexInfo{{
				Name:  timodel.NewCIStr("idx"),
				Table: timodel.NewCIStr("test"),
				Columns: []*timodel.IndexColumn{
					{
						Name:   timodel.NewCIStr("A"),
						Offset: 0,
						Length: 10,
					},
				},
				Unique:  true,
				Primary: true,
				State:   timodel.StatePublic,
			}},
		}, FinishedTS: 30},
		Query: "create table test.test(A int primary key)",
	})
	tester.UpdateDDLPullerResolvedTs(30)
}
