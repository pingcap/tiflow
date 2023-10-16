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

package owner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller"
	credo "github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/sink/observer"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

var _ puller.DDLPuller = (*mockDDLPuller)(nil)

type mockDDLPuller struct {
	// DDLPuller
	resolvedTs    model.Ts
	ddlQueue      []*timodel.Job
	schemaStorage entry.SchemaStorage
}

func (m *mockDDLPuller) PopFrontDDL() (uint64, *timodel.Job) {
	if len(m.ddlQueue) > 0 {
		job := m.ddlQueue[0]
		m.ddlQueue = m.ddlQueue[1:]
		err := m.schemaStorage.HandleDDLJob(job)
		if err != nil {
			panic(fmt.Sprintf("handle ddl job failed: %v", err))
		}
		return job.BinlogInfo.FinishedTS, job
	}
	return m.resolvedTs, nil
}

func (m *mockDDLPuller) Close() {}

func (m *mockDDLPuller) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (m *mockDDLPuller) ResolvedTs() model.Ts {
	if len(m.ddlQueue) > 0 {
		return m.ddlQueue[0].BinlogInfo.FinishedTS
	}
	return m.resolvedTs
}

type mockDDLSink struct {
	// DDLSink
	ddlExecuting *model.DDLEvent
	ddlDone      bool
	// whether to reset ddlDone flag, only for rename table
	resetDDLDone bool
	// whether to record the DDL history, only for rename table
	recordDDLHistory bool
	// a slice of DDL history, only for rename table
	ddlHistory []string
	mu         struct {
		sync.Mutex
		checkpointTs  model.Ts
		currentTables []*model.TableInfo
	}
	syncPoint    model.Ts
	syncPointHis []model.Ts

	wg sync.WaitGroup
}

func (m *mockDDLSink) run(ctx context.Context) {
	m.wg.Add(1)
	go func() {
		<-ctx.Done()
		m.wg.Done()
	}()
}

func (m *mockDDLSink) emitDDLEvent(ctx context.Context, ddl *model.DDLEvent) (bool, error) {
	m.ddlExecuting = ddl
	defer func() {
		if m.resetDDLDone {
			m.ddlDone = false
		}
	}()
	if m.recordDDLHistory {
		m.ddlHistory = append(m.ddlHistory, ddl.Query)
	} else {
		m.ddlHistory = nil
	}
	return m.ddlDone, nil
}

func (m *mockDDLSink) emitSyncPoint(ctx context.Context, checkpointTs uint64) error {
	if checkpointTs == m.syncPoint {
		return nil
	}
	m.syncPoint = checkpointTs
	m.syncPointHis = append(m.syncPointHis, checkpointTs)
	return nil
}

func (m *mockDDLSink) emitCheckpointTs(ts uint64, tables []*model.TableInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.checkpointTs = ts
	m.mu.currentTables = tables
}

func (m *mockDDLSink) getCheckpointTsAndTableNames() (uint64, []*model.TableInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.checkpointTs, m.mu.currentTables
}

func (m *mockDDLSink) close(ctx context.Context) error {
	m.wg.Wait()
	return nil
}

func (m *mockDDLSink) Barrier(ctx context.Context) error {
	return nil
}

type mockScheduler struct {
	currentTables []model.TableID
}

func (m *mockScheduler) Tick(
	ctx context.Context,
	checkpointTs model.Ts,
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
	barrier *schedulepb.BarrierWithMinTs,
) (newCheckpointTs, newResolvedTs model.Ts, err error) {
	m.currentTables = currentTables
	return barrier.MinTableBarrierTs, barrier.GlobalBarrierTs, nil
}

// MoveTable is used to trigger manual table moves.
func (m *mockScheduler) MoveTable(tableID model.TableID, target model.CaptureID) {}

// Rebalance is used to trigger manual workload rebalances.
func (m *mockScheduler) Rebalance() {}

// DrainCapture implement scheduler interface
func (m *mockScheduler) DrainCapture(target model.CaptureID) (int, error) {
	return 0, nil
}

// Close closes the scheduler and releases resources.
func (m *mockScheduler) Close(ctx context.Context) {}

func createChangefeed4Test(ctx cdcContext.Context, t *testing.T,
) (
	*changefeed, map[model.CaptureID]*model.CaptureInfo, *orchestrator.ReactorStateTester,
) {
	up := upstream.NewUpstream4Test(&gc.MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return safePoint - 1, nil
		},
	})

	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		info = ctx.ChangefeedVars().Info
		return info, true, nil
	})
	tester.MustApplyPatches()
	cf := newChangefeed4Test(ctx.ChangefeedVars().ID, state, up,
		// new ddl puller
		func(ctx context.Context,
			replicaConfig *config.ReplicaConfig,
			up *upstream.Upstream,
			startTs uint64,
			changefeed model.ChangeFeedID,
			schemaStorage entry.SchemaStorage,
			filter filter.Filter,
		) (puller.DDLPuller, error) {
			return &mockDDLPuller{resolvedTs: startTs - 1, schemaStorage: schemaStorage}, nil
		},
		// new ddl ddlSink
		func(_ model.ChangeFeedID, _ *model.ChangeFeedInfo, _ func(error), _ func(error)) DDLSink {
			return &mockDDLSink{
				resetDDLDone:     true,
				recordDDLHistory: false,
			}
		},
		// new scheduler
		func(
			ctx cdcContext.Context, up *upstream.Upstream, epoch uint64,
			cfg *config.SchedulerConfig, redoMetaManager credo.MetaManager,
		) (scheduler.Scheduler, error) {
			return &mockScheduler{}, nil
		},
		// new downstream observer
		func(
			ctx context.Context, sinkURIStr string, replCfg *config.ReplicaConfig,
			opts ...observer.NewObserverOption,
		) (observer.Observer, error) {
			return observer.NewDummyObserver(), nil
		},
	)

	cf.upstream = up

	tester.MustUpdate(fmt.Sprintf("%s/capture/%s",
		etcd.DefaultClusterAndMetaPrefix, ctx.GlobalVars().CaptureInfo.ID),
		[]byte(`{"id":"`+ctx.GlobalVars().CaptureInfo.ID+`","address":"127.0.0.1:8300"}`))
	tester.MustApplyPatches()
	captures := map[model.CaptureID]*model.CaptureInfo{ctx.GlobalVars().CaptureInfo.ID: ctx.GlobalVars().CaptureInfo}
	return cf, captures, tester
}

func TestPreCheck(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, captures, tester := createChangefeed4Test(ctx, t)
	cf.Tick(ctx, captures)
	tester.MustApplyPatches()
	require.NotNil(t, cf.state.Status)

	// test clean the meta data of offline capture
	offlineCaputreID := "offline-capture"
	cf.state.PatchTaskPosition(offlineCaputreID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		return new(model.TaskPosition), true, nil
	})
	tester.MustApplyPatches()

	cf.Tick(ctx, captures)
	tester.MustApplyPatches()
	require.NotNil(t, cf.state.Status)
	require.NotContains(t, cf.state.TaskPositions, offlineCaputreID)
}

func TestInitialize(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, captures, tester := createChangefeed4Test(ctx, t)
	defer cf.Close(ctx)
	// pre check
	cf.Tick(ctx, captures)
	tester.MustApplyPatches()

	// initialize
	ctx.GlobalVars().EtcdClient = &etcd.CDCEtcdClientImpl{}
	cf.Tick(ctx, captures)
	tester.MustApplyPatches()
	require.Equal(t, cf.state.Status.CheckpointTs, ctx.ChangefeedVars().Info.StartTs)
}

func TestChangefeedHandleError(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, captures, tester := createChangefeed4Test(ctx, t)
	defer cf.Close(ctx)
	// pre check
	cf.Tick(ctx, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, captures)
	tester.MustApplyPatches()

	cf.errCh <- errors.New("fake error")
	// handle error
	cf.Tick(ctx, captures)
	tester.MustApplyPatches()
	require.Equal(t, cf.state.Status.CheckpointTs, ctx.ChangefeedVars().Info.StartTs)
	require.Equal(t, cf.state.Info.Error.Message, "fake error")
}

func TestExecDDL(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	// Creates a table, which will be deleted at the start-ts of the changefeed.
	// It is expected that the changefeed DOES NOT replicate this table.
	helper.DDL2Job("create database test0")
	job := helper.DDL2Job("create table test0.table0(id int primary key)")
	startTs := job.BinlogInfo.FinishedTS + 1000

	ctx := cdcContext.NewContext4Test(context.Background(), true)
	ctx.ChangefeedVars().Info.StartTs = startTs

	cf, captures, tester := createChangefeed4Test(ctx, t)
	cf.upstream.KVStorage = helper.Storage()
	defer cf.Close(ctx)
	tickThreeTime := func() {
		cf.Tick(ctx, captures)
		tester.MustApplyPatches()
		cf.Tick(ctx, captures)
		tester.MustApplyPatches()
		cf.Tick(ctx, captures)
		tester.MustApplyPatches()
	}
	// pre check and initialize
	tickThreeTime()
	tableIDs, err := cf.schema.AllPhysicalTables(ctx, startTs-1)
	require.Nil(t, err)
	require.Len(t, tableIDs, 1)

	job = helper.DDL2Job("drop table test0.table0")
	// ddl puller resolved ts grow up
	mockDDLPuller := cf.ddlManager.ddlPuller.(*mockDDLPuller)
	mockDDLPuller.resolvedTs = startTs
	mockDDLSink := cf.ddlManager.ddlSink.(*mockDDLSink)
	job.BinlogInfo.FinishedTS = mockDDLPuller.resolvedTs
	mockDDLPuller.ddlQueue = append(mockDDLPuller.ddlQueue, job)
	// three tick to make sure all barrier set in initialize is handled
	tickThreeTime()
	require.Equal(t, cf.state.Status.CheckpointTs, mockDDLPuller.resolvedTs)
	// The ephemeral table should have left no trace in the schema cache
	tableIDs, err = cf.schema.AllPhysicalTables(ctx, mockDDLPuller.resolvedTs)
	require.Nil(t, err)
	require.Len(t, tableIDs, 0)

	// executing the ddl finished
	mockDDLSink.ddlDone = true
	mockDDLPuller.resolvedTs += 1000
	tickThreeTime()
	require.Equal(t, mockDDLPuller.resolvedTs, cf.state.Status.CheckpointTs)

	// handle create database
	job = helper.DDL2Job("create database test1")
	mockDDLPuller.resolvedTs += 1000
	job.BinlogInfo.FinishedTS = mockDDLPuller.resolvedTs
	mockDDLPuller.ddlQueue = append(mockDDLPuller.ddlQueue, job)
	tickThreeTime()
	require.Equal(t, cf.state.Status.CheckpointTs, mockDDLPuller.resolvedTs)
	require.Equal(t, "create database test1", mockDDLSink.ddlExecuting.Query)

	// executing the ddl finished
	mockDDLSink.ddlDone = true
	mockDDLPuller.resolvedTs += 1000
	tickThreeTime()
	require.Equal(t, cf.state.Status.CheckpointTs, mockDDLPuller.resolvedTs)

	// handle create table
	job = helper.DDL2Job("create table test1.test1(id int primary key)")
	mockDDLPuller.resolvedTs += 1000
	job.BinlogInfo.FinishedTS = mockDDLPuller.resolvedTs
	mockDDLPuller.ddlQueue = append(mockDDLPuller.ddlQueue, job)
	tickThreeTime()

	require.Equal(t, cf.state.Status.CheckpointTs, mockDDLPuller.resolvedTs)
	require.Equal(t, "create table test1.test1(id int primary key)", mockDDLSink.ddlExecuting.Query)

	// executing the ddl finished
	mockDDLSink.ddlDone = true
	mockDDLPuller.resolvedTs += 1000
	tickThreeTime()
	require.Contains(t, cf.scheduler.(*mockScheduler).currentTables, job.TableID)
}

func TestEmitCheckpointTs(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	// Creates a table, which will be deleted at the start-ts of the changefeed.
	// It is expected that the changefeed DOES NOT replicate this table.
	helper.DDL2Job("create database test0")
	job := helper.DDL2Job("create table test0.table0(id int primary key)")
	startTs := job.BinlogInfo.FinishedTS + 1000

	ctx := cdcContext.NewContext4Test(context.Background(), true)
	ctx.ChangefeedVars().Info.StartTs = startTs

	cf, captures, tester := createChangefeed4Test(ctx, t)
	cf.upstream.KVStorage = helper.Storage()

	defer cf.Close(ctx)
	tickThreeTime := func() {
		cf.Tick(ctx, captures)
		tester.MustApplyPatches()
		cf.Tick(ctx, captures)
		tester.MustApplyPatches()
		cf.Tick(ctx, captures)
		tester.MustApplyPatches()
	}
	// pre check and initialize
	tickThreeTime()
	mockDDLSink := cf.ddlManager.ddlSink.(*mockDDLSink)

	tables, err := cf.ddlManager.allTables(ctx)
	require.Nil(t, err)

	require.Len(t, tables, 1)
	ts, names := mockDDLSink.getCheckpointTsAndTableNames()
	require.Equal(t, ts, startTs)
	require.Len(t, names, 1)

	job = helper.DDL2Job("drop table test0.table0")
	// ddl puller resolved ts grow up
	mockDDLPuller := cf.ddlManager.ddlPuller.(*mockDDLPuller)
	mockDDLPuller.resolvedTs = startTs + 1000
	cf.ddlManager.schema.AdvanceResolvedTs(mockDDLPuller.resolvedTs)
	cf.state.Status.CheckpointTs = mockDDLPuller.resolvedTs
	job.BinlogInfo.FinishedTS = mockDDLPuller.resolvedTs
	mockDDLPuller.ddlQueue = append(mockDDLPuller.ddlQueue, job)
	// three tick to make sure all barrier set in initialize is handled
	tickThreeTime()
	require.Equal(t, cf.state.Status.CheckpointTs, mockDDLPuller.resolvedTs)
	tables, err = cf.ddlManager.allTables(ctx)
	require.Nil(t, err)
	// The ephemeral table should only be deleted after the ddl is executed.
	require.Len(t, tables, 1)
	// We can't use the new schema because the ddl hasn't been executed yet.
	ts, names = mockDDLSink.getCheckpointTsAndTableNames()
	require.Equal(t, ts, mockDDLPuller.resolvedTs)
	require.Len(t, names, 1)

	// executing the ddl finished
	mockDDLSink.ddlDone = true
	mockDDLPuller.resolvedTs += 2000
	tickThreeTime()
	require.Equal(t, cf.state.Status.CheckpointTs, mockDDLPuller.resolvedTs)
	ts, names = mockDDLSink.getCheckpointTsAndTableNames()
	require.Equal(t, ts, mockDDLPuller.resolvedTs)
	require.Len(t, names, 0)
}

func TestSyncPoint(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	ctx.ChangefeedVars().Info.Config.EnableSyncPoint = true
	ctx.ChangefeedVars().Info.Config.SyncPointInterval = 1 * time.Second
	cf, captures, tester := createChangefeed4Test(ctx, t)
	defer cf.Close(ctx)

	// pre check
	cf.Tick(ctx, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, captures)
	tester.MustApplyPatches()

	mockDDLPuller := cf.ddlManager.ddlPuller.(*mockDDLPuller)
	mockDDLSink := cf.ddlManager.ddlSink.(*mockDDLSink)
	// add 5s to resolvedTs
	mockDDLPuller.resolvedTs = oracle.GoTimeToTS(oracle.GetTimeFromTS(mockDDLPuller.resolvedTs).Add(5 * time.Second))
	// tick 20 times
	for i := 0; i <= 20; i++ {
		cf.Tick(ctx, captures)
		tester.MustApplyPatches()
	}
	for i := 1; i < len(mockDDLSink.syncPointHis); i++ {
		// check the time interval between adjacent sync points is less or equal than one second
		require.LessOrEqual(t, mockDDLSink.syncPointHis[i]-mockDDLSink.syncPointHis[i-1], uint64(1000<<18))
	}
	require.GreaterOrEqual(t, len(mockDDLSink.syncPointHis), 5)
}

func TestFinished(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	ctx.ChangefeedVars().Info.TargetTs = ctx.ChangefeedVars().Info.StartTs + 1000
	cf, captures, tester := createChangefeed4Test(ctx, t)
	defer cf.Close(ctx)

	// pre check
	cf.Tick(ctx, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, captures)
	tester.MustApplyPatches()

	mockDDLPuller := cf.ddlManager.ddlPuller.(*mockDDLPuller)
	mockDDLPuller.resolvedTs += 2000
	// tick many times to make sure the change feed is stopped
	for i := 0; i <= 10; i++ {
		cf.Tick(ctx, captures)
		tester.MustApplyPatches()
	}
	fmt.Println("checkpoint ts", cf.state.Status.CheckpointTs)
	fmt.Println("target ts", cf.state.Info.TargetTs)
	require.Equal(t, cf.state.Status.CheckpointTs, cf.state.Info.TargetTs)
	require.Equal(t, cf.state.Info.State, model.StateFinished)
}

func TestRemoveChangefeed(t *testing.T) {
	baseCtx, cancel := context.WithCancel(context.Background())
	ctx := cdcContext.NewContext4Test(baseCtx, true)
	info := ctx.ChangefeedVars().Info
	dir := t.TempDir()
	info.Config.Consistent = &config.ConsistentConfig{
		Level:             "eventual",
		Storage:           filepath.Join("nfs://", dir),
		FlushIntervalInMs: redo.DefaultFlushIntervalInMs,
	}
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID:   ctx.ChangefeedVars().ID,
		Info: info,
	})
	testChangefeedReleaseResource(t, ctx, cancel, dir, true /*expectedInitialized*/)
}

func TestRemovePausedChangefeed(t *testing.T) {
	baseCtx, cancel := context.WithCancel(context.Background())
	ctx := cdcContext.NewContext4Test(baseCtx, true)
	info := ctx.ChangefeedVars().Info
	info.State = model.StateStopped
	dir := t.TempDir()
	info.Config.Consistent = &config.ConsistentConfig{
		Level:             "eventual",
		Storage:           filepath.Join("nfs://", dir),
		FlushIntervalInMs: redo.DefaultFlushIntervalInMs,
	}
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID:   ctx.ChangefeedVars().ID,
		Info: info,
	})
	testChangefeedReleaseResource(t, ctx, cancel, dir, false /*expectedInitialized*/)
}

func testChangefeedReleaseResource(
	t *testing.T,
	ctx cdcContext.Context,
	cancel context.CancelFunc,
	redoLogDir string,
	expectedInitialized bool,
) {
	cf, captures, tester := createChangefeed4Test(ctx, t)

	// pre check
	cf.Tick(ctx, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, captures)
	tester.MustApplyPatches()
	require.Equal(t, cf.initialized, expectedInitialized)

	// remove changefeed from state manager by admin job
	cf.feedStateManager.PushAdminJob(&model.AdminJob{
		CfID: cf.id,
		Type: model.AdminRemove,
	})
	cf.isReleased = false
	// changefeed tick will release resources
	err := cf.tick(ctx, captures)
	require.Nil(t, err)
	cancel()

	if cf.state.Info.Config.Consistent.UseFileBackend {
		// check redo log dir is deleted
		_, err = os.Stat(redoLogDir)
		require.True(t, os.IsNotExist(err))
	} else {
		files, err := os.ReadDir(redoLogDir)
		require.NoError(t, err)
		require.Len(t, files, 1) // only delete mark
	}
}

func TestBarrierAdvance(t *testing.T) {
	for i := 0; i < 2; i++ {
		ctx := cdcContext.NewBackendContext4Test(true)
		if i == 1 {
			ctx.ChangefeedVars().Info.Config.EnableSyncPoint = true
			ctx.ChangefeedVars().Info.Config.SyncPointInterval = 100 * time.Second
		}

		cf, captures, tester := createChangefeed4Test(ctx, t)
		defer cf.Close(ctx)

		// The changefeed load the info from etcd.
		cf.state.Status = &model.ChangeFeedStatus{
			CheckpointTs:      cf.state.Info.StartTs,
			MinTableBarrierTs: cf.state.Info.StartTs + 5,
		}

		// Do the preflightCheck and initialize the changefeed.
		cf.Tick(ctx, captures)
		tester.MustApplyPatches()
		if i == 1 {
			cf.ddlManager.ddlResolvedTs += 10
		}
		_, barrier, err := cf.ddlManager.tick(ctx, cf.state.Status.CheckpointTs, nil)

		require.Nil(t, err)

		err = cf.handleBarrier(ctx, barrier)
		require.Nil(t, err)

		if i == 0 {
			require.Equal(t, cf.state.Info.StartTs, barrier.GlobalBarrierTs)
		}
		// sync-point is enabled, sync point barrier is ticked
		if i == 1 {
			require.Equal(t, cf.state.Info.StartTs+10, barrier.GlobalBarrierTs)
		}

		// Suppose tableCheckpoint has been advanced.
		cf.state.Status.CheckpointTs += 10

		// Need more 1 tick to advance barrier if sync-point is enabled.
		if i == 1 {
			err = cf.handleBarrier(ctx, barrier)
			require.Nil(t, err)
			require.Equal(t, cf.state.Info.StartTs+10, barrier.GlobalBarrierTs)
			// Then the last tick barrier must be advanced correctly.
			cf.ddlManager.ddlResolvedTs += 1000000000000
			_, barrier, err = cf.ddlManager.tick(ctx, cf.state.Status.CheckpointTs+10, nil)
			require.Nil(t, err)
			err = cf.handleBarrier(ctx, barrier)

			nextSyncPointTs := oracle.GoTimeToTS(
				oracle.GetTimeFromTS(cf.state.Status.CheckpointTs + 10).
					Add(cf.state.Info.Config.SyncPointInterval))

			require.Nil(t, err)
			require.Equal(t, nextSyncPointTs, barrier.GlobalBarrierTs)
			require.Less(t, cf.state.Status.CheckpointTs+10, barrier.GlobalBarrierTs)
			require.Less(t, barrier.GlobalBarrierTs, cf.ddlManager.ddlResolvedTs)
		}

	}
}
