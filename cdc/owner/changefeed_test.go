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
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/pdtime"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type mockDDLPuller struct {
	// DDLPuller
	resolvedTs model.Ts
	ddlQueue   []*timodel.Job
}

func (m *mockDDLPuller) FrontDDL() (uint64, *timodel.Job) {
	if len(m.ddlQueue) > 0 {
		return m.ddlQueue[0].BinlogInfo.FinishedTS, m.ddlQueue[0]
	}
	return m.resolvedTs, nil
}

func (m *mockDDLPuller) PopFrontDDL() (uint64, *timodel.Job) {
	if len(m.ddlQueue) > 0 {
		job := m.ddlQueue[0]
		m.ddlQueue = m.ddlQueue[1:]
		return job.BinlogInfo.FinishedTS, job
	}
	return m.resolvedTs, nil
}

func (m *mockDDLPuller) Close() {}

func (m *mockDDLPuller) Run(ctx cdcContext.Context) error {
	<-ctx.Done()
	return nil
}

type mockDDLSink struct {
	// DDLSink
	ddlExecuting *model.DDLEvent
	ddlDone      bool
	mu           struct {
		sync.Mutex
		checkpointTs      model.Ts
		currentTableNames []model.TableName
	}
	syncPoint    model.Ts
	syncPointHis []model.Ts

	wg sync.WaitGroup
}

func (m *mockDDLSink) run(ctx cdcContext.Context, _ model.ChangeFeedID, _ *model.ChangeFeedInfo) {
	m.wg.Add(1)
	go func() {
		<-ctx.Done()
		m.wg.Done()
	}()
}

func (m *mockDDLSink) emitDDLEvent(ctx cdcContext.Context, ddl *model.DDLEvent) (bool, error) {
	m.ddlExecuting = ddl
	defer func() { m.ddlDone = false }()
	return m.ddlDone, nil
}

func (m *mockDDLSink) emitSyncPoint(ctx cdcContext.Context, checkpointTs uint64) error {
	if checkpointTs == m.syncPoint {
		return nil
	}
	m.syncPoint = checkpointTs
	m.syncPointHis = append(m.syncPointHis, checkpointTs)
	return nil
}

func (m *mockDDLSink) emitCheckpointTs(ts uint64, tableNames []model.TableName) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.checkpointTs = ts
	m.mu.currentTableNames = tableNames
}

func (m *mockDDLSink) getCheckpointTsAndTableNames() (uint64, []model.TableName) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.checkpointTs, m.mu.currentTableNames
}

func (m *mockDDLSink) close(ctx context.Context) error {
	m.wg.Wait()
	return nil
}

func (m *mockDDLSink) Barrier(ctx context.Context) error {
	return nil
}

func createChangefeed4Test(ctx cdcContext.Context, t *testing.T) (*changefeed, *orchestrator.ChangefeedReactorState,
	map[model.CaptureID]*model.CaptureInfo, *orchestrator.ReactorStateTester) {
	ctx.GlobalVars().PDClient = &gc.MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return safePoint, nil
		},
	}
	gcManager := gc.NewManager(ctx.GlobalVars().PDClient)
	cf := newChangefeed4Test(ctx.ChangefeedVars().ID, gcManager, func(ctx cdcContext.Context, startTs uint64) (DDLPuller, error) {
		return &mockDDLPuller{resolvedTs: startTs - 1}, nil
	}, func() DDLSink {
		return &mockDDLSink{}
	})
	cf.newScheduler = func(ctx cdcContext.Context, startTs uint64) (scheduler, error) {
		return newSchedulerV1(), nil
	}
	state := orchestrator.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		info = ctx.ChangefeedVars().Info
		return info, true, nil
	})
	tester.MustUpdate("/tidb/cdc/capture/"+ctx.GlobalVars().CaptureInfo.ID, []byte(`{"id":"`+ctx.GlobalVars().CaptureInfo.ID+`","address":"127.0.0.1:8300"}`))
	tester.MustApplyPatches()
	captures := map[model.CaptureID]*model.CaptureInfo{ctx.GlobalVars().CaptureInfo.ID: ctx.GlobalVars().CaptureInfo}
	return cf, state, captures, tester
}

func TestPreCheck(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, t)
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	require.NotNil(t, state.Status)
	require.Contains(t, state.TaskStatuses, ctx.GlobalVars().CaptureInfo.ID)

	// test clean the meta data of offline capture
	offlineCaputreID := "offline-capture"
	state.PatchTaskStatus(offlineCaputreID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		return new(model.TaskStatus), true, nil
	})
	state.PatchTaskPosition(offlineCaputreID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		return new(model.TaskPosition), true, nil
	})
	state.PatchTaskWorkload(offlineCaputreID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		return make(model.TaskWorkload), true, nil
	})
	tester.MustApplyPatches()

	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	require.NotNil(t, state.Status)
	require.Contains(t, state.TaskStatuses, ctx.GlobalVars().CaptureInfo.ID)
	require.NotContains(t, state.TaskStatuses, offlineCaputreID)
	require.NotContains(t, state.TaskPositions, offlineCaputreID)
	require.NotContains(t, state.Workloads, offlineCaputreID)
}

func TestInitialize(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, t)
	defer cf.Close(ctx)
	// pre check
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	require.Equal(t, state.Status.CheckpointTs, ctx.ChangefeedVars().Info.StartTs)
}

func TestChangefeedHandleError(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, t)
	defer cf.Close(ctx)
	// pre check
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	cf.errCh <- errors.New("fake error")
	// handle error
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	require.Equal(t, state.Status.CheckpointTs, ctx.ChangefeedVars().Info.StartTs)
	require.Equal(t, state.Info.Error.Message, "fake error")
}

func TestExecDDL(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	// Creates a table, which will be deleted at the start-ts of the changefeed.
	// It is expected that the changefeed DOES NOT replicate this table.
	helper.DDL2Job("create database test0")
	job := helper.DDL2Job("create table test0.table0(id int primary key)")
	startTs := job.BinlogInfo.FinishedTS + 1000

	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{
		KVStorage: helper.Storage(),
		CaptureInfo: &model.CaptureInfo{
			ID:            "capture-id-test",
			AdvertiseAddr: "127.0.0.1:0000",
			Version:       version.ReleaseVersion,
		},
		PDClock: pdtime.NewClock4Test(),
	})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test",
		Info: &model.ChangeFeedInfo{
			StartTs: startTs,
			Config:  config.GetDefaultReplicaConfig(),
		},
	})

	cf, state, captures, tester := createChangefeed4Test(ctx, t)
	defer cf.Close(ctx)
	tickThreeTime := func() {
		cf.Tick(ctx, state, captures)
		tester.MustApplyPatches()
		cf.Tick(ctx, state, captures)
		tester.MustApplyPatches()
		cf.Tick(ctx, state, captures)
		tester.MustApplyPatches()
	}
	// pre check and initialize
	tickThreeTime()
	require.Len(t, cf.schema.AllPhysicalTables(), 1)
	require.Len(t, state.TaskStatuses[ctx.GlobalVars().CaptureInfo.ID].Operation, 0)
	require.Len(t, state.TaskStatuses[ctx.GlobalVars().CaptureInfo.ID].Tables, 0)

	job = helper.DDL2Job("drop table test0.table0")
	// ddl puller resolved ts grow up
	mockDDLPuller := cf.ddlPuller.(*mockDDLPuller)
	mockDDLPuller.resolvedTs = startTs
	mockDDLSink := cf.sink.(*mockDDLSink)
	job.BinlogInfo.FinishedTS = mockDDLPuller.resolvedTs
	mockDDLPuller.ddlQueue = append(mockDDLPuller.ddlQueue, job)
	// three tick to make sure all barriers set in initialize is handled
	tickThreeTime()
	require.Equal(t, state.Status.CheckpointTs, mockDDLPuller.resolvedTs)
	// The ephemeral table should have left no trace in the schema cache
	require.Len(t, cf.schema.AllPhysicalTables(), 0)

	// executing the ddl finished
	mockDDLSink.ddlDone = true
	mockDDLPuller.resolvedTs += 1000
	tickThreeTime()
	require.Equal(t, state.Status.CheckpointTs, mockDDLPuller.resolvedTs)

	// handle create database
	job = helper.DDL2Job("create database test1")
	mockDDLPuller.resolvedTs += 1000
	job.BinlogInfo.FinishedTS = mockDDLPuller.resolvedTs
	mockDDLPuller.ddlQueue = append(mockDDLPuller.ddlQueue, job)
	tickThreeTime()
	require.Equal(t, state.Status.CheckpointTs, mockDDLPuller.resolvedTs)
	require.Equal(t, mockDDLSink.ddlExecuting.Query, "CREATE DATABASE `test1`")

	// executing the ddl finished
	mockDDLSink.ddlDone = true
	mockDDLPuller.resolvedTs += 1000
	tickThreeTime()
	require.Equal(t, state.Status.CheckpointTs, mockDDLPuller.resolvedTs)

	// handle create table
	job = helper.DDL2Job("create table test1.test1(id int primary key)")
	mockDDLPuller.resolvedTs += 1000
	job.BinlogInfo.FinishedTS = mockDDLPuller.resolvedTs
	mockDDLPuller.ddlQueue = append(mockDDLPuller.ddlQueue, job)
	tickThreeTime()

	require.Equal(t, state.Status.CheckpointTs, mockDDLPuller.resolvedTs)
	require.Equal(t, mockDDLSink.ddlExecuting.Query, "CREATE TABLE `test1`.`test1` (`id` INT PRIMARY KEY)")

	// executing the ddl finished
	mockDDLSink.ddlDone = true
	mockDDLPuller.resolvedTs += 1000
	tickThreeTime()
	require.Contains(t, state.TaskStatuses[ctx.GlobalVars().CaptureInfo.ID].Tables, job.TableID)
}

func TestEmitCheckpointTs(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	// Creates a table, which will be deleted at the start-ts of the changefeed.
	// It is expected that the changefeed DOES NOT replicate this table.
	helper.DDL2Job("create database test0")
	job := helper.DDL2Job("create table test0.table0(id int primary key)")
	startTs := job.BinlogInfo.FinishedTS + 1000

	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{
		KVStorage: helper.Storage(),
		CaptureInfo: &model.CaptureInfo{
			ID:            "capture-id-test",
			AdvertiseAddr: "127.0.0.1:0000",
			Version:       version.ReleaseVersion,
		},
		PDClock: pdtime.NewClock4Test(),
	})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test",
		Info: &model.ChangeFeedInfo{
			StartTs: startTs,
			Config:  config.GetDefaultReplicaConfig(),
		},
	})

	cf, state, captures, tester := createChangefeed4Test(ctx, t)
	defer cf.Close(ctx)
	tickThreeTime := func() {
		cf.Tick(ctx, state, captures)
		tester.MustApplyPatches()
		cf.Tick(ctx, state, captures)
		tester.MustApplyPatches()
		cf.Tick(ctx, state, captures)
		tester.MustApplyPatches()
	}
	// pre check and initialize
	tickThreeTime()
	mockDDLSink := cf.sink.(*mockDDLSink)

	require.Len(t, cf.schema.AllTableNames(), 1)
	require.Len(t, state.TaskStatuses[ctx.GlobalVars().CaptureInfo.ID].Operation, 0)
	require.Len(t, state.TaskStatuses[ctx.GlobalVars().CaptureInfo.ID].Tables, 0)
	ts, names := mockDDLSink.getCheckpointTsAndTableNames()
	require.Equal(t, ts, startTs)
	require.Len(t, names, 1)

	job = helper.DDL2Job("drop table test0.table0")
	// ddl puller resolved ts grow up
	mockDDLPuller := cf.ddlPuller.(*mockDDLPuller)
	mockDDLPuller.resolvedTs = startTs
	job.BinlogInfo.FinishedTS = mockDDLPuller.resolvedTs
	mockDDLPuller.ddlQueue = append(mockDDLPuller.ddlQueue, job)
	// three tick to make sure all barriers set in initialize is handled
	tickThreeTime()
	require.Equal(t, state.Status.CheckpointTs, mockDDLPuller.resolvedTs)
	// The ephemeral table should have left no trace in the schema cache
	require.Len(t, cf.schema.AllTableNames(), 0)
	// We can't use the new schema because the ddl hasn't been executed yet.
	ts, names = mockDDLSink.getCheckpointTsAndTableNames()
	require.Equal(t, ts, mockDDLPuller.resolvedTs)
	require.Len(t, names, 1)

	// executing the ddl finished
	mockDDLSink.ddlDone = true
	mockDDLPuller.resolvedTs += 1000
	tickThreeTime()
	require.Equal(t, state.Status.CheckpointTs, mockDDLPuller.resolvedTs)
	ts, names = mockDDLSink.getCheckpointTsAndTableNames()
	require.Equal(t, ts, mockDDLPuller.resolvedTs)
	require.Len(t, names, 0)
}

func TestSyncPoint(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	ctx.ChangefeedVars().Info.SyncPointEnabled = true
	ctx.ChangefeedVars().Info.SyncPointInterval = 1 * time.Second
	cf, state, captures, tester := createChangefeed4Test(ctx, t)
	defer cf.Close(ctx)

	// pre check
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	mockDDLPuller := cf.ddlPuller.(*mockDDLPuller)
	mockDDLSink := cf.sink.(*mockDDLSink)
	// add 5s to resolvedTs
	mockDDLPuller.resolvedTs = oracle.GoTimeToTS(oracle.GetTimeFromTS(mockDDLPuller.resolvedTs).Add(5 * time.Second))
	// tick 20 times
	for i := 0; i <= 20; i++ {
		cf.Tick(ctx, state, captures)
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
	cf, state, captures, tester := createChangefeed4Test(ctx, t)
	defer cf.Close(ctx)

	// pre check
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	mockDDLPuller := cf.ddlPuller.(*mockDDLPuller)
	mockDDLPuller.resolvedTs += 2000
	// tick many times to make sure the change feed is stopped
	for i := 0; i <= 10; i++ {
		cf.Tick(ctx, state, captures)
		tester.MustApplyPatches()
	}

	require.Equal(t, state.Status.CheckpointTs, state.Info.TargetTs)
	require.Equal(t, state.Info.State, model.StateFinished)
}

func TestRemoveChangefeed(t *testing.T) {
	baseCtx, cancel := context.WithCancel(context.Background())
	ctx := cdcContext.NewContext4Test(baseCtx, true)
	info := ctx.ChangefeedVars().Info
	dir, err := ioutil.TempDir("", "remove-changefeed-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	info.Config.Consistent = &config.ConsistentConfig{
		Level:   "eventual",
		Storage: filepath.Join("nfs://", dir),
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
	dir, err := ioutil.TempDir("", "remove-paused-changefeed-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	info.Config.Consistent = &config.ConsistentConfig{
		Level:   "eventual",
		Storage: filepath.Join("nfs://", dir),
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
	cf, state, captures, tester := createChangefeed4Test(ctx, t)

	// pre check
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	require.Equal(t, cf.initialized, expectedInitialized)

	// remove changefeed from state manager by admin job
	cf.feedStateManager.PushAdminJob(&model.AdminJob{
		CfID: cf.id,
		Type: model.AdminRemove,
	})
	// changefeed tick will release resources
	err := cf.tick(ctx, state, captures)
	require.Nil(t, err)
	cancel()
	// check redo log dir is deleted
	_, err = os.Stat(redoLogDir)
	require.True(t, os.IsNotExist(err))
}

func TestAddSpecialComment(t *testing.T) {
	testCase := []struct {
		input  string
		result string
	}{
		{
			"create table t1 (id int ) shard_row_id_bits=2;",
			"CREATE TABLE `t1` (`id` INT) /*T! SHARD_ROW_ID_BITS = 2 */",
		},
		{
			"create table t1 (id int ) shard_row_id_bits=2 pre_split_regions=2;",
			"CREATE TABLE `t1` (`id` INT) " +
				"/*T! SHARD_ROW_ID_BITS = 2 */ /*T! PRE_SPLIT_REGIONS = 2 */",
		},
		{
			"create table t1 (id int ) shard_row_id_bits=2     pre_split_regions=2;",
			"CREATE TABLE `t1` (`id` INT) " +
				"/*T! SHARD_ROW_ID_BITS = 2 */ /*T! PRE_SPLIT_REGIONS = 2 */",
		},
		{
			"create table t1 (id int ) shard_row_id_bits=2 engine=innodb pre_split_regions=2;",
			"CREATE TABLE `t1` (`id` INT) /*T! SHARD_ROW_ID_BITS = 2 */" +
				" ENGINE = innodb /*T! PRE_SPLIT_REGIONS = 2 */",
		},
		{
			"create table t1 (id int ) pre_split_regions=2 shard_row_id_bits=2;",
			"CREATE TABLE `t1` (`id` INT) /*T! PRE_SPLIT_REGIONS = 2 */" +
				" /*T! SHARD_ROW_ID_BITS = 2 */",
		},
		{
			"create table t6 (id int ) " +
				"shard_row_id_bits=2 shard_row_id_bits=3 pre_split_regions=2;",
			"CREATE TABLE `t6` (`id` INT) /*T! SHARD_ROW_ID_BITS = 2 */ " +
				"/*T! SHARD_ROW_ID_BITS = 3 */ /*T! PRE_SPLIT_REGIONS = 2 */",
		},
		{
			"create table t1 (id int primary key auto_random(2));",
			"CREATE TABLE `t1` (`id` INT PRIMARY KEY /*T![auto_rand] AUTO_RANDOM(2) */)",
		},
		{
			"create table t1 (id int primary key auto_random);",
			"CREATE TABLE `t1` (`id` INT PRIMARY KEY /*T![auto_rand] AUTO_RANDOM */)",
		},
		{
			"create table t1 (id int auto_random ( 4 ) primary key);",
			"CREATE TABLE `t1` (`id` INT /*T![auto_rand] AUTO_RANDOM(4) */ PRIMARY KEY)",
		},
		{
			"create table t1 (id int  auto_random  (   4    ) primary key);",
			"CREATE TABLE `t1` (`id` INT /*T![auto_rand] AUTO_RANDOM(4) */ PRIMARY KEY)",
		},
		{
			"create table t1 (id int auto_random ( 3 ) primary key) auto_random_base = 100;",
			"CREATE TABLE `t1` (`id` INT /*T![auto_rand] AUTO_RANDOM(3) */" +
				" PRIMARY KEY) /*T![auto_rand_base] AUTO_RANDOM_BASE = 100 */",
		},
		{
			"create table t1 (id int auto_random primary key) auto_random_base = 50;",
			"CREATE TABLE `t1` (`id` INT /*T![auto_rand] AUTO_RANDOM */ PRIMARY KEY)" +
				" /*T![auto_rand_base] AUTO_RANDOM_BASE = 50 */",
		},
		{
			"create table t1 (id int auto_increment key) auto_id_cache 100;",
			"CREATE TABLE `t1` (`id` INT AUTO_INCREMENT PRIMARY KEY) " +
				"/*T![auto_id_cache] AUTO_ID_CACHE = 100 */",
		},
		{
			"create table t1 (id int auto_increment unique) auto_id_cache 10;",
			"CREATE TABLE `t1` (`id` INT AUTO_INCREMENT UNIQUE KEY) " +
				"/*T![auto_id_cache] AUTO_ID_CACHE = 10 */",
		},
		{
			"create table t1 (id int) auto_id_cache = 5;",
			"CREATE TABLE `t1` (`id` INT) /*T![auto_id_cache] AUTO_ID_CACHE = 5 */",
		},
		{
			"create table t1 (id int) auto_id_cache=5;",
			"CREATE TABLE `t1` (`id` INT) /*T![auto_id_cache] AUTO_ID_CACHE = 5 */",
		},
		{
			"create table t1 (id int) /*T![auto_id_cache] auto_id_cache=5 */ ;",
			"CREATE TABLE `t1` (`id` INT) /*T![auto_id_cache] AUTO_ID_CACHE = 5 */",
		},
		{
			"create table t1 (id int, a varchar(255), primary key (a, b) clustered);",
			"CREATE TABLE `t1` (`id` INT,`a` VARCHAR(255),PRIMARY KEY(`a`, `b`)" +
				" /*T![clustered_index] CLUSTERED */)",
		},
		{
			"create table t1(id int, v int, primary key(a) clustered);",
			"CREATE TABLE `t1` (`id` INT,`v` INT,PRIMARY KEY(`a`) " +
				"/*T![clustered_index] CLUSTERED */)",
		},
		{
			"create table t1(id int primary key clustered, v int);",
			"CREATE TABLE `t1` (`id` INT PRIMARY KEY " +
				"/*T![clustered_index] CLUSTERED */,`v` INT)",
		},
		{
			"alter table t add primary key(a) clustered;",
			"ALTER TABLE `t` ADD PRIMARY KEY(`a`) /*T![clustered_index] CLUSTERED */",
		},
		{
			"create table t1 (id int, a varchar(255), primary key (a, b) nonclustered);",
			"CREATE TABLE `t1` (`id` INT,`a` VARCHAR(255),PRIMARY KEY(`a`, `b`)" +
				" /*T![clustered_index] NONCLUSTERED */)",
		},
		{
			"create table t1 (id int, a varchar(255), primary key (a, b) " +
				"/*T![clustered_index] nonclustered */);",
			"CREATE TABLE `t1` (`id` INT,`a` VARCHAR(255),PRIMARY KEY(`a`, `b`)" +
				" /*T![clustered_index] NONCLUSTERED */)",
		},
		{
			"create table clustered_test(id int)",
			"CREATE TABLE `clustered_test` (`id` INT)",
		},
		{
			"create database clustered_test",
			"CREATE DATABASE `clustered_test`",
		},
		{
			"create database clustered",
			"CREATE DATABASE `clustered`",
		},
		{
			"create table clustered (id int)",
			"CREATE TABLE `clustered` (`id` INT)",
		},
		{
			"create table t1 (id int, a varchar(255) key clustered);",
			"CREATE TABLE `t1` (" +
				"`id` INT,`a` VARCHAR(255) PRIMARY KEY /*T![clustered_index] CLUSTERED */)",
		},
		{
			"alter table t force auto_increment = 12;",
			"ALTER TABLE `t` /*T![force_inc] FORCE */ AUTO_INCREMENT = 12",
		},
		{
			"alter table t force, auto_increment = 12;",
			"ALTER TABLE `t` FORCE /* AlterTableForce is not supported */ , AUTO_INCREMENT = 12",
		},
		{
			"create table cdc_test (id varchar(10) primary key ,c1 varchar(10)) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin" +
				"/*!90000  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=3 */",
			"CREATE TABLE `cdc_test` (`id` VARCHAR(10) PRIMARY KEY,`c1` VARCHAR(10)) " +
				"ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_BIN " +
				"/*T! SHARD_ROW_ID_BITS = 4 */ /*T! PRE_SPLIT_REGIONS = 3 */",
		},
		{
			"CREATE TABLE t1 (id BIGINT NOT NULL PRIMARY KEY auto_increment, " +
				"b varchar(255)) PLACEMENT POLICY=placement1;",
			"CREATE TABLE `t1` (`id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,`b` VARCHAR(255)) ",
		},
		{
			"CREATE TABLE `t1` (\n  `a` int(11) DEFAULT NULL\n) " +
				"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin " +
				"/*T![placement] PLACEMENT POLICY=`p2` */",
			"CREATE TABLE `t1` (`a` INT(11) DEFAULT NULL) " +
				"ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_BIN ",
		},
		{
			"CREATE TABLE t4 (" +
				"firstname VARCHAR(25) NOT NULL," +
				"lastname VARCHAR(25) NOT NULL," +
				"username VARCHAR(16) NOT NULL," +
				"email VARCHAR(35)," +
				"joined DATE NOT NULL) " +
				"PARTITION BY RANGE( YEAR(joined) )" +
				" (PARTITION p0 VALUES LESS THAN (1960) PLACEMENT POLICY=p1," +
				"PARTITION p1 VALUES LESS THAN (1970),PARTITION p2 VALUES LESS THAN (1980)," +
				"PARTITION p3 VALUES LESS THAN (1990),PARTITION p4 VALUES LESS THAN MAXVALUE);",
			"CREATE TABLE `t4` (" +
				"`firstname` VARCHAR(25) NOT NULL," +
				"`lastname` VARCHAR(25) NOT NULL," +
				"`username` VARCHAR(16) NOT NULL," +
				"`email` VARCHAR(35)," +
				"`joined` DATE NOT NULL) " +
				"PARTITION BY RANGE (YEAR(`joined`)) " +
				"(PARTITION `p0` VALUES LESS THAN (1960) ,PARTITION `p1` VALUES LESS THAN (1970)," +
				"PARTITION `p2` VALUES LESS THAN (1980),PARTITION `p3` VALUES LESS THAN (1990)," +
				"PARTITION `p4` VALUES LESS THAN (MAXVALUE))",
		},
		{
			"ALTER TABLE t3 PLACEMENT POLICY=DEFAULT;",
			"ALTER TABLE `t3`",
		},
		{
			"ALTER TABLE t1 PLACEMENT POLICY=p10",
			"ALTER TABLE `t1`",
		},
		{
			"ALTER TABLE t1 PLACEMENT POLICY=p10, add d text(50)",
			"ALTER TABLE `t1` ADD COLUMN `d` TEXT(50)",
		},
		{
			"alter table tp PARTITION p1 placement policy p2",
			"",
		},
		{
			"alter table t add d text(50) PARTITION p1 placement policy p2",
			"ALTER TABLE `t` ADD COLUMN `d` TEXT(50)",
		},
		{
			"alter table tp set tiflash replica 1 PARTITION p1 placement policy p2",
			"ALTER TABLE `tp` SET TIFLASH REPLICA 1",
		},
		{
			"ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY SET DEFAULT",
			"",
		},

		{
			"ALTER DATABASE TestResetPlacementDB PLACEMENT POLICY p1 charset utf8mb4",
			"ALTER DATABASE `TestResetPlacementDB`  CHARACTER SET = utf8mb4",
		},
		{
			"/*T![placement] ALTER DATABASE `db1` PLACEMENT POLICY = `p1` */",
			"",
		},
		{
			"ALTER PLACEMENT POLICY p3 PRIMARY_REGION='us-east-1' " +
				"REGIONS='us-east-1,us-east-2,us-west-1';",
			"",
		},
	}
	for _, ca := range testCase {
		re, err := addSpecialComment(ca.input)
		require.Nil(t, err)
		require.Equal(t, re, ca.result)
	}
	require.Panics(t, func() {
		_, _ = addSpecialComment("alter table t force, auto_increment = 12;alter table t force, auto_increment = 12;")
	}, "invalid ddlQuery statement size")
}
