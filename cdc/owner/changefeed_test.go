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
	"encoding/json"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/labstack/gommon/log"
	"github.com/pingcap/errors"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
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
	// whether to reset ddlDone flag, only for rename table
	resetDDLDone bool
	// whether to record the DDL history, only for rename table
	recordDDLHistory bool
	// a slice of DDL history, only for rename table
	ddlHistory []string
	mu         struct {
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

func (m *mockDDLSink) isInitialized() bool {
	return true
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
) (newCheckpointTs, newResolvedTs model.Ts, err error) {
	m.currentTables = currentTables
	return model.Ts(math.MaxUint64), model.Ts(math.MaxUint64), nil
}

// MoveTable is used to trigger manual table moves.
func (m *mockScheduler) MoveTable(tableID model.TableID, target model.CaptureID) {}

// Rebalance is used to trigger manual workload rebalances.
func (m *mockScheduler) Rebalance() {}

// Close closes the scheduler and releases resources.
func (m *mockScheduler) Close(ctx context.Context) {}

func createChangefeed4Test(ctx cdcContext.Context, t *testing.T) (
	*changefeed, *orchestrator.ChangefeedReactorState,
	map[model.CaptureID]*model.CaptureInfo, *orchestrator.ReactorStateTester,
) {
	upStream := upstream.NewUpstream4Test(&gc.MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return safePoint, nil
		},
	})

	cf := newChangefeed4Test(ctx.ChangefeedVars().ID, upStream, func(ctx cdcContext.Context, upStream *upstream.Upstream, startTs uint64) (DDLPuller, error) {
		return &mockDDLPuller{resolvedTs: startTs - 1}, nil
	}, func() DDLSink {
		return &mockDDLSink{
			resetDDLDone:     true,
			recordDDLHistory: false,
		}
	})
	cf.newScheduler = func(
		ctx cdcContext.Context, startTs uint64,
	) (scheduler.Scheduler, error) {
		return &mockScheduler{}, nil
	}
	cf.upStream = upStream
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

	// test clean the meta data of offline capture
	offlineCaputreID := "offline-capture"
	state.PatchTaskPosition(offlineCaputreID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		return new(model.TaskPosition), true, nil
	})
	tester.MustApplyPatches()

	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	require.NotNil(t, state.Status)
	require.NotContains(t, state.TaskPositions, offlineCaputreID)
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
		CaptureInfo: &model.CaptureInfo{
			ID:            "capture-id-test",
			AdvertiseAddr: "127.0.0.1:0000",
			Version:       version.ReleaseVersion,
		},
	})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: model.DefaultChangeFeedID("changefeed-id-test"),
		Info: &model.ChangeFeedInfo{
			StartTs: startTs,
			Config:  config.GetDefaultReplicaConfig(),
		},
	})

	cf, state, captures, tester := createChangefeed4Test(ctx, t)
	cf.upStream.KVStorage = helper.Storage()
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

	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{
		CaptureInfo: &model.CaptureInfo{
			ID:            "capture-id-test",
			AdvertiseAddr: "127.0.0.1:0000",
			Version:       version.ReleaseVersion,
		},
	})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: model.DefaultChangeFeedID("changefeed-id-test"),
		Info: &model.ChangeFeedInfo{
			StartTs: startTs,
			Config:  config.GetDefaultReplicaConfig(),
		},
	})

	cf, state, captures, tester := createChangefeed4Test(ctx, t)
	cf.upStream.KVStorage = helper.Storage()

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
		Level:             "eventual",
		Storage:           filepath.Join("nfs://", dir),
		FlushIntervalInMs: config.DefaultFlushIntervalInMs,
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
	cf.isReleased = false
	// changefeed tick will release resources
	err := cf.tick(ctx, state, captures)
	require.Nil(t, err)
	cancel()
	// check redo log dir is deleted
	_, err = os.Stat(redoLogDir)
	log.Error(err)
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

func TestExecRenameTablesDDL(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, t)
	defer cf.Close(ctx)
	// pre check
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	mockDDLSink := cf.sink.(*mockDDLSink)

	var oldSchemaIDs, newSchemaIDs, oldTableIDs []int64
	var newTableNames, oldSchemaNames []timodel.CIStr

	execCreateStmt := func(tp, actualDDL, expectedDDL string) {
		job := helper.DDL2Job(actualDDL)
		done, err := cf.asyncExecDDLJob(ctx, job)
		if tp == "database" {
			oldSchemaIDs = append(oldSchemaIDs, job.SchemaID)
		} else {
			oldTableIDs = append(oldTableIDs, job.TableID)
		}
		require.Nil(t, err)
		require.Equal(t, false, done)
		require.Equal(t, expectedDDL, mockDDLSink.ddlExecuting.Query)
		mockDDLSink.ddlDone = true
		done, err = cf.asyncExecDDLJob(ctx, job)
		require.Nil(t, err)
		require.Equal(t, true, done)
		require.Equal(t, expectedDDL, mockDDLSink.ddlExecuting.Query)
	}

	execCreateStmt("database", "create database test1",
		"CREATE DATABASE `test1`")
	execCreateStmt("table", "create table test1.tb1(id int primary key)",
		"CREATE TABLE `test1`.`tb1` (`id` INT PRIMARY KEY)")
	execCreateStmt("database", "create database test2",
		"CREATE DATABASE `test2`")
	execCreateStmt("table", "create table test2.tb2(id int primary key)",
		"CREATE TABLE `test2`.`tb2` (`id` INT PRIMARY KEY)")

	require.Len(t, oldSchemaIDs, 2)
	require.Len(t, oldTableIDs, 2)
	newSchemaIDs = []int64{oldSchemaIDs[1], oldSchemaIDs[0]}
	oldSchemaNames = []timodel.CIStr{
		timodel.NewCIStr("test1"),
		timodel.NewCIStr("test2"),
	}
	newTableNames = []timodel.CIStr{
		timodel.NewCIStr("tb20"),
		timodel.NewCIStr("tb10"),
	}
	require.Len(t, newSchemaIDs, 2)
	require.Len(t, oldSchemaNames, 2)
	require.Len(t, newTableNames, 2)
	args := []interface{}{
		oldSchemaIDs, newSchemaIDs, newTableNames,
		oldTableIDs, oldSchemaNames,
	}
	rawArgs, err := json.Marshal(args)
	require.Nil(t, err)
	job := helper.DDL2Job(
		"rename table test1.tb1 to test2.tb10, test2.tb2 to test1.tb20")
	// the RawArgs field in job fetched from tidb snapshot meta is incorrent,
	// so we manually construct `job.RawArgs` to do the workaround.
	job.RawArgs = rawArgs

	mockDDLSink.recordDDLHistory = true
	done, err := cf.asyncExecDDLJob(ctx, job)
	require.Nil(t, err)
	require.Equal(t, false, done)
	require.Len(t, mockDDLSink.ddlHistory, 2)
	require.Equal(t, "RENAME TABLE `test1`.`tb1` TO `test2`.`tb10`",
		mockDDLSink.ddlHistory[0])
	require.Equal(t, "RENAME TABLE `test2`.`tb2` TO `test1`.`tb20`",
		mockDDLSink.ddlHistory[1])

	// mock all of the rename table statements have been done
	mockDDLSink.resetDDLDone = false
	mockDDLSink.ddlDone = true
	done, err = cf.asyncExecDDLJob(ctx, job)
	require.Nil(t, err)
	require.Equal(t, true, done)
}

func TestExecDropTablesDDL(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, t)
	defer cf.Close(ctx)

	// pre check
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	// initialize
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	mockDDLSink := cf.sink.(*mockDDLSink)
	execCreateStmt := func(actualDDL, expectedDDL string) {
		job := helper.DDL2Job(actualDDL)
		done, err := cf.asyncExecDDLJob(ctx, job)
		require.Nil(t, err)
		require.Equal(t, false, done)
		require.Equal(t, expectedDDL, mockDDLSink.ddlExecuting.Query)
		mockDDLSink.ddlDone = true
		done, err = cf.asyncExecDDLJob(ctx, job)
		require.Nil(t, err)
		require.Equal(t, true, done)
	}

	execCreateStmt("create database test1",
		"CREATE DATABASE `test1`")
	execCreateStmt("create table test1.tb1(id int primary key)",
		"CREATE TABLE `test1`.`tb1` (`id` INT PRIMARY KEY)")
	execCreateStmt("create table test1.tb2(id int primary key)",
		"CREATE TABLE `test1`.`tb2` (`id` INT PRIMARY KEY)")

	// drop tables is different from rename tables, it will generate
	// multiple DDL jobs instead of one.
	jobs := helper.DDL2Jobs("drop table test1.tb1, test1.tb2", 2)
	require.Len(t, jobs, 2)

	execDropStmt := func(job *timodel.Job, expectedDDL string) {
		done, err := cf.asyncExecDDLJob(ctx, job)
		require.Nil(t, err)
		require.Equal(t, false, done)
		require.Equal(t, mockDDLSink.ddlExecuting.Query, expectedDDL)
		mockDDLSink.ddlDone = true
		done, err = cf.asyncExecDDLJob(ctx, job)
		require.Nil(t, err)
		require.Equal(t, true, done)
	}

	execDropStmt(jobs[0], "DROP TABLE `test1`.`tb2`")
	execDropStmt(jobs[1], "DROP TABLE `test1`.`tb1`")
}

func TestExecDropViewsDDL(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, t)
	defer cf.Close(ctx)

	// pre check
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	// initialize
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	mockDDLSink := cf.sink.(*mockDDLSink)
	execCreateStmt := func(actualDDL, expectedDDL string) {
		job := helper.DDL2Job(actualDDL)
		done, err := cf.asyncExecDDLJob(ctx, job)
		require.Nil(t, err)
		require.Equal(t, false, done)
		require.Equal(t, expectedDDL, mockDDLSink.ddlExecuting.Query)
		mockDDLSink.ddlDone = true
		done, err = cf.asyncExecDDLJob(ctx, job)
		require.Nil(t, err)
		require.Equal(t, true, done)
	}
	execCreateStmt("create database test1",
		"CREATE DATABASE `test1`")
	execCreateStmt("create table test1.tb1(id int primary key)",
		"CREATE TABLE `test1`.`tb1` (`id` INT PRIMARY KEY)")
	execCreateStmt("create view test1.view1 as "+
		"select * from test1.tb1 where id > 100",
		"CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL "+
			"SECURITY DEFINER VIEW `test1`.`view1` AS "+
			"SELECT * FROM `test1`.`tb1` WHERE `id`>100")
	execCreateStmt("create view test1.view2 as "+
		"select * from test1.tb1 where id > 200",
		"CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL "+
			"SECURITY DEFINER VIEW `test1`.`view2` AS "+
			"SELECT * FROM `test1`.`tb1` WHERE `id`>200")

	// drop views is similar to drop tables, it will also generate
	// multiple DDL jobs.
	jobs := helper.DDL2Jobs("drop view test1.view1, test1.view2", 2)
	require.Len(t, jobs, 2)

	execDropStmt := func(job *timodel.Job, expectedDDL string) {
		done, err := cf.asyncExecDDLJob(ctx, job)
		require.Nil(t, err)
		require.Equal(t, false, done)
		require.Equal(t, expectedDDL, mockDDLSink.ddlExecuting.Query)
		mockDDLSink.ddlDone = true
		done, err = cf.asyncExecDDLJob(ctx, job)
		require.Nil(t, err)
		require.Equal(t, true, done)
	}

	execDropStmt(jobs[0], "DROP VIEW `test1`.`view2`")
	execDropStmt(jobs[1], "DROP VIEW `test1`.`view1`")
}

func TestBarrierAdvance(t *testing.T) {
	for i := 0; i < 2; i++ {
		ctx := cdcContext.NewBackendContext4Test(true)
		if i == 1 {
			ctx.ChangefeedVars().Info.SyncPointEnabled = true
			ctx.ChangefeedVars().Info.SyncPointInterval = 100 * time.Second
		}

		cf, state, captures, tester := createChangefeed4Test(ctx, t)
		defer cf.Close(ctx)

		// Pre tick, to fill cf.state.Info.
		cf.Tick(ctx, state, captures)
		tester.MustApplyPatches()

		// The changefeed load the info from etcd.
		cf.state.Status = &model.ChangeFeedStatus{
			ResolvedTs:   cf.state.Info.StartTs + 10,
			CheckpointTs: cf.state.Info.StartTs,
		}

		// Do the preflightCheck and initialize the changefeed.
		cf.Tick(ctx, state, captures)
		tester.MustApplyPatches()

		// add 5s to resolvedTs.
		mockDDLPuller := cf.ddlPuller.(*mockDDLPuller)
		mockDDLPuller.resolvedTs = oracle.GoTimeToTS(oracle.GetTimeFromTS(mockDDLPuller.resolvedTs).Add(5 * time.Second))

		// Then the first tick barrier won't be changed.
		barrier, err := cf.handleBarrier(ctx)
		require.Nil(t, err)
		require.Equal(t, cf.state.Info.StartTs, barrier)

		// If sync-point is enabled, must tick more 1 time to advance barrier.
		if i == 1 {
			barrier, err := cf.handleBarrier(ctx)
			require.Nil(t, err)
			require.Equal(t, cf.state.Info.StartTs+10, barrier)
		}

		// Suppose checkpoint has been advanced.
		cf.state.Status.CheckpointTs = cf.state.Status.ResolvedTs

		// Need more 1 tick to advance barrier if sync-point is enabled.
		if i == 1 {
			barrier, err := cf.handleBarrier(ctx)
			require.Nil(t, err)
			require.Equal(t, cf.state.Info.StartTs+10, barrier)
		}

		// Then the last tick barrier must be advanced correctly.
		barrier, err = cf.handleBarrier(ctx)
		require.Nil(t, err)
		require.Equal(t, mockDDLPuller.resolvedTs, barrier)
	}
}
