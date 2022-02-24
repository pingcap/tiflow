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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/pdtime"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/pingcap/tiflow/pkg/version"
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
	checkpointTs model.Ts
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

func (m *mockDDLSink) emitCheckpointTs(ctx cdcContext.Context, ts uint64) {
	atomic.StoreUint64(&m.checkpointTs, ts)
}

func (m *mockDDLSink) close(ctx context.Context) error {
	m.wg.Wait()
	return nil
}

func (m *mockDDLSink) Barrier(ctx context.Context) error {
	return nil
}

var _ = check.Suite(&changefeedSuite{})

type changefeedSuite struct {
}

func createChangefeed4Test(ctx cdcContext.Context, c *check.C) (*changefeed, *model.ChangefeedReactorState,
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
	state := model.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(c, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		c.Assert(info, check.IsNil)
		info = ctx.ChangefeedVars().Info
		return info, true, nil
	})
	tester.MustUpdate("/tidb/cdc/capture/"+ctx.GlobalVars().CaptureInfo.ID, []byte(`{"id":"`+ctx.GlobalVars().CaptureInfo.ID+`","address":"127.0.0.1:8300"}`))
	tester.MustApplyPatches()
	captures := map[model.CaptureID]*model.CaptureInfo{ctx.GlobalVars().CaptureInfo.ID: ctx.GlobalVars().CaptureInfo}
	return cf, state, captures, tester
}

func (s *changefeedSuite) TestPreCheck(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, c)
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	c.Assert(state.Status, check.NotNil)
	c.Assert(state.TaskStatuses, check.HasKey, ctx.GlobalVars().CaptureInfo.ID)

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
	c.Assert(state.Status, check.NotNil)
	c.Assert(state.TaskStatuses, check.HasKey, ctx.GlobalVars().CaptureInfo.ID)
	c.Assert(state.TaskStatuses, check.Not(check.HasKey), offlineCaputreID)
	c.Assert(state.TaskPositions, check.Not(check.HasKey), offlineCaputreID)
	c.Assert(state.Workloads, check.Not(check.HasKey), offlineCaputreID)
}

func (s *changefeedSuite) TestInitialize(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, c)
	defer cf.Close()
	// pre check
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	c.Assert(state.Status.CheckpointTs, check.Equals, ctx.ChangefeedVars().Info.StartTs)
}

func (s *changefeedSuite) TestHandleError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, c)
	defer cf.Close()
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
	c.Assert(state.Status.CheckpointTs, check.Equals, ctx.ChangefeedVars().Info.StartTs)
	c.Assert(state.Info.Error.Message, check.Equals, "fake error")
}

func (s *changefeedSuite) TestExecDDL(c *check.C) {
	defer testleak.AfterTest(c)()

	helper := entry.NewSchemaTestHelper(c)
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
		TimeAcquirer: pdtime.NewTimeAcquirer4Test(),
	})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test",
		Info: &model.ChangeFeedInfo{
			StartTs: startTs,
			Config:  config.GetDefaultReplicaConfig(),
		},
	})

	cf, state, captures, tester := createChangefeed4Test(ctx, c)
	defer cf.Close()
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

	c.Assert(cf.schema.AllPhysicalTables(), check.HasLen, 1)
	c.Assert(state.TaskStatuses[ctx.GlobalVars().CaptureInfo.ID].Operation, check.HasLen, 0)
	c.Assert(state.TaskStatuses[ctx.GlobalVars().CaptureInfo.ID].Tables, check.HasLen, 0)

	job = helper.DDL2Job("drop table test0.table0")
	// ddl puller resolved ts grow uo
	mockDDLPuller := cf.ddlPuller.(*mockDDLPuller)
	mockDDLPuller.resolvedTs = startTs
	mockDDLSink := cf.sink.(*mockDDLSink)
	job.BinlogInfo.FinishedTS = mockDDLPuller.resolvedTs
	mockDDLPuller.ddlQueue = append(mockDDLPuller.ddlQueue, job)
	// three tick to make sure all barriers set in initialize is handled
	tickThreeTime()
	c.Assert(state.Status.CheckpointTs, check.Equals, mockDDLPuller.resolvedTs)
	// The ephemeral table should have left no trace in the schema cache
	c.Assert(cf.schema.AllPhysicalTables(), check.HasLen, 0)

	// executing the ddl finished
	mockDDLSink.ddlDone = true
	mockDDLPuller.resolvedTs += 1000
	tickThreeTime()
	c.Assert(state.Status.CheckpointTs, check.Equals, mockDDLPuller.resolvedTs)

	// handle create database
	job = helper.DDL2Job("create database test1")
	mockDDLPuller.resolvedTs += 1000
	job.BinlogInfo.FinishedTS = mockDDLPuller.resolvedTs
	mockDDLPuller.ddlQueue = append(mockDDLPuller.ddlQueue, job)
	tickThreeTime()
	c.Assert(state.Status.CheckpointTs, check.Equals, mockDDLPuller.resolvedTs)
	c.Assert(mockDDLSink.ddlExecuting.Query, check.Equals, "create database test1")

	// executing the ddl finished
	mockDDLSink.ddlDone = true
	mockDDLPuller.resolvedTs += 1000
	tickThreeTime()
	c.Assert(state.Status.CheckpointTs, check.Equals, mockDDLPuller.resolvedTs)

	// handle create table
	job = helper.DDL2Job("create table test1.test1(id int primary key)")
	mockDDLPuller.resolvedTs += 1000
	job.BinlogInfo.FinishedTS = mockDDLPuller.resolvedTs
	mockDDLPuller.ddlQueue = append(mockDDLPuller.ddlQueue, job)
	tickThreeTime()
	c.Assert(state.Status.CheckpointTs, check.Equals, mockDDLPuller.resolvedTs)
	c.Assert(mockDDLSink.ddlExecuting.Query, check.Equals, "create table test1.test1(id int primary key)")

	// executing the ddl finished
	mockDDLSink.ddlDone = true
	mockDDLPuller.resolvedTs += 1000
	tickThreeTime()
	c.Assert(state.TaskStatuses[ctx.GlobalVars().CaptureInfo.ID].Tables, check.HasKey, job.TableID)
}

func (s *changefeedSuite) TestSyncPoint(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	ctx.ChangefeedVars().Info.SyncPointEnabled = true
	ctx.ChangefeedVars().Info.SyncPointInterval = 1 * time.Second
	cf, state, captures, tester := createChangefeed4Test(ctx, c)
	defer cf.Close()

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
		c.Assert(mockDDLSink.syncPointHis[i]-mockDDLSink.syncPointHis[i-1], check.LessEqual, uint64(1000<<18))
	}
	c.Assert(len(mockDDLSink.syncPointHis), check.GreaterEqual, 5)
}

func (s *changefeedSuite) TestFinished(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	ctx.ChangefeedVars().Info.TargetTs = ctx.ChangefeedVars().Info.StartTs + 1000
	cf, state, captures, tester := createChangefeed4Test(ctx, c)
	defer cf.Close()

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

	c.Assert(state.Status.CheckpointTs, check.Equals, state.Info.TargetTs)
	c.Assert(state.Info.State, check.Equals, model.StateFinished)
}
