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
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/tidb/store/tikv/oracle"
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

type mockAsyncSink struct {
	// AsyncSink
	ddlExecuting *model.DDLEvent
	ddlDone      bool
	checkpointTs model.Ts
	syncPoint    model.Ts
	syncPointHis []model.Ts
}

func (m *mockAsyncSink) EmitDDLEvent(ctx cdcContext.Context, ddl *model.DDLEvent) (bool, error) {
	m.ddlExecuting = ddl
	defer func() { m.ddlDone = false }()
	return m.ddlDone, nil
}

func (m *mockAsyncSink) SinkSyncpoint(ctx cdcContext.Context, checkpointTs uint64) error {
	if checkpointTs == m.syncPoint {
		return nil
	}
	m.syncPoint = checkpointTs
	m.syncPointHis = append(m.syncPointHis, checkpointTs)
	return nil
}

func (m *mockAsyncSink) Initialize(ctx cdcContext.Context, tableInfo []*model.SimpleTableInfo) error {
	return nil
}

func (m *mockAsyncSink) EmitCheckpointTs(ctx cdcContext.Context, ts uint64) {
	atomic.StoreUint64(&m.checkpointTs, ts)
}

func (m *mockAsyncSink) Close() error {
	return nil
}

var _ = check.Suite(&changefeedSuite{})

type changefeedSuite struct {
}

func createChangefeed4Test(ctx cdcContext.Context, c *check.C) (*changefeed, *model.ChangefeedReactorState,
	map[model.CaptureID]*model.CaptureInfo, *orchestrator.ReactorStateTester) {
	ctx.GlobalVars().PDClient = &mockPDClient{updateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		return safePoint, nil
	}}
	gcManager := newGCManager()
	cf := newChangefeed4Test(ctx.ChangefeedVars().ID, gcManager, func(ctx cdcContext.Context, startTs uint64) (DDLPuller, error) {
		return &mockDDLPuller{resolvedTs: startTs - 1}, nil
	}, func(ctx cdcContext.Context) (AsyncSink, error) {
		return &mockAsyncSink{}, nil
	})
	state := model.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(c, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		c.Assert(info, check.IsNil)
		info = ctx.ChangefeedVars().Info
		return info, true, nil
	})
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
	c.Assert(state.Status.CheckpointTs, check.Equals, ctx.ChangefeedVars().Info.StartTs-1)
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
	c.Assert(state.Status.CheckpointTs, check.Equals, ctx.ChangefeedVars().Info.StartTs-1)
	c.Assert(state.Info.Error.Message, check.Equals, "fake error")
}

func (s *changefeedSuite) TestExecDDL(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, c)
	defer cf.Close()
	helper := entry.NewSchemaTestHelper(c)
	defer helper.Close()
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

	// ddl puller resolved ts grow uo
	mockDDLPuller := cf.ddlPuller.(*mockDDLPuller)
	mockDDLPuller.resolvedTs += 1000
	mockAsyncSink := cf.sink.(*mockAsyncSink)
	// three tick to make sure all barriers set in initialize is handled
	tickThreeTime()
	c.Assert(state.Status.CheckpointTs, check.Equals, mockDDLPuller.resolvedTs)

	// handle create database
	job := helper.DDL2Job("create database test1")
	mockDDLPuller.resolvedTs += 1000
	job.BinlogInfo.FinishedTS = mockDDLPuller.resolvedTs
	mockDDLPuller.ddlQueue = append(mockDDLPuller.ddlQueue, job)
	tickThreeTime()
	c.Assert(state.Status.CheckpointTs, check.Equals, mockDDLPuller.resolvedTs)
	c.Assert(mockAsyncSink.ddlExecuting.Query, check.Equals, "create database test1")

	// executing the ddl finished
	mockAsyncSink.ddlDone = true
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
	c.Assert(mockAsyncSink.ddlExecuting.Query, check.Equals, "create table test1.test1(id int primary key)")

	// executing the ddl finished
	mockAsyncSink.ddlDone = true
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
	mockAsyncSink := cf.sink.(*mockAsyncSink)
	// add 5s to resolvedTs
	mockDDLPuller.resolvedTs = oracle.GoTimeToTS(oracle.GetTimeFromTS(mockDDLPuller.resolvedTs).Add(5 * time.Second))
	// tick 20 times
	for i := 0; i <= 20; i++ {
		cf.Tick(ctx, state, captures)
		tester.MustApplyPatches()
	}

	c.Assert(mockAsyncSink.syncPointHis[0], check.Equals, ctx.ChangefeedVars().Info.StartTs-1)
	c.Assert(mockAsyncSink.syncPointHis[len(mockAsyncSink.syncPointHis)-1], check.Equals, state.Status.CheckpointTs)
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
