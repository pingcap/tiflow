package owner

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/orchestrator"
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
}

func (m *mockAsyncSink) EmitDDLEvent(ctx cdcContext.Context, ddl *model.DDLEvent) (bool, error) {
	m.ddlExecuting = ddl
	defer func() { m.ddlDone = false }()
	return m.ddlDone, nil
}

func (m *mockAsyncSink) SinkSyncpoint(ctx cdcContext.Context, checkpointTs uint64) error {
	panic("implement me")
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
	cf := newChangefeed4Test(gcManager, func(ctx cdcContext.Context, startTs uint64) (DDLPuller, error) {
		return &mockDDLPuller{resolvedTs: startTs - 1}, nil
	}, func(ctx cdcContext.Context) (AsyncSink, error) {
		return &mockAsyncSink{}, nil
	})
	state := model.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(c, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		info = ctx.ChangefeedVars().Info
		return info, true, nil
	})
	tester.MustApplyPatches()
	captures := map[model.CaptureID]*model.CaptureInfo{ctx.GlobalVars().CaptureInfo.ID: ctx.GlobalVars().CaptureInfo}
	return cf, state, captures, tester
}

func (s *changefeedSuite) TestPreCheck(c *check.C) {
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
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, c)
	// pre check
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()

	// initialize
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
	c.Assert(state.Status.CheckpointTs, check.Equals, ctx.ChangefeedVars().Info.StartTs)
}

func (s *changefeedSuite) TestHandleError(c *check.C) {
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, c)
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
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, c)
	helper := entry.NewSchemaTestHelper(c)
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
