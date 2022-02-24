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

package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync/atomic"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	tablepipeline "github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/sink"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func Test(t *testing.T) { check.TestingT(t) }

type processorSuite struct{}

var _ = check.Suite(&processorSuite{})

func newProcessor4Test(
	ctx cdcContext.Context,
	c *check.C,
	createTablePipeline func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error),
) *processor {
	p := newProcessor(ctx)
	p.lazyInit = func(ctx cdcContext.Context) error { return nil }
	p.createTablePipeline = createTablePipeline
	p.sinkManager = &sink.Manager{}
	p.schemaStorage = &mockSchemaStorage{c: c}
	return p
}

func initProcessor4Test(ctx cdcContext.Context, c *check.C) (*processor, *orchestrator.ReactorStateTester) {
	p := newProcessor4Test(ctx, c, func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error) {
		return &mockTablePipeline{
			tableID:      tableID,
			name:         fmt.Sprintf("`test`.`table%d`", tableID),
			status:       tablepipeline.TableStatusRunning,
			resolvedTs:   replicaInfo.StartTs,
			checkpointTs: replicaInfo.StartTs,
		}, nil
	})
	p.changefeed = model.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	return p, orchestrator.NewReactorStateTester(c, p.changefeed, map[string]string{
		"/tidb/cdc/capture/" + ctx.GlobalVars().CaptureInfo.ID:                                     `{"id":"` + ctx.GlobalVars().CaptureInfo.ID + `","address":"127.0.0.1:8300"}`,
		"/tidb/cdc/changefeed/info/" + ctx.ChangefeedVars().ID:                                     `{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":0,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":".","config":{"case-sensitive":true,"enable-old-value":false,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
		"/tidb/cdc/job/" + ctx.ChangefeedVars().ID:                                                 `{"resolved-ts":0,"checkpoint-ts":0,"admin-job-type":0}`,
		"/tidb/cdc/task/status/" + ctx.GlobalVars().CaptureInfo.ID + "/" + ctx.ChangefeedVars().ID: `{"tables":{},"operation":null,"admin-job-type":0}`,
	})
}

type mockTablePipeline struct {
	tableID      model.TableID
	name         string
	resolvedTs   model.Ts
	checkpointTs model.Ts
	barrierTs    model.Ts
	stopTs       model.Ts
	status       tablepipeline.TableStatus
	canceled     bool
}

func (m *mockTablePipeline) ID() (tableID int64, markTableID int64) {
	return m.tableID, 0
}

func (m *mockTablePipeline) Name() string {
	return m.name
}

func (m *mockTablePipeline) ResolvedTs() model.Ts {
	return m.resolvedTs
}

func (m *mockTablePipeline) CheckpointTs() model.Ts {
	return m.checkpointTs
}

func (m *mockTablePipeline) UpdateBarrierTs(ts model.Ts) {
	m.barrierTs = ts
}

func (m *mockTablePipeline) AsyncStop(targetTs model.Ts) bool {
	m.stopTs = targetTs
	return true
}

func (m *mockTablePipeline) Workload() model.WorkloadInfo {
	return model.WorkloadInfo{Workload: 1}
}

func (m *mockTablePipeline) Status() tablepipeline.TableStatus {
	return m.status
}

func (m *mockTablePipeline) Cancel() {
	if m.canceled {
		log.Panic("cancel a canceled table pipeline")
	}
	m.canceled = true
}

func (m *mockTablePipeline) Wait() {
	// do nothing
}

type mockSchemaStorage struct {
	// dummy to provide default versions of unimplemented interface methods,
	// as we only need ResolvedTs() and DoGC() in unit tests.
	entry.SchemaStorage

	c        *check.C
	lastGcTs uint64
}

func (s *mockSchemaStorage) ResolvedTs() uint64 {
	return math.MaxUint64
}

func (s *mockSchemaStorage) DoGC(ts uint64) uint64 {
	s.c.Assert(s.lastGcTs, check.LessEqual, ts)
	atomic.StoreUint64(&s.lastGcTs, ts)
	return ts
}

func (s *processorSuite) TestCheckTablesNum(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals,
		&model.TaskPosition{
			CheckPointTs: 0,
			ResolvedTs:   0,
			Count:        0,
			Error:        nil,
		})

	p, tester = initProcessor4Test(ctx, c)
	p.changefeed.Info.StartTs = 66
	p.changefeed.Status.CheckpointTs = 88
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals,
		&model.TaskPosition{
			CheckPointTs: 88,
			ResolvedTs:   88,
			Count:        0,
			Error:        nil,
		})
}

func (s *processorSuite) TestHandleTableOperation4SingleTable(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 90
		status.ResolvedTs = 100
		return status, true, nil
	})
	p.changefeed.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		position.ResolvedTs = 100
		return position, true, nil
	})
	tester.MustApplyPatches()

	// no operation
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// add table, in processing
	// in current implementation of owner, the startTs and BoundaryTs of add table operation should be always equaled.
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.AddTable(66, &model.TableReplicaInfo{StartTs: 60}, 60)
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: false, BoundaryTs: 60, Done: false, Status: model.OperProcessed},
		},
	})

	// add table, not finished
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: false, BoundaryTs: 60, Done: false, Status: model.OperProcessed},
		},
	})

	// add table, push the resolvedTs
	table66 := p.tables[66].(*mockTablePipeline)
	table66.resolvedTs = 101
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: false, BoundaryTs: 60, Done: false, Status: model.OperProcessed},
		},
	})
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs, check.Equals, uint64(101))

	// finish the operation
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: false, BoundaryTs: 60, Done: true, Status: model.OperFinished},
		},
	})

	// clear finished operations
	cleanUpFinishedOpOperation(p.changefeed, p.captureInfo.ID, tester)

	// remove table, in processing
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.RemoveTable(66, 120, false)
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: true, BoundaryTs: 120, Done: false, Status: model.OperProcessed},
		},
	})
	c.Assert(table66.stopTs, check.Equals, uint64(120))

	// remove table, not finished
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: true, BoundaryTs: 120, Done: false, Status: model.OperProcessed},
		},
	})

	// remove table, finished
	table66.status = tablepipeline.TableStatusStopped
	table66.checkpointTs = 121
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: true, BoundaryTs: 121, Done: true, Status: model.OperFinished},
		},
	})
	c.Assert(table66.canceled, check.IsTrue)
	c.Assert(p.tables[66], check.IsNil)
}

func (s *processorSuite) TestHandleTableOperation4MultiTable(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 20
		status.ResolvedTs = 20
		return status, true, nil
	})
	p.changefeed.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		position.ResolvedTs = 100
		position.CheckPointTs = 90
		return position, true, nil
	})
	tester.MustApplyPatches()

	// no operation
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// add table, in processing
	// in current implementation of owner, the startTs and BoundaryTs of add table operation should be always equaled.
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.AddTable(1, &model.TableReplicaInfo{StartTs: 60}, 60)
		status.AddTable(2, &model.TableReplicaInfo{StartTs: 50}, 50)
		status.AddTable(3, &model.TableReplicaInfo{StartTs: 40}, 40)
		status.Tables[4] = &model.TableReplicaInfo{StartTs: 30}
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			3: {StartTs: 40},
			4: {StartTs: 30},
		},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: false, BoundaryTs: 60, Done: false, Status: model.OperProcessed},
			2: {Delete: false, BoundaryTs: 50, Done: false, Status: model.OperProcessed},
			3: {Delete: false, BoundaryTs: 40, Done: false, Status: model.OperProcessed},
		},
	})
	c.Assert(p.tables, check.HasLen, 4)
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID].CheckPointTs, check.Equals, uint64(30))
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs, check.Equals, uint64(30))

	// add table, push the resolvedTs, finished add table
	table1 := p.tables[1].(*mockTablePipeline)
	table2 := p.tables[2].(*mockTablePipeline)
	table3 := p.tables[3].(*mockTablePipeline)
	table4 := p.tables[4].(*mockTablePipeline)
	table1.resolvedTs = 101
	table2.resolvedTs = 101
	table3.resolvedTs = 102
	table4.resolvedTs = 103
	// removed table 3
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.RemoveTable(3, 60, false)
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			4: {StartTs: 30},
		},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: false, BoundaryTs: 60, Done: true, Status: model.OperFinished},
			2: {Delete: false, BoundaryTs: 50, Done: true, Status: model.OperFinished},
			3: {Delete: true, BoundaryTs: 60, Done: false, Status: model.OperProcessed},
		},
	})
	c.Assert(p.tables, check.HasLen, 4)
	c.Assert(table3.canceled, check.IsFalse)
	c.Assert(table3.stopTs, check.Equals, uint64(60))
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs, check.Equals, uint64(101))

	// finish remove operations
	table3.status = tablepipeline.TableStatusStopped
	table3.checkpointTs = 65
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			4: {StartTs: 30},
		},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: false, BoundaryTs: 60, Done: true, Status: model.OperFinished},
			2: {Delete: false, BoundaryTs: 50, Done: true, Status: model.OperFinished},
			3: {Delete: true, BoundaryTs: 65, Done: true, Status: model.OperFinished},
		},
	})
	c.Assert(p.tables, check.HasLen, 3)
	c.Assert(table3.canceled, check.IsTrue)

	// clear finished operations
	cleanUpFinishedOpOperation(p.changefeed, p.captureInfo.ID, tester)

	// remove table, in processing
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.RemoveTable(1, 120, false)
		status.RemoveTable(4, 120, false)
		delete(status.Tables, 2)
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: true, BoundaryTs: 120, Done: false, Status: model.OperProcessed},
			4: {Delete: true, BoundaryTs: 120, Done: false, Status: model.OperProcessed},
		},
	})
	c.Assert(table1.stopTs, check.Equals, uint64(120))
	c.Assert(table4.stopTs, check.Equals, uint64(120))
	c.Assert(table2.canceled, check.IsTrue)
	c.Assert(p.tables, check.HasLen, 2)

	// remove table, not finished
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: true, BoundaryTs: 120, Done: false, Status: model.OperProcessed},
			4: {Delete: true, BoundaryTs: 120, Done: false, Status: model.OperProcessed},
		},
	})

	// remove table, finished
	table1.status = tablepipeline.TableStatusStopped
	table1.checkpointTs = 121
	table4.status = tablepipeline.TableStatusStopped
	table4.checkpointTs = 122
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: true, BoundaryTs: 121, Done: true, Status: model.OperFinished},
			4: {Delete: true, BoundaryTs: 122, Done: true, Status: model.OperFinished},
		},
	})
	c.Assert(table1.canceled, check.IsTrue)
	c.Assert(table4.canceled, check.IsTrue)
	c.Assert(p.tables, check.HasLen, 0)
}

func (s *processorSuite) TestInitTable(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Tables[1] = &model.TableReplicaInfo{StartTs: 20}
		status.Tables[2] = &model.TableReplicaInfo{StartTs: 30}
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.tables[1], check.Not(check.IsNil))
	c.Assert(p.tables[2], check.Not(check.IsNil))
}

func (s *processorSuite) TestProcessorError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// send a abnormal error
	p.sendError(cerror.ErrSinkURIInvalid)
	_, err = p.Tick(ctx, p.changefeed)
	tester.MustApplyPatches()
	c.Assert(cerror.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		Error: &model.RunningError{
			Addr:    "127.0.0.1:0000",
			Code:    "CDC:ErrSinkURIInvalid",
			Message: "[CDC:ErrSinkURIInvalid]sink uri invalid",
		},
	})

	p, tester = initProcessor4Test(ctx, c)
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// send a normal error
	p.sendError(context.Canceled)
	_, err = p.Tick(ctx, p.changefeed)
	tester.MustApplyPatches()
	c.Assert(cerror.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		Error: nil,
	})
}

func (s *processorSuite) TestProcessorExit(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// stop the changefeed
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.AdminJobType = model.AdminStop
		return status, true, nil
	})
	p.changefeed.PatchTaskStatus(ctx.GlobalVars().CaptureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.AdminJobType = model.AdminStop
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(cerror.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		Error: nil,
	})
}

func (s *processorSuite) TestProcessorClose(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// add tables
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Tables[1] = &model.TableReplicaInfo{StartTs: 20}
		status.Tables[2] = &model.TableReplicaInfo{StartTs: 30}
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// push the resolvedTs and checkpointTs
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.ResolvedTs = 100
		return status, true, nil
	})
	tester.MustApplyPatches()
	p.tables[1].(*mockTablePipeline).resolvedTs = 110
	p.tables[2].(*mockTablePipeline).resolvedTs = 90
	p.tables[1].(*mockTablePipeline).checkpointTs = 90
	p.tables[2].(*mockTablePipeline).checkpointTs = 95
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 90,
		ResolvedTs:   90,
		Error:        nil,
	})
	c.Assert(p.changefeed.TaskStatuses[p.captureInfo.ID], check.DeepEquals, &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{1: {StartTs: 20}, 2: {StartTs: 30}},
	})
	c.Assert(p.changefeed.Workloads[p.captureInfo.ID], check.DeepEquals, model.TaskWorkload{1: {Workload: 1}, 2: {Workload: 1}})

	c.Assert(p.Close(), check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.tables[1].(*mockTablePipeline).canceled, check.IsTrue)
	c.Assert(p.tables[2].(*mockTablePipeline).canceled, check.IsTrue)

	p, tester = initProcessor4Test(ctx, c)
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// add tables
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Tables[1] = &model.TableReplicaInfo{StartTs: 20}
		status.Tables[2] = &model.TableReplicaInfo{StartTs: 30}
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// send error
	p.sendError(cerror.ErrSinkURIInvalid)
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(cerror.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	tester.MustApplyPatches()

	c.Assert(p.Close(), check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID].Error, check.DeepEquals, &model.RunningError{
		Addr:    "127.0.0.1:0000",
		Code:    "CDC:ErrSinkURIInvalid",
		Message: "[CDC:ErrSinkURIInvalid]sink uri invalid",
	})
	c.Assert(p.tables[1].(*mockTablePipeline).canceled, check.IsTrue)
	c.Assert(p.tables[2].(*mockTablePipeline).canceled, check.IsTrue)
}

func (s *processorSuite) TestPositionDeleted(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Tables[1] = &model.TableReplicaInfo{StartTs: 30}
		status.Tables[2] = &model.TableReplicaInfo{StartTs: 40}
		return status, true, nil
	})
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// cal position
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 30,
		ResolvedTs:   30,
	})

	// some other delete the task position
	p.changefeed.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		return nil, true, nil
	})
	tester.MustApplyPatches()
	// position created again
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 0,
		ResolvedTs:   0,
	})

	// cal position
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(p.changefeed.TaskPositions[p.captureInfo.ID], check.DeepEquals, &model.TaskPosition{
		CheckPointTs: 30,
		ResolvedTs:   30,
	})
}

func (s *processorSuite) TestSchemaGC(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, c)
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Tables[1] = &model.TableReplicaInfo{StartTs: 30}
		status.Tables[2] = &model.TableReplicaInfo{StartTs: 40}
		return status, true, nil
	})

	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	updateChangeFeedPosition(c, tester, "changefeed-id-test", 50, 50)
	_, err = p.Tick(ctx, p.changefeed)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()

	// GC Ts should be (checkpoint - 1).
	c.Assert(p.schemaStorage.(*mockSchemaStorage).lastGcTs, check.Equals, uint64(49))
	c.Assert(p.lastSchemaTs, check.Equals, uint64(49))
}

func cleanUpFinishedOpOperation(state *model.ChangefeedReactorState, captureID model.CaptureID, tester *orchestrator.ReactorStateTester) {
	state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		if status == nil || status.Operation == nil {
			return status, false, nil
		}
		for tableID, opt := range status.Operation {
			if opt.Done && opt.Status == model.OperFinished {
				delete(status.Operation, tableID)
			}
		}
		return status, true, nil
	})
	tester.MustApplyPatches()
}

func updateChangeFeedPosition(c *check.C, tester *orchestrator.ReactorStateTester, cfID model.ChangeFeedID, resolvedTs, checkpointTs model.Ts) {
	key := etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangeFeedStatus,
		ChangefeedID: cfID,
	}
	keyStr := key.String()

	cfStatus := &model.ChangeFeedStatus{
		ResolvedTs:   resolvedTs,
		CheckpointTs: checkpointTs,
	}
	valueBytes, err := json.Marshal(cfStatus)
	c.Assert(err, check.IsNil)

	tester.MustUpdate(keyStr, valueBytes)
}
