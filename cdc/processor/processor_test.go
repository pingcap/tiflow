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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	tablepipeline "github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/sink"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/stretchr/testify/require"
)

// processor needs to implement TableExecutor.
var _ scheduler.TableExecutor = (*processor)(nil)

func newProcessor4Test(
	ctx cdcContext.Context,
	t *testing.T,
	createTablePipeline func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error),
) *processor {
	p := newProcessor(ctx)
	// disable new scheduler to pass old test cases
	// TODO refactor the test cases so that new scheduler can be enabled
	p.newSchedulerEnabled = false
	p.lazyInit = func(ctx cdcContext.Context) error { return nil }
	p.sinkManager = &sink.Manager{}
	p.redoManager = redo.NewDisabledManager()
	p.createTablePipeline = createTablePipeline
	p.schemaStorage = &mockSchemaStorage{t: t, resolvedTs: math.MaxUint64}
	return p
}

func initProcessor4Test(ctx cdcContext.Context, t *testing.T) (*processor, *orchestrator.ReactorStateTester) {
	p := newProcessor4Test(ctx, t, func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error) {
		return &mockTablePipeline{
			tableID:      tableID,
			name:         fmt.Sprintf("`test`.`table%d`", tableID),
			status:       tablepipeline.TableStatusRunning,
			resolvedTs:   replicaInfo.StartTs,
			checkpointTs: replicaInfo.StartTs,
		}, nil
	})
	p.changefeed = orchestrator.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	return p, orchestrator.NewReactorStateTester(t, p.changefeed, map[string]string{
		"/tidb/cdc/capture/" + ctx.GlobalVars().CaptureInfo.ID:                                     `{"id":"` + ctx.GlobalVars().CaptureInfo.ID + `","address":"127.0.0.1:8300"}`,
		"/tidb/cdc/changefeed/info/" + ctx.ChangefeedVars().ID:                                     `{"sink-uri":"blackhole://","opts":{},"create-time":"2020-02-02T00:00:00.000000+00:00","start-ts":0,"target-ts":0,"admin-job-type":0,"sort-engine":"memory","sort-dir":".","config":{"case-sensitive":true,"enable-old-value":false,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"open-protocol"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}},"state":"normal","history":null,"error":null,"sync-point-enabled":false,"sync-point-interval":600000000000}`,
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

	t          *testing.T
	lastGcTs   uint64
	resolvedTs uint64
}

func (s *mockSchemaStorage) ResolvedTs() uint64 {
	return s.resolvedTs
}

func (s *mockSchemaStorage) DoGC(ts uint64) uint64 {
	require.LessOrEqual(s.t, s.lastGcTs, ts)
	atomic.StoreUint64(&s.lastGcTs, ts)
	return ts
}

type mockAgent struct {
	// dummy to satisfy the interface
	processorAgent

	executor         scheduler.TableExecutor
	lastCheckpointTs model.Ts
	isClosed         bool
}

func (a *mockAgent) Tick(_ cdcContext.Context) error {
	if len(a.executor.GetAllCurrentTables()) == 0 {
		return nil
	}
	a.lastCheckpointTs, _ = a.executor.GetCheckpoint()
	return nil
}

func (a *mockAgent) GetLastSentCheckpointTs() (checkpointTs model.Ts) {
	return a.lastCheckpointTs
}

func (a *mockAgent) Close() error {
	a.isClosed = true
	return nil
}

func TestCheckTablesNum(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, t)
	var err error
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID],
		&model.TaskPosition{
			CheckPointTs: 0,
			ResolvedTs:   0,
			Count:        0,
			Error:        nil,
		})

	p, tester = initProcessor4Test(ctx, t)
	p.changefeed.Info.StartTs = 66
	p.changefeed.Status.CheckpointTs = 88
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID],
		&model.TaskPosition{
			CheckPointTs: 88,
			ResolvedTs:   88,
			Count:        0,
			Error:        nil,
		})
}

func TestHandleTableOperation4SingleTable(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, t)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
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
	require.Nil(t, err)
	tester.MustApplyPatches()

	// add table, in processing
	// in current implementation of owner, the startTs and BoundaryTs of add table operation should be always equaled.
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.AddTable(66, &model.TableReplicaInfo{StartTs: 60}, 60)
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: false, BoundaryTs: 60, Status: model.OperProcessed},
		},
	})

	// add table, not finished
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: false, BoundaryTs: 60, Status: model.OperProcessed},
		},
	})

	// add table, push the resolvedTs
	table66 := p.tables[66].(*mockTablePipeline)
	table66.resolvedTs = 101
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: false, BoundaryTs: 60, Status: model.OperProcessed},
		},
	})
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs, uint64(101))

	// finish the operation
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			66: {StartTs: 60},
		},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: false, BoundaryTs: 60, Status: model.OperFinished},
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
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: true, BoundaryTs: 120, Status: model.OperProcessed},
		},
	})
	require.Equal(t, table66.stopTs, uint64(120))

	// remove table, not finished
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: true, BoundaryTs: 120, Status: model.OperProcessed},
		},
	})

	// remove table, finished
	table66.status = tablepipeline.TableStatusStopped
	table66.checkpointTs = 121
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			66: {Delete: true, BoundaryTs: 121, Status: model.OperFinished},
		},
	})
	require.True(t, table66.canceled)
	require.Nil(t, p.tables[66])
}

func TestHandleTableOperation4MultiTable(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, t)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
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
	require.Nil(t, err)
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
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			3: {StartTs: 40},
			4: {StartTs: 30},
		},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: false, BoundaryTs: 60, Status: model.OperProcessed},
			2: {Delete: false, BoundaryTs: 50, Status: model.OperProcessed},
			3: {Delete: false, BoundaryTs: 40, Status: model.OperProcessed},
		},
	})
	require.Len(t, p.tables, 4)
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID].CheckPointTs, uint64(30))
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs, uint64(30))

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
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			4: {StartTs: 30},
		},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: false, BoundaryTs: 60, Status: model.OperFinished},
			2: {Delete: false, BoundaryTs: 50, Status: model.OperFinished},
			3: {Delete: true, BoundaryTs: 60, Status: model.OperProcessed},
		},
	})
	require.Len(t, p.tables, 4)
	require.False(t, table3.canceled)
	require.Equal(t, table3.stopTs, uint64(60))
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID].ResolvedTs, uint64(101))

	// finish remove operations
	table3.status = tablepipeline.TableStatusStopped
	table3.checkpointTs = 65
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{
			1: {StartTs: 60},
			2: {StartTs: 50},
			4: {StartTs: 30},
		},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: false, BoundaryTs: 60, Status: model.OperFinished},
			2: {Delete: false, BoundaryTs: 50, Status: model.OperFinished},
			3: {Delete: true, BoundaryTs: 65, Status: model.OperFinished},
		},
	})
	require.Len(t, p.tables, 3)
	require.True(t, table3.canceled)

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
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: true, BoundaryTs: 120, Status: model.OperProcessed},
			4: {Delete: true, BoundaryTs: 120, Status: model.OperProcessed},
		},
	})
	require.Equal(t, table1.stopTs, uint64(120))
	require.Equal(t, table4.stopTs, uint64(120))
	require.True(t, table2.canceled)
	require.Len(t, p.tables, 2)

	// remove table, not finished
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: true, BoundaryTs: 120, Status: model.OperProcessed},
			4: {Delete: true, BoundaryTs: 120, Status: model.OperProcessed},
		},
	})

	// remove table, finished
	table1.status = tablepipeline.TableStatusStopped
	table1.checkpointTs = 121
	table4.status = tablepipeline.TableStatusStopped
	table4.checkpointTs = 122
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
		Operation: map[int64]*model.TableOperation{
			1: {Delete: true, BoundaryTs: 121, Status: model.OperFinished},
			4: {Delete: true, BoundaryTs: 122, Status: model.OperFinished},
		},
	})
	require.True(t, table1.canceled)
	require.True(t, table4.canceled)
	require.Len(t, p.tables, 0)
}

func TestTableExecutor(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, t)
	p.newSchedulerEnabled = true
	p.lazyInit = func(ctx cdcContext.Context) error {
		p.agent = &mockAgent{executor: p}
		return nil
	}

	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
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
	require.Nil(t, err)
	tester.MustApplyPatches()

	ok, err := p.AddTable(ctx, 1)
	require.Nil(t, err)
	require.True(t, ok)
	ok, err = p.AddTable(ctx, 2)
	require.Nil(t, err)
	require.True(t, ok)
	ok, err = p.AddTable(ctx, 3)
	require.Nil(t, err)
	require.True(t, ok)
	ok, err = p.AddTable(ctx, 4)
	require.Nil(t, err)
	require.True(t, ok)
	require.Len(t, p.tables, 4)

	checkpointTs := p.agent.GetLastSentCheckpointTs()
	require.Equal(t, checkpointTs, uint64(0))

	done := p.IsAddTableFinished(ctx, 1)
	require.False(t, done)
	done = p.IsAddTableFinished(ctx, 2)
	require.False(t, done)
	done = p.IsAddTableFinished(ctx, 3)
	require.False(t, done)
	done = p.IsAddTableFinished(ctx, 4)
	require.False(t, done)
	require.Len(t, p.tables, 4)

	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// add table, push the resolvedTs, finished add table
	table1 := p.tables[1].(*mockTablePipeline)
	table2 := p.tables[2].(*mockTablePipeline)
	table3 := p.tables[3].(*mockTablePipeline)
	table4 := p.tables[4].(*mockTablePipeline)
	table1.resolvedTs = 101
	table2.resolvedTs = 101
	table3.resolvedTs = 102
	table4.resolvedTs = 103

	table1.checkpointTs = 30
	table2.checkpointTs = 30
	table3.checkpointTs = 30
	table4.checkpointTs = 30

	done = p.IsAddTableFinished(ctx, 1)
	require.True(t, done)
	done = p.IsAddTableFinished(ctx, 2)
	require.True(t, done)
	done = p.IsAddTableFinished(ctx, 3)
	require.True(t, done)
	done = p.IsAddTableFinished(ctx, 4)
	require.True(t, done)

	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	table1.checkpointTs = 75
	table2.checkpointTs = 75
	table3.checkpointTs = 60
	table4.checkpointTs = 75

	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	checkpointTs = p.agent.GetLastSentCheckpointTs()
	require.Equal(t, checkpointTs, uint64(60))

	updateChangeFeedPosition(t, tester, ctx.ChangefeedVars().ID, 103, 60)

	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	ok, err = p.RemoveTable(ctx, 3)
	require.Nil(t, err)
	require.True(t, ok)

	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)

	tester.MustApplyPatches()

	require.Len(t, p.tables, 4)
	require.False(t, table3.canceled)
	require.Equal(t, table3.stopTs, uint64(60))

	done = p.IsRemoveTableFinished(ctx, 3)
	require.False(t, done)

	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	checkpointTs = p.agent.GetLastSentCheckpointTs()
	require.Equal(t, checkpointTs, uint64(60))

	// finish remove operations
	table3.status = tablepipeline.TableStatusStopped
	table3.checkpointTs = 65

	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)

	tester.MustApplyPatches()

	require.Len(t, p.tables, 4)
	require.False(t, table3.canceled)

	done = p.IsRemoveTableFinished(ctx, 3)
	require.True(t, done)

	require.Len(t, p.tables, 3)
	require.True(t, table3.canceled)

	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	checkpointTs = p.agent.GetLastSentCheckpointTs()
	require.Equal(t, checkpointTs, uint64(75))

	err = p.Close()
	require.Nil(t, err)
	require.Nil(t, p.agent)
}

func TestInitTable(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, t)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Tables[1] = &model.TableReplicaInfo{StartTs: 20}
		status.Tables[2] = &model.TableReplicaInfo{StartTs: 30}
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.NotNil(t, p.tables[1])
	require.NotNil(t, p.tables[2])
}

func TestProcessorError(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, t)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// send a abnormal error
	p.sendError(cerror.ErrSinkURIInvalid)
	_, err = p.Tick(ctx, p.changefeed)
	tester.MustApplyPatches()
	require.True(t, cerror.ErrReactorFinished.Equal(errors.Cause(err)))
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID], &model.TaskPosition{
		Error: &model.RunningError{
			Addr:    "127.0.0.1:0000",
			Code:    "CDC:ErrSinkURIInvalid",
			Message: "[CDC:ErrSinkURIInvalid]sink uri invalid",
		},
	})

	p, tester = initProcessor4Test(ctx, t)
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// send a normal error
	p.sendError(context.Canceled)
	_, err = p.Tick(ctx, p.changefeed)
	tester.MustApplyPatches()
	require.True(t, cerror.ErrReactorFinished.Equal(errors.Cause(err)))
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID], &model.TaskPosition{
		Error: nil,
	})
}

func TestProcessorExit(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, t)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
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
	require.True(t, cerror.ErrReactorFinished.Equal(errors.Cause(err)))
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID], &model.TaskPosition{
		Error: nil,
	})
}

func TestProcessorClose(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, t)
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// add tables
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Tables[1] = &model.TableReplicaInfo{StartTs: 20}
		status.Tables[2] = &model.TableReplicaInfo{StartTs: 30}
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
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
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID], &model.TaskPosition{
		CheckPointTs: 90,
		ResolvedTs:   90,
		Error:        nil,
	})
	require.Equal(t, p.changefeed.TaskStatuses[p.captureInfo.ID], &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{1: {StartTs: 20}, 2: {StartTs: 30}},
	})
	require.Equal(t, p.changefeed.Workloads[p.captureInfo.ID], model.TaskWorkload{1: {Workload: 1}, 2: {Workload: 1}})

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
	require.True(t, p.tables[1].(*mockTablePipeline).canceled)
	require.True(t, p.tables[2].(*mockTablePipeline).canceled)

	p, tester = initProcessor4Test(ctx, t)
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// add tables
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Tables[1] = &model.TableReplicaInfo{StartTs: 20}
		status.Tables[2] = &model.TableReplicaInfo{StartTs: 30}
		return status, true, nil
	})
	tester.MustApplyPatches()
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// send error
	p.sendError(cerror.ErrSinkURIInvalid)
	_, err = p.Tick(ctx, p.changefeed)
	require.True(t, cerror.ErrReactorFinished.Equal(errors.Cause(err)))
	tester.MustApplyPatches()

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID].Error, &model.RunningError{
		Addr:    "127.0.0.1:0000",
		Code:    "CDC:ErrSinkURIInvalid",
		Message: "[CDC:ErrSinkURIInvalid]sink uri invalid",
	})
	require.True(t, p.tables[1].(*mockTablePipeline).canceled)
	require.True(t, p.tables[2].(*mockTablePipeline).canceled)
}

func TestPositionDeleted(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, t)
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Tables[1] = &model.TableReplicaInfo{StartTs: 30}
		status.Tables[2] = &model.TableReplicaInfo{StartTs: 40}
		return status, true, nil
	})
	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// cal position
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID], &model.TaskPosition{
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
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID], &model.TaskPosition{
		CheckPointTs: 0,
		ResolvedTs:   0,
	})

	// cal position
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID], &model.TaskPosition{
		CheckPointTs: 30,
		ResolvedTs:   30,
	})
}

func TestSchemaGC(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, t)
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Tables[1] = &model.TableReplicaInfo{StartTs: 30}
		status.Tables[2] = &model.TableReplicaInfo{StartTs: 40}
		return status, true, nil
	})

	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	updateChangeFeedPosition(t, tester, "changefeed-id-test", 50, 50)
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// GC Ts should be (checkpoint - 1).
	require.Equal(t, p.schemaStorage.(*mockSchemaStorage).lastGcTs, uint64(49))
	require.Equal(t, p.lastSchemaTs, uint64(49))
}

func cleanUpFinishedOpOperation(state *orchestrator.ChangefeedReactorState, captureID model.CaptureID, tester *orchestrator.ReactorStateTester) {
	state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		if status == nil || status.Operation == nil {
			return status, false, nil
		}
		for tableID, opt := range status.Operation {
			if opt.Status == model.OperFinished {
				delete(status.Operation, tableID)
			}
		}
		return status, true, nil
	})
	tester.MustApplyPatches()
}

func updateChangeFeedPosition(t *testing.T, tester *orchestrator.ReactorStateTester, cfID model.ChangeFeedID, resolvedTs, checkpointTs model.Ts) {
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
	require.Nil(t, err)

	tester.MustUpdate(keyStr, valueBytes)
}

func TestIgnorableError(t *testing.T) {
	testCases := []struct {
		err       error
		ignorable bool
	}{
		{nil, true},
		{cerror.ErrAdminStopProcessor.GenWithStackByArgs(), true},
		{cerror.ErrReactorFinished.GenWithStackByArgs(), true},
		{cerror.ErrRedoWriterStopped.GenWithStackByArgs(), true},
		{errors.Trace(context.Canceled), true},
		{cerror.ErrProcessorTableNotFound.GenWithStackByArgs(), false},
		{errors.New("test error"), false},
	}
	for _, tc := range testCases {
		require.Equal(t, isProcessorIgnorableError(tc.err), tc.ignorable)
	}
}

func TestUpdateBarrierTs(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, t)
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 5
		status.ResolvedTs = 10
		return status, true, nil
	})
	p.changefeed.PatchTaskStatus(p.captureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.AddTable(1, &model.TableReplicaInfo{StartTs: 5}, 5)
		return status, true, nil
	})
	p.schemaStorage.(*mockSchemaStorage).resolvedTs = 10

	// init tick, add table OperDispatched.
	_, err := p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	// tick again, add table OperProcessed.
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// Global resolved ts has advanced while schema storage stalls.
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.ResolvedTs = 20
		return status, true, nil
	})
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	tb := p.tables[model.TableID(1)].(*mockTablePipeline)
	require.Equal(t, tb.barrierTs, uint64(10))

	// Schema storage has advanced too.
	p.schemaStorage.(*mockSchemaStorage).resolvedTs = 15
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	tb = p.tables[model.TableID(1)].(*mockTablePipeline)
	require.Equal(t, tb.barrierTs, uint64(15))
}
