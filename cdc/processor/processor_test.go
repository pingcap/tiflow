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
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
)

// processor needs to implement TableExecutor.
var _ scheduler.TableExecutor = (*processor)(nil)

func newProcessor4Test(
	ctx cdcContext.Context,
	t *testing.T,
	createTablePipeline func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error),
) *processor {
	upStream := upstream.NewUpstream4Test(nil)
	p := newProcessor(ctx, upStream)
	p.lazyInit = func(ctx cdcContext.Context) error {
		p.agent = &mockAgent{executor: p}
		return nil
	}
	p.redoManager = redo.NewDisabledManager()
	p.createTablePipeline = createTablePipeline
	p.schemaStorage = &mockSchemaStorage{t: t, resolvedTs: math.MaxUint64}
	return p
}

func initProcessor4Test(ctx cdcContext.Context, t *testing.T) (*processor, *orchestrator.ReactorStateTester) {
	changefeedInfo := `
{
    "sink-uri": "blackhole://",
    "opts": {},
    "create-time": "2020-02-02T00:00:00.000000+00:00",
    "start-ts": 0,
    "target-ts": 0,
    "admin-job-type": 0,
    "sort-engine": "memory",
    "sort-dir": ".",
    "config": {
        "case-sensitive": true,
        "enable-old-value": false,
        "force-replicate": false,
        "check-gc-safe-point": true,
        "filter": {
            "rules": [
                "*.*"
            ],
            "ignore-txn-start-ts": null,
            "ddl-allow-list": null
        },
        "mounter": {
            "worker-num": 16
        },
        "sink": {
            "dispatchers": null,
            "protocol": "open-protocol"
        },
        "cyclic-replication": {
            "enable": false,
            "replica-id": 0,
            "filter-replica-ids": null,
            "id-buckets": 0,
            "sync-ddl": false
        }
    },
    "state": "normal",
    "history": null,
    "error": null,
    "sync-point-enabled": false,
    "sync-point-interval": 600000000000
}
`
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
	captureID := ctx.GlobalVars().CaptureInfo.ID
	changefeedID := ctx.ChangefeedVars().ID
	return p, orchestrator.NewReactorStateTester(t, p.changefeed, map[string]string{
		"/tidb/cdc/capture/" +
			captureID: `{"id":"` + captureID + `","address":"127.0.0.1:8300"}`,
		"/tidb/cdc/changefeed/info/" +
			changefeedID.ID: changefeedInfo,
		"/tidb/cdc/job/" +
			ctx.ChangefeedVars().ID.ID: `{"resolved-ts":0,"checkpoint-ts":0,"admin-job-type":0}`,
		"/tidb/cdc/task/status/" +
			captureID + "/" + changefeedID.ID: `{"tables":{},"operation":null,"admin-job-type":0}`,
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
	scheduler.Agent

	executor         scheduler.TableExecutor
	lastCheckpointTs model.Ts
	isClosed         bool
}

func (a *mockAgent) Tick(_ context.Context) error {
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

func TestTableExecutor(t *testing.T) {
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
	tester.MustApplyPatches()

	// no operation
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	ok, err := p.AddTable(ctx, 1, 20)
	require.Nil(t, err)
	require.True(t, ok)
	ok, err = p.AddTable(ctx, 2, 20)
	require.Nil(t, err)
	require.True(t, ok)
	ok, err = p.AddTable(ctx, 3, 20)
	require.Nil(t, err)
	require.True(t, ok)
	ok, err = p.AddTable(ctx, 4, 20)
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
			Message: "[CDC:ErrSinkURIInvalid]sink uri invalid '%s'",
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
	err = p.addTable(ctx, model.TableID(1), &model.TableReplicaInfo{StartTs: 20})
	require.Nil(t, err)
	err = p.addTable(ctx, model.TableID(2), &model.TableReplicaInfo{StartTs: 30})
	require.Nil(t, err)

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
	require.EqualValues(t, p.checkpointTs, 90)
	require.EqualValues(t, p.resolvedTs, 90)
	require.Contains(t, p.changefeed.TaskPositions, p.captureInfo.ID)

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
	err = p.addTable(ctx, model.TableID(1), &model.TableReplicaInfo{StartTs: 20})
	require.Nil(t, err)
	err = p.addTable(ctx, model.TableID(2), &model.TableReplicaInfo{StartTs: 30})
	require.Nil(t, err)
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
		Message: "[CDC:ErrSinkURIInvalid]sink uri invalid '%s'",
	})
	require.True(t, p.tables[1].(*mockTablePipeline).canceled)
	require.True(t, p.tables[2].(*mockTablePipeline).canceled)
}

func TestPositionDeleted(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, t)
	var err error
	// add table
	err = p.addTable(ctx, model.TableID(1), &model.TableReplicaInfo{StartTs: 30})
	require.Nil(t, err)
	err = p.addTable(ctx, model.TableID(2), &model.TableReplicaInfo{StartTs: 40})
	require.Nil(t, err)
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// cal position
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.EqualValues(t, 30, p.checkpointTs)
	require.EqualValues(t, 30, p.resolvedTs)
	require.Contains(t, p.changefeed.TaskPositions, p.captureInfo.ID)

	// some other delete the task position
	p.changefeed.PatchTaskPosition(p.captureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		return nil, true, nil
	})
	tester.MustApplyPatches()
	// position created again
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, &model.TaskPosition{}, p.changefeed.TaskPositions[p.captureInfo.ID])

	// cal position
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.EqualValues(t, 30, p.checkpointTs)
	require.EqualValues(t, 30, p.resolvedTs)
	require.Contains(t, p.changefeed.TaskPositions, p.captureInfo.ID)
}

func TestSchemaGC(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	p, tester := initProcessor4Test(ctx, t)

	var err error
	// init tick
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	updateChangeFeedPosition(t, tester,
		model.DefaultChangeFeedID("changefeed-id-test"),
		50, 50)
	_, err = p.Tick(ctx, p.changefeed)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// GC Ts should be (checkpoint - 1).
	require.Equal(t, p.schemaStorage.(*mockSchemaStorage).lastGcTs, uint64(49))
	require.Equal(t, p.lastSchemaTs, uint64(49))
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
	p.schemaStorage.(*mockSchemaStorage).resolvedTs = 10

	err := p.addTable(ctx, model.TableID(1), &model.TableReplicaInfo{StartTs: 5})
	require.Nil(t, err)
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
