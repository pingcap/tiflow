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
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	mocksink "github.com/pingcap/tiflow/cdc/sink/mock"
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
	t *testing.T,
	state *orchestrator.ChangefeedReactorState,
	captureInfo *model.CaptureInfo,
	createTablePipeline func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepb.TablePipeline, error),
	liveness *model.Liveness,
) *processor {
	up := upstream.NewUpstream4Test(nil)
	p := newProcessor(
		state,
		captureInfo,
		model.ChangeFeedID4Test("processor-test", "processor-test"), up, liveness, 0)
	p.lazyInit = func(ctx cdcContext.Context) error {
		p.agent = &mockAgent{executor: p}
		p.sinkV1 = mocksink.NewNormalMockSink()
		return nil
	}
	p.redoDMLMgr = redo.NewDisabledDMLManager()
	p.createTablePipeline = createTablePipeline
	p.schemaStorage = &mockSchemaStorage{t: t, resolvedTs: math.MaxUint64}
	return p
}

func initProcessor4Test(
	ctx cdcContext.Context, t *testing.T, liveness *model.Liveness,
) (*processor, *orchestrator.ReactorStateTester) {
	changefeedInfo := `
{
    "sink-uri": "blackhole://",
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
            "ignore-txn-start-ts": null
        },
        "mounter": {
            "worker-num": 16
        },
        "sink": {
            "dispatchers": null,
            "protocol": "open-protocol",
            "advance-timeout-in-sec": 150
        }
    },
    "state": "normal",
    "history": null,
    "error": null,
    "sync-point-enabled": false,
    "sync-point-interval": 600000000000
}
`
	changefeed := orchestrator.NewChangefeedReactorState(
		etcd.DefaultCDCClusterID, ctx.ChangefeedVars().ID)
	captureInfo := &model.CaptureInfo{ID: "capture-test", AdvertiseAddr: "127.0.0.1:0000"}
	p := newProcessor4Test(t, changefeed, captureInfo, newMockTablePipeline, liveness)

	captureID := ctx.GlobalVars().CaptureInfo.ID
	changefeedID := ctx.ChangefeedVars().ID
	return p, orchestrator.NewReactorStateTester(t, p.changefeed, map[string]string{
		fmt.Sprintf("%s/capture/%s",
			etcd.DefaultClusterAndMetaPrefix,
			captureID): `{"id":"` + captureID + `","address":"127.0.0.1:8300"}`,
		fmt.Sprintf("%s/changefeed/info/%s",
			etcd.DefaultClusterAndNamespacePrefix,
			changefeedID.ID): changefeedInfo,
		fmt.Sprintf("%s/changefeed/status/%s",
			etcd.DefaultClusterAndNamespacePrefix,
			ctx.ChangefeedVars().ID.ID): `{"resolved-ts":0,"checkpoint-ts":0,"admin-job-type":0}`,
	})
}

func newMockTablePipeline(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepb.TablePipeline, error) {
	return &mockTablePipeline{
		tableID:      tableID,
		name:         fmt.Sprintf("`test`.`table%d`", tableID),
		state:        tablepb.TableStatePreparing,
		resolvedTs:   replicaInfo.StartTs,
		checkpointTs: replicaInfo.StartTs,
	}, nil
}

type mockTablePipeline struct {
	tableID      model.TableID
	name         string
	resolvedTs   model.Ts
	checkpointTs model.Ts
	barrierTs    model.Ts
	state        tablepb.TableState
	canceled     bool

	sinkStartTs model.Ts
}

func (m *mockTablePipeline) ID() int64 {
	return m.tableID
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

func (m *mockTablePipeline) AsyncStop() bool {
	return true
}

func (m *mockTablePipeline) Stats() tablepb.Stats {
	return tablepb.Stats{}
}

func (m *mockTablePipeline) RemainEvents() int64 {
	return 1
}

func (m *mockTablePipeline) State() tablepb.TableState {
	if m.state == tablepb.TableStateStopped {
		return m.state
	}

	if m.state == tablepb.TableStatePreparing {
		// `resolvedTs` and `checkpointTs` is initialized by the same `start-ts`
		// once `resolvedTs` > `checkpointTs`, is means the sorter received the first
		// resolved event, let it become prepared.
		if m.resolvedTs > m.checkpointTs {
			m.state = tablepb.TableStatePrepared
		}
	}

	if m.sinkStartTs != model.Ts(0) {
		if m.checkpointTs > m.sinkStartTs {
			m.state = tablepb.TableStateReplicating
		}
	}
	return m.state
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

func (m *mockTablePipeline) Start(ts model.Ts) {
	m.sinkStartTs = ts
}

// MemoryConsumption return the memory consumption in bytes
func (m *mockTablePipeline) MemoryConsumption() uint64 {
	return 0
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

	executor scheduler.TableExecutor
	liveness *model.Liveness
	isClosed bool
}

func (a *mockAgent) Tick(_ context.Context) (*schedulepb.Barrier, error) {
	return nil, nil
}

func (a *mockAgent) Close() error {
	a.isClosed = true
	return nil
}

func TestTableExecutorAddingTableIndirectly(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness)

	var err error
	// init tick
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 20
		return status, true, nil
	})
	tester.MustApplyPatches()

	// no operation
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// table-1: `preparing` -> `prepared` -> `replicating`
	ok, err := p.AddTable(ctx, 1, tablepb.Checkpoint{CheckpointTs: 20}, true)
	require.NoError(t, err)
	require.True(t, ok)

	table1 := p.tables[1].(*mockTablePipeline)
	require.Equal(t, model.Ts(20), table1.resolvedTs)
	require.Equal(t, model.Ts(20), table1.checkpointTs)
	require.Equal(t, model.Ts(0), table1.sinkStartTs)

	require.Len(t, p.tables, 1)

	done := p.IsAddTableFinished(1, true)
	require.False(t, done)
	require.Equal(t, tablepb.TableStatePreparing, table1.State())

	// push the resolved ts, mock that sorterNode receive first resolved event
	table1.resolvedTs = 101

	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	done = p.IsAddTableFinished(1, true)
	require.True(t, done)
	require.Equal(t, tablepb.TableStatePrepared, table1.State())

	ok, err = p.AddTable(ctx, 1, tablepb.Checkpoint{CheckpointTs: 30}, true)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, model.Ts(0), table1.sinkStartTs)

	ok, err = p.AddTable(ctx, 1, tablepb.Checkpoint{CheckpointTs: 30}, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, model.Ts(30), table1.sinkStartTs)

	table1.checkpointTs = 60

	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	done = p.IsAddTableFinished(1, false)
	require.True(t, done)
	require.Equal(t, tablepb.TableStateReplicating, table1.State())

	err = p.Close(ctx)
	require.Nil(t, err)
	require.Nil(t, p.agent)
}

func TestProcessorError(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness)
	var err error
	// init tick
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// send a abnormal error
	p.sendError(cerror.ErrSinkURIInvalid)
	err = p.Tick(ctx)
	tester.MustApplyPatches()
	require.Error(t, err)
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID], &model.TaskPosition{
		Error: &model.RunningError{
			Time:    p.changefeed.TaskPositions[p.captureInfo.ID].Error.Time,
			Addr:    "127.0.0.1:0000",
			Code:    "CDC:ErrSinkURIInvalid",
			Message: "[CDC:ErrSinkURIInvalid]sink uri invalid '%s'",
		},
	})

	p, tester = initProcessor4Test(ctx, t, &liveness)
	// init tick
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// send a normal error
	p.sendError(context.Canceled)
	err = p.Tick(ctx)
	tester.MustApplyPatches()
	require.True(t, cerror.ErrReactorFinished.Equal(errors.Cause(err)))
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID], &model.TaskPosition{
		Error: nil,
	})
}

func TestProcessorExit(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness)
	var err error
	// init tick
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// stop the changefeed
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.AdminJobType = model.AdminStop
		return status, true, nil
	})
	tester.MustApplyPatches()
	err = p.Tick(ctx)
	require.True(t, cerror.ErrReactorFinished.Equal(errors.Cause(err)))
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID], &model.TaskPosition{
		Error: nil,
	})
}

func TestProcessorClose(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness)
	var err error
	// init tick
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// add tables
	done, err := p.AddTable(ctx, 1, tablepb.Checkpoint{CheckpointTs: 20}, false)
	require.Nil(t, err)
	require.True(t, done)
	done, err = p.AddTable(ctx, 2, tablepb.Checkpoint{CheckpointTs: 30}, false)
	require.Nil(t, err)
	require.True(t, done)

	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// push the resolvedTs and checkpointTs
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return status, true, nil
	})
	tester.MustApplyPatches()

	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Contains(t, p.changefeed.TaskPositions, p.captureInfo.ID)

	require.Nil(t, p.Close(ctx))
	tester.MustApplyPatches()
	require.True(t, p.tables[1].(*mockTablePipeline).canceled)
	require.True(t, p.tables[2].(*mockTablePipeline).canceled)

	p, tester = initProcessor4Test(ctx, t, &liveness)
	// init tick
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// add tables
	done, err = p.AddTable(ctx, 1, tablepb.Checkpoint{CheckpointTs: 20}, false)
	require.Nil(t, err)
	require.True(t, done)
	done, err = p.AddTable(ctx, 2, tablepb.Checkpoint{CheckpointTs: 30}, false)
	require.Nil(t, err)
	require.True(t, done)
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// send error
	p.sendError(cerror.ErrSinkURIInvalid)
	err = p.Tick(ctx)
	require.Error(t, err)
	tester.MustApplyPatches()

	require.Nil(t, p.Close(ctx))
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID].Error, &model.RunningError{
		Time:    p.changefeed.TaskPositions[p.captureInfo.ID].Error.Time,
		Addr:    "127.0.0.1:0000",
		Code:    "CDC:ErrSinkURIInvalid",
		Message: "[CDC:ErrSinkURIInvalid]sink uri invalid '%s'",
	})
	require.True(t, p.tables[1].(*mockTablePipeline).canceled)
	require.True(t, p.tables[2].(*mockTablePipeline).canceled)
}

func TestPositionDeleted(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness)
	var err error
	// add table
	done, err := p.AddTable(ctx, 1, tablepb.Checkpoint{CheckpointTs: 30}, false)
	require.Nil(t, err)
	require.True(t, done)
	done, err = p.AddTable(ctx, 2, tablepb.Checkpoint{CheckpointTs: 40}, false)
	require.Nil(t, err)
	require.True(t, done)
	// init tick
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	require.Contains(t, p.changefeed.TaskPositions, p.captureInfo.ID)

	// some others delete the task position
	p.changefeed.PatchTaskPosition(p.captureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return nil, true, nil
		})
	tester.MustApplyPatches()

	// position created again
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, &model.TaskPosition{}, p.changefeed.TaskPositions[p.captureInfo.ID])
	require.Contains(t, p.changefeed.TaskPositions, p.captureInfo.ID)
}

func TestSchemaGC(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness)

	var err error
	// init tick
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	updateChangeFeedPosition(t, tester,
		model.DefaultChangeFeedID("changefeed-id-test"),
		50)
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// GC Ts should be (checkpoint - 1).
	require.Equal(t, p.schemaStorage.(*mockSchemaStorage).lastGcTs, uint64(49))
	require.Equal(t, p.lastSchemaTs, uint64(49))
}

//nolint:unused
func updateChangeFeedPosition(t *testing.T, tester *orchestrator.ReactorStateTester, cfID model.ChangeFeedID, checkpointTs model.Ts) {
	key := etcd.CDCKey{
		ClusterID:    etcd.DefaultCDCClusterID,
		Tp:           etcd.CDCKeyTypeChangeFeedStatus,
		ChangefeedID: cfID,
	}
	keyStr := key.String()

	cfStatus := &model.ChangeFeedStatus{
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
		{cerror.ErrRedoWriterStopped.GenWithStackByArgs(), false},
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
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness)
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 5
		return status, true, nil
	})
	p.schemaStorage.(*mockSchemaStorage).resolvedTs = 10

	done, err := p.AddTable(ctx, 1, tablepb.Checkpoint{CheckpointTs: 5}, false)
	require.True(t, done)
	require.Nil(t, err)
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// Global resolved ts has advanced while schema storage stalls.
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return status, true, nil
	})
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()
	p.updateBarrierTs(&schedulepb.Barrier{GlobalBarrierTs: 20, TableBarriers: nil})
	tb := p.tables[model.TableID(1)].(*mockTablePipeline)
	require.Equal(t, uint64(10), tb.barrierTs)

	// Schema storage has advanced too.
	p.schemaStorage.(*mockSchemaStorage).resolvedTs = 15
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()
	p.updateBarrierTs(&schedulepb.Barrier{GlobalBarrierTs: 20, TableBarriers: nil})
	tb = p.tables[model.TableID(1)].(*mockTablePipeline)
	require.Equal(t, tb.barrierTs, uint64(15))
}

func TestProcessorLiveness(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness)
	p.lazyInit = func(ctx cdcContext.Context) error {
		// Mock the newAgent procedure in p.lazyInitImpl,
		// by passing p.liveness to mockAgent.
		p.agent = &mockAgent{executor: p, liveness: p.liveness}
		return nil
	}

	// First tick for creating position.
	err := p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// Second tick for init.
	err = p.Tick(ctx)
	require.Nil(t, err)

	// Changing p.liveness affects p.agent liveness.
	p.liveness.Store(model.LivenessCaptureStopping)
	require.Equal(t, model.LivenessCaptureStopping, p.agent.(*mockAgent).liveness.Load())

	// Changing p.agent liveness affects p.liveness.
	// Force set liveness to alive.
	*p.agent.(*mockAgent).liveness = model.LivenessCaptureAlive
	require.Equal(t, model.LivenessCaptureAlive, p.liveness.Load())
}
