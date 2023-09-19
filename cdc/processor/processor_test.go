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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sinkmanager"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	redoPkg "github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
)

func newProcessor4Test(
	t *testing.T,
	state *orchestrator.ChangefeedReactorState,
	captureInfo *model.CaptureInfo,
	liveness *model.Liveness,
	cfg *config.SchedulerConfig,
	enableRedo bool,
) *processor {
	changefeedID := model.ChangeFeedID4Test("processor-test", "processor-test")
	up := upstream.NewUpstream4Test(&sinkmanager.MockPD{})
	p := newProcessor(
		state,
		captureInfo,
		changefeedID, up, liveness, 0, cfg)
	// Some cases want to send errors to the processor without initializing it.
	p.sinkManager.errors = make(chan error, 16)
	p.lazyInit = func(ctx cdcContext.Context) error {
		if p.initialized {
			return nil
		}

		stdCtx := contextutil.PutChangefeedIDInCtx(ctx, changefeedID)
		if !enableRedo {
			p.redo.r = redo.NewDisabledDMLManager()
		} else {
			tmpDir := t.TempDir()
			redoDir := fmt.Sprintf("%s/%s", tmpDir, changefeedID)
			dmlMgr, err := redo.NewDMLManager(ctx, &config.ConsistentConfig{
				Level:             string(redoPkg.ConsistentLevelEventual),
				MaxLogSize:        redoPkg.DefaultMaxLogSize,
				FlushIntervalInMs: redoPkg.DefaultFlushIntervalInMs,
				Storage:           "file://" + redoDir,
				UseFileBackend:    false,
			})
			require.NoError(t, err)
			p.redo.r = dmlMgr
		}
		p.redo.name = "RedoManager"
		p.redo.spawn(ctx)

		p.agent = &mockAgent{executor: p, liveness: liveness}
		p.sinkManager.r, p.sourceManager.r, _ = sinkmanager.NewManagerWithMemEngine(
			t, changefeedID, state.Info, p.redo.r)
		p.sinkManager.name = "SinkManager"
		p.sinkManager.spawn(stdCtx)
		p.sourceManager.name = "SourceManager"
		p.sourceManager.spawn(stdCtx)

		// NOTICE: we have to bind the sourceManager to the sinkManager
		// otherwise the sinkManager will not receive the resolvedTs.
		p.sourceManager.r.OnResolve(p.sinkManager.r.UpdateReceivedSorterResolvedTs)

		p.initialized = true
		return nil
	}

	p.ddlHandler.r = &ddlHandler{
		schemaStorage: &mockSchemaStorage{t: t, resolvedTs: math.MaxUint64},
	}
	return p
}

func initProcessor4Test(
	ctx cdcContext.Context, t *testing.T, liveness *model.Liveness, enableRedo bool,
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
	cfg := config.NewDefaultSchedulerConfig()
	p := newProcessor4Test(t, changefeed, captureInfo, liveness, cfg, enableRedo)

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

func (s *mockSchemaStorage) GetLastSnapshot() *schema.Snapshot {
	return schema.NewEmptySnapshot(false)
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
	p, tester := initProcessor4Test(ctx, t, &liveness, false)

	// init tick
	err := p.Tick(ctx)
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
	span := spanz.TableIDToComparableSpan(1)
	ok, err := p.AddTableSpan(ctx, span, tablepb.Checkpoint{CheckpointTs: 20}, true)
	require.NoError(t, err)
	require.True(t, ok)
	p.sinkManager.r.UpdateBarrierTs(20, nil)
	stats := p.sinkManager.r.GetTableStats(span)
	require.Equal(t, model.Ts(20), stats.CheckpointTs)
	require.Equal(t, model.Ts(20), stats.ResolvedTs)
	require.Equal(t, model.Ts(20), stats.BarrierTs)
	require.Len(t, p.sinkManager.r.GetAllCurrentTableSpans(), 1)
	require.Equal(t, 1, p.sinkManager.r.GetAllCurrentTableSpansCount())

	done := p.IsAddTableSpanFinished(spanz.TableIDToComparableSpan(1), true)
	require.False(t, done)
	state, ok := p.sinkManager.r.GetTableState(span)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStatePreparing, state)

	// Push the resolved ts, mock that sorterNode receive first resolved event.
	p.sourceManager.r.Add(
		span,
		[]*model.PolymorphicEvent{{
			CRTs: 101,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
				CRTs:   101,
			},
		}}...,
	)

	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	done = p.IsAddTableSpanFinished(span, true)
	require.True(t, done)
	state, ok = p.sinkManager.r.GetTableState(span)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStatePrepared, state)

	ok, err = p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(1), tablepb.Checkpoint{CheckpointTs: 30}, true)
	require.NoError(t, err)
	require.True(t, ok)
	stats = p.sinkManager.r.GetTableStats(span)
	require.Equal(t, model.Ts(20), stats.CheckpointTs)
	require.Equal(t, model.Ts(101), stats.ResolvedTs)
	require.Equal(t, model.Ts(20), stats.BarrierTs)

	// Start to replicate table-1.
	ok, err = p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(1), tablepb.Checkpoint{CheckpointTs: 30}, false)
	require.NoError(t, err)
	require.True(t, ok)

	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// table-1: `prepared` -> `replicating`
	state, ok = p.sinkManager.r.GetTableState(span)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStateReplicating, state)

	err = p.Close()
	require.Nil(t, err)
	require.Nil(t, p.agent)
}

func TestTableExecutorAddingTableIndirectlyWithRedoEnabled(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness, true)

	// init tick
	err := p.Tick(ctx)
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
	span := spanz.TableIDToComparableSpan(1)
	ok, err := p.AddTableSpan(ctx, span, tablepb.Checkpoint{CheckpointTs: 20}, true)
	require.NoError(t, err)
	require.True(t, ok)
	p.sinkManager.r.UpdateBarrierTs(20, nil)
	stats := p.sinkManager.r.GetTableStats(span)
	require.Equal(t, model.Ts(20), stats.CheckpointTs)
	require.Equal(t, model.Ts(20), stats.ResolvedTs)
	require.Equal(t, model.Ts(20), stats.BarrierTs)
	require.Len(t, p.sinkManager.r.GetAllCurrentTableSpans(), 1)
	require.Equal(t, 1, p.sinkManager.r.GetAllCurrentTableSpansCount())

	done := p.IsAddTableSpanFinished(spanz.TableIDToComparableSpan(1), true)
	require.False(t, done)
	state, ok := p.sinkManager.r.GetTableState(span)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStatePreparing, state)

	// Push the resolved ts, mock that sorterNode receive first resolved event.
	p.sourceManager.r.Add(
		span,
		[]*model.PolymorphicEvent{{
			CRTs: 101,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
				CRTs:   101,
			},
		}}...,
	)

	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	done = p.IsAddTableSpanFinished(span, true)
	require.True(t, done)
	state, ok = p.sinkManager.r.GetTableState(span)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStatePrepared, state)

	// ignore duplicate add request
	ok, err = p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(1), tablepb.Checkpoint{CheckpointTs: 30}, true)
	require.NoError(t, err)
	require.True(t, ok)
	stats = p.sinkManager.r.GetTableStats(span)
	require.Equal(t, model.Ts(20), stats.CheckpointTs)
	require.Equal(t, model.Ts(20), stats.ResolvedTs)
	require.Equal(t, model.Ts(20), stats.BarrierTs)

	p.sinkManager.r.UpdateBarrierTs(50, nil)
	stats = p.sinkManager.r.GetTableStats(span)
	require.Equal(t, model.Ts(20), stats.ResolvedTs)
	require.Equal(t, model.Ts(50), stats.BarrierTs)

	// Start to replicate table-1.
	ok, err = p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(1), tablepb.Checkpoint{CheckpointTs: 30, ResolvedTs: 60}, false)
	require.NoError(t, err)
	require.True(t, ok)

	stats = p.sinkManager.r.GetTableStats(span)
	require.Equal(t, model.Ts(60), stats.ResolvedTs)
	require.Equal(t, model.Ts(50), stats.BarrierTs)

	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// table-1: `prepared` -> `replicating`
	state, ok = p.sinkManager.r.GetTableState(span)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStateReplicating, state)

	err = p.Close()
	require.Nil(t, err)
	require.Nil(t, p.agent)
}

func TestProcessorError(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness, false)
	// init tick
	err := p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// send a abnormal error
	p.sinkManager.errors <- cerror.ErrSinkURIInvalid
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

	p, tester = initProcessor4Test(ctx, t, &liveness, false)
	// init tick
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// send a normal error
	p.sinkManager.errors <- context.Canceled
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
	p, tester := initProcessor4Test(ctx, t, &liveness, false)
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
	p, tester := initProcessor4Test(ctx, t, &liveness, false)
	// init tick
	err := p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// Do a no operation tick to lazy init the processor.
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// add tables
	done, err := p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(1), tablepb.Checkpoint{CheckpointTs: 20}, false)
	require.Nil(t, err)
	require.True(t, done)
	done, err = p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(2), tablepb.Checkpoint{CheckpointTs: 30}, false)
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

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
	require.Nil(t, p.sinkManager.r)
	require.Nil(t, p.sourceManager.r)
	require.Nil(t, p.agent)

	p, tester = initProcessor4Test(ctx, t, &liveness, false)
	// init tick
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// Do a no operation tick to lazy init the processor.
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// add tables
	done, err = p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(1), tablepb.Checkpoint{CheckpointTs: 20}, false)
	require.Nil(t, err)
	require.True(t, done)
	done, err = p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(2), tablepb.Checkpoint{CheckpointTs: 30}, false)
	require.Nil(t, err)
	require.True(t, done)
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// send error
	p.sinkManager.errors <- cerror.ErrSinkURIInvalid
	err = p.Tick(ctx)
	require.Error(t, err)
	tester.MustApplyPatches()

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
	require.Equal(t, p.changefeed.TaskPositions[p.captureInfo.ID].Error, &model.RunningError{
		Time:    p.changefeed.TaskPositions[p.captureInfo.ID].Error.Time,
		Addr:    "127.0.0.1:0000",
		Code:    "CDC:ErrSinkURIInvalid",
		Message: "[CDC:ErrSinkURIInvalid]sink uri invalid '%s'",
	})
	require.Nil(t, p.sinkManager.r)
	require.Nil(t, p.sourceManager.r)
	require.Nil(t, p.agent)
}

func TestPositionDeleted(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness, false)
	// init tick
	err := p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Contains(t, p.changefeed.TaskPositions, p.captureInfo.ID)

	// Do a no operation tick to lazy init the processor.
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// add table
	done, err := p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(1), tablepb.Checkpoint{CheckpointTs: 30}, false)
	require.Nil(t, err)
	require.True(t, done)
	done, err = p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(2), tablepb.Checkpoint{CheckpointTs: 40}, false)
	require.Nil(t, err)
	require.True(t, done)

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

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
}

func TestSchemaGC(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness, false)

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
	require.Equal(t, p.ddlHandler.r.schemaStorage.(*mockSchemaStorage).lastGcTs, uint64(49))
	require.Equal(t, p.lastSchemaTs, uint64(49))

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
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
	p, tester := initProcessor4Test(ctx, t, &liveness, false)
	p.changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 5
		return status, true, nil
	})
	p.ddlHandler.r.schemaStorage.(*mockSchemaStorage).resolvedTs = 10

	// init tick
	err := p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Contains(t, p.changefeed.TaskPositions, p.captureInfo.ID)

	// Do a no operation tick to lazy init the processor.
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	span := spanz.TableIDToComparableSpan(1)
	done, err := p.AddTableSpan(ctx, span, tablepb.Checkpoint{CheckpointTs: 5}, false)
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
	status := p.sinkManager.r.GetTableStats(span)
	require.Equal(t, uint64(10), status.BarrierTs)

	// Schema storage has advanced too.
	p.ddlHandler.r.schemaStorage.(*mockSchemaStorage).resolvedTs = 15
	err = p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()
	p.updateBarrierTs(&schedulepb.Barrier{GlobalBarrierTs: 20, TableBarriers: nil})
	status = p.sinkManager.r.GetTableStats(span)
	require.Equal(t, uint64(15), status.BarrierTs)

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
}

func TestProcessorLiveness(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness, false)

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

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
}

func TestProcessorDostNotStuckInInit(t *testing.T) {
	_ = failpoint.
		Enable("github.com/pingcap/tiflow/cdc/processor/sinkmanager/SinkManagerRunError",
			"1*return(true)")
	defer func() {
		_ = failpoint.
			Disable("github.com/pingcap/tiflow/cdc/processor/sinkmanager/SinkManagerRunError")
	}()

	ctx := cdcContext.NewBackendContext4Test(true)
	liveness := model.LivenessCaptureAlive
	p, tester := initProcessor4Test(ctx, t, &liveness, false)

	// First tick for creating position.
	err := p.Tick(ctx)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// Second tick for init.
	err = p.Tick(ctx)
	require.Nil(t, err)

	// TODO(qupeng): third tick for handle a warning.
	err = p.Tick(ctx)
	require.Nil(t, err)

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
}
