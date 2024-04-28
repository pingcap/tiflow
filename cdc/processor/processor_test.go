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
	"os"
	"sync/atomic"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/cdc/async"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sinkmanager"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/cdc/vars"
	"github.com/pingcap/tiflow/pkg/config"
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
	info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus,
	captureInfo *model.CaptureInfo,
	liveness *model.Liveness,
	cfg *config.SchedulerConfig,
	enableRedo bool,
	client etcd.OwnerCaptureInfoClient,
	globalVars *vars.GlobalVars,
) *processor {
	changefeedID := model.ChangeFeedID4Test("processor-test", "processor-test")
	up := upstream.NewUpstream4Test(&sinkmanager.MockPD{})
	p := NewProcessor(
		info,
		status,
		captureInfo,
		changefeedID, up, liveness, 0, cfg, client, globalVars)
	// Some cases want to send errors to the processor without initializing it.
	p.sinkManager.errors = make(chan error, 16)
	p.lazyInit = func(ctx context.Context) error {
		if p.initialized.Load() {
			return nil
		}

		if !enableRedo {
			p.redo.r = redo.NewDisabledDMLManager()
		} else {
			tmpDir := t.TempDir()
			redoDir := fmt.Sprintf("%s/%s", tmpDir, changefeedID)
			dmlMgr := redo.NewDMLManager(changefeedID, &config.ConsistentConfig{
				Level:                 string(redoPkg.ConsistentLevelEventual),
				MaxLogSize:            redoPkg.DefaultMaxLogSize,
				FlushIntervalInMs:     redoPkg.DefaultFlushIntervalInMs,
				MetaFlushIntervalInMs: redoPkg.DefaultMetaFlushIntervalInMs,
				EncodingWorkerNum:     redoPkg.DefaultEncodingWorkerNum,
				FlushWorkerNum:        redoPkg.DefaultFlushWorkerNum,
				Storage:               "file://" + redoDir,
				UseFileBackend:        false,
			})
			p.redo.r = dmlMgr
		}
		p.redo.name = "RedoManager"
		p.redo.changefeedID = changefeedID
		p.redo.spawn(ctx)

		p.agent = &mockAgent{executor: p, liveness: liveness}
		p.sinkManager.r, p.sourceManager.r, _ = sinkmanager.NewManagerWithMemEngine(
			t, changefeedID, info, p.redo.r)
		p.sinkManager.name = "SinkManager"
		p.sinkManager.changefeedID = changefeedID
		p.sinkManager.spawn(ctx)
		p.sourceManager.name = "SourceManager"
		p.sourceManager.changefeedID = changefeedID
		p.sourceManager.spawn(ctx)

		// NOTICE: we have to bind the sourceManager to the sinkManager
		// otherwise the sinkManager will not receive the resolvedTs.
		p.sourceManager.r.OnResolve(p.sinkManager.r.UpdateReceivedSorterResolvedTs)

		p.initialized.Store(true)
		return nil
	}
	p.initializer = async.NewInitializer()

	p.ddlHandler.r = &ddlHandler{
		schemaStorage: &mockSchemaStorage{t: t, resolvedTs: math.MaxUint64},
	}
	return p
}

// nolint
func initProcessor4Test(t *testing.T, liveness *model.Liveness, enableRedo bool,
	globalVars *vars.GlobalVars, changefeedVars *model.ChangeFeedInfo,
) (*processor, *orchestrator.ReactorStateTester, *orchestrator.ChangefeedReactorState) {
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
		etcd.DefaultCDCClusterID, model.DefaultChangeFeedID(changefeedVars.ID))
	captureInfo := &model.CaptureInfo{ID: "capture-test", AdvertiseAddr: "127.0.0.1:0000"}
	cfg := config.NewDefaultSchedulerConfig()

	captureID := globalVars.CaptureInfo.ID
	changefeedID := changefeedVars.ID
	tester := orchestrator.NewReactorStateTester(t, changefeed, map[string]string{
		fmt.Sprintf("%s/capture/%s",
			etcd.DefaultClusterAndMetaPrefix,
			captureID): `{"id":"` + captureID + `","address":"127.0.0.1:8300"}`,
		fmt.Sprintf("%s/changefeed/info/%s",
			etcd.DefaultClusterAndNamespacePrefix,
			changefeedID): changefeedInfo,
		fmt.Sprintf("%s/changefeed/status/%s",
			etcd.DefaultClusterAndNamespacePrefix,
			changefeedVars.ID): `{"resolved-ts":0,"checkpoint-ts":0,"admin-job-type":0}`,
	})
	p := newProcessor4Test(t, changefeed.Info, changefeed.Status, captureInfo, liveness, cfg, enableRedo, nil, globalVars)

	return p, tester, changefeed
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
	globalVars, changefeedVars := vars.NewGlobalVarsAndChangefeedInfo4Test()
	ctx := context.Background()
	liveness := model.LivenessCaptureAlive
	p, tester, changefeed := initProcessor4Test(t, &liveness, false, globalVars, changefeedVars)

	// init tick
	checkChangefeedNormal(changefeed)
	require.Nil(t, p.lazyInit(ctx))
	createTaskPosition(changefeed, p.captureInfo)
	tester.MustApplyPatches()
	changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 20
		return status, true, nil
	})
	tester.MustApplyPatches()

	// no operation
	err, _ := p.Tick(ctx, changefeed.Info, changefeed.Status)
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

	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
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

	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
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
	globalVars, changefeedVars := vars.NewGlobalVarsAndChangefeedInfo4Test()
	ctx := context.Background()
	liveness := model.LivenessCaptureAlive
	p, tester, changefeed := initProcessor4Test(t, &liveness, true, globalVars, changefeedVars)

	// init tick
	checkChangefeedNormal(changefeed)
	require.Nil(t, p.lazyInit(ctx))
	createTaskPosition(changefeed, p.captureInfo)
	tester.MustApplyPatches()
	changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 20
		return status, true, nil
	})
	tester.MustApplyPatches()

	// no operation
	err, _ := p.Tick(ctx, changefeed.Info, changefeed.Status)
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

	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
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

	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
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
	globalVars, changefeedVars := vars.NewGlobalVarsAndChangefeedInfo4Test()
	ctx := context.Background()
	liveness := model.LivenessCaptureAlive
	p, tester, changefeed := initProcessor4Test(t, &liveness, false, globalVars, changefeedVars)

	// init tick
	require.Nil(t, p.lazyInit(ctx))
	err, _ := p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	createTaskPosition(changefeed, p.captureInfo)
	tester.MustApplyPatches()

	// send a abnormal error
	p.sinkManager.errors <- cerror.ErrSinkURIInvalid
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Error(t, err)
	patchProcessorErr(p.captureInfo, changefeed, err)
	tester.MustApplyPatches()
	require.Equal(t, changefeed.TaskPositions[p.captureInfo.ID], &model.TaskPosition{
		Error: &model.RunningError{
			Time:    changefeed.TaskPositions[p.captureInfo.ID].Error.Time,
			Addr:    "127.0.0.1:0000",
			Code:    "CDC:ErrSinkURIInvalid",
			Message: "[CDC:ErrSinkURIInvalid]sink uri invalid '%s'",
		},
	})
	require.Nil(t, p.Close())
	tester.MustApplyPatches()

	p, tester, changefeed = initProcessor4Test(t, &liveness, false, globalVars, changefeedVars)
	// init tick
	require.Nil(t, p.lazyInit(ctx))
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	createTaskPosition(changefeed, p.captureInfo)
	tester.MustApplyPatches()

	// send a normal error
	p.sinkManager.errors <- context.Canceled
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	patchProcessorErr(p.captureInfo, changefeed, err)
	tester.MustApplyPatches()
	require.True(t, cerror.ErrReactorFinished.Equal(errors.Cause(err)))
	require.Equal(t, changefeed.TaskPositions[p.captureInfo.ID], &model.TaskPosition{
		Error: nil,
	})
	require.Nil(t, p.Close())
	tester.MustApplyPatches()
}

func TestProcessorExit(t *testing.T) {
	globalVars, changefeedVars := vars.NewGlobalVarsAndChangefeedInfo4Test()
	liveness := model.LivenessCaptureAlive
	p, tester, changefeed := initProcessor4Test(t, &liveness, false, globalVars, changefeedVars)
	// init tick
	checkChangefeedNormal(changefeed)
	require.Nil(t, p.lazyInit(context.Background()))
	createTaskPosition(changefeed, p.captureInfo)
	tester.MustApplyPatches()

	// stop the changefeed
	changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.AdminJobType = model.AdminStop
		return status, true, nil
	})
	tester.MustApplyPatches()
	require.False(t, checkChangefeedNormal(changefeed))
	tester.MustApplyPatches()
	require.Equal(t, changefeed.TaskPositions[p.captureInfo.ID], &model.TaskPosition{
		Error: nil,
	})
	require.Nil(t, p.Close())
	tester.MustApplyPatches()
}

func TestProcessorClose(t *testing.T) {
	globalVars, changefeedVars := vars.NewGlobalVarsAndChangefeedInfo4Test()
	ctx := context.Background()
	liveness := model.LivenessCaptureAlive
	p, tester, changefeed := initProcessor4Test(t, &liveness, false, globalVars, changefeedVars)
	// init tick
	checkChangefeedNormal(changefeed)
	require.Nil(t, p.lazyInit(ctx))
	createTaskPosition(changefeed, p.captureInfo)
	tester.MustApplyPatches()

	// Do a no operation tick to lazy init the processor.
	err, _ := p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// add tables
	done, err := p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(1), tablepb.Checkpoint{CheckpointTs: 20}, false)
	require.Nil(t, err)
	require.True(t, done)
	done, err = p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(2), tablepb.Checkpoint{CheckpointTs: 30}, false)
	require.Nil(t, err)
	require.True(t, done)

	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// push the resolvedTs and checkpointTs
	changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return status, true, nil
	})
	tester.MustApplyPatches()
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Contains(t, changefeed.TaskPositions, p.captureInfo.ID)

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
	require.Nil(t, p.sinkManager.r)
	require.Nil(t, p.sourceManager.r)
	require.Nil(t, p.agent)

	p, tester, changefeed = initProcessor4Test(t, &liveness, false, globalVars, changefeedVars)
	// init tick
	checkChangefeedNormal(changefeed)
	require.Nil(t, p.lazyInit(ctx))
	createTaskPosition(changefeed, p.captureInfo)
	tester.MustApplyPatches()

	// Do a no operation tick to lazy init the processor.
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// add tables
	done, err = p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(1), tablepb.Checkpoint{CheckpointTs: 20}, false)
	require.Nil(t, err)
	require.True(t, done)
	done, err = p.AddTableSpan(ctx, spanz.TableIDToComparableSpan(2), tablepb.Checkpoint{CheckpointTs: 30}, false)
	require.Nil(t, err)
	require.True(t, done)
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// send error
	p.sinkManager.errors <- cerror.ErrSinkURIInvalid
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Error(t, err)
	patchProcessorErr(p.captureInfo, changefeed, err)
	tester.MustApplyPatches()

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
	require.Equal(t, changefeed.TaskPositions[p.captureInfo.ID].Error, &model.RunningError{
		Time:    changefeed.TaskPositions[p.captureInfo.ID].Error.Time,
		Addr:    "127.0.0.1:0000",
		Code:    "CDC:ErrSinkURIInvalid",
		Message: "[CDC:ErrSinkURIInvalid]sink uri invalid '%s'",
	})
	require.Nil(t, p.sinkManager.r)
	require.Nil(t, p.sourceManager.r)
	require.Nil(t, p.agent)
}

func TestPositionDeleted(t *testing.T) {
	globalVars, changefeedVars := vars.NewGlobalVarsAndChangefeedInfo4Test()
	ctx := context.Background()
	liveness := model.LivenessCaptureAlive
	p, tester, changefeed := initProcessor4Test(t, &liveness, false, globalVars, changefeedVars)
	// init tick
	checkChangefeedNormal(changefeed)
	require.Nil(t, p.lazyInit(ctx))
	createTaskPosition(changefeed, p.captureInfo)
	tester.MustApplyPatches()
	require.Contains(t, changefeed.TaskPositions, p.captureInfo.ID)

	// Do a no operation tick to lazy init the processor.
	err, _ := p.Tick(ctx, changefeed.Info, changefeed.Status)
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
	changefeed.PatchTaskPosition(p.captureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return nil, true, nil
		})
	tester.MustApplyPatches()

	// position created again
	checkChangefeedNormal(changefeed)
	createTaskPosition(changefeed, p.captureInfo)
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Equal(t, &model.TaskPosition{}, changefeed.TaskPositions[p.captureInfo.ID])
	require.Contains(t, changefeed.TaskPositions, p.captureInfo.ID)

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
}

func TestSchemaGC(t *testing.T) {
	globalVars, changefeedVars := vars.NewGlobalVarsAndChangefeedInfo4Test()
	ctx := context.Background()
	liveness := model.LivenessCaptureAlive
	p, tester, changefeed := initProcessor4Test(t, &liveness, false, globalVars, changefeedVars)

	var err error
	// init tick
	checkChangefeedNormal(changefeed)
	require.Nil(t, p.lazyInit(ctx))
	createTaskPosition(changefeed, p.captureInfo)
	tester.MustApplyPatches()

	updateChangeFeedPosition(t, tester,
		model.DefaultChangeFeedID("changefeed-id-test"),
		50)
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
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
	globalVars, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	ctx := context.Background()
	liveness := model.LivenessCaptureAlive
	p, tester, changefeed := initProcessor4Test(t, &liveness, false, globalVars, changefeedInfo)
	changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.CheckpointTs = 5
		return status, true, nil
	})
	p.ddlHandler.r.schemaStorage.(*mockSchemaStorage).resolvedTs = 10

	// init tick
	checkChangefeedNormal(changefeed)
	require.Nil(t, p.lazyInit(ctx))
	createTaskPosition(changefeed, p.captureInfo)
	tester.MustApplyPatches()
	require.Contains(t, changefeed.TaskPositions, p.captureInfo.ID)

	// Do a no operation tick to lazy init the processor.
	err, _ := p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	tester.MustApplyPatches()

	span := spanz.TableIDToComparableSpan(1)
	done, err := p.AddTableSpan(ctx, span, tablepb.Checkpoint{CheckpointTs: 5}, false)
	require.True(t, done)
	require.Nil(t, err)
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// Global resolved ts has advanced while schema storage stalls.
	changefeed.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return status, true, nil
	})
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	tester.MustApplyPatches()
	p.updateBarrierTs(&schedulepb.Barrier{GlobalBarrierTs: 20, TableBarriers: nil})
	status := p.sinkManager.r.GetTableStats(span)
	require.Equal(t, uint64(10), status.BarrierTs)

	// Schema storage has advanced too.
	p.ddlHandler.r.schemaStorage.(*mockSchemaStorage).resolvedTs = 15
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	tester.MustApplyPatches()
	p.updateBarrierTs(&schedulepb.Barrier{GlobalBarrierTs: 20, TableBarriers: nil})
	status = p.sinkManager.r.GetTableStats(span)
	require.Equal(t, uint64(15), status.BarrierTs)

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
}

func TestProcessorLiveness(t *testing.T) {
	globalVars, changefeedVars := vars.NewGlobalVarsAndChangefeedInfo4Test()
	ctx := context.Background()
	liveness := model.LivenessCaptureAlive
	p, tester, changefeed := initProcessor4Test(t, &liveness, false, globalVars, changefeedVars)

	// First tick for creating position.
	require.Nil(t, p.lazyInit(ctx))
	err, _ := p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// Second tick for init.
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
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

	globalVars, changefeedVars := vars.NewGlobalVarsAndChangefeedInfo4Test()
	ctx := context.Background()
	liveness := model.LivenessCaptureAlive
	p, tester, changefeed := initProcessor4Test(t, &liveness, false, globalVars, changefeedVars)
	require.Nil(t, p.lazyInit(ctx))

	// First tick for creating position.
	err, _ := p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)
	tester.MustApplyPatches()

	// Second tick for init.
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)

	// TODO(qupeng): third tick for handle a warning.
	err, _ = p.Tick(ctx, changefeed.Info, changefeed.Status)
	require.Nil(t, err)

	require.Nil(t, p.Close())
	tester.MustApplyPatches()
}

func TestProcessorNotInitialized(t *testing.T) {
	globalVars, changefeedVars := vars.NewGlobalVarsAndChangefeedInfo4Test()
	liveness := model.LivenessCaptureAlive
	p, _, _ := initProcessor4Test(t, &liveness, false, globalVars, changefeedVars)
	require.Nil(t, p.WriteDebugInfo(os.Stdout))
}
