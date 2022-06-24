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

package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline/system"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/pkg/actor"
	"github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/config"
	serverConfig "github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestAsyncStopFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	sys, router := actor.NewSystemBuilder[pmessage.Message](t.Name()).Build()
	tableActorSystem, tableActorRouter := sys, router
	tableActorSystem.Start(ctx)
	defer func() {
		cancel()
		tableActorSystem.Stop()
	}()

	tbl := &tableActor{
		stopped:     0,
		tableID:     1,
		router:      tableActorRouter,
		redoManager: redo.NewDisabledManager(),
		cancel:      func() {},
		reportErr:   func(err error) {},
		state:       TableStatePreparing,
	}
	tbl.sinkNode = newSinkNode(1, &mockSink{}, 0, 0, &mockFlowController{}, tbl.redoManager,
		&tbl.state, model.DefaultChangeFeedID("changefeed-test"))
	require.True(t, tbl.AsyncStop(1))

	mb := actor.NewMailbox[pmessage.Message](actor.ID(1), 0)
	tbl.actorID = actor.ID(1)
	require.Nil(t, tableActorSystem.Spawn(mb, tbl))
	tbl.mb = mb
	tableActorSystem.Stop()
	require.True(t, tbl.AsyncStop(1))
}

func TestTableActorInterface(t *testing.T) {
	table := &tableActor{
		tableID:     1,
		markTableID: 2,
		redoManager: redo.NewDisabledManager(),
		tableName:   "t1",
		state:       TableStatePreparing,
		replicaConfig: &serverConfig.ReplicaConfig{
			Consistent: &serverConfig.ConsistentConfig{
				Level: "node",
			},
		},
	}
	table.sinkNode = &sinkNode{state: &table.state}
	table.sortNode = &sorterNode{state: &table.state, resolvedTs: 5}

	tableID, markID := table.ID()
	require.Equal(t, int64(1), tableID)
	require.Equal(t, int64(2), markID)
	require.Equal(t, "t1", table.Name())
	require.Equal(t, TableStatePreparing, table.State())

	table.sortNode.state.Store(TableStatePrepared)
	require.Equal(t, TableStatePrepared, table.State())

	require.Equal(t, uint64(1), table.Workload().Workload)

	table.sinkNode.checkpointTs.Store(model.NewResolvedTs(3))
	require.Equal(t, model.Ts(3), table.CheckpointTs())

	require.Equal(t, model.Ts(5), table.ResolvedTs())
	table.replicaConfig.Consistent.Level = string(redo.ConsistentLevelEventual)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	table.redoManager, _ = redo.NewMockManager(ctx)
	table.sinkNode.resolvedTs.Store(model.NewResolvedTs(6))
	require.Equal(t, model.Ts(6), table.ResolvedTs())

	table.sinkNode.state.Store(TableStateStopped)
	require.Equal(t, TableStateStopped, table.State())
}

func TestTableActorCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	sys, router := actor.NewSystemBuilder[pmessage.Message](t.Name()).Build()
	tableActorSystem, tableActorRouter := sys, router
	tableActorSystem.Start(ctx)
	defer func() {
		cancel()
		tableActorSystem.Stop()
	}()

	tbl := &tableActor{
		state:       TableStatePreparing,
		stopped:     0,
		tableID:     1,
		redoManager: redo.NewDisabledManager(),
		router:      tableActorRouter,
		cancel:      func() {},
		reportErr:   func(err error) {},
	}
	tbl.sinkNode = &sinkNode{
		state:          &tbl.state,
		flowController: &mockFlowController{},
		sink:           &mockSink{},
	}
	mb := actor.NewMailbox[pmessage.Message](actor.ID(1), 0)
	tbl.actorID = actor.ID(1)
	require.Nil(t, tableActorSystem.Spawn(mb, tbl))
	tbl.mb = mb
	tbl.Cancel()
	require.Equal(t, stopped, tbl.stopped)
	require.Equal(t, TableStateStopped, tbl.State())
}

func TestTableActorWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	eg, _ := errgroup.WithContext(ctx)
	tbl := &tableActor{wg: eg, redoManager: redo.NewDisabledManager()}
	wg := sync.WaitGroup{}
	wg.Add(1)
	stopped := false
	go func() {
		defer wg.Done()
		tbl.Wait()
		stopped = true
	}()
	cancel()
	wg.Wait()
	require.True(t, stopped)
}

func TestHandleError(t *testing.T) {
	canceled := false
	reporterErr := false
	table := &tableActor{
		redoManager: redo.NewDisabledManager(),
		cancel: func() {
			canceled = true
		},
		reportErr: func(err error) {
			reporterErr = true
		},

		state:   TableStateReplicating,
		stopped: stopped,
	}
	flowController := &mockFlowController{}
	table.sinkNode = &sinkNode{
		sink:           &errorCloseSink{},
		state:          &table.state,
		flowController: flowController,
	}
	table.sortNode = &sorterNode{
		flowController: flowController,
	}

	// table is already stopped
	table.handleError(nil)
	require.Equal(t, TableStateReplicating, table.sinkNode.state.Load())
	require.False(t, canceled)
	require.True(t, reporterErr)

	table.stopped = 0
	reporterErr = false
	table.handleError(nil)
	require.True(t, canceled)
	require.True(t, reporterErr)
	require.Equal(t, stopped, table.stopped)
	require.Equal(t, TableStateStopped, table.sinkNode.state.Load())
}

func TestPollStoppedActor(t *testing.T) {
	tbl := tableActor{stopped: stopped}
	require.False(t, tbl.Poll(context.TODO(), nil))
	tbl = tableActor{stopped: stopped}
	require.False(t, tbl.Poll(context.TODO(), []message.Message[pmessage.Message]{
		message.ValueMessage[pmessage.Message](pmessage.TickMessage()),
	}))
	require.False(t, tbl.Poll(context.TODO(), nil))
}

func TestPollTickMessage(t *testing.T) {
	startTime := time.Now().Add(-sinkFlushInterval)
	table := tableActor{
		state:             TableStatePreparing,
		lastFlushSinkTime: time.Now().Add(-2 * sinkFlushInterval),
		cancel:            func() {},
		reportErr:         func(err error) {},
	}

	table.sinkNode = &sinkNode{
		state:          &table.state,
		sink:           &mockSink{},
		flowController: &mockFlowController{},
		targetTs:       11,
	}
	table.sinkNode.resolvedTs.Store(model.NewResolvedTs(10))
	table.sinkNode.checkpointTs.Store(model.NewResolvedTs(10))

	require.True(t, table.Poll(context.TODO(), []message.Message[pmessage.Message]{
		message.ValueMessage[pmessage.Message](pmessage.TickMessage()),
	}))
	require.True(t, table.lastFlushSinkTime.After(startTime))
	startTime = table.lastFlushSinkTime
	require.True(t, table.Poll(context.TODO(), []message.Message[pmessage.Message]{
		message.ValueMessage[pmessage.Message](pmessage.TickMessage()),
	}))
	require.True(t, table.lastFlushSinkTime.Equal(startTime))
	table.lastFlushSinkTime = time.Now().Add(-2 * sinkFlushInterval)
	table.state.Store(TableStateStopped)
	require.False(t, table.Poll(context.TODO(), []message.Message[pmessage.Message]{
		message.ValueMessage[pmessage.Message](pmessage.TickMessage()),
	}))
}

func TestPollStopMessage(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	tbl := tableActor{
		state: TableStateStopped,
		cancel: func() {
			wg.Done()
		},
		reportErr: func(err error) {},
	}
	tbl.sinkNode = &sinkNode{
		state:          &tbl.state,
		sink:           &mockSink{},
		flowController: &mockFlowController{},
	}
	tbl.Poll(context.TODO(), []message.Message[pmessage.Message]{
		message.StopMessage[pmessage.Message](),
	})
	wg.Wait()
	require.Equal(t, stopped, tbl.stopped)
}

func TestPollBarrierTsMessage(t *testing.T) {
	sn := &sinkNode{
		targetTs:  10,
		barrierTs: 8,
	}
	sn.resolvedTs.Store(model.NewResolvedTs(5))
	sn.checkpointTs.Store(model.NewResolvedTs(5))

	tbl := tableActor{
		sinkNode: sn,
		sortNode: &sorterNode{
			barrierTs: 8,
		},
	}
	require.True(t, tbl.Poll(context.TODO(), []message.Message[pmessage.Message]{
		message.ValueMessage(pmessage.BarrierMessage(7)),
	}))
	require.Equal(t, model.Ts(7), tbl.sinkNode.BarrierTs())
	require.Equal(t, model.Ts(8), tbl.sortNode.barrierTs)
}

func TestPollDataFailed(t *testing.T) {
	// process failed
	var pN asyncMessageHolderFunc = func() *pmessage.Message {
		return &pmessage.Message{
			Tp:        pmessage.MessageTypeBarrier,
			BarrierTs: 1,
		}
	}
	var dp asyncMessageProcessorFunc = func(
		ctx context.Context, msg pmessage.Message,
	) (bool, error) {
		return false, errors.New("error")
	}
	tbl := tableActor{
		state:             TableStatePreparing,
		cancel:            func() {},
		reportErr:         func(err error) {},
		lastFlushSinkTime: time.Now(),
		nodes: []*ActorNode{
			{
				parentNode:       pN,
				messageProcessor: dp,
			},
		},
	}
	tbl.sinkNode = &sinkNode{
		sink:           &mockSink{},
		flowController: &mockFlowController{},
		state:          &tbl.state,
	}
	require.False(t, tbl.Poll(context.TODO(), []message.Message[pmessage.Message]{
		message.ValueMessage[pmessage.Message](pmessage.TickMessage()),
	}))
	require.Equal(t, stopped, tbl.stopped)
}

func TestPollDataAfterSinkStopped(t *testing.T) {
	// process failed
	msgPulled := false
	var pN asyncMessageHolderFunc = func() *pmessage.Message {
		msgPulled = true
		return &pmessage.Message{
			Tp:        pmessage.MessageTypeBarrier,
			BarrierTs: 1,
		}
	}
	var dp asyncMessageProcessorFunc = func(
		ctx context.Context, msg pmessage.Message,
	) (bool, error) {
		return false, errors.New("error")
	}
	tbl := tableActor{
		cancel:            func() {},
		reportErr:         func(err error) {},
		sinkNode:          &sinkNode{sink: &mockSink{}, flowController: &mockFlowController{}},
		lastFlushSinkTime: time.Now(),
		nodes: []*ActorNode{
			{
				parentNode:       pN,
				messageProcessor: dp,
			},
		},
		sinkStopped: true,
	}
	require.True(t, tbl.Poll(context.TODO(), []message.Message[pmessage.Message]{
		message.ValueMessage[pmessage.Message](pmessage.TickMessage()),
	}))
	require.False(t, msgPulled)
	require.NotEqual(t, stopped, tbl.stopped)
}

func TestNewTableActor(t *testing.T) {
	realStartPullerFunc := startPuller
	realStartSorterFunc := startSorter
	defer func() {
		startPuller = realStartPullerFunc
		startSorter = realStartSorterFunc
	}()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	sys := system.NewSystem()
	require.Nil(t, sys.Start(ctx))
	globalVars := &cdcContext.GlobalVars{
		TableActorSystem: sys,
	}

	cctx := cdcContext.WithChangefeedVars(
		cdcContext.NewContext(ctx, globalVars),
		&cdcContext.ChangefeedVars{
			ID: model.DefaultChangeFeedID("changefeed-id-test"),
			Info: &model.ChangeFeedInfo{
				Config: config.GetDefaultReplicaConfig(),
			},
		})

	startPuller = func(t *tableActor, ctx *actorNodeContext) error {
		return nil
	}
	startSorter = func(t *tableActor, ctx *actorNodeContext) error {
		return nil
	}
	tbl, err := NewTableActor(cctx, nil, nil, 1, "t1",
		&model.TableReplicaInfo{
			StartTs:     0,
			MarkTableID: 1,
		}, &mockSink{}, redo.NewDisabledManager(), 10)
	require.NotNil(t, tbl)
	require.Nil(t, err)
	require.Equal(t, TableStatePreparing, tbl.State())
	require.NotPanics(t, func() {
		tbl.UpdateBarrierTs(model.Ts(5))
	})

	// start puller failed
	startPuller = func(t *tableActor, ctx *actorNodeContext) error {
		return errors.New("failed to start puller")
	}

	tbl, err = NewTableActor(cctx, nil, nil, 1, "t1",
		&model.TableReplicaInfo{
			StartTs:     0,
			MarkTableID: 1,
		}, &mockSink{}, redo.NewDisabledManager(), 10)
	require.Nil(t, tbl)
	require.NotNil(t, err)

	sys.Stop()
}

func TestTableActorStart(t *testing.T) {
	realStartPullerFunc := startPuller
	realStartSorterFunc := startSorter
	ctx, cancel := context.WithCancel(context.TODO())
	sys := system.NewSystem()
	require.Nil(t, sys.Start(ctx))
	globalVars := &cdcContext.GlobalVars{
		TableActorSystem: sys,
	}
	defer func() {
		cancel()
		startPuller = realStartPullerFunc
		startSorter = realStartSorterFunc
		sys.Stop()
	}()
	startPuller = func(t *tableActor, ctx *actorNodeContext) error {
		return nil
	}
	startSorter = func(t *tableActor, ctx *actorNodeContext) error {
		return nil
	}
	tbl := &tableActor{
		redoManager: redo.NewDisabledManager(),
		globalVars:  globalVars,
		changefeedVars: &cdcContext.ChangefeedVars{
			ID: model.DefaultChangeFeedID("changefeed-id-test"),
			Info: &model.ChangeFeedInfo{
				Config: config.GetDefaultReplicaConfig(),
			},
		},
		replicaInfo: &model.TableReplicaInfo{
			StartTs:     0,
			MarkTableID: 1,
		},
	}
	require.Nil(t, tbl.start(ctx))
	require.Equal(t, 1, len(tbl.nodes))
	require.True(t, tbl.started)

	// already started
	tbl.started = true
	require.Panics(t, func() {
		_ = tbl.start(context.TODO())
	})
}

type errorCloseSink struct {
	mockSink
}

func (e *errorCloseSink) Close(ctx context.Context) error {
	return errors.New("close sink failed")
}
