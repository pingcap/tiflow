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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/common"
	"github.com/pingcap/ticdc/pkg/actor"
	"github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestNewTableActor(t *testing.T) {
	tableActorSystem, tableActorRouter := actor.NewSystemBuilder("table").Build()
	tableActorSystem.Start(context.Background())
	defer tableActorSystem.Stop()

	ctx := context.TODO()
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Cyclic = &config.CyclicConfig{Enable: true}
	cctx := cdcContext.WithChangefeedVars(cdcContext.NewContext(ctx, &cdcContext.GlobalVars{
		TableActorSystem: tableActorSystem,
		TableActorRouter: tableActorRouter,
		CaptureInfo:      &model.CaptureInfo{ID: "1", AdvertiseAddr: "1", Version: "v5.3.0"}}),
		&cdcContext.ChangefeedVars{
			ID: "1",
			Info: &model.ChangeFeedInfo{
				Config: replicaConfig,
				Engine: model.SortInMemory,
			},
		})

	nodeCreator := &FakeTableNodeCreator{}
	tbl, err := NewTableActor(cctx, nil, 1, "t1",
		&model.TableReplicaInfo{StartTs: 1, MarkTableID: 2}, nil, 100, nodeCreator)
	require.Nil(t, err)
	actor := tbl.(*tableActor)
	require.Equal(t, 3, len(actor.nodes))
	id, markId := actor.ID()
	require.Equal(t, TableStatusInitializing, actor.Status())
	require.Equal(t, "t1", actor.Name())
	require.Equal(t, int64(1), id)
	require.Equal(t, int64(2), markId)
	require.Equal(t, uint64(1), actor.ResolvedTs())
	require.Equal(t, uint64(1), actor.CheckpointTs())
	require.Equal(t, uint64(1), actor.Workload().Workload)
	actor.UpdateBarrierTs(2)

	ok, err := nodeCreator.actorPullerNode.TryHandleDataMessage(ctx, pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{
		StartTs: 2,
		CRTs:    2,
		RawKV: &model.RawKVEntry{
			OpType:  model.OpTypePut,
			StartTs: 2,
			CRTs:    2,
		},
		Row: nil,
	}))
	require.True(t, ok)
	require.Nil(t, err)
	require.Nil(t, tableActorRouter.Send(1, message.TickMessage()))
	time.Sleep(time.Millisecond * 500)

	replicaConfig.Cyclic = &config.CyclicConfig{Enable: false}
	cctx = cdcContext.WithChangefeedVars(cdcContext.NewContext(ctx, &cdcContext.GlobalVars{
		TableActorSystem: tableActorSystem,
		TableActorRouter: tableActorRouter,
		CaptureInfo:      &model.CaptureInfo{ID: "1", AdvertiseAddr: "1", Version: "v5.3.0"}}),
		&cdcContext.ChangefeedVars{ID: "2", Info: &model.ChangeFeedInfo{
			Config: replicaConfig,
			Engine: model.SortInMemory,
		},
		})
	tbl2, err := NewTableActor(cctx, nil, 2, "t2",
		&model.TableReplicaInfo{StartTs: 3, MarkTableID: 4}, nil, 100, nodeCreator)
	require.Nil(t, err)
	actor2 := tbl2.(*tableActor)
	require.Equal(t, 2, len(actor2.nodes))
	id, markId = actor2.ID()
	require.Equal(t, TableStatusInitializing, actor2.Status())
	require.Equal(t, "t2", actor2.Name())
	require.Equal(t, int64(2), id)
	require.Equal(t, int64(4), markId)
	require.Equal(t, uint64(3), actor2.ResolvedTs())
	require.Equal(t, uint64(3), actor2.CheckpointTs())
	require.Equal(t, uint64(1), actor2.Workload().Workload)

	actor1WaitReturned := false
	go func() {
		actor.Wait()
		actor1WaitReturned = true
	}()
	actor2WaitReturned := false
	go func() {
		actor2.Wait()
		actor2WaitReturned = true
	}()
	actor.AsyncStop(1)
	time.Sleep(time.Millisecond * 500)
	require.True(t, actor.stopped)
	require.True(t, actor1WaitReturned)
	require.False(t, actor2.stopped)
	require.False(t, actor2WaitReturned)

	actor2.Cancel()
	time.Sleep(time.Millisecond * 500)
	require.True(t, actor2.stopped)
	require.True(t, actor2WaitReturned)
}

func TestPollStartAndStoppedActor(t *testing.T) {
	tbl := &tableActor{stopped: false}
	var called = false
	var dataHolderFunc AsyncDataHolderFunc = func() *pipeline.Message {
		called = true
		return nil
	}
	tbl.nodes = []*Node{{
		tableActor:    tbl,
		eventStash:    nil,
		parentNode:    dataHolderFunc,
		dataProcessor: nil,
	},
	}
	require.True(t, tbl.Poll(context.TODO(), []message.Message{message.TickMessage()}))
	require.True(t, called)
	tbl.stopped = true
	called = false
	require.False(t, tbl.Poll(context.TODO(), []message.Message{message.TickMessage()}))
	require.False(t, called)
}

func TestTryRun(t *testing.T) {
	var pN AsyncDataHolderFunc = func() *pipeline.Message { return nil }
	var dp AsyncDataProcessorFunc = func(ctx context.Context, msg pipeline.Message) (bool, error) {
		return false, errors.New("error")
	}
	n := &Node{}
	n.parentNode = pN
	n.dataProcessor = dp
	require.Nil(t, n.TryRun(context.TODO()))
	require.Nil(t, n.eventStash)
	// process failed
	pN = func() *pipeline.Message {
		return &pipeline.Message{
			Tp:        pipeline.MessageTypeBarrier,
			BarrierTs: 1,
		}
	}
	n = &Node{}
	n.parentNode = pN
	n.dataProcessor = dp
	require.NotNil(t, n.TryRun(context.TODO()))
	require.NotNil(t, n.eventStash)
	require.Equal(t, pipeline.MessageTypeBarrier, n.eventStash.Tp)
	require.Equal(t, model.Ts(1), n.eventStash.BarrierTs)
	//data process is blocked
	dp = func(ctx context.Context, msg pipeline.Message) (bool, error) {
		return false, nil
	}
	n.dataProcessor = dp
	require.Nil(t, n.TryRun(context.TODO()))
	require.NotNil(t, n.eventStash)
	require.Equal(t, pipeline.MessageTypeBarrier, n.eventStash.Tp)
	require.Equal(t, model.Ts(1), n.eventStash.BarrierTs)

	//data process is ok
	dp = func(ctx context.Context, msg pipeline.Message) (bool, error) { return true, nil }
	msg := 0
	pN = func() *pipeline.Message {
		if msg > 0 {
			return nil
		}
		msg++
		return &pipeline.Message{
			Tp:        pipeline.MessageTypeBarrier,
			BarrierTs: 1,
		}
	}
	n = &Node{}
	n.parentNode = pN
	n.dataProcessor = dp
	require.Nil(t, n.TryRun(context.TODO()))
	require.Nil(t, n.eventStash)
}

func TestStartFailed(t *testing.T) {
	tableActorSystem, tableActorRouter := actor.NewSystemBuilder("table").Build()
	tableActorSystem.Start(context.Background())
	defer tableActorSystem.Stop()

	ctx := context.TODO()
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Cyclic = &config.CyclicConfig{Enable: true}
	cctx := cdcContext.WithChangefeedVars(cdcContext.NewContext(ctx, &cdcContext.GlobalVars{
		TableActorSystem: tableActorSystem,
		TableActorRouter: tableActorRouter,
		CaptureInfo:      &model.CaptureInfo{ID: "1", AdvertiseAddr: "1", Version: "v5.3.0"}}),
		&cdcContext.ChangefeedVars{
			ID: "1",
			Info: &model.ChangeFeedInfo{
				Config: replicaConfig,
				Engine: model.SortInMemory,
			},
		})

	nodeCreator := &FakeTableNodeCreator{failStart: true}
	tbl, err := NewTableActor(cctx, nil, 1, "t1",
		&model.TableReplicaInfo{StartTs: 1, MarkTableID: 2}, nil, 100, nodeCreator)
	require.NotNil(t, err)
	require.Nil(t, tbl)
}

type FakeTableNodeCreator struct {
	nodeCreatorImpl
	actorPullerNode TableActorDataNode
	failStart       bool
}

func TestNewTablePipelineNodeCreator(t *testing.T) {
	creator := NewTablePipelineNodeCreator()
	require.NotNil(t, creator)
	_, ok := creator.NewPullerNode(1, nil, "t1").(*pullerNode)
	require.True(t, ok)
	_, ok = creator.NewSorterNode("t1", 1, 1, common.NewTableFlowController(2), nil).(*sorterNode)
	require.True(t, ok)
	_, ok = creator.NewCyclicNode(1).(*cyclicMarkNode)
	require.True(t, ok)
	_, ok = creator.NewSinkNode(nil, 1, 1, common.NewTableFlowController(2)).(*sinkNode)
	require.True(t, ok)
}

func (n *FakeTableNodeCreator) NewPullerNode(_ model.TableID, _ *model.TableReplicaInfo, tableName string) TableActorDataNode {
	n.actorPullerNode = &FakeTableActorDataNode{
		outputCh:  make(chan pipeline.Message, 1),
		failStart: n.failStart,
	}
	return n.actorPullerNode
}

type FakeTableActorDataNode struct {
	outputCh  chan pipeline.Message
	failStart bool
}

func (n *FakeTableActorDataNode) TryHandleDataMessage(ctx context.Context, msg pipeline.Message) (bool, error) {
	select {
	case n.outputCh <- msg:
		return true, nil
	default:
		return false, nil
	}
}

func (n *FakeTableActorDataNode) Start(ctx context.Context, _ *actor.Router, _ *errgroup.Group, _ *cdcContext.ChangefeedVars, _ *cdcContext.GlobalVars) error {
	if n.failStart {
		return errors.New("failed to start")
	}
	return nil
}

func (n *FakeTableActorDataNode) TryGetProcessedMessage() *pipeline.Message {
	var msg pipeline.Message
	select {
	case msg = <-n.outputCh:
		return &msg
	default:
		return nil
	}
}
