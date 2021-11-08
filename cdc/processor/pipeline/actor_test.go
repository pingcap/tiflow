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

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestNewTableActor(t *testing.T) {
	ctx := context.TODO()

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Cyclic = &config.CyclicConfig{Enable: true}
	cctx := cdcContext.WithChangefeedVars(cdcContext.NewContext(ctx, &cdcContext.GlobalVars{CaptureInfo: &model.CaptureInfo{ID: "1", AdvertiseAddr: "1", Version: "v5.3.0"}}),
		&cdcContext.ChangefeedVars{
			ID: "1",
			Info: &model.ChangeFeedInfo{
				Config: replicaConfig,
				Engine: model.SortInMemory,
			},
		})

	nodeCreator := &FakeTableNodeCreator{}
	actor, err := NewTableActor(cctx, nil, 1, "t1",
		&model.TableReplicaInfo{
			StartTs:     1,
			MarkTableID: 0,
		},
		nil, 100, nodeCreator)
	require.Nil(t, err)

	defaultRouter.Send(1, message.BarrierMessage(2))
	nodeCreator.actorPullerNode.TryHandleDataMessage(ctx, pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{
		StartTs: 2,
		CRTs:    2,
		RawKV: &model.RawKVEntry{
			OpType:  model.OpTypePut,
			StartTs: 2,
			CRTs:    2,
		},
		Row: nil,
	}))
	defaultRouter.Send(1, message.TickMessage())
	time.Sleep(time.Second)

	defaultRouter.Send(1, message.StopMessage())
	time.Sleep(time.Second)
	require.True(t, actor.(*tableActor).stopped)
	defaultSystem.Stop()
}

type FakeTableNodeCreator struct {
	nodeCreatorImpl
	actorPullerNode TableActorDataNode
}

func (n *FakeTableNodeCreator) NewPullerNode(tableID model.TableID, replicaInfo *model.TableReplicaInfo, tableName string) TableActorDataNode {
	n.actorPullerNode = &FakeTableActorDataNode{outputCh: make(chan pipeline.Message, 1)}
	return n.actorPullerNode
}

func (n *FakeTableNodeCreator) NewCyclicNode(markTableID model.TableID) TableActorDataNode {
	return newCyclicMarkNode(markTableID).(*cyclicMarkNode)
}

func (n *FakeTableNodeCreator) NewSinkNode(sink sink.Sink, startTs model.Ts, targetTs model.Ts, flowController tableFlowController) TableActorSinkNode {
	return newSinkNode(sink, startTs, targetTs, flowController)
}

type FakeTableActorDataNode struct {
	outputCh chan pipeline.Message
}

func (n *FakeTableActorDataNode) TryHandleDataMessage(ctx context.Context, msg pipeline.Message) (bool, error) {
	select {
	case n.outputCh <- msg:
		return true, nil
	default:
		return false, nil
	}
}

func (n *FakeTableActorDataNode) Start(ctx context.Context, isTableActor bool, wg *errgroup.Group, info *cdcContext.ChangefeedVars, vars *cdcContext.GlobalVars) error {
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
