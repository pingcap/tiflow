// Copyright 2020 PingCAP, Inc.
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
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
)

type safeStopperNode struct {
	targetTs   model.Ts
	shouldStop bool
}

func newSafeStopperNode(targetTs model.Ts) pipeline.Node {
	return &safeStopperNode{
		targetTs: targetTs,
	}
}

func (n *safeStopperNode) Init(ctx pipeline.NodeContext) error {
	// do nothing
	return nil
}

// Receive receives the message from the previous node
func (n *safeStopperNode) Receive(ctx pipeline.NodeContext) error {
	msg := ctx.Message()
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		if n.handlePolymorphicEvent(ctx, msg.PolymorphicEvent) {
			ctx.SendToNextNode(pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStopped}))
			return cerror.ErrTableProcessorStoppedSafely.GenWithStackByArgs()
		}
	case pipeline.MessageTypeCommand:
		if msg.Command.Tp == pipeline.CommandTypeShouldStop {
			n.shouldStop = true
			return nil
		}
		ctx.SendToNextNode(msg)
	default:
		ctx.SendToNextNode(msg)
	}
	return nil
}

func (n *safeStopperNode) handlePolymorphicEvent(ctx pipeline.NodeContext, event *model.PolymorphicEvent) (shouldExit bool) {
	if event == nil {
		return false
	}
	// for every event which of TS more than `targetTs`, it should not to send to next nodes.
	if event.CRTs > n.targetTs {
		// when we reveice the the first resolved event which of TS more than `targetTs`,
		// we can ensure that any events which of TS less than or equal to `targetTs` are sent to next nodes
		// so we can send a resolved event and exit this node.
		if event.RawKV.OpType == model.OpTypeResolved {
			ctx.SendToNextNode(pipeline.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, n.targetTs)))
			return true
		}
		return false
	}
	ctx.SendToNextNode(pipeline.PolymorphicEventMessage(event))
	if n.shouldStop && event.RawKV.OpType == model.OpTypeResolved {
		return true
	}
	return false
}

func (n *safeStopperNode) Destroy(ctx pipeline.NodeContext) error {
	// do nothing
	return nil
}
