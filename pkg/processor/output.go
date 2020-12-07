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
	"sync/atomic"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/pipeline"
)

type outputNode struct {
	outputCh   chan *model.PolymorphicEvent
	resolvedTs *model.Ts
	stopped    *int32
}

func newOutputNode(outputCh chan *model.PolymorphicEvent, resolvedTs *model.Ts, stopped *int32) pipeline.Node {
	return &outputNode{
		outputCh:   outputCh,
		resolvedTs: resolvedTs,
		stopped:    stopped,
	}
}

func (n *outputNode) Init(ctx pipeline.NodeContext) error {
	// do nothing
	return nil
}

// Receive receives the message from the previous node
func (n *outputNode) Receive(ctx pipeline.NodeContext) error {
	msg := ctx.Message()
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		if msg.PolymorphicEvent.RawKV.OpType == model.OpTypeResolved {
			atomic.StoreUint64(n.resolvedTs, msg.PolymorphicEvent.CRTs)
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case n.outputCh <- msg.PolymorphicEvent:
		}
	case pipeline.MessageTypeCommand:
		if msg.Command.Tp == pipeline.CommandTypeStopped {
			atomic.StoreInt32(n.stopped, 1)
		}
	}
	return nil
}

func (n *outputNode) Destroy(ctx pipeline.NodeContext) error {
	atomic.StoreInt32(n.stopped, 1)
	return nil
}
