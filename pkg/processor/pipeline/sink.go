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

package pipeline

import (
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"go.uber.org/zap"
)

// TableStatus is status of the table pipeline
type TableStatus = int32

// TableStatus for table pipeline
const (
	TableStatusInitializing TableStatus = iota
	TableStatusRunning
	TableStatusStopped
)

type sinkNode struct {
	sink   sink.Sink
	status TableStatus

	resolvedTs   model.Ts
	checkpointTs model.Ts
	targetTs     model.Ts
	barrierTs    model.Ts
}

func newSinkNode(sink sink.Sink, startTs model.Ts, targetTs model.Ts) pipeline.Node {
	return &sinkNode{
		sink:         sink,
		status:       TableStatusInitializing,
		targetTs:     targetTs,
		resolvedTs:   startTs,
		checkpointTs: startTs,
		barrierTs:    startTs,
	}
}

func (n *sinkNode) ResolvedTs() model.Ts   { return atomic.LoadUint64(&n.resolvedTs) }
func (n *sinkNode) CheckpointTs() model.Ts { return atomic.LoadUint64(&n.checkpointTs) }
func (n *sinkNode) Status() TableStatus    { return atomic.LoadInt32(&n.status) }

func (n *sinkNode) Init(ctx pipeline.NodeContext) error {
	// do nothing
	return nil
}

func (n *sinkNode) flushSink(ctx pipeline.NodeContext) error {
	resolvedTs := n.resolvedTs
	if resolvedTs > n.barrierTs {
		resolvedTs = n.barrierTs
	}
	if resolvedTs > n.targetTs {
		resolvedTs = n.targetTs
	}
	if resolvedTs <= n.checkpointTs {
		return nil
	}
	checkpointTs, err := n.sink.FlushRowChangedEvents(ctx.StdContext(), resolvedTs)
	if err != nil {
		return errors.Trace(err)
	}
	atomic.StoreUint64(&n.checkpointTs, checkpointTs)
	if checkpointTs >= n.barrierTs {
		atomic.StoreInt32(&n.status, TableStatusStopped)
		if err := n.sink.Close(); err != nil {
			return errors.Trace(err)
		}
		return cerror.ErrTableProcessorStoppedSafely.GenWithStackByArgs()
	}
	return nil
}

// Receive receives the message from the previous node
func (n *sinkNode) Receive(ctx pipeline.NodeContext) error {
	msg := ctx.Message()
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		event := msg.PolymorphicEvent
		switch event.RawKV.OpType {
		case model.OpTypeResolved:
			atomic.StoreInt32(&n.status, TableStatusRunning)
			atomic.StoreUint64(&n.resolvedTs, msg.PolymorphicEvent.RawKV.CRTs)
			if err := n.flushSink(ctx); err != nil {
				return errors.Trace(err)
			}
		default:
			err := n.sink.EmitRowChangedEvents(ctx.StdContext(), msg.PolymorphicEvent.Row)
			if err != nil {
				return errors.Trace(err)
			}
		}
	case pipeline.MessageTypeTick:
		if err := n.flushSink(ctx); err != nil {
			return errors.Trace(err)
		}
	case pipeline.MessageTypeCommand:
		if msg.Command.Tp == pipeline.CommandTypeStopAtTs {
			if msg.Command.StoppedTs < n.checkpointTs {
				log.Warn("the stopped ts is less than the checkpoint ts, "+
					"the table pipeline can't be stopped accurately, will be stopped soon",
					zap.Uint64("stoppedTs", msg.Command.StoppedTs), zap.Uint64("checkpointTs", n.checkpointTs))
			}
			n.targetTs = msg.Command.StoppedTs
		}
	case pipeline.MessageTypeBarrier:
		n.barrierTs = msg.BarrierTs
	}
	return nil
}

func (n *sinkNode) Destroy(ctx pipeline.NodeContext) error {
	atomic.StoreInt32(&n.status, TableStatusStopped)
	return n.sink.Close()
}
