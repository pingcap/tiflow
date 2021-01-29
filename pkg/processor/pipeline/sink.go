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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"go.uber.org/zap"
)

const (
	defaultSyncResolvedBatch = 1024
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

	eventBuffer []*model.PolymorphicEvent
	rowBuffer   []*model.RowChangedEvent
}

func newSinkNode(sink sink.Sink, startTs model.Ts, targetTs model.Ts) *sinkNode {
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

func (n *sinkNode) flushSink(ctx pipeline.NodeContext, resolvedTs model.Ts) error {
	if resolvedTs > n.barrierTs {
		resolvedTs = n.barrierTs
	}
	if resolvedTs > n.targetTs {
		resolvedTs = n.targetTs
	}
	if resolvedTs <= n.checkpointTs {
		return nil
	}
	if err := n.flushRow2Sink(ctx); err != nil {
		return errors.Trace(err)
	}
	checkpointTs, err := n.sink.FlushRowChangedEvents(ctx.StdContext(), resolvedTs)
	if err != nil {
		return errors.Trace(err)
	}
	atomic.StoreUint64(&n.checkpointTs, checkpointTs)
	if checkpointTs >= n.targetTs {
		atomic.StoreInt32(&n.status, TableStatusStopped)
		if err := n.sink.Close(); err != nil {
			return errors.Trace(err)
		}
		return cerror.ErrTableProcessorStoppedSafely.GenWithStackByArgs()
	}
	return nil
}

func (n *sinkNode) emitEvent(ctx pipeline.NodeContext, event *model.PolymorphicEvent) error {
	n.eventBuffer = append(n.eventBuffer, event)
	if len(n.eventBuffer) >= defaultSyncResolvedBatch {
		if err := n.flushRow2Sink(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (n *sinkNode) flushRow2Sink(ctx pipeline.NodeContext) error {
	stdCtx := ctx.StdContext()
	for _, ev := range n.eventBuffer {
		log.Debug("LEOPPRO wait", zap.Reflect("e", ev), zap.String("p", fmt.Sprintf("%p", ev)))
		err := ev.WaitPrepare(stdCtx)
		if err != nil {
			return errors.Trace(err)
		}
		if ev.Row == nil {
			continue
		}
		n.rowBuffer = append(n.rowBuffer, ev.Row)
	}
	failpoint.Inject("ProcessorSyncResolvedPreEmit", func() {
		log.Info("Prepare to panic for ProcessorSyncResolvedPreEmit")
		time.Sleep(10 * time.Second)
		panic("ProcessorSyncResolvedPreEmit")
	})
	err := n.sink.EmitRowChangedEvents(stdCtx, n.rowBuffer...)
	if err != nil {
		return errors.Trace(err)
	}
	n.rowBuffer = n.rowBuffer[:0]
	n.eventBuffer = n.eventBuffer[:0]
	return nil
}

// Receive receives the message from the previous node
func (n *sinkNode) Receive(ctx pipeline.NodeContext) error {
	msg := ctx.Message()
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		event := msg.PolymorphicEvent
		if event.RawKV.OpType == model.OpTypeResolved {
			if n.status == TableStatusInitializing {
				atomic.StoreInt32(&n.status, TableStatusRunning)
			}
			if err := n.flushSink(ctx, msg.PolymorphicEvent.CRTs); err != nil {
				return errors.Trace(err)
			}
			atomic.StoreUint64(&n.resolvedTs, msg.PolymorphicEvent.CRTs)
			return nil
		}
		if err := n.emitEvent(ctx, event); err != nil {
			return errors.Trace(err)
		}
	case pipeline.MessageTypeTick:
		if err := n.flushSink(ctx, n.resolvedTs); err != nil {
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
		if err := n.flushSink(ctx, n.resolvedTs); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (n *sinkNode) Destroy(ctx pipeline.NodeContext) error {
	atomic.StoreInt32(&n.status, TableStatusStopped)
	return n.sink.Close()
}
