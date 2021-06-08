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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
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
type TableStatus int32

// TableStatus for table pipeline
const (
	TableStatusInitializing TableStatus = iota
	TableStatusRunning
	TableStatusStopped
)

func (s TableStatus) String() string {
	switch s {
	case TableStatusInitializing:
		return "Initializing"
	case TableStatusRunning:
		return "Running"
	case TableStatusStopped:
		return "Stopped"
	}
	return "Unknown"
}

// Load TableStatus with THREAD-SAFE
func (s *TableStatus) Load() TableStatus {
	return TableStatus(atomic.LoadInt32((*int32)(s)))
}

// Store TableStatus with THREAD-SAFE
func (s *TableStatus) Store(new TableStatus) {
	atomic.StoreInt32((*int32)(s), int32(new))
}

type sinkNode struct {
	sink   sink.Sink
	status TableStatus

	resolvedTs   model.Ts
	checkpointTs model.Ts
	targetTs     model.Ts
	barrierTs    model.Ts

	eventBuffer []*model.PolymorphicEvent
	rowBuffer   []*model.RowChangedEvent

	flowController tableFlowController
}

func newSinkNode(sink sink.Sink, startTs model.Ts, targetTs model.Ts, flowController tableFlowController) *sinkNode {
	return &sinkNode{
		sink:         sink,
		status:       TableStatusInitializing,
		targetTs:     targetTs,
		resolvedTs:   startTs,
		checkpointTs: startTs,
		barrierTs:    startTs,

		flowController: flowController,
	}
}

// UpdateBarrierTs is used as an alternative way to update the barrierTs (AKA the global resolved-ts).
// TODO remove it after refactoring Unified Sorter to guarantee non-blocking
func (n *sinkNode) UpdateBarrierTs(barrierTs uint64) {
	atomic.StoreUint64(&n.barrierTs, barrierTs)
}

func (n *sinkNode) ResolvedTs() model.Ts   { return atomic.LoadUint64(&n.resolvedTs) }
func (n *sinkNode) CheckpointTs() model.Ts { return atomic.LoadUint64(&n.checkpointTs) }
func (n *sinkNode) Status() TableStatus    { return n.status.Load() }

func (n *sinkNode) Init(ctx pipeline.NodeContext) error {
	// do nothing
	return nil
}

func (n *sinkNode) flushSink(ctx pipeline.NodeContext, resolvedTs model.Ts) (err error) {
	defer func() {
		if err != nil {
			n.status.Store(TableStatusStopped)
			return
		}
		if n.checkpointTs >= n.targetTs {
			n.status.Store(TableStatusStopped)
			err = n.sink.Close()
			if err != nil {
				err = errors.Trace(err)
				return
			}
			err = cerror.ErrTableProcessorStoppedSafely.GenWithStackByArgs()
		}
	}()

	barrierTs := atomic.LoadUint64(&n.barrierTs)
	if resolvedTs > barrierTs {
		resolvedTs = barrierTs
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
	checkpointTs, err := n.sink.FlushRowChangedEvents(ctx, resolvedTs)
	if err != nil {
		return errors.Trace(err)
	}
	if checkpointTs <= n.checkpointTs {
		return nil
	}
	atomic.StoreUint64(&n.checkpointTs, checkpointTs)

	n.flowController.Release(checkpointTs)
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
	for _, ev := range n.eventBuffer {
		err := ev.WaitPrepare(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if ev.Row == nil {
			continue
		}
		ev.Row.ReplicaID = ev.ReplicaID
		n.rowBuffer = append(n.rowBuffer, ev.Row)
	}
	failpoint.Inject("ProcessorSyncResolvedPreEmit", func() {
		log.Info("Prepare to panic for ProcessorSyncResolvedPreEmit")
		time.Sleep(10 * time.Second)
		panic("ProcessorSyncResolvedPreEmit")
	})
	err := n.sink.EmitRowChangedEvents(ctx, n.rowBuffer...)
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
				n.status.Store(TableStatusRunning)
			}
			failpoint.Inject("ProcessorSyncResolvedError", func() {
				failpoint.Return(errors.New("processor sync resolved injected error"))
			})
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
		atomic.StoreUint64(&n.barrierTs, msg.BarrierTs)
		if err := n.flushSink(ctx, n.resolvedTs); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (n *sinkNode) Destroy(ctx pipeline.NodeContext) error {
	n.status.Store(TableStatusStopped)
	n.flowController.Abort()
	return n.sink.Close()
}
