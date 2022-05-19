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
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pipeline"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"go.uber.org/zap"
)

const (
	defaultSyncResolvedBatch = 64
)

type sinkNode struct {
	sink    sink.Sink
	status  TableStatus
	tableID model.TableID

	resolvedTs   model.Ts
	checkpointTs model.Ts
	targetTs     model.Ts
	barrierTs    model.Ts

	flowController tableFlowController

	replicaConfig    *config.ReplicaConfig
	isTableActorMode bool
}

func newSinkNode(tableID model.TableID, sink sink.Sink, startTs model.Ts, targetTs model.Ts,
	flowController tableFlowController,
) *sinkNode {
	return &sinkNode{
		tableID: tableID,
		sink:    sink,
		// sink is always at least prepared, for receiving data from upstream.
		status:       TableStatusPrepared,
		targetTs:     targetTs,
		resolvedTs:   startTs,
		checkpointTs: startTs,
		barrierTs:    startTs,

		flowController: flowController,
	}
}

func (n *sinkNode) ResolvedTs() model.Ts   { return atomic.LoadUint64(&n.resolvedTs) }
func (n *sinkNode) CheckpointTs() model.Ts { return atomic.LoadUint64(&n.checkpointTs) }
func (n *sinkNode) BarrierTs() model.Ts    { return atomic.LoadUint64(&n.barrierTs) }
func (n *sinkNode) Status() TableStatus    { return n.status.Load() }

func (n *sinkNode) Init(ctx pipeline.NodeContext) error {
	n.replicaConfig = ctx.ChangefeedVars().Info.Config
	n.initWithReplicaConfig(false, ctx.ChangefeedVars().Info.Config)
	return nil
}

func (n *sinkNode) initWithReplicaConfig(isTableActorMode bool, replicaConfig *config.ReplicaConfig) {
	n.isTableActorMode = isTableActorMode
	n.replicaConfig = replicaConfig
}

// stop is called when sink receives a stop command or checkpointTs reaches targetTs.
// In this method, the builtin table sink will be closed by calling `Close`, and
// no more events can be sent to this sink node afterwards.
func (n *sinkNode) stop(ctx context.Context) (err error) {
	// table stopped status must be set after underlying sink is closed
	defer n.status.Store(TableStatusStopped)
	err = n.sink.Close(ctx)
	if err != nil {
		return
	}
	log.Info("sink is closed", zap.Int64("tableID", n.tableID))
	err = cerror.ErrTableProcessorStoppedSafely.GenWithStackByArgs()
	return
}

// flushSink emits all rows in rowBuffer to the backend sink and flushes
// the backend sink.
func (n *sinkNode) flushSink(ctx context.Context, resolvedTs model.Ts) (err error) {
	defer func() {
		if err != nil {
			n.status.Store(TableStatusStopped)
			return
		}
		if atomic.LoadUint64(&n.checkpointTs) >= n.targetTs {
			err = n.stop(ctx)
		}
	}()
	currentBarrierTs := atomic.LoadUint64(&n.barrierTs)
	currentCheckpointTs := atomic.LoadUint64(&n.checkpointTs)
	if resolvedTs > currentBarrierTs {
		resolvedTs = currentBarrierTs
	}
	if resolvedTs > n.targetTs {
		resolvedTs = n.targetTs
	}
	if resolvedTs <= currentCheckpointTs {
		return nil
	}
	checkpointTs, err := n.sink.FlushRowChangedEvents(ctx, n.tableID, resolvedTs)
	if err != nil {
		return errors.Trace(err)
	}

	// we must call flowController.Release immediately after we call
	// FlushRowChangedEvents to prevent deadlock cause by checkpointTs
	// fall back
	n.flowController.Release(checkpointTs)

	// the checkpointTs may fall back in some situation such as:
	//   1. This table is newly added to the processor
	//   2. There is one table in the processor that has a smaller
	//   checkpointTs than this one
	if checkpointTs <= currentCheckpointTs {
		return nil
	}
	atomic.StoreUint64(&n.checkpointTs, checkpointTs)

	return nil
}

// emitRowToSink checks event and emits event.Row to sink.
func (n *sinkNode) emitRowToSink(ctx context.Context, event *model.PolymorphicEvent) error {
	failpoint.Inject("ProcessorSyncResolvedPreEmit", func() {
		log.Info("Prepare to panic for ProcessorSyncResolvedPreEmit")
		time.Sleep(10 * time.Second)
		panic("ProcessorSyncResolvedPreEmit")
	})

	if event == nil || event.Row == nil {
		log.Warn("skip emit nil event", zap.Any("event", event))
		return nil
	}

	colLen := len(event.Row.Columns)
	preColLen := len(event.Row.PreColumns)
	// Some transactions could generate empty row change event, such as
	// begin; insert into t (id) values (1); delete from t where id=1; commit;
	// Just ignore these row changed events.
	if colLen == 0 && preColLen == 0 {
		log.Warn("skip emit empty row event", zap.Any("event", event))
		return nil
	}

	// This indicates that it is an update event,
	// and after enable old value internally by default(but disable in the configuration).
	// We need to handle the update event to be compatible with the old format.
	if !n.replicaConfig.EnableOldValue && colLen != 0 && preColLen != 0 && colLen == preColLen {
		if shouldSplitUpdateEvent(event) {
			deleteEvent, insertEvent, err := splitUpdateEvent(event)
			if err != nil {
				return errors.Trace(err)
			}
			// NOTICE: Please do not change the order, the delete event always comes before the insert event.
			return n.sink.EmitRowChangedEvents(ctx, deleteEvent.Row, insertEvent.Row)
		}
		// If the handle key columns are not updated, PreColumns is directly ignored.
		event.Row.PreColumns = nil
	}

	return n.sink.EmitRowChangedEvents(ctx, event.Row)
}

// shouldSplitUpdateEvent determines if the split event is needed to align the old format based on
// whether the handle key column has been modified.
// If the handle key column is modified,
// we need to use splitUpdateEvent to split the update event into a delete and an insert event.
func shouldSplitUpdateEvent(updateEvent *model.PolymorphicEvent) bool {
	// nil event will never be split.
	if updateEvent == nil {
		return false
	}

	handleKeyCount := 0
	equivalentHandleKeyCount := 0
	for i := range updateEvent.Row.Columns {
		if updateEvent.Row.Columns[i].Flag.IsHandleKey() && updateEvent.Row.PreColumns[i].Flag.IsHandleKey() {
			handleKeyCount++
			colValueString := model.ColumnValueString(updateEvent.Row.Columns[i].Value)
			preColValueString := model.ColumnValueString(updateEvent.Row.PreColumns[i].Value)
			if colValueString == preColValueString {
				equivalentHandleKeyCount++
			}
		}
	}

	// If the handle key columns are not updated, so we do **not** need to split the event row.
	return !(handleKeyCount == equivalentHandleKeyCount)
}

// splitUpdateEvent splits an update event into a delete and an insert event.
func splitUpdateEvent(updateEvent *model.PolymorphicEvent) (*model.PolymorphicEvent, *model.PolymorphicEvent, error) {
	if updateEvent == nil {
		return nil, nil, errors.New("nil event cannot be split")
	}

	// If there is an update to handle key columns,
	// we need to split the event into two events to be compatible with the old format.
	// NOTICE: Here we don't need a full deep copy because our two events need Columns and PreColumns respectively,
	// so it won't have an impact and no more full deep copy wastes memory.
	deleteEvent := *updateEvent
	deleteEventRow := *updateEvent.Row
	deleteEventRowKV := *updateEvent.RawKV
	deleteEvent.Row = &deleteEventRow
	deleteEvent.RawKV = &deleteEventRowKV

	deleteEvent.Row.Columns = nil
	for i := range deleteEvent.Row.PreColumns {
		// NOTICE: Only the handle key pre column is retained in the delete event.
		if !deleteEvent.Row.PreColumns[i].Flag.IsHandleKey() {
			deleteEvent.Row.PreColumns[i] = nil
		}
	}
	// Align with the old format if old value disabled.
	deleteEvent.Row.TableInfoVersion = 0

	insertEvent := *updateEvent
	insertEventRow := *updateEvent.Row
	insertEventRowKV := *updateEvent.RawKV
	insertEvent.Row = &insertEventRow
	insertEvent.RawKV = &insertEventRowKV
	// NOTICE: clean up pre cols for insert event.
	insertEvent.Row.PreColumns = nil

	return &deleteEvent, &insertEvent, nil
}

// Receive receives the message from the previous node
func (n *sinkNode) Receive(ctx pipeline.NodeContext) error {
	_, err := n.HandleMessage(ctx, ctx.Message())
	return err
}

func (n *sinkNode) HandleMessage(ctx context.Context, msg pmessage.Message) (bool, error) {
	if n.status.Load() == TableStatusStopped {
		return false, cerror.ErrTableProcessorStoppedSafely.GenWithStackByArgs()
	}
	switch msg.Tp {
	case pmessage.MessageTypePolymorphicEvent:
		event := msg.PolymorphicEvent
		if event.RawKV.OpType == model.OpTypeResolved {
			// first resolved event received, let the sinkNode become `replicating`
			if n.status.Load() == TableStatusPrepared {
				n.status.Store(TableStatusReplicating)
			}
			failpoint.Inject("ProcessorSyncResolvedError", func() {
				failpoint.Return(false, errors.New("processor sync resolved injected error"))
			})
			if err := n.flushSink(ctx, msg.PolymorphicEvent.CRTs); err != nil {
				return false, errors.Trace(err)
			}
			atomic.StoreUint64(&n.resolvedTs, msg.PolymorphicEvent.CRTs)
			return true, nil
		}
		if err := n.emitRowToSink(ctx, event); err != nil {
			return false, errors.Trace(err)
		}
	case pmessage.MessageTypeTick:
		if err := n.flushSink(ctx, atomic.LoadUint64(&n.resolvedTs)); err != nil {
			return false, errors.Trace(err)
		}
	case pmessage.MessageTypeCommand:
		if msg.Command.Tp == pmessage.CommandTypeStop {
			if err := n.stop(ctx); err != nil {
				return false, errors.Trace(err)
			}
		}
	case pmessage.MessageTypeBarrier:
		if err := n.updateBarrierTs(ctx, msg.BarrierTs); err != nil {
			return false, errors.Trace(err)
		}
	}
	return true, nil
}

func (n *sinkNode) updateBarrierTs(ctx context.Context, ts model.Ts) error {
	atomic.StoreUint64(&n.barrierTs, ts)
	if err := n.flushSink(ctx, atomic.LoadUint64(&n.resolvedTs)); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (n *sinkNode) Destroy(ctx pipeline.NodeContext) error {
	return n.releaseResource(ctx)
}

func (n *sinkNode) releaseResource(ctx context.Context) error {
	n.status.Store(TableStatusStopped)
	n.flowController.Abort()
	return n.sink.Close(ctx)
}
