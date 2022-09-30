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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo"
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
	sink    sink.Sink
	status  TableStatus
	tableID model.TableID

	// atomic oprations for model.ResolvedTs
	resolvedTs   atomic.Value
	checkpointTs atomic.Value
	targetTs     model.Ts
	barrierTs    model.Ts

	flowController tableFlowController
	redoManager    redo.LogManager

	replicaConfig    *config.ReplicaConfig
	isTableActorMode bool
	splitTxn         bool
}

func newSinkNode(
	tableID model.TableID,
	sink sink.Sink,
	startTs model.Ts,
	targetTs model.Ts,
	flowController tableFlowController,
	splitTxn bool,
	redoManager redo.LogManager,
) *sinkNode {
	sn := &sinkNode{
		tableID:        tableID,
		sink:           sink,
		status:         TableStatusInitializing,
		targetTs:       targetTs,
		barrierTs:      startTs,
		flowController: flowController,
		splitTxn:       splitTxn,
		redoManager:    redoManager,
	}
	sn.resolvedTs.Store(model.NewResolvedTs(startTs))
	sn.checkpointTs.Store(model.NewResolvedTs(startTs))
	return sn
}

func (n *sinkNode) ResolvedTs() model.Ts   { return n.getResolvedTs().ResolvedMark() }
func (n *sinkNode) CheckpointTs() model.Ts { return n.getCheckpointTs().ResolvedMark() }
func (n *sinkNode) BarrierTs() model.Ts    { return atomic.LoadUint64(&n.barrierTs) }
func (n *sinkNode) Status() TableStatus    { return n.status.Load() }

func (n *sinkNode) getResolvedTs() model.ResolvedTs {
	return n.resolvedTs.Load().(model.ResolvedTs)
}

func (n *sinkNode) getCheckpointTs() model.ResolvedTs {
	return n.checkpointTs.Load().(model.ResolvedTs)
}

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
func (n *sinkNode) flushSink(ctx context.Context, resolved model.ResolvedTs) (err error) {
	defer func() {
		if err != nil {
			n.status.Store(TableStatusStopped)
			return
		}
		if n.CheckpointTs() >= n.targetTs {
			err = n.stop(ctx)
		}
	}()

	if resolved.Ts > n.targetTs {
		resolved = model.NewResolvedTs(n.targetTs)
	}

	if n.redoManager != nil && n.redoManager.Enabled() {
		// redo log do not support batch resolve mode, hence we
		// use `ResolvedMark` to restore a normal resolved ts
		resolved = model.NewResolvedTs(resolved.ResolvedMark())
		err = n.redoManager.UpdateResolvedTs(ctx, n.tableID, resolved.Ts)
	}

	// Flush sink with barrierTs, which is broadcasted by owner.
	currentBarrierTs := atomic.LoadUint64(&n.barrierTs)
	if resolved.Ts > currentBarrierTs {
		resolved = model.NewResolvedTs(currentBarrierTs)
	}
	if n.redoManager != nil && n.redoManager.Enabled() {
		redoFlushed := n.redoManager.GetResolvedTs(n.tableID)
		if currentBarrierTs > redoFlushed {
			// NOTE: How can barrierTs be greater than rodoTs?
			// When scheduler moves a table from one place to another place, the table
			// start position will be checkpointTs instead of resolvedTs, which means
			// redoTs can be less than barrierTs.
			log.Info("redo flushedTs is less than current barrierTs",
				zap.Int64("tableID", n.tableID),
				zap.Uint64("barrierTs", currentBarrierTs),
				zap.Uint64("tableRedoFlushed", redoFlushed))

			// The latest above comment shows why barrierTs can be greater than redoTs.
			// So here the aim is to avoid slow tables holding back the whole processor.
			resolved = model.NewResolvedTs(redoFlushed)
		}
	}

	currentCheckpointTs := n.getCheckpointTs()
	if currentCheckpointTs.EqualOrGreater(resolved) {
		return nil
	}

	checkpoint, err := n.sink.FlushRowChangedEvents(ctx, n.tableID, resolved)
	if err != nil {
		return errors.Trace(err)
	}

	// we must call flowController.Release immediately after we call
	// FlushRowChangedEvents to prevent deadlock cause by checkpointTs
	// fall back
	n.flowController.Release(checkpoint)

	// the checkpointTs may fall back in some situation such as:
	//   1. This table is newly added to the processor
	//   2. There is one table in the processor that has a smaller
	//   checkpointTs than this one
	if currentCheckpointTs.EqualOrGreater(checkpoint) {
		return nil
	}
	n.checkpointTs.Store(checkpoint)

	return nil
}

// emitRowToSink checks event and emits event.Row to sink.
func (n *sinkNode) emitRowToSink(ctx context.Context, event *model.PolymorphicEvent) error {
	failpoint.Inject("ProcessorSyncResolvedPreEmit", func() {
		log.Info("Prepare to panic for ProcessorSyncResolvedPreEmit")
		time.Sleep(10 * time.Second)
		panic("ProcessorSyncResolvedPreEmit")
	})

	emitRows := func(rows ...*model.RowChangedEvent) error {
		if n.redoManager != nil && n.redoManager.Enabled() {
			err := n.redoManager.EmitRowChangedEvents(ctx, n.tableID, rows...)
			if err != nil {
				return err
			}
		}
		return n.sink.EmitRowChangedEvents(ctx, rows...)
	}

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
			return emitRows(deleteEvent.Row, insertEvent.Row)
		}
		// If the handle key columns are not updated, PreColumns is directly ignored.
		event.Row.PreColumns = nil
	}

	return emitRows(event.Row)
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
		col := updateEvent.Row.Columns[i]
		preCol := updateEvent.Row.PreColumns[i]
		if col != nil && col.Flag.IsHandleKey() && preCol != nil && preCol.Flag.IsHandleKey() {
			handleKeyCount++
			colValueString := model.ColumnValueString(col.Value)
			preColValueString := model.ColumnValueString(preCol.Value)
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
		if deleteEvent.Row.PreColumns[i] != nil &&
			!deleteEvent.Row.PreColumns[i].Flag.IsHandleKey() {
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
		if err := n.verifySplitTxn(event); err != nil {
			return false, errors.Trace(err)
		}
		if event.IsResolved() {
			if n.status.Load() == TableStatusInitializing {
				n.status.Store(TableStatusRunning)
			}
			failpoint.Inject("ProcessorSyncResolvedError", func() {
				failpoint.Return(false, errors.New("processor sync resolved injected error"))
			})

			resolved := model.NewResolvedTs(event.CRTs)
			if event.Resolved != nil {
				resolved = *(event.Resolved)
			}

			if err := n.flushSink(ctx, resolved); err != nil {
				return false, errors.Trace(err)
			}
			n.resolvedTs.Store(resolved)
			return true, nil
		}
		if err := n.emitRowToSink(ctx, event); err != nil {
			return false, errors.Trace(err)
		}
	case pmessage.MessageTypeTick:
		if err := n.flushSink(ctx, n.getResolvedTs()); err != nil {
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
	if err := n.flushSink(ctx, n.getResolvedTs()); err != nil {
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

// Verify that TxnAtomicity compatibility with BatchResolved event and RowChangedEvent
// with `SplitTxn==true`.
func (n *sinkNode) verifySplitTxn(e *model.PolymorphicEvent) error {
	if n.splitTxn {
		return nil
	}

	// Fail-fast check, this situation should never happen normally when split transactions
	// are not supported.
	if e.Resolved != nil && e.Resolved.IsBatchMode() {
		msg := fmt.Sprintf("batch mode resolved ts is not supported "+
			"when sink.splitTxn is %+v", n.splitTxn)
		return cerror.ErrSinkInvalidConfig.GenWithStackByArgs(msg)
	}

	if e.Row != nil && e.Row.SplitTxn {
		msg := fmt.Sprintf("should not split txn when sink.splitTxn is %+v", n.splitTxn)
		return cerror.ErrSinkInvalidConfig.GenWithStackByArgs(msg)
	}
	return nil
}
