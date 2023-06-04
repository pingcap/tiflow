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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo"
	mocksink "github.com/pingcap/tiflow/cdc/sink/mock"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"github.com/stretchr/testify/require"
)

// mockFlowController is created because a real tableFlowController cannot be used
// we are testing sinkNode by itself.
type mockFlowController struct{}

func (c *mockFlowController) Consume(
	msg *model.PolymorphicEvent,
	size uint64,
	blockCallBack func(uint64) error,
) error {
	return nil
}

func (c *mockFlowController) Release(resolved model.ResolvedTs) {
}

func (c *mockFlowController) Abort() {
}

func (c *mockFlowController) GetConsumption() uint64 {
	return 0
}

func TestState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	state := tablepb.TableStatePrepared
	// test stop at targetTs
	targetTs := model.Ts(10)
	node := newSinkNode(1, mocksink.NewNormalMockSink(), nil,
		0, targetTs, &mockFlowController{}, redo.NewDisabledDMLManager(),
		&state, model.DefaultChangeFeedID("changefeed-id-test-status"), true, true)
	require.Equal(t, tablepb.TableStatePrepared, node.State())

	ok, err := node.HandleMessage(ctx, pmessage.BarrierMessage(20))
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStatePrepared, node.State())
	require.Equal(t, model.Ts(20), node.BarrierTs())

	msg := pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStatePrepared, node.State())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStatePrepared, node.State())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	// resolve event handled by the sink, which means also handled by the sorter
	// it's time to make the sorter node become replicating.
	state = tablepb.TableStateReplicating
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStateReplicating, node.State())

	batchResolved := model.ResolvedTs{
		Mode:    model.BatchResolvedMode,
		Ts:      targetTs,
		BatchID: rand.Uint64() % 10,
	}
	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs:     targetTs,
		Resolved: &batchResolved,
		RawKV:    &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row:      &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, batchResolved, node.getResolvedTs())
	require.Equal(t, batchResolved, node.getCheckpointTs())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 15, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.False(t, ok)
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, tablepb.TableStateStopped, node.State())
	require.Equal(t, targetTs, node.CheckpointTs())

	// test the stop at ts command
	state = tablepb.TableStatePrepared
	node = newSinkNode(1, mocksink.NewNormalMockSink(), nil,
		0, 10, &mockFlowController{}, redo.NewDisabledDMLManager(),
		&state, model.DefaultChangeFeedID("changefeed-id-test-status"), true, false)
	require.Equal(t, tablepb.TableStatePrepared, node.State())

	msg = pmessage.BarrierMessage(20)
	ok, err = node.HandleMessage(ctx, msg)
	require.True(t, ok)
	require.Nil(t, err)
	require.Equal(t, tablepb.TableStatePrepared, node.State())
	require.Equal(t, model.Ts(20), node.BarrierTs())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	state = tablepb.TableStateReplicating
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStateReplicating, node.State())

	msg = pmessage.CommandMessage(&pmessage.Command{Tp: pmessage.CommandTypeStop})
	ok, err = node.HandleMessage(ctx, msg)
	require.False(t, ok)
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, tablepb.TableStateStopped, node.State())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.False(t, ok)
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, tablepb.TableStateStopped, node.State())
	require.Equal(t, uint64(2), node.CheckpointTs())

	// test the stop at ts command is after then resolvedTs and checkpointTs is greater than stop ts
	state = tablepb.TableStatePrepared
	node = newSinkNode(1, mocksink.NewNormalMockSink(), nil,
		0, 10, &mockFlowController{}, redo.NewDisabledDMLManager(),
		&state, model.DefaultChangeFeedID("changefeed-id-test-status"), true, false)
	require.Equal(t, tablepb.TableStatePrepared, node.State())

	msg = pmessage.BarrierMessage(20)
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStatePrepared, node.State())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	state = tablepb.TableStateReplicating
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStateReplicating, node.State())

	msg = pmessage.CommandMessage(&pmessage.Command{Tp: pmessage.CommandTypeStop})
	ok, err = node.HandleMessage(ctx, msg)
	require.False(t, ok)
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, tablepb.TableStateStopped, node.State())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.False(t, ok)
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, tablepb.TableStateStopped, node.State())
	require.Equal(t, uint64(7), node.CheckpointTs())
}

// TestStopStatus tests the table state of a pipeline is not set to stopped
// until the underlying sink is closed
func TestStopStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := tablepb.TableStatePrepared
	closeCh := make(chan interface{}, 1)
	node := newSinkNode(1,
		mocksink.NewMockCloseControlSink(closeCh),
		nil, 0, 100,
		&mockFlowController{}, redo.NewDisabledDMLManager(), &state,
		model.DefaultChangeFeedID("changefeed-id-test-state"), true, false)
	require.Equal(t, tablepb.TableStatePrepared, node.State())

	msg := pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	state = tablepb.TableStateReplicating
	ok, err := node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStateReplicating, node.State())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		msg := pmessage.CommandMessage(&pmessage.Command{Tp: pmessage.CommandTypeStop})
		ok, err := node.HandleMessage(ctx, msg)
		require.False(t, ok)
		require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
		require.Equal(t, tablepb.TableStateStopped, node.State())
	}()
	require.Equal(t, tablepb.TableStateReplicating, node.State())
	closeCh <- struct{}{}
	wg.Wait()
}

func TestManyTs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := tablepb.TableStatePrepared
	sink := mocksink.NewNormalMockSink()
	node := newSinkNode(1, sink, nil, 0, 10, &mockFlowController{}, redo.NewDisabledDMLManager(),
		&state, model.DefaultChangeFeedID("changefeed-id-test"), true, false)
	require.Equal(t, tablepb.TableStatePrepared, node.State())

	msg := pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}, Row: &model.RowChangedEvent{
			CommitTs: 1,
			Columns: []*model.Column{
				{
					Name:  "col1",
					Flag:  model.BinaryFlag,
					Value: "col1-value-updated",
				},
				{
					Name:  "col2",
					Flag:  model.HandleKeyFlag,
					Value: "col2-value",
				},
			},
		},
	})
	require.Equal(t, tablepb.TableStatePrepared, node.State())
	ok, err := node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}, Row: &model.RowChangedEvent{
			CommitTs: 2,
			Columns: []*model.Column{
				{
					Name:  "col1",
					Flag:  model.BinaryFlag,
					Value: "col1-value-updated",
				},
				{
					Name:  "col2",
					Flag:  model.HandleKeyFlag,
					Value: "col2-value",
				},
			},
		},
	})
	require.Equal(t, tablepb.TableStatePrepared, node.State())
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	state = tablepb.TableStateReplicating
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStateReplicating, node.State())
	sink.Check(t, []mocksink.ReceivedData{
		{
			Row: &model.RowChangedEvent{
				CommitTs: 1,
				Columns: []*model.Column{
					{
						Name:  "col1",
						Flag:  model.BinaryFlag,
						Value: "col1-value-updated",
					},
					{
						Name:  "col2",
						Flag:  model.HandleKeyFlag,
						Value: "col2-value",
					},
				},
			},
		},
		{
			Row: &model.RowChangedEvent{
				CommitTs: 2,
				Columns: []*model.Column{
					{
						Name:  "col1",
						Flag:  model.BinaryFlag,
						Value: "col1-value-updated",
					},
					{
						Name:  "col2",
						Flag:  model.HandleKeyFlag,
						Value: "col2-value",
					},
				},
			},
		},
	})

	msg = pmessage.BarrierMessage(1)
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStateReplicating, node.State())

	sink.Check(t, []mocksink.ReceivedData{
		{
			Row: &model.RowChangedEvent{
				CommitTs: 1,
				Columns: []*model.Column{
					{
						Name:  "col1",
						Flag:  model.BinaryFlag,
						Value: "col1-value-updated",
					},
					{
						Name:  "col2",
						Flag:  model.HandleKeyFlag,
						Value: "col2-value",
					},
				},
			},
		},
		{
			Row: &model.RowChangedEvent{
				CommitTs: 2,
				Columns: []*model.Column{
					{
						Name:  "col1",
						Flag:  model.BinaryFlag,
						Value: "col1-value-updated",
					},
					{
						Name:  "col2",
						Flag:  model.HandleKeyFlag,
						Value: "col2-value",
					},
				},
			},
		},
		{ResolvedTs: 1},
	})
	sink.Reset()
	require.Equal(t, model.NewResolvedTs(uint64(2)), node.getResolvedTs())
	require.Equal(t, uint64(1), node.CheckpointTs())

	msg = pmessage.BarrierMessage(5)
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, tablepb.TableStateReplicating, node.State())
	sink.Check(t, []mocksink.ReceivedData{
		{ResolvedTs: 2},
	})
	sink.Reset()
	require.Equal(t, model.NewResolvedTs(uint64(2)), node.getResolvedTs())
	require.Equal(t, uint64(2), node.CheckpointTs())
}

func TestIgnoreEmptyRowChangeEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := tablepb.TableStatePreparing
	sink := mocksink.NewNormalMockSink()
	node := newSinkNode(1, sink, nil, 0, 10, &mockFlowController{}, redo.NewDisabledDMLManager(),
		&state, model.DefaultChangeFeedID("changefeed-id-test"), true, false)

	// empty row, no Columns and PreColumns.
	msg := pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
		Row: &model.RowChangedEvent{CommitTs: 1},
	})
	ok, err := node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Len(t, sink.Received, 0)
}

func TestSplitUpdateEventWhenEnableOldValue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := tablepb.TableStatePreparing
	sink := mocksink.NewNormalMockSink()
	node := newSinkNode(1, sink, nil, 0, 10, &mockFlowController{}, redo.NewDisabledDMLManager(),
		&state, model.DefaultChangeFeedID("changefeed-id-test"), true, false)

	// nil row.
	msg := pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
	})
	ok, err := node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Len(t, sink.Received, 0)

	columns := []*model.Column{
		{
			Name:  "col1",
			Flag:  model.BinaryFlag,
			Value: "col1-value-updated",
		},
		{
			Name:  "col2",
			Flag:  model.HandleKeyFlag,
			Value: "col2-value",
		},
	}
	preColumns := []*model.Column{
		{
			Name:  "col1",
			Flag:  model.BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  model.HandleKeyFlag,
			Value: "col2-value",
		},
	}
	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs:  1,
		RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
		Row:   &model.RowChangedEvent{CommitTs: 1, Columns: columns, PreColumns: preColumns},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Len(t, sink.Received, 1)
	require.Len(t, sink.Received[0].Row.Columns, 2)
	require.Len(t, sink.Received[0].Row.PreColumns, 2)
}

func TestSplitUpdateEventWhenDisableOldValue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := tablepb.TableStatePreparing
	sink := mocksink.NewNormalMockSink()
	enableOldValue := false
	node := newSinkNode(1, sink, nil, 0, 10, &mockFlowController{}, redo.NewDisabledDMLManager(),
		&state, model.DefaultChangeFeedID("changefeed-id-test"), enableOldValue, false)

	// nil row.
	msg := pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
	})
	ok, err := node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Len(t, sink.Received, 0)

	// No update to the handle key column.
	columns := []*model.Column{
		{
			Name:  "col1",
			Flag:  model.BinaryFlag,
			Value: "col1-value-updated",
		},
		{
			Name:  "col2",
			Flag:  model.HandleKeyFlag,
			Value: "col2-value",
		},
	}
	preColumns := []*model.Column{
		{
			Name:  "col1",
			Flag:  model.BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  model.HandleKeyFlag,
			Value: "col2-value",
		},
	}

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs:  1,
		RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
		Row:   &model.RowChangedEvent{CommitTs: 1, Columns: columns, PreColumns: preColumns},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Len(t, sink.Received, 1)
	require.Len(t, sink.Received[0].Row.Columns, 2)
	require.Len(t, sink.Received[0].Row.PreColumns, 0)

	// Cleanup.
	sink.Reset()
	// Update to the handle key column.
	columns = []*model.Column{
		{
			Name:  "col0",
			Flag:  model.BinaryFlag,
			Value: "col1-value-updated",
		},
		{
			Name:  "col1",
			Flag:  model.BinaryFlag,
			Value: "col1-value-updated",
		},
		{
			Name:  "col2",
			Flag:  model.HandleKeyFlag,
			Value: "col2-value-updated",
		},
	}
	preColumns = []*model.Column{
		// It is possible that the pre columns are nil. For example, when you do `add column` DDL.
		nil,
		{
			Name:  "col1",
			Flag:  model.BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  model.HandleKeyFlag,
			Value: "col2-value",
		},
	}

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs:  1,
		RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
		Row:   &model.RowChangedEvent{CommitTs: 1, Columns: columns, PreColumns: preColumns},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	// Split an update event into a delete and an insert event.
	require.Len(t, sink.Received, 2)

	deleteEventIndex := 0
	require.Len(t, sink.Received[deleteEventIndex].Row.Columns, 0)
	require.Len(t, sink.Received[deleteEventIndex].Row.PreColumns, 3)
	nilColIndex := 0
	require.Nil(t, sink.Received[deleteEventIndex].Row.PreColumns[nilColIndex])

	insertEventIndex := 1
	require.Len(t, sink.Received[insertEventIndex].Row.Columns, 3)
	require.Len(t, sink.Received[insertEventIndex].Row.PreColumns, 0)
}

type flushFlowController struct {
	mockFlowController
	releaseCounter int
}

func (c *flushFlowController) Release(resolved model.ResolvedTs) {
	c.releaseCounter++
}

// TestFlushSinkReleaseFlowController tests sinkNode.flushSink method will always
// call flowController.Release to release the memory quota of the table to avoid
// deadlock if there is no error occur
func TestFlushSinkReleaseFlowController(t *testing.T) {
	state := tablepb.TableStatePreparing
	flowController := &flushFlowController{}
	sink := mocksink.NewMockFlushSink()
	// sNode is a sinkNode
	sNode := newSinkNode(1, sink, nil, 0, 10, flowController, redo.NewDisabledDMLManager(),
		&state, model.DefaultChangeFeedID("changefeed-id-test"), true, false)
	sNode.barrierTs = 10

	err := sNode.flushSink(context.Background(), model.NewResolvedTs(uint64(8)))
	require.Nil(t, err)
	require.Equal(t, uint64(8), sNode.CheckpointTs())
	require.Equal(t, 1, flowController.releaseCounter)
	// resolvedTs will fall back in this call
	err = sNode.flushSink(context.Background(), model.NewResolvedTs(uint64(10)))
	require.Nil(t, err)
	require.Equal(t, uint64(8), sNode.CheckpointTs())
	require.Equal(t, 2, flowController.releaseCounter)
}

func TestSplitTxn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := tablepb.TableStatePrepared
	flowController := &flushFlowController{}
	sink := mocksink.NewMockFlushSink()
	// sNode is a sinkNode
	sNode := newSinkNode(1, sink, nil, 0, 10, flowController, redo.NewDisabledDMLManager(),
		&state, model.DefaultChangeFeedID("changefeed-id-test"), true, false)
	msg := pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs:  1,
		RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row:   &model.RowChangedEvent{},
	})
	_, err := sNode.HandleMessage(ctx, msg)
	require.Nil(t, err)

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs:  1,
		RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
		Row:   &model.RowChangedEvent{CommitTs: 2, SplitTxn: true},
	})
	_, err = sNode.HandleMessage(ctx, msg)
	require.Regexp(t, ".*should not split txn when sink.splitTxn is.*", err)

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs:  1,
		RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
		Row:   &model.RowChangedEvent{CommitTs: 2},
	})
	_, err = sNode.HandleMessage(ctx, msg)
	require.Nil(t, err)

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 7,
		Resolved: &model.ResolvedTs{
			Mode:    model.BatchResolvedMode,
			Ts:      7,
			BatchID: 1,
		},
		RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row:   &model.RowChangedEvent{},
	})
	_, err = sNode.HandleMessage(ctx, msg)
	require.Regexp(t, ".*batch mode resolved ts is not supported.*", err)
}

func TestSinkStatsRace(t *testing.T) {
	t.Parallel()

	state := tablepb.TableStatePreparing
	flowController := &flushFlowController{}
	sink := mocksink.NewMockFlushSink()
	// sNode is a sinkNode
	sNode := newSinkNode(1, sink, nil, 0, 10, flowController, redo.NewDisabledDMLManager(),
		&state, model.DefaultChangeFeedID("changefeed-id-test"), true, false)

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		// Update barrier ts
		defer wg.Done()
		barrierTs := uint64(0)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			barrierTs++
			_ = sNode.updateBarrierTs(ctx, barrierTs)
		}
	}()

	go func() {
		// Read barrier ts
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_ = sNode.Stats()
		}
	}()

	time.Sleep(time.Second)
	cancel()
	wg.Wait()
}
