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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pipeline"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type mockSink struct {
	received []struct {
		resolvedTs model.Ts
		row        *model.RowChangedEvent
	}
}

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

func (s *mockSink) AddTable(tableID model.TableID) error {
	return nil
}

func (s *mockSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	for _, row := range rows {
		s.received = append(s.received, struct {
			resolvedTs model.Ts
			row        *model.RowChangedEvent
		}{row: row})
	}
	return nil
}

func (s *mockSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	panic("unreachable")
}

func (s *mockSink) FlushRowChangedEvents(
	ctx context.Context, _ model.TableID, resolved model.ResolvedTs,
) (model.ResolvedTs, error) {
	s.received = append(s.received, struct {
		resolvedTs model.Ts
		row        *model.RowChangedEvent
	}{resolvedTs: resolved.Ts})
	return resolved, nil
}

func (s *mockSink) EmitCheckpointTs(_ context.Context, _ uint64, _ []model.TableName) error {
	panic("unreachable")
}

func (s *mockSink) Close(ctx context.Context) error {
	return nil
}

func (s *mockSink) RemoveTable(ctx context.Context, tableID model.TableID) error {
	return nil
}

func (s *mockSink) Check(t *testing.T, expected []struct {
	resolvedTs model.Ts
	row        *model.RowChangedEvent
},
) {
	require.Equal(t, expected, s.received)
}

func (s *mockSink) Reset() {
	s.received = s.received[:0]
}

type mockCloseControlSink struct {
	mockSink
	closeCh chan interface{}
}

func (s *mockCloseControlSink) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return nil
	}
}

func TestState(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: model.DefaultChangeFeedID("changefeed-id-test-status"),
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		},
	})

	state := TableStatePrepared
	// test stop at targetTs
	node := newSinkNode(1, &mockSink{}, 0, 10, &mockFlowController{}, redo.NewDisabledManager(),
		&state, ctx.ChangefeedVars().ID)
	node.initWithReplicaConfig(pipeline.MockNodeContext4Test(ctx, pmessage.Message{}, nil).
		ChangefeedVars().Info.Config)
	require.Equal(t, TableStatePrepared, node.State())

	ok, err := node.HandleMessage(ctx, pmessage.BarrierMessage(20))
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, TableStatePrepared, node.State())
	require.Equal(t, model.Ts(20), node.BarrierTs())

	msg := pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, TableStatePrepared, node.State())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, TableStatePrepared, node.State())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, TableStateReplicating, node.State())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 15, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.False(t, ok)
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, TableStateStopped, node.State())
	require.Equal(t, model.Ts(10), node.CheckpointTs())

	// test the stop at ts command
	state = TableStatePrepared
	node = newSinkNode(1, &mockSink{}, 0, 10, &mockFlowController{}, redo.NewDisabledManager(),
		&state, ctx.ChangefeedVars().ID)
	node.initWithReplicaConfig(pipeline.MockNodeContext4Test(ctx,
		pmessage.Message{}, nil).ChangefeedVars().Info.Config)
	require.Equal(t, TableStatePrepared, node.State())

	msg = pmessage.BarrierMessage(20)
	ok, err = node.HandleMessage(ctx, msg)
	require.True(t, ok)
	require.Nil(t, err)
	require.Equal(t, TableStatePrepared, node.State())
	require.Equal(t, model.Ts(20), node.BarrierTs())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, TableStateReplicating, node.State())

	msg = pmessage.CommandMessage(&pmessage.Command{Tp: pmessage.CommandTypeStop})
	ok, err = node.HandleMessage(ctx, msg)
	require.False(t, ok)
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, TableStateStopped, node.State())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.False(t, ok)
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, TableStateStopped, node.State())
	require.Equal(t, uint64(2), node.CheckpointTs())

	// test the stop at ts command is after then resolvedTs and checkpointTs is greater than stop ts
	state = TableStatePrepared
	node = newSinkNode(1, &mockSink{}, 0, 10, &mockFlowController{}, redo.NewDisabledManager(),
		&state, ctx.ChangefeedVars().ID)
	node.initWithReplicaConfig(pipeline.MockNodeContext4Test(ctx,
		pmessage.Message{}, nil).ChangefeedVars().Info.Config)
	require.Equal(t, TableStatePrepared, node.State())

	msg = pmessage.BarrierMessage(20)
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, TableStatePrepared, node.State())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, TableStateReplicating, node.State())

	msg = pmessage.CommandMessage(&pmessage.Command{Tp: pmessage.CommandTypeStop})
	ok, err = node.HandleMessage(ctx, msg)
	require.False(t, ok)
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, TableStateStopped, node.State())

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.False(t, ok)
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, TableStateStopped, node.State())
	require.Equal(t, uint64(7), node.CheckpointTs())
}

// TestStopStatus tests the table state of a pipeline is not set to stopped
// until the underlying sink is closed
func TestStopStatus(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: model.DefaultChangeFeedID("changefeed-id-test-state"),
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		},
	})

	state := TableStatePrepared
	closeCh := make(chan interface{}, 1)
	node := newSinkNode(1,
		&mockCloseControlSink{mockSink: mockSink{}, closeCh: closeCh}, 0, 100,
		&mockFlowController{}, redo.NewDisabledManager(),
		&state, ctx.ChangefeedVars().ID)
	node.initWithReplicaConfig(pipeline.MockNodeContext4Test(ctx,
		pmessage.Message{}, nil).ChangefeedVars().Info.Config)
	require.Equal(t, TableStatePrepared, node.State())

	msg := pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	ok, err := node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, TableStateReplicating, node.State())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// This will block until sink Close returns
		msg := pmessage.CommandMessage(&pmessage.Command{Tp: pmessage.CommandTypeStop})
		ok, err := node.HandleMessage(ctx, msg)
		require.False(t, ok)
		require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
		require.Equal(t, TableStateStopped, node.State())
	}()
	// wait to ensure stop message is sent to the sink node
	time.Sleep(time.Millisecond * 50)
	require.Equal(t, TableStateReplicating, node.State())
	closeCh <- struct{}{}
	wg.Wait()
}

func TestManyTs(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: model.DefaultChangeFeedID("changefeed-id-test"),
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		},
	})
	state := TableStatePrepared
	sink := &mockSink{}
	node := newSinkNode(1, sink, 0, 10, &mockFlowController{}, redo.NewDisabledManager(),
		&state, ctx.ChangefeedVars().ID)
	node.initWithReplicaConfig(pipeline.MockNodeContext4Test(ctx,
		pmessage.Message{}, nil).ChangefeedVars().Info.Config)
	require.Equal(t, TableStatePrepared, node.State())

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
	require.Equal(t, TableStatePrepared, node.State())
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
	require.Equal(t, TableStatePrepared, node.State())
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)

	msg = pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved},
		Row: &model.RowChangedEvent{},
	})
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, TableStateReplicating, node.State())
	sink.Check(t, []struct {
		resolvedTs model.Ts
		row        *model.RowChangedEvent
	}{
		{
			row: &model.RowChangedEvent{
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
			row: &model.RowChangedEvent{
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
	require.Equal(t, TableStateReplicating, node.State())

	sink.Check(t, []struct {
		resolvedTs model.Ts
		row        *model.RowChangedEvent
	}{
		{
			row: &model.RowChangedEvent{
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
			row: &model.RowChangedEvent{
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
		{resolvedTs: 1},
	})
	sink.Reset()
	require.Equal(t, model.NewResolvedTs(uint64(2)), node.getResolvedTs())
	require.Equal(t, uint64(1), node.CheckpointTs())

	msg = pmessage.BarrierMessage(5)
	ok, err = node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Equal(t, TableStateReplicating, node.State())
	sink.Check(t, []struct {
		resolvedTs model.Ts
		row        *model.RowChangedEvent
	}{
		{resolvedTs: 2},
	})
	sink.Reset()
	require.Equal(t, model.NewResolvedTs(uint64(2)), node.getResolvedTs())
	require.Equal(t, uint64(2), node.CheckpointTs())
}

func TestIgnoreEmptyRowChangeEvent(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: model.DefaultChangeFeedID("changefeed-id-test"),
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		},
	})
	state := TableStatePreparing
	sink := &mockSink{}
	node := newSinkNode(1, sink, 0, 10, &mockFlowController{}, redo.NewDisabledManager(),
		&state, ctx.ChangefeedVars().ID)
	node.initWithReplicaConfig(pipeline.MockNodeContext4Test(ctx,
		pmessage.Message{}, nil).ChangefeedVars().Info.Config)

	// empty row, no Columns and PreColumns.
	msg := pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
		Row: &model.RowChangedEvent{CommitTs: 1},
	})
	ok, err := node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Len(t, sink.received, 0)
}

func TestSplitUpdateEventWhenEnableOldValue(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: model.DefaultChangeFeedID("changefeed-id-test"),
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		},
	})
	state := TableStatePreparing
	sink := &mockSink{}
	node := newSinkNode(1, sink, 0, 10, &mockFlowController{}, redo.NewDisabledManager(),
		&state, ctx.ChangefeedVars().ID)
	node.initWithReplicaConfig(pipeline.MockNodeContext4Test(ctx,
		pmessage.Message{}, nil).ChangefeedVars().Info.Config)

	// nil row.
	msg := pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
	})
	ok, err := node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Len(t, sink.received, 0)

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
	require.Len(t, sink.received, 1)
	require.Len(t, sink.received[0].row.Columns, 2)
	require.Len(t, sink.received[0].row.PreColumns, 2)
}

func TestSplitUpdateEventWhenDisableOldValue(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	cfg := config.GetDefaultReplicaConfig()
	cfg.EnableOldValue = false
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: model.DefaultChangeFeedID("changefeed-id-test"),
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  cfg,
		},
	})
	state := TableStatePreparing
	sink := &mockSink{}
	node := newSinkNode(1, sink, 0, 10, &mockFlowController{}, redo.NewDisabledManager(),
		&state, ctx.ChangefeedVars().ID)
	node.initWithReplicaConfig(pipeline.MockNodeContext4Test(ctx,
		pmessage.Message{}, nil).ChangefeedVars().Info.Config)

	// nil row.
	msg := pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
	})
	ok, err := node.HandleMessage(ctx, msg)
	require.Nil(t, err)
	require.True(t, ok)
	require.Len(t, sink.received, 0)

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
	require.Len(t, sink.received, 1)
	require.Len(t, sink.received[0].row.Columns, 2)
	require.Len(t, sink.received[0].row.PreColumns, 0)

	// Cleanup.
	sink.Reset()
	// Update to the handle key column.
	columns = []*model.Column{
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
	require.Len(t, sink.received, 2)

	deleteEventIndex := 0
	require.Len(t, sink.received[deleteEventIndex].row.Columns, 0)
	require.Len(t, sink.received[deleteEventIndex].row.PreColumns, 2)
	nonHandleKeyColIndex := 0
	handleKeyColIndex := 1
	// NOTICE: When old value disabled, we only keep the handle key pre cols.
	require.Nil(t, sink.received[deleteEventIndex].row.PreColumns[nonHandleKeyColIndex])
	require.Equal(t, "col2", sink.received[deleteEventIndex].row.PreColumns[handleKeyColIndex].Name)
	require.True(t,
		sink.received[deleteEventIndex].row.PreColumns[handleKeyColIndex].Flag.IsHandleKey(),
	)

	insertEventIndex := 1
	require.Len(t, sink.received[insertEventIndex].row.Columns, 2)
	require.Len(t, sink.received[insertEventIndex].row.PreColumns, 0)
}

type flushFlowController struct {
	mockFlowController
	releaseCounter int
}

func (c *flushFlowController) Release(resolved model.ResolvedTs) {
	c.releaseCounter++
}

type flushSink struct {
	mockSink
}

// use to simulate the situation that resolvedTs return from sink manager
// fall back
var fallBackResolvedTs = uint64(10)

func (s *flushSink) FlushRowChangedEvents(
	ctx context.Context, _ model.TableID, resolved model.ResolvedTs,
) (model.ResolvedTs, error) {
	if resolved.Ts == fallBackResolvedTs {
		return model.NewResolvedTs(0), nil
	}
	return resolved, nil
}

// TestFlushSinkReleaseFlowController tests sinkNode.flushSink method will always
// call flowController.Release to release the memory quota of the table to avoid
// deadlock if there is no error occur
func TestFlushSinkReleaseFlowController(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	cfg := config.GetDefaultReplicaConfig()
	cfg.EnableOldValue = false
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: model.DefaultChangeFeedID("changefeed-id-test"),
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  cfg,
		},
	})
	state := TableStatePreparing
	flowController := &flushFlowController{}
	sink := &flushSink{}
	// sNode is a sinkNode
	sNode := newSinkNode(1, sink, 0, 10, flowController, redo.NewDisabledManager(),
		&state, ctx.ChangefeedVars().ID)
	sNode.initWithReplicaConfig(pipeline.MockNodeContext4Test(ctx,
		pmessage.Message{}, nil).ChangefeedVars().Info.Config)
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
