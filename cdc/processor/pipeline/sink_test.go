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
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pipeline"
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

func (c *mockFlowController) Consume(commitTs uint64, size uint64, blockCallBack func() error) error {
	return nil
}

func (c *mockFlowController) Release(resolvedTs uint64) {
}

func (c *mockFlowController) Abort() {
}

func (c *mockFlowController) GetConsumption() uint64 {
	return 0
}

func (s *mockSink) TryEmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) (bool, error) {
	_ = s.EmitRowChangedEvents(ctx, rows...)
	return true, nil
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

func (s *mockSink) FlushRowChangedEvents(ctx context.Context, _ model.TableID, resolvedTs uint64) (uint64, error) {
	s.received = append(s.received, struct {
		resolvedTs model.Ts
		row        *model.RowChangedEvent
	}{resolvedTs: resolvedTs})
	return resolvedTs, nil
}

func (s *mockSink) EmitCheckpointTs(_ context.Context, _ uint64, _ []model.TableName) error {
	panic("unreachable")
}

func (s *mockSink) Close(ctx context.Context) error {
	return nil
}

func (s *mockSink) Barrier(ctx context.Context, tableID model.TableID) error {
	return nil
}

func (s *mockSink) Check(t *testing.T, expected []struct {
	resolvedTs model.Ts
	row        *model.RowChangedEvent
}) {
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

func TestStatus(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test-status",
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		},
	})

	// test stop at targetTs
	node := newSinkNode(1, &mockSink{}, 0, 10, &mockFlowController{})
	require.Nil(t, node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))
	require.Equal(t, TableStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(20), nil)))
	require.Equal(t, TableStatusInitializing, node.Status())
	require.Equal(t, model.Ts(20), node.BarrierTs())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}, Row: &model.RowChangedEvent{}}), nil)))
	require.Equal(t, TableStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}, Row: &model.RowChangedEvent{}}), nil)))
	require.Equal(t, TableStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil)))
	require.Equal(t, TableStatusRunning, node.Status())

	err := node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 15, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil))
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, TableStatusStopped, node.Status())
	require.Equal(t, uint64(10), node.CheckpointTs())

	// test the stop at ts command
	node = newSinkNode(1, &mockSink{}, 0, 10, &mockFlowController{})
	require.Nil(t, node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))
	require.Equal(t, TableStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(20), nil)))
	require.Equal(t, TableStatusInitializing, node.Status())
	require.Equal(t, model.Ts(20), node.BarrierTs())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil)))
	require.Equal(t, TableStatusRunning, node.Status())

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStop}), nil))
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, TableStatusStopped, node.Status())

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil))
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, TableStatusStopped, node.Status())
	require.Equal(t, uint64(2), node.CheckpointTs())

	// test the stop at ts command is after then resolvedTs and checkpointTs is greater than stop ts
	node = newSinkNode(1, &mockSink{}, 0, 10, &mockFlowController{})
	require.Nil(t, node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))
	require.Equal(t, TableStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(20), nil)))
	require.Equal(t, TableStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil)))
	require.Equal(t, TableStatusRunning, node.Status())

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStop}), nil))
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, TableStatusStopped, node.Status())

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil))
	require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
	require.Equal(t, TableStatusStopped, node.Status())
	require.Equal(t, uint64(7), node.CheckpointTs())
}

// TestStopStatus tests the table status of a pipeline is not set to stopped
// until the underlying sink is closed
func TestStopStatus(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test-status",
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		},
	})

	closeCh := make(chan interface{}, 1)
	node := newSinkNode(1, &mockCloseControlSink{mockSink: mockSink{}, closeCh: closeCh}, 0, 100, &mockFlowController{})
	require.Nil(t, node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))
	require.Equal(t, TableStatusInitializing, node.Status())
	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil)))
	require.Equal(t, TableStatusRunning, node.Status())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// This will block until sink Close returns
		err := node.Receive(pipeline.MockNodeContext4Test(ctx,
			pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStop}), nil))
		require.True(t, cerrors.ErrTableProcessorStoppedSafely.Equal(err))
		require.Equal(t, TableStatusStopped, node.Status())
	}()
	// wait to ensure stop message is sent to the sink node
	time.Sleep(time.Millisecond * 50)
	require.Equal(t, TableStatusRunning, node.Status())
	closeCh <- struct{}{}
	wg.Wait()
}

func TestManyTs(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test-many-ts",
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		},
	})
	sink := &mockSink{}
	node := newSinkNode(1, sink, 0, 10, &mockFlowController{})
	require.Nil(t, node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))
	require.Equal(t, TableStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{
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
		}), nil)))
	require.Equal(t, TableStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{
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
		}), nil)))
	require.Equal(t, TableStatusInitializing, node.Status())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil)))
	require.Equal(t, TableStatusRunning, node.Status())
	sink.Check(t, nil)

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(1), nil)))
	require.Equal(t, TableStatusRunning, node.Status())

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
	require.Equal(t, uint64(2), node.ResolvedTs())
	require.Equal(t, uint64(1), node.CheckpointTs())

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(5), nil)))
	require.Equal(t, TableStatusRunning, node.Status())
	sink.Check(t, []struct {
		resolvedTs model.Ts
		row        *model.RowChangedEvent
	}{
		{resolvedTs: 2},
	})
	sink.Reset()
	require.Equal(t, uint64(2), node.ResolvedTs())
	require.Equal(t, uint64(2), node.CheckpointTs())
}

func TestIgnoreEmptyRowChangeEvent(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test-ignore-empty-row-change-event",
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		},
	})
	sink := &mockSink{}
	node := newSinkNode(1, sink, 0, 10, &mockFlowController{})
	require.Nil(t, node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))

	// empty row, no Columns and PreColumns.
	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}, Row: &model.RowChangedEvent{CommitTs: 1}}), nil)))
	require.Equal(t, 0, len(node.rowBuffer))
}

func TestSplitUpdateEventWhenEnableOldValue(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test-split-update-event",
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  config.GetDefaultReplicaConfig(),
		},
	})
	sink := &mockSink{}
	node := newSinkNode(1, sink, 0, 10, &mockFlowController{})
	require.Nil(t, node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))

	// nil row.
	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}), nil)))
	require.Equal(t, 0, len(node.rowBuffer))

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
	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(
		ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{
			CRTs:  1,
			RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
			Row:   &model.RowChangedEvent{CommitTs: 1, Columns: columns, PreColumns: preColumns},
		}), nil)))
	require.Equal(t, 1, len(node.rowBuffer))
	require.Equal(t, 2, len(node.rowBuffer[0].Columns))
	require.Equal(t, 2, len(node.rowBuffer[0].PreColumns))
}

func TestSplitUpdateEventWhenDisableOldValue(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	cfg := config.GetDefaultReplicaConfig()
	cfg.EnableOldValue = false
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test-split-update-event",
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  cfg,
		},
	})
	sink := &mockSink{}
	node := newSinkNode(1, sink, 0, 10, &mockFlowController{})
	require.Nil(t, node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))

	// nil row.
	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}), nil)))
	require.Equal(t, 0, len(node.rowBuffer))

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

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(
		ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{
			CRTs:  1,
			RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
			Row:   &model.RowChangedEvent{CommitTs: 1, Columns: columns, PreColumns: preColumns},
		}), nil)))
	require.Equal(t, 1, len(node.rowBuffer))
	require.Equal(t, 2, len(node.rowBuffer[0].Columns))
	require.Equal(t, 0, len(node.rowBuffer[0].PreColumns))

	// Cleanup.
	node.rowBuffer = []*model.RowChangedEvent{}
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

	require.Nil(t, node.Receive(pipeline.MockNodeContext4Test(
		ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{
			CRTs:  1,
			RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
			Row:   &model.RowChangedEvent{CommitTs: 1, Columns: columns, PreColumns: preColumns},
		}), nil)))
	// Split an update event into a delete and an insert event.
	require.Equal(t, 2, len(node.rowBuffer))

	deleteEventIndex := 0
	require.Equal(t, 0, len(node.rowBuffer[deleteEventIndex].Columns))
	require.Equal(t, 2, len(node.rowBuffer[deleteEventIndex].PreColumns))
	nonHandleKeyColIndex := 0
	handleKeyColIndex := 1
	// NOTICE: When old value disabled, we only keep the handle key pre cols.
	require.Nil(t, node.rowBuffer[deleteEventIndex].PreColumns[nonHandleKeyColIndex])
	require.Equal(t, "col2", node.rowBuffer[deleteEventIndex].PreColumns[handleKeyColIndex].Name)
	require.True(t, node.rowBuffer[deleteEventIndex].PreColumns[handleKeyColIndex].Flag.IsHandleKey())

	insertEventIndex := 1
	require.Equal(t, 2, len(node.rowBuffer[insertEventIndex].Columns))
	require.Equal(t, 0, len(node.rowBuffer[insertEventIndex].PreColumns))
}

type flushFlowController struct {
	mockFlowController
	releaseCounter int
}

func (c *flushFlowController) Release(resolvedTs uint64) {
	c.releaseCounter++
}

type flushSink struct {
	mockSink
}

// use to simulate the situation that resolvedTs return from sink manager
// fall back
var fallBackResolvedTs = uint64(10)

func (s *flushSink) FlushRowChangedEvents(ctx context.Context, _ model.TableID, resolvedTs uint64) (uint64, error) {
	if resolvedTs == fallBackResolvedTs {
		return 0, nil
	}
	return resolvedTs, nil
}

// TestFlushSinkReleaseFlowController tests sinkNode.flushSink method will always
// call flowController.Release to release the memory quota of the table to avoid
// deadlock if there is no error occur
func TestFlushSinkReleaseFlowController(t *testing.T) {
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	cfg := config.GetDefaultReplicaConfig()
	cfg.EnableOldValue = false
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: "changefeed-id-test-flushSink",
		Info: &model.ChangeFeedInfo{
			StartTs: oracle.GoTimeToTS(time.Now()),
			Config:  cfg,
		},
	})
	flowController := &flushFlowController{}
	sink := &flushSink{}
	// sNode is a sinkNode
	sNode := newSinkNode(1, sink, 0, 10, flowController)
	require.Nil(t, sNode.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)))
	sNode.barrierTs = 10

	err := sNode.flushSink(context.Background(), uint64(8))
	require.Nil(t, err)
	require.Equal(t, uint64(8), sNode.checkpointTs)
	require.Equal(t, 1, flowController.releaseCounter)
	// resolvedTs will fall back in this call
	err = sNode.flushSink(context.Background(), uint64(10))
	require.Nil(t, err)
	require.Equal(t, uint64(8), sNode.checkpointTs)
	require.Equal(t, 2, flowController.releaseCounter)
}
