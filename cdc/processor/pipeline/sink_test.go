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

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pipeline"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

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

func (s *mockSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	panic("unreachable")
}

func (s *mockSink) Close(ctx context.Context) error {
	return nil
}

func (s *mockSink) Barrier(ctx context.Context, tableID model.TableID) error {
	return nil
}

func (s *mockSink) Check(c *check.C, expected []struct {
	resolvedTs model.Ts
	row        *model.RowChangedEvent
}) {
	c.Assert(s.received, check.DeepEquals, expected)
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

type outputSuite struct{}

var _ = check.Suite(&outputSuite{})

func (s *outputSuite) TestStatus(c *check.C) {
	defer testleak.AfterTest(c)()
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
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(20), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}, Row: &model.RowChangedEvent{}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}, Row: &model.RowChangedEvent{}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)

	err := node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 15, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil))
	c.Assert(cerrors.ErrTableProcessorStoppedSafely.Equal(err), check.IsTrue)
	c.Assert(node.Status(), check.Equals, TableStatusStopped)
	c.Assert(node.CheckpointTs(), check.Equals, uint64(10))

	// test the stop at ts command
	node = newSinkNode(1, &mockSink{}, 0, 10, &mockFlowController{})
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(20), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStop}), nil))
	c.Assert(cerrors.ErrTableProcessorStoppedSafely.Equal(err), check.IsTrue)
	c.Assert(node.Status(), check.Equals, TableStatusStopped)

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil))
	c.Assert(cerrors.ErrTableProcessorStoppedSafely.Equal(err), check.IsTrue)
	c.Assert(node.Status(), check.Equals, TableStatusStopped)
	c.Assert(node.CheckpointTs(), check.Equals, uint64(2))

	// test the stop at ts command is after then resolvedTs and checkpointTs is greater than stop ts
	node = newSinkNode(1, &mockSink{}, 0, 10, &mockFlowController{})
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(20), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStop}), nil))
	c.Assert(cerrors.ErrTableProcessorStoppedSafely.Equal(err), check.IsTrue)
	c.Assert(node.Status(), check.Equals, TableStatusStopped)

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil))
	c.Assert(cerrors.ErrTableProcessorStoppedSafely.Equal(err), check.IsTrue)
	c.Assert(node.Status(), check.Equals, TableStatusStopped)
	c.Assert(node.CheckpointTs(), check.Equals, uint64(7))
}

// TestStopStatus tests the table status of a pipeline is not set to stopped
// until the underlying sink is closed
func (s *outputSuite) TestStopStatus(c *check.C) {
	defer testleak.AfterTest(c)()
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
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)
	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// This will block until sink Close returns
		err := node.Receive(pipeline.MockNodeContext4Test(ctx,
			pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStop}), nil))
		c.Assert(cerrors.ErrTableProcessorStoppedSafely.Equal(err), check.IsTrue)
		c.Assert(node.Status(), check.Equals, TableStatusStopped)
	}()
	// wait to ensure stop message is sent to the sink node
	time.Sleep(time.Millisecond * 50)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)
	closeCh <- struct{}{}
	wg.Wait()
}

func (s *outputSuite) TestManyTs(c *check.C) {
	defer testleak.AfterTest(c)()
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
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
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
		}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
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
		}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}, Row: &model.RowChangedEvent{}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)

	sink.Check(c, nil)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(1), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)

	sink.Check(c, []struct {
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
	c.Assert(node.ResolvedTs(), check.Equals, uint64(2))
	c.Assert(node.CheckpointTs(), check.Equals, uint64(1))

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(5), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)
	sink.Check(c, []struct {
		resolvedTs model.Ts
		row        *model.RowChangedEvent
	}{
		{resolvedTs: 2},
	})
	sink.Reset()
	c.Assert(node.ResolvedTs(), check.Equals, uint64(2))
	c.Assert(node.CheckpointTs(), check.Equals, uint64(2))
}

func (s *outputSuite) TestIgnoreEmptyRowChangeEvent(c *check.C) {
	defer testleak.AfterTest(c)()
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
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)), check.IsNil)

	// empty row, no Columns and PreColumns.
	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}, Row: &model.RowChangedEvent{CommitTs: 1}}), nil)), check.IsNil)
	c.Assert(node.eventBuffer, check.HasLen, 0)
}

func (s *outputSuite) TestSplitUpdateEventWhenEnableOldValue(c *check.C) {
	defer testleak.AfterTest(c)()
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
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)), check.IsNil)

	// nil row.
	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}), nil)), check.IsNil)
	c.Assert(node.eventBuffer, check.HasLen, 0)

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
	c.Assert(node.Receive(pipeline.MockNodeContext4Test(
		ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{
			CRTs:  1,
			RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
			Row:   &model.RowChangedEvent{CommitTs: 1, Columns: columns, PreColumns: preColumns},
		}), nil)),
		check.IsNil,
	)
	c.Assert(node.eventBuffer, check.HasLen, 1)
	c.Assert(node.eventBuffer[0].Row.Columns, check.HasLen, 2)
	c.Assert(node.eventBuffer[0].Row.PreColumns, check.HasLen, 2)
}

func (s *outputSuite) TestSplitUpdateEventWhenDisableOldValue(c *check.C) {
	defer testleak.AfterTest(c)()
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
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)), check.IsNil)

	// nil row.
	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}), nil)), check.IsNil)
	c.Assert(node.eventBuffer, check.HasLen, 0)

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

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(
		ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{
			CRTs:  1,
			RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
			Row:   &model.RowChangedEvent{CommitTs: 1, Columns: columns, PreColumns: preColumns},
		}), nil)),
		check.IsNil,
	)
	c.Assert(node.eventBuffer, check.HasLen, 1)
	c.Assert(node.eventBuffer[0].Row.Columns, check.HasLen, 2)
	c.Assert(node.eventBuffer[0].Row.PreColumns, check.HasLen, 0)

	// Cleanup.
	node.eventBuffer = []*model.PolymorphicEvent{}
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

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(
		ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{
			CRTs:  1,
			RawKV: &model.RawKVEntry{OpType: model.OpTypePut},
			Row:   &model.RowChangedEvent{CommitTs: 1, Columns: columns, PreColumns: preColumns},
		}), nil)),
		check.IsNil,
	)
	// Split an update event into a delete and an insert event.
	c.Assert(node.eventBuffer, check.HasLen, 2)

	deleteEventIndex := 0
	c.Assert(node.eventBuffer[deleteEventIndex].Row.Columns, check.HasLen, 0)
	c.Assert(node.eventBuffer[deleteEventIndex].Row.PreColumns, check.HasLen, 2)
	nonHandleKeyColIndex := 0
	handleKeyColIndex := 1
	// NOTICE: When old value disabled, we only keep the handle key pre cols.
	c.Assert(node.eventBuffer[deleteEventIndex].Row.PreColumns[nonHandleKeyColIndex], check.IsNil)
	c.Assert(node.eventBuffer[deleteEventIndex].Row.PreColumns[handleKeyColIndex].Name, check.Equals, "col2")
	c.Assert(node.eventBuffer[deleteEventIndex].Row.PreColumns[handleKeyColIndex].Flag.IsHandleKey(), check.IsTrue)

	insertEventIndex := 1
	c.Assert(node.eventBuffer[insertEventIndex].Row.Columns, check.HasLen, 2)
	c.Assert(node.eventBuffer[insertEventIndex].Row.PreColumns, check.HasLen, 0)
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
func (s *outputSuite) TestFlushSinkReleaseFlowController(c *check.C) {
	defer testleak.AfterTest(c)()
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
	c.Assert(sNode.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil)), check.IsNil)
	sNode.barrierTs = 10

	cctx := pipeline.MockNodeContext4Test(nil, pipeline.TickMessage(), nil)
	err := sNode.flushSink(cctx, uint64(8))
	c.Assert(err, check.IsNil)
	c.Assert(sNode.checkpointTs, check.Equals, uint64(8))
	c.Assert(flowController.releaseCounter, check.Equals, 1)
	// resolvedTs will fall back in this call
	err = sNode.flushSink(cctx, uint64(10))
	c.Assert(err, check.IsNil)
	c.Assert(sNode.checkpointTs, check.Equals, uint64(8))
	c.Assert(flowController.releaseCounter, check.Equals, 2)
}
