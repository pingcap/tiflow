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
	stdContext "context"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/context"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/pingcap/ticdc/pkg/util/testleak"
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

func (s *mockSink) Initialize(ctx stdContext.Context, tableInfo []*model.SimpleTableInfo) error {
	return nil
}

func (s *mockSink) EmitRowChangedEvents(ctx stdContext.Context, rows ...*model.RowChangedEvent) error {
	for _, row := range rows {
		s.received = append(s.received, struct {
			resolvedTs model.Ts
			row        *model.RowChangedEvent
		}{row: row})
	}
	return nil
}

func (s *mockSink) EmitDDLEvent(ctx stdContext.Context, ddl *model.DDLEvent) error {
	panic("unreachable")
}

func (s *mockSink) FlushRowChangedEvents(ctx stdContext.Context, resolvedTs uint64) (uint64, error) {
	s.received = append(s.received, struct {
		resolvedTs model.Ts
		row        *model.RowChangedEvent
	}{resolvedTs: resolvedTs})
	return resolvedTs, nil
}

func (s *mockSink) EmitCheckpointTs(ctx stdContext.Context, ts uint64) error {
	panic("unreachable")
}

func (s *mockSink) Close() error {
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

type outputSuite struct{}

var _ = check.Suite(&outputSuite{})

func (s *outputSuite) TestStatus(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.NewContext(stdContext.Background(), &context.Vars{})

	// test stop at targetTs
	node := newSinkNode(&mockSink{}, 0, 10)
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, nil, nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(20), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)

	err := node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 15, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil))
	c.Assert(cerrors.ErrTableProcessorStoppedSafely.Equal(err), check.IsTrue)
	c.Assert(node.Status(), check.Equals, TableStatusStopped)
	c.Assert(node.CheckpointTs(), check.Equals, uint64(10))

	// test the stop at ts command
	node = newSinkNode(&mockSink{}, 0, 10)
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, nil, nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(20), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStopAtTs, StoppedTs: 6}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil))
	c.Assert(cerrors.ErrTableProcessorStoppedSafely.Equal(err), check.IsTrue)
	c.Assert(node.Status(), check.Equals, TableStatusStopped)
	c.Assert(node.CheckpointTs(), check.Equals, uint64(6))

	// test the stop at ts command is after then resolvedTs and checkpointTs is greater than stop ts
	node = newSinkNode(&mockSink{}, 0, 10)
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, nil, nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(20), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStopAtTs, StoppedTs: 6}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)

	err = node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil))
	c.Assert(cerrors.ErrTableProcessorStoppedSafely.Equal(err), check.IsTrue)
	c.Assert(node.Status(), check.Equals, TableStatusStopped)
	c.Assert(node.CheckpointTs(), check.Equals, uint64(7))
}

func (s *outputSuite) TestManyTs(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.NewContext(stdContext.Background(), &context.Vars{})
	sink := &mockSink{}
	node := newSinkNode(sink, 0, 10)
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, nil, nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}, Row: &model.RowChangedEvent{CommitTs: 1}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}, Row: &model.RowChangedEvent{CommitTs: 2}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)

	sink.Check(c, nil)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx, pipeline.BarrierMessage(1), nil)), check.IsNil)
	c.Assert(node.Status(), check.Equals, TableStatusRunning)

	sink.Check(c, []struct {
		resolvedTs model.Ts
		row        *model.RowChangedEvent
	}{
		{row: &model.RowChangedEvent{CommitTs: 1}},
		{row: &model.RowChangedEvent{CommitTs: 2}},
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
