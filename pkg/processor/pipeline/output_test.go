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

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type outputSuite struct{}

var _ = check.Suite(&outputSuite{})

func (s *outputSuite) TestStatus(c *check.C) {
	defer testleak.AfterTest(c)()
	outputCh := make(chan *model.PolymorphicEvent, 128)
	ctx := context.NewContext(stdContext.Background(), &context.Vars{})
	var status TableStatus
	node := newOutputNode(outputCh, &status, func(model.Ts) {})
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, nil, nil)), check.IsNil)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}), nil)), check.IsNil)
	c.Assert(status, check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}), nil)), check.IsNil)
	c.Assert(status, check.Equals, TableStatusInitializing)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)), check.IsNil)
	c.Assert(status, check.Equals, TableStatusRunning)

	status = TableStatusStopping

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 4, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)), check.IsNil)
	c.Assert(status, check.Equals, TableStatusStopping)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStopped}), nil)), check.IsNil)
	c.Assert(status, check.Equals, TableStatusStopped)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)), check.IsNil)
	c.Assert(status, check.Equals, TableStatusStopped)

	c.Assert(node.Destroy(pipeline.MockNodeContext4Test(ctx, nil, nil)), check.IsNil)
	c.Assert(status, check.Equals, TableStatusStopped)

	close(outputCh)
	expect := []*model.PolymorphicEvent{
		{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
	}
	i := 0
	for msg := range outputCh {
		c.Assert(msg, check.DeepEquals, expect[i])
		i++
	}
	c.Assert(i, check.Equals, len(expect))
}

func (s *outputSuite) TestResolvedListener(c *check.C) {
	defer testleak.AfterTest(c)()
	outputCh := make(chan *model.PolymorphicEvent, 128)
	ctx := context.NewContext(stdContext.Background(), &context.Vars{})
	var status TableStatus
	exportResolvedEvent := []model.Ts{2, 4, 5}
	j := 0
	node := newOutputNode(outputCh, &status, func(ts model.Ts) {
		c.Assert(ts, check.Equals, exportResolvedEvent[j])
		j++
	})
	c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, nil, nil)), check.IsNil)

	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}), nil)), check.IsNil)
	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}), nil)), check.IsNil)
	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)), check.IsNil)
	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 4, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)), check.IsNil)
	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStopped}), nil)), check.IsNil)
	c.Assert(node.Receive(pipeline.MockNodeContext4Test(ctx,
		pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}), nil)), check.IsNil)
	c.Assert(node.Destroy(pipeline.MockNodeContext4Test(ctx, nil, nil)), check.IsNil)

	close(outputCh)
	expect := []*model.PolymorphicEvent{
		{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
	}
	i := 0
	for msg := range outputCh {
		c.Assert(msg, check.DeepEquals, expect[i])
		i++
	}
	c.Assert(i, check.Equals, len(expect))
	c.Assert(j, check.Equals, len(exportResolvedEvent))
}
