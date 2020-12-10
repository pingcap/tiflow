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

package processor

import (
	stdContext "context"
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/context"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type safeStopperSuite struct{}

var _ = check.Suite(&safeStopperSuite{})

func (s *safeStopperSuite) TestSafeStopper(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		input          []*pipeline.Message
		expectedOutput []*pipeline.Message
		targetTs       model.Ts
		err            *errors.Error
		msgCauseErr    *pipeline.Message
	}{
		// test command should stop
		{
			input: []*pipeline.Message{
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 3, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 4, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 6, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeShouldStop}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 8, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 10, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 11, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
			},
			expectedOutput: []*pipeline.Message{
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 3, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 4, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 6, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 8, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStopped}),
			},
			targetTs:    math.MaxUint64,
			err:         cerrors.ErrTableProcessorStoppedSafely,
			msgCauseErr: pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
		},
		// test command should stop with disorderly event
		{
			input: []*pipeline.Message{
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 3, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 4, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 6, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeShouldStop}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 11, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 10, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 11, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
			},
			expectedOutput: []*pipeline.Message{
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 3, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 4, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 6, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 11, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStopped}),
			},
			targetTs:    math.MaxUint64,
			err:         cerrors.ErrTableProcessorStoppedSafely,
			msgCauseErr: pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
		},
		// test targetTs
		{
			input: []*pipeline.Message{
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 3, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 4, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 6, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 8, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 10, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 11, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
			},
			expectedOutput: []*pipeline.Message{
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 3, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 4, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 6, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{CRTs: 7, OpType: model.OpTypeResolved}}),
				pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStopped}),
			},
			targetTs:    7,
			err:         cerrors.ErrTableProcessorStoppedSafely,
			msgCauseErr: pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
		},
		// test targetTs with disorderly event
		{
			input: []*pipeline.Message{
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 3, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 8, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 8, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 10, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 11, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
			},
			expectedOutput: []*pipeline.Message{
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 3, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}}),
				pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 7, RawKV: &model.RawKVEntry{CRTs: 7, OpType: model.OpTypeResolved}}),
				pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStopped}),
			},
			targetTs:    7,
			err:         cerrors.ErrTableProcessorStoppedSafely,
			msgCauseErr: pipeline.PolymorphicEventMessage(&model.PolymorphicEvent{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}}),
		},
	}
	for _, tc := range testCases {
		ctx := context.NewContext(stdContext.Background(), &context.Vars{})
		node := newSafeStopperNode(tc.targetTs)
		outputCh := make(chan *pipeline.Message, 128)
		c.Assert(node.Init(pipeline.MockNodeContext4Test(ctx, nil, outputCh)), check.IsNil)
		msg, err := pipeline.SendMessageToNode4Test(ctx, node, tc.input, outputCh)
		close(outputCh)
		c.Assert(msg, check.DeepEquals, tc.msgCauseErr)
		c.Assert(tc.err.Equal(err), check.IsTrue)
		c.Assert(node.Destroy(pipeline.MockNodeContext4Test(ctx, nil, outputCh)), check.IsNil)
		i := 0
		for msg := range outputCh {
			c.Assert(msg, check.DeepEquals, tc.expectedOutput[i],
				check.Commentf("%s", cmp.Diff(msg, tc.expectedOutput[i], cmp.AllowUnexported(model.PolymorphicEvent{}))))
			i++
		}
		c.Assert(i, check.Equals, len(tc.expectedOutput))
	}
}
