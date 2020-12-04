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
	"errors"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"go.uber.org/zap"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type pipelineSuite struct{}

var _ = check.Suite(&pipelineSuite{})

type echoNode struct {
	name string
}

func (e echoNode) Receive(ctx Context) error {
	msg := ctx.Message()
	log.Info("Receive message in echo node", zap.Any("msg", msg))
	if msg.Tp == MessageTypeLifecycle {
		return nil
	}
	ctx.SendToNextNode(msg)
	ctx.SendToNextNode((&Message{}).SetRowChangedEvent(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: msg.RowChangedEvent.Table.Schema + ", " + e.name,
			Table:  msg.RowChangedEvent.Table.Table,
		},
	}))
	return nil
}

func (s *pipelineSuite) TestPipelineUsage(c *check.C) {
	defer testleak.AfterTest(c)()
	expected := []*Message{
		(&Message{}).SetLifecycleMessage((&LifecycleMessage{}).SetStarted()),
		(&Message{}).SetRowChangedEvent(&model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "AA1",
			},
		}),
		(&Message{}).SetRowChangedEvent(&model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function, echo node",
				Table:  "AA1",
			},
		}),
		(&Message{}).SetRowChangedEvent(&model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "BB2",
			},
		}),
		(&Message{}).SetRowChangedEvent(&model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function, echo node",
				Table:  "BB2",
			},
		}),
		(&Message{}).SetLifecycleMessage((&LifecycleMessage{}).SetStopped()),
	}
	index := 0

	ctx := NewRootContext(&config.ReplicaConfig{})
	p := NewPipeline(ctx)
	p.AppendNode(ctx, "echo node", echoNode{name: "echo node"})
	p.AppendNode(ctx, "func node", NodeFunc(func(ctx Context) error {
		msg := ctx.Message()
		log.Info("Receive message in func node", zap.Any("msg", msg))
		c.Assert(msg, check.DeepEquals, expected[index])
		index++
		return nil
	}))

	p.SendToFirstNode((&Message{}).SetRowChangedEvent(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "I am built by test function",
			Table:  "AA1",
		},
	}))
	p.SendToFirstNode((&Message{}).SetRowChangedEvent(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "I am built by test function",
			Table:  "BB2",
		},
	}))
	ctx.Cancel()
	errs := p.Wait()
	c.Assert(len(errs), check.Equals, 0)
	c.Assert(index, check.Equals, len(expected))
}

func (s *pipelineSuite) TestPipelineError(c *check.C) {
	defer testleak.AfterTest(c)()
	expected := []*Message{
		(&Message{}).SetLifecycleMessage((&LifecycleMessage{}).SetStarted()),
		(&Message{}).SetRowChangedEvent(&model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "AA1",
			},
		}),
		(&Message{}).SetRowChangedEvent(&model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function, echo node2",
				Table:  "AA1",
			},
		}),
		(&Message{}).SetRowChangedEvent(&model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function, echo node1",
				Table:  "AA1",
			},
		}),
		(&Message{}).SetRowChangedEvent(&model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function, echo node1, echo node2",
				Table:  "AA1",
			},
		}),
		(&Message{}).SetLifecycleMessage((&LifecycleMessage{}).SetStopped()),
	}
	index := 0

	errIndex := 0
	expectedError := errors.New("error from node")
	ctx := NewRootContext(&config.ReplicaConfig{})
	p := NewPipeline(ctx)
	p.AppendNode(ctx, "echo node1", echoNode{name: "echo node1"})
	p.AppendNode(ctx, "error node", NodeFunc(func(ctx Context) error {
		if ctx.Message().Tp == MessageTypeLifecycle {
			return nil
		}
		errIndex++
		if errIndex > 2 {
			return expectedError
		}
		ctx.SendToNextNode(ctx.Message())
		return nil
	}))
	p.AppendNode(ctx, "echo node2", echoNode{name: "echo node2"})
	p.AppendNode(ctx, "assert node", NodeFunc(func(ctx Context) error {
		log.Info("Receive message in assert node", zap.Any("msg", ctx.Message()))
		c.Assert(ctx.Message(), check.DeepEquals, expected[index])
		index++
		return nil
	}))

	p.SendToFirstNode((&Message{}).SetRowChangedEvent(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "I am built by test function",
			Table:  "AA1",
		},
	}))
	p.SendToFirstNode((&Message{}).SetRowChangedEvent(&model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "I am built by test function",
			Table:  "BB2",
		},
	}))
	errs := p.Wait()
	c.Assert(errs, check.DeepEquals, []error{expectedError})
	c.Assert(index, check.Equals, len(expected))
	ctx.Cancel()
}
