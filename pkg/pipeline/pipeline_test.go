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
	stdCtx "context"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"go.uber.org/zap"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type pipelineSuite struct{}

var _ = check.Suite(&pipelineSuite{})

type echoNode struct {
}

func (e echoNode) Init(ctx NodeContext) error {
	ctx.SendToNextNode(PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "init function is called in echo node",
			},
		},
	}))
	return nil
}

func (e echoNode) Receive(ctx NodeContext) error {
	msg := ctx.Message()
	log.Info("Receive message in echo node", zap.Any("msg", msg))
	ctx.SendToNextNode(msg)
	ctx.SendToNextNode(PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "ECHO: " + msg.PolymorphicEvent.Row.Table.Schema,
				Table:  "ECHO: " + msg.PolymorphicEvent.Row.Table.Table,
			},
		},
	}))
	return nil
}

func (e echoNode) Destroy(ctx NodeContext) error {
	ctx.SendToNextNode(PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "destory function is called in echo node",
			},
		},
	}))
	return nil
}

type checkNode struct {
	c        *check.C
	expected []*Message
	index    int
}

func (n *checkNode) Init(ctx NodeContext) error {
	// do nothing
	return nil
}

func (n *checkNode) Receive(ctx NodeContext) error {
	msg := ctx.Message()

	log.Info("Receive message in check node", zap.Any("msg", msg))
	n.c.Assert(msg, check.DeepEquals, n.expected[n.index], check.Commentf("index: %d", n.index))
	n.index++
	return nil
}

func (n *checkNode) Destroy(ctx NodeContext) error {
	n.c.Assert(n.index, check.Equals, len(n.expected))
	return nil
}

func (s *pipelineSuite) TestPipelineUsage(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.NewContext(stdCtx.Background(), &context.Vars{})
	ctx, cancel := context.WithCancel(ctx)
	ctx, p := NewPipeline(ctx)
	p.AppendNode(ctx, "echo node", echoNode{})
	p.AppendNode(ctx, "check node", &checkNode{
		c: c,
		expected: []*Message{
			PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "init function is called in echo node",
					},
				},
			}),
			PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "I am built by test function",
						Table:  "AA1",
					},
				},
			}),
			PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "ECHO: I am built by test function",
						Table:  "ECHO: AA1",
					},
				},
			}),
			PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "I am built by test function",
						Table:  "BB2",
					},
				},
			}),
			PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "ECHO: I am built by test function",
						Table:  "ECHO: BB2",
					},
				},
			}),
			PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "destory function is called in echo node",
					},
				},
			}),
		},
	})

	err := p.SendToFirstNode(PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "AA1",
			},
		},
	}))
	c.Assert(err, check.IsNil)
	err = p.SendToFirstNode(PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "BB2",
			},
		},
	}))
	c.Assert(err, check.IsNil)
	cancel()
	errs := p.Wait()
	c.Assert(len(errs), check.Equals, 0)
}

type errorNode struct {
	c     *check.C
	index int
}

func (n *errorNode) Init(ctx NodeContext) error {
	// do nothing
	return nil
}

func (n *errorNode) Receive(ctx NodeContext) error {
	msg := ctx.Message()
	log.Info("Receive message in error node", zap.Any("msg", msg))
	n.index++
	if n.index >= 3 {
		return errors.Errorf("error node throw an error, index: %d", n.index)
	}
	ctx.SendToNextNode(msg)
	return nil
}

func (n *errorNode) Destroy(ctx NodeContext) error {
	n.c.Assert(n.index, check.Equals, 3)
	return nil
}

func (s *pipelineSuite) TestPipelineError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.NewContext(stdCtx.Background(), &context.Vars{})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx, p := NewPipeline(ctx)
	p.AppendNode(ctx, "echo node", echoNode{})
	p.AppendNode(ctx, "error node", &errorNode{c: c})
	p.AppendNode(ctx, "check node", &checkNode{
		c: c,
		expected: []*Message{
			PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "init function is called in echo node",
					},
				},
			}),
			PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "I am built by test function",
						Table:  "CC1",
					},
				},
			}),
		},
	})

	err := p.SendToFirstNode(PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "CC1",
			},
		},
	}))
	c.Assert(err, check.IsNil)
	// this line may be return an error because the pipeline maybe closed before this line was executed
	//nolint:errcheck
	p.SendToFirstNode(PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "DD2",
			},
		},
	}))
	errs := p.Wait()
	c.Assert(len(errs), check.Equals, 1)
	c.Assert(errs[0].Error(), check.Equals, "error node throw an error, index: 3")
}

type throwNode struct {
	c     *check.C
	index int
}

func (n *throwNode) Init(ctx NodeContext) error {
	// do nothing
	return nil
}

func (n *throwNode) Receive(ctx NodeContext) error {
	msg := ctx.Message()
	log.Info("Receive message in error node", zap.Any("msg", msg))
	n.index++
	if n.index >= 3 {
		ctx.Throw(errors.Errorf("error node throw an error, index: %d", n.index))
		return nil
	}
	ctx.SendToNextNode(msg)
	return nil
}

func (n *throwNode) Destroy(ctx NodeContext) error {
	n.c.Assert(n.index, check.Equals, 6)
	return nil
}

func (s *pipelineSuite) TestPipelineThrow(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.NewContext(stdCtx.Background(), &context.Vars{})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx, p := NewPipeline(ctx)
	p.AppendNode(ctx, "echo node", echoNode{})
	p.AppendNode(ctx, "error node", &throwNode{c: c})
	err := p.SendToFirstNode(PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "CC1",
			},
		},
	}))
	c.Assert(err, check.IsNil)
	// this line may be return an error because the pipeline maybe closed before this line was executed
	//nolint:errcheck
	p.SendToFirstNode(PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "DD2",
			},
		},
	}))
	errs := p.Wait()
	c.Assert(len(errs), check.Equals, 4)
	c.Assert(errs[0].Error(), check.Equals, "error node throw an error, index: 3")
	c.Assert(errs[1].Error(), check.Equals, "error node throw an error, index: 4")
	c.Assert(errs[2].Error(), check.Equals, "error node throw an error, index: 5")
	c.Assert(errs[3].Error(), check.Equals, "error node throw an error, index: 6")
}
