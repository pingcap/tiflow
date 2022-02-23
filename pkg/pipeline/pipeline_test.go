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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util/testleak"
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
	expected []Message
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
	ctx := context.NewContext(stdCtx.Background(), &context.GlobalVars{})
	ctx, cancel := context.WithCancel(ctx)
	ctx = context.WithErrorHandler(ctx, func(err error) error {
		c.Fatal(err)
		return err
	})
	runnersSize, outputChannelSize := 2, 64
	p := NewPipeline(ctx, -1, runnersSize, outputChannelSize)
	p.AppendNode(ctx, "echo node", echoNode{})
	p.AppendNode(ctx, "check node", &checkNode{
		c: c,
		expected: []Message{
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
	p.Wait()
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
	ctx := context.NewContext(stdCtx.Background(), &context.GlobalVars{})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx = context.WithErrorHandler(ctx, func(err error) error {
		c.Assert(err.Error(), check.Equals, "error node throw an error, index: 3")
		return nil
	})
	runnersSize, outputChannelSize := 3, 64
	p := NewPipeline(ctx, -1, runnersSize, outputChannelSize)
	p.AppendNode(ctx, "echo node", echoNode{})
	p.AppendNode(ctx, "error node", &errorNode{c: c})
	p.AppendNode(ctx, "check node", &checkNode{
		c: c,
		expected: []Message{
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
	p.Wait()
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
	n.c.Assert(map[int]bool{4: true, 6: true}, check.HasKey, n.index)
	return nil
}

func (s *pipelineSuite) TestPipelineThrow(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.NewContext(stdCtx.Background(), &context.GlobalVars{})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var errs []error
	ctx = context.WithErrorHandler(ctx, func(err error) error {
		errs = append(errs, err)
		return nil
	})
	runnersSize, outputChannelSize := 2, 64
	p := NewPipeline(ctx, -1, runnersSize, outputChannelSize)
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
	// whether err is nil is not determined
	// If add some delay here, such as sleep 50ms, there will be more probability
	// that the second message is not sent.
	// time.Sleep(time.Millisecond * 50)
	err = p.SendToFirstNode(PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "DD2",
			},
		},
	}))
	if err != nil {
		// pipeline closed before the second message was sent
		c.Assert(cerror.ErrSendToClosedPipeline.Equal(err), check.IsTrue)
		p.Wait()
		c.Assert(len(errs), check.Equals, 2)
		c.Assert(errs[0].Error(), check.Equals, "error node throw an error, index: 3")
		c.Assert(errs[1].Error(), check.Equals, "error node throw an error, index: 4")
	} else {
		// the second message was sent before pipeline closed
		p.Wait()
		c.Assert(len(errs), check.Equals, 4)
		c.Assert(errs[0].Error(), check.Equals, "error node throw an error, index: 3")
		c.Assert(errs[1].Error(), check.Equals, "error node throw an error, index: 4")
		c.Assert(errs[2].Error(), check.Equals, "error node throw an error, index: 5")
		c.Assert(errs[3].Error(), check.Equals, "error node throw an error, index: 6")
	}
}

func (s *pipelineSuite) TestPipelineAppendNode(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.NewContext(stdCtx.Background(), &context.GlobalVars{})
	ctx, cancel := context.WithCancel(ctx)
	ctx = context.WithErrorHandler(ctx, func(err error) error {
		c.Fatal(err)
		return err
	})
	runnersSize, outputChannelSize := 2, 64
	p := NewPipeline(ctx, -1, runnersSize, outputChannelSize)
	err := p.SendToFirstNode(PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "CC1",
			},
		},
	}))
	c.Assert(err, check.IsNil)
	err = p.SendToFirstNode(PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "DD2",
			},
		},
	}))
	c.Assert(err, check.IsNil)
	p.AppendNode(ctx, "echo node", echoNode{})
	// wait the echo node sent all messages to next node
	time.Sleep(1 * time.Second)

	p.AppendNode(ctx, "check node", &checkNode{
		c: c,
		expected: []Message{
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
			PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "ECHO: I am built by test function",
						Table:  "ECHO: CC1",
					},
				},
			}),
			PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "I am built by test function",
						Table:  "DD2",
					},
				},
			}),
			PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "ECHO: I am built by test function",
						Table:  "ECHO: DD2",
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

	cancel()
	p.Wait()
}

type panicNode struct {
}

func (e panicNode) Init(ctx NodeContext) error {
	panic("panic in panicNode")
}

func (e panicNode) Receive(ctx NodeContext) error {
	return nil
}

func (e panicNode) Destroy(ctx NodeContext) error {
	return nil
}

func (s *pipelineSuite) TestPipelinePanic(c *check.C) {
	defer testleak.AfterTest(c)()
	// why skip this test?
	// this test is panic expected, but the panic is not happened at the main goroutine.
	// so we can't recover the panic through the defer code block at the main goroutine.
	// the c.ExpectFailure() is not warking cause the same reason.
	c.Skip("this test should be panic")
	defer func() {
		panicInfo := recover().(string)
		c.Assert(panicInfo, check.Equals, "panic in panicNode")
	}()
	ctx := context.NewContext(stdCtx.Background(), &context.GlobalVars{})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx = context.WithErrorHandler(ctx, func(err error) error {
		c.Fatal(err)
		return err
	})
	ctx = context.WithErrorHandler(ctx, func(err error) error {
		return nil
	})
	runnersSize, outputChannelSize := 1, 64
	p := NewPipeline(ctx, -1, runnersSize, outputChannelSize)
	p.AppendNode(ctx, "panic", panicNode{})
	p.Wait()
}

type forward struct {
	ch chan Message
}

func (n *forward) Init(ctx NodeContext) error {
	return nil
}

func (n *forward) Receive(ctx NodeContext) error {
	m := ctx.Message()
	if n.ch != nil {
		n.ch <- m
	} else {
		ctx.SendToNextNode(m)
	}
	return nil
}

func (n *forward) Destroy(ctx NodeContext) error {
	return nil
}

// Run the benchmark
// go test -benchmem -run='^$' -bench '^(BenchmarkPipeline)$' github.com/pingcap/tiflow/pkg/pipeline
func BenchmarkPipeline(b *testing.B) {
	ctx := context.NewContext(stdCtx.Background(), &context.GlobalVars{})
	runnersSize, outputChannelSize := 2, 64

	b.Run("BenchmarkPipeline", func(b *testing.B) {
		for i := 1; i <= 8; i++ {
			ctx, cancel := context.WithCancel(ctx)
			ctx = context.WithErrorHandler(ctx, func(err error) error {
				b.Fatal(err)
				return err
			})

			ch := make(chan Message)
			p := NewPipeline(ctx, -1, runnersSize, outputChannelSize)
			for j := 0; j < i; j++ {
				if (j + 1) == i {
					// The last node
					p.AppendNode(ctx, "forward node", &forward{ch: ch})
				} else {
					p.AppendNode(ctx, "forward node", &forward{})
				}
			}

			b.ResetTimer()
			b.Run(fmt.Sprintf("%d node(s)", i), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					err := p.SendToFirstNode(BarrierMessage(1))
					if err != nil {
						b.Fatal(err)
					}
					<-ch
				}
			})
			b.StopTimer()
			cancel()
			p.Wait()
		}
	})
}
