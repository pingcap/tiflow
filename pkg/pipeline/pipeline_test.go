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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type echoNode struct{}

func (e echoNode) Init(ctx NodeContext) error {
	ctx.SendToNextNode(pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
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
	ctx.SendToNextNode(pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
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
	ctx.SendToNextNode(pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "destroy function is called in echo node",
			},
		},
	}))
	return nil
}

type checkNode struct {
	t        *testing.T
	expected []pmessage.Message
	index    int
}

func (n *checkNode) Init(ctx NodeContext) error {
	// do nothing
	return nil
}

func (n *checkNode) Receive(ctx NodeContext) error {
	msg := ctx.Message()

	log.Info("Receive message in check node", zap.Any("msg", msg))
	require.Equal(n.t, n.expected[n.index], msg, "%d", n.index)
	n.index++
	return nil
}

func (n *checkNode) Destroy(ctx NodeContext) error {
	require.Equal(n.t, len(n.expected), n.index)
	return nil
}

func TestPipelineUsage(t *testing.T) {
	ctx := context.NewContext(stdCtx.Background(), &context.GlobalVars{})
	ctx, cancel := context.WithCancel(ctx)
	ctx = context.WithErrorHandler(ctx, func(err error) error {
		t.Fatal(err)
		return err
	})
	runnersSize, outputChannelSize := 2, 64
	p := NewPipeline(ctx, -1, runnersSize, outputChannelSize)
	p.AppendNode(ctx, "echo node", echoNode{})
	p.AppendNode(ctx, "check node", &checkNode{
		t: t,
		expected: []pmessage.Message{
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "init function is called in echo node",
					},
				},
			}),
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "I am built by test function",
						Table:  "AA1",
					},
				},
			}),
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "ECHO: I am built by test function",
						Table:  "ECHO: AA1",
					},
				},
			}),
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "I am built by test function",
						Table:  "BB2",
					},
				},
			}),
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "ECHO: I am built by test function",
						Table:  "ECHO: BB2",
					},
				},
			}),
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "destroy function is called in echo node",
					},
				},
			}),
		},
	})

	err := p.SendToFirstNode(pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "AA1",
			},
		},
	}))
	require.Nil(t, err)
	err = p.SendToFirstNode(pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "BB2",
			},
		},
	}))
	require.Nil(t, err)
	cancel()
	p.Wait()
}

type errorNode struct {
	t     *testing.T
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
	require.Equal(n.t, 3, n.index)
	return nil
}

func TestPipelineError(t *testing.T) {
	ctx := context.NewContext(stdCtx.Background(), &context.GlobalVars{})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx = context.WithErrorHandler(ctx, func(err error) error {
		require.Equal(t, "error node throw an error, index: 3", err.Error())
		return nil
	})
	runnersSize, outputChannelSize := 3, 64
	p := NewPipeline(ctx, -1, runnersSize, outputChannelSize)
	p.AppendNode(ctx, "echo node", echoNode{})
	p.AppendNode(ctx, "error node", &errorNode{t: t})
	p.AppendNode(ctx, "check node", &checkNode{
		t: t,
		expected: []pmessage.Message{
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "init function is called in echo node",
					},
				},
			}),
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "I am built by test function",
						Table:  "CC1",
					},
				},
			}),
		},
	})

	err := p.SendToFirstNode(pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "CC1",
			},
		},
	}))
	require.Nil(t, err)
	// this line may be return an error because the pipeline maybe closed before this line was executed
	//nolint:errcheck
	p.SendToFirstNode(pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
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
	t     *testing.T
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
	require.Contains(n.t, map[int]bool{4: true, 6: true}, n.index)
	return nil
}

func TestPipelineThrow(t *testing.T) {
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
	p.AppendNode(ctx, "error node", &throwNode{t: t})
	err := p.SendToFirstNode(pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "CC1",
			},
		},
	}))
	require.Nil(t, err)
	// whether err is nil is not determined
	// If add some delay here, such as sleep 50ms, there will be more probability
	// that the second message is not sent.
	// time.Sleep(time.Millisecond * 50)
	err = p.SendToFirstNode(pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "DD2",
			},
		},
	}))
	if err != nil {
		// pipeline closed before the second message was sent
		require.True(t, cerror.ErrSendToClosedPipeline.Equal(err))
		p.Wait()
		require.Equal(t, 2, len(errs))
		require.Equal(t, "error node throw an error, index: 3", errs[0].Error())
		require.Equal(t, "error node throw an error, index: 4", errs[1].Error())
	} else {
		// the second message was sent before pipeline closed
		p.Wait()
		require.Equal(t, 4, len(errs))
		require.Equal(t, "error node throw an error, index: 3", errs[0].Error())
		require.Equal(t, "error node throw an error, index: 4", errs[1].Error())
		require.Equal(t, "error node throw an error, index: 5", errs[2].Error())
		require.Equal(t, "error node throw an error, index: 6", errs[3].Error())
	}
}

func TestPipelineAppendNode(t *testing.T) {
	ctx := context.NewContext(stdCtx.Background(), &context.GlobalVars{})
	ctx, cancel := context.WithCancel(ctx)
	ctx = context.WithErrorHandler(ctx, func(err error) error {
		t.Fatal(err)
		return err
	})
	runnersSize, outputChannelSize := 2, 64
	p := NewPipeline(ctx, -1, runnersSize, outputChannelSize)
	err := p.SendToFirstNode(pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "CC1",
			},
		},
	}))
	require.Nil(t, err)
	err = p.SendToFirstNode(pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
		Row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "I am built by test function",
				Table:  "DD2",
			},
		},
	}))
	require.Nil(t, err)
	p.AppendNode(ctx, "echo node", echoNode{})
	// wait the echo node sent all messages to next node
	time.Sleep(1 * time.Second)

	p.AppendNode(ctx, "check node", &checkNode{
		t: t,
		expected: []pmessage.Message{
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "init function is called in echo node",
					},
				},
			}),
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "I am built by test function",
						Table:  "CC1",
					},
				},
			}),
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "ECHO: I am built by test function",
						Table:  "ECHO: CC1",
					},
				},
			}),
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "I am built by test function",
						Table:  "DD2",
					},
				},
			}),
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "ECHO: I am built by test function",
						Table:  "ECHO: DD2",
					},
				},
			}),
			pmessage.PolymorphicEventMessage(&model.PolymorphicEvent{
				Row: &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: "destroy function is called in echo node",
					},
				},
			}),
		},
	})

	cancel()
	p.Wait()
}

type forward struct {
	ch chan pmessage.Message
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

			ch := make(chan pmessage.Message)
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
					err := p.SendToFirstNode(pmessage.BarrierMessage(1))
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
