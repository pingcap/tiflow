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
	"log"
	"sync"
	"time"

	"github.com/edwingeng/deque"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	waitEventMountedBatchSize = 1024
)

type mounterNode struct {
	mu    sync.Mutex
	queue deque.Deque

	wg     errgroup.Group
	cancel context.CancelFunc

	notifier notify.Notifier
	rl       *rate.Limiter
}

func newMounterNode() pipeline.Node {
	return &mounterNode{
		queue: deque.NewDeque(),
		rl:    rate.NewLimiter(20.0, 1),
	}
}

func (n *mounterNode) Init(ctx pipeline.NodeContext) error {
	stdCtx, cancel := context.WithCancel(ctx)
	n.cancel = cancel

	receiver, err := n.notifier.NewReceiver(time.Millisecond * 100)
	if err != nil {
		log.Panic("unexpected error", zap.Error(err))
	}
	defer receiver.Stop()

	n.wg.Go(func() error {
		for {
			select {
			case <-stdCtx.Done():
				return nil
			case <-receiver.C:
				for {
					n.mu.Lock()
					msgs := n.queue.PopManyFront(waitEventMountedBatchSize)
					n.mu.Unlock()
					if len(msgs) == 0 {
						break // inner loop
					}

					for _, msg := range msgs {
						msg := msg.(*pipeline.Message)
						if msg.Tp != pipeline.MessageTypePolymorphicEvent {
							// sends the control message directly to the next node
							ctx.SendToNextNode(msg)
							continue
						}

						// handles PolymorphicEvents
						event := msg.PolymorphicEvent
						if event.RawKV.OpType != model.OpTypeResolved {
							err := event.WaitPrepare(stdCtx)
							if err != nil {
								ctx.Throw(err)
								return nil
							}
						}

						ctx.SendToNextNode(msg)
					}
				}
			}
		}
	})

	return nil
}

// Receive receives the message from the previous node
func (n *mounterNode) Receive(ctx pipeline.NodeContext) error {
	msg := ctx.Message()
	n.queue.PushBack(msg)

	if n.rl.Allow() {
		n.notifier.Notify()
	}
	return nil
}

func (n *mounterNode) Destroy(ctx pipeline.NodeContext) error {
	defer n.notifier.Close()
	n.cancel()
	return n.wg.Wait()
}
