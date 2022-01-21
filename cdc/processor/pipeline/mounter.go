// Copyright 2021 PingCAP, Inc.
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
	"time"

	"github.com/edwingeng/deque"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/notify"
	"github.com/pingcap/tiflow/pkg/pipeline"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	waitEventMountedBatchSize = 128 * 1024 // larger means less CPU overhead used by `select` but more memory overhead.
	maxNotificationsPerSecond = 10.0       // larger means lower latency, but more CPU used by `select`.
)

// mounterNode is now used to buffer unmounted events.
// TODO rename this node, or refactor the mounter to make it synchronous.
type mounterNode struct {
	mu    sync.Mutex
	queue deque.Deque // we use Deque for better memory consumption and support for batching

	wg     errgroup.Group
	cancel context.CancelFunc

	// notifies new events pushed to the queue
	notifier notify.Notifier
	// limits the rate at which notifications are sent
	rl *rate.Limiter
}

func newMounterNode() pipeline.Node {
	return &mounterNode{
		queue: deque.NewDeque(),
		rl:    rate.NewLimiter(maxNotificationsPerSecond, 1 /* burst */),
	}
}

func (n *mounterNode) Init(ctx pipeline.NodeContext) error {
	stdCtx, cancel := context.WithCancel(ctx)
	n.cancel = cancel

	receiver, err := n.notifier.NewReceiver(time.Millisecond * 100)
	if err != nil {
		log.Panic("unexpected error", zap.Error(err))
	}

	n.wg.Go(func() error {
		defer receiver.Stop()
		for {
			select {
			case <-stdCtx.Done():
				return nil
			case <-receiver.C:
				// handles writes to the queue
				for {
					n.mu.Lock()
					msgs := n.queue.PopManyFront(waitEventMountedBatchSize)
					n.mu.Unlock()
					if len(msgs) == 0 {
						break // inner loop
					}

					for _, msg := range msgs {
						msg := msg.(pipeline.Message)
						if msg.Tp != pipeline.MessageTypePolymorphicEvent {
							// sends the control message directly to the next node
							ctx.SendToNextNode(msg)
							continue // to handling the next message
						}

						// handles PolymorphicEvents
						event := msg.PolymorphicEvent
						if event.RawKV.OpType != model.OpTypeResolved {
							failpoint.Inject("MounterNodeWaitPrepare", func() {})
							// only RowChangedEvents need mounting
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
	n.mu.Lock()
	n.queue.PushBack(msg)
	n.mu.Unlock()

	if n.rl.Allow() {
		// send notification under the rate limiter
		n.notifier.Notify()
	}
	return nil
}

func (n *mounterNode) Destroy(ctx pipeline.NodeContext) error {
	defer n.notifier.Close()
	n.cancel()
	return n.wg.Wait()
}
