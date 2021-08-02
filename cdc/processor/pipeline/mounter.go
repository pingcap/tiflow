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
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/pkg/pipeline"
)

type mounterNode struct {
	mounter entry.Mounter
}

func newMounterNode(mounter entry.Mounter) pipeline.Node {
	return &mounterNode{
		mounter: mounter,
	}
}

func (n *mounterNode) Init(ctx pipeline.NodeContext) error {
<<<<<<< HEAD
	// do nothing
=======
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

>>>>>>> c60bdb68 (pipeline: use Message value to reduce GC pressure (#2415))
	return nil
}

// Receive receives the message from the previous node
func (n *mounterNode) Receive(ctx pipeline.NodeContext) error {
	msg := ctx.Message()
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		msg.PolymorphicEvent.SetUpFinishedChan()
		select {
		case <-ctx.Done():
			return nil
		case n.mounter.Input() <- msg.PolymorphicEvent:
		}
	}
	ctx.SendToNextNode(msg)
	return nil
}

func (n *mounterNode) Destroy(ctx pipeline.NodeContext) error {
	// do nothing
	return nil
}
