package processor

import (
	"sync/atomic"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/pipeline"
)

type outputNode struct {
	outputCh   chan *model.PolymorphicEvent
	resolvedTs *model.Ts
	stopped    *int32
}

func (n *outputNode) Init(ctx pipeline.Context) error {
	// do nothing
	return nil
}

// Receive receives the message from the previous node
func (n *outputNode) Receive(ctx pipeline.Context) error {
	msg := ctx.Message()
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		if msg.PolymorphicEvent.RawKV.OpType == model.OpTypeResolved {
			atomic.StoreUint64(n.resolvedTs, msg.PolymorphicEvent.CRTs)
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case n.outputCh <- msg.PolymorphicEvent:
		}
	case pipeline.MessageTypeCommand:
		if msg.Command.Tp == pipeline.CommandTypeStopped {
			atomic.StoreInt32(n.stopped, 1)
		}
	}
	return nil
}

func (n *outputNode) Destroy(ctx pipeline.Context) error {
	// do nothing
	return nil
}
