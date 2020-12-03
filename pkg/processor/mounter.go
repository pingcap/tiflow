package processor

import (
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/pkg/pipeline"
)

type mounterNode struct {
	mounter entry.Mounter
}

func (n *mounterNode) Init(ctx pipeline.Context) error {
	// do nothing
	return nil
}

// Receive receives the message from the previous node
func (n *mounterNode) Receive(ctx pipeline.Context) error {
	msg := ctx.Message()
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		select {
		case <-ctx.Done():
			return nil
		case n.mounter.Input() <- msg.PolymorphicEvent:
		}
	}
	ctx.SendToNextNode(msg)
	return nil
}

func (n *mounterNode) Destroy(ctx pipeline.Context) error {
	// do nothing
	return nil
}
