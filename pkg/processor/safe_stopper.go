package processor

import (
	"errors"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/pipeline"
)

type safeStopperNode struct {
	targetTs   model.Ts
	shouldStop bool
}

func (n *safeStopperNode) Init(ctx pipeline.Context) error {
	// do nothing
	return nil
}

// Receive receives the message from the previous node
func (n *safeStopperNode) Receive(ctx pipeline.Context) error {
	msg := ctx.Message()
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		if n.handlePolymorphicEvent(ctx, msg.PolymorphicEvent) {
			ctx.SendToNextNode(pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStopped}))
			return errors.New("") // TODO using pkg error
		}
	case pipeline.MessageTypeCommand:
		if msg.Command.Tp == pipeline.CommandTypeShouldStop {
			n.shouldStop = true
			return nil
		}
		ctx.SendToNextNode(msg)
	default:
		ctx.SendToNextNode(msg)
	}
	return nil
}

func (n *safeStopperNode) handlePolymorphicEvent(ctx pipeline.Context, event *model.PolymorphicEvent) (shouldExit bool) {
	if event == nil {
		return false
	}
	// for every event which of TS more than `targetTs`, it should not to send to next nodes.
	if event.CRTs > n.targetTs {
		// when we reveice the the first resolved event which of TS more than `targetTs`,
		// we can ensure that any events which of TS less than or equal to `targetTs` are sent to next nodes
		// so we can send a resolved event and exit this node.
		if event.RawKV.OpType == model.OpTypeResolved {
			ctx.SendToNextNode(pipeline.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, n.targetTs)))
			return true
		}
		return false
	}
	ctx.SendToNextNode(pipeline.PolymorphicEventMessage(event))
	if n.shouldStop && event.RawKV.OpType == model.OpTypeResolved {
		return true
	}
	return false
}

func (n *safeStopperNode) Destroy(ctx pipeline.Context) error {
	// do nothing
	return nil
}
