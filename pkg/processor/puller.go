package processor

import (
	"context"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pkg/errors"
)

type pullerNode struct {
	tableID     model.TableID
	replicaInfo *model.TableReplicaInfo
	cancel      context.CancelFunc
}

func newPullerNode(tableID model.TableID, replicaInfo *model.TableReplicaInfo) *pullerNode {
	return &pullerNode{
		tableID:     tableID,
		replicaInfo: replicaInfo,
	}
}

func (n *pullerNode) Init(ctx pipeline.Context) error {
	// start table puller
	enableOldValue := ctx.ReplicaConfig().EnableOldValue
	span := regionspan.GetTableSpan(n.tableID, enableOldValue)

	plr := puller.NewPuller(p.pdCli, p.credential, kvStorage, n.replicaInfo.StartTs, []regionspan.Span{span}, p.limitter, enableOldValue)
	ctxC, cancel := context.WithCancel(ctx.StdContext())
	go func() {
		err := plr.Run(ctxC)
		if errors.Cause(err) != context.Canceled {
			ctx.Throw(err)
		}
	}()
	//todo read puller.output
	go func() {
		for {
			select {
			case <-ctxC.Done():
				if errors.Cause(ctx.Err()) != context.Canceled {
					ctx.Throw(err)
				}
				return
			case rawKV := <-plr.Output():
				if rawKV == nil {
					continue
				}
				pEvent := model.NewPolymorphicEvent(rawKV)

			}
		}
	}
	n.cancel = cancel
}

// Receive receives the message from the previous node
func (n *pullerNode) Receive(ctx pipeline.Context) error {
	panic("not implemented") // TODO: Implement
}

func (n *pullerNode) Destroy(ctx pipeline.Context) error {
	n.cancel()
}
