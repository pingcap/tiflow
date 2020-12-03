package processor

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	tidbkv "github.com/pingcap/tidb/kv"
	"go.uber.org/zap"
)

type pullerNode struct {
	credential *security.Credential
	kvStorage  tidbkv.Storage
	limitter   *puller.BlurResourceLimitter

	tableID     model.TableID
	replicaInfo *model.TableReplicaInfo
	cancel      context.CancelFunc
}

func newPullerNode(
	credential *security.Credential,
	kvStorage tidbkv.Storage,
	limitter *puller.BlurResourceLimitter,
	tableID model.TableID, replicaInfo *model.TableReplicaInfo) pipeline.Node {
	return &pullerNode{
		credential:  credential,
		kvStorage:   kvStorage,
		limitter:    limitter,
		tableID:     tableID,
		replicaInfo: replicaInfo,
	}
}

func (n *pullerNode) Init(ctx pipeline.Context) error {
	// start table puller
	enableOldValue := ctx.Vars().Config.EnableOldValue
	spans := make([]regionspan.Span, 0, 2)
	spans = append(spans, regionspan.GetTableSpan(n.tableID, enableOldValue))

	if ctx.Vars().Config.Cyclic.IsEnabled() && n.replicaInfo.MarkTableID != 0 {
		spans = append(spans, regionspan.GetTableSpan(n.replicaInfo.MarkTableID, enableOldValue))
	}

	var tableName string
	err := retry.Run(time.Millisecond*5, 3, func() error {
		if name, ok := ctx.Vars().SchemaStorage.GetLastSnapshot().GetTableNameByID(n.tableID); ok {
			tableName = name.QuoteString()
			return nil
		}
		return errors.Errorf("failed to get table name, fallback to use table id: %d", n.tableID)
	})
	if err != nil {
		log.Warn("get table name for metric", zap.Error(err))
		tableName = strconv.Itoa(int(n.tableID))
	}

	plr := puller.NewPuller(ctx.Vars().PDClient, n.credential, n.kvStorage, n.replicaInfo.StartTs, spans, n.limitter, enableOldValue)
	ctxC, cancel := context.WithCancel(ctx)
	ctxC = util.PutTableInfoInCtx(ctxC, n.tableID, tableName)
	go func() {
		err := plr.Run(ctxC)
		if errors.Cause(err) != context.Canceled {
			ctx.Throw(err)
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case rawKV := <-plr.Output():
				if rawKV == nil {
					continue
				}
				pEvent := model.NewPolymorphicEvent(rawKV)
				ctx.SendToNextNode(pipeline.PolymorphicEventMessage(pEvent))
			}
		}
	}()
	n.cancel = cancel
	return nil
}

// Receive receives the message from the previous node
func (n *pullerNode) Receive(ctx pipeline.Context) error {
	// just forward any messages to the next node
	ctx.SendToNextNode(ctx.Message())
	return nil
}

func (n *pullerNode) Destroy(ctx pipeline.Context) error {
	n.cancel()
	return nil
}
