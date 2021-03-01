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
	stdContext "context"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"golang.org/x/sync/errgroup"
)

type pullerNode struct {
	credential *security.Credential
	kvStorage  tidbkv.Storage
	limitter   *puller.BlurResourceLimitter

	changefeedID model.ChangeFeedID
	tableName    string // quoted schema and table, used in metircs only

	tableID     model.TableID
	replicaInfo *model.TableReplicaInfo
	cancel      stdContext.CancelFunc
	wg          errgroup.Group
}

func newPullerNode(
	changefeedID model.ChangeFeedID,
	credential *security.Credential,
	kvStorage tidbkv.Storage,
	limitter *puller.BlurResourceLimitter,
	tableID model.TableID, replicaInfo *model.TableReplicaInfo, tableName string) pipeline.Node {
	return &pullerNode{
		credential:   credential,
		kvStorage:    kvStorage,
		limitter:     limitter,
		tableID:      tableID,
		replicaInfo:  replicaInfo,
		tableName:    tableName,
		changefeedID: changefeedID,
	}
}

func (n *pullerNode) tableSpan(ctx context.Context) []regionspan.Span {
	// start table puller
	enableOldValue := ctx.Vars().Config.EnableOldValue
	spans := make([]regionspan.Span, 0, 4)
	spans = append(spans, regionspan.GetTableSpan(n.tableID, enableOldValue))

	if ctx.Vars().Config.Cyclic.IsEnabled() && n.replicaInfo.MarkTableID != 0 {
		spans = append(spans, regionspan.GetTableSpan(n.replicaInfo.MarkTableID, enableOldValue))
	}
	return spans
}

func (n *pullerNode) Init(ctx pipeline.NodeContext) error {
	metricTableResolvedTsGauge := tableResolvedTsGauge.WithLabelValues(n.changefeedID, ctx.Vars().CaptureAddr, n.tableName)
	enableOldValue := ctx.Vars().Config.EnableOldValue
	ctxC, cancel := stdContext.WithCancel(ctx.StdContext())
	ctxC = util.PutTableInfoInCtx(ctxC, n.tableID, n.tableName)
	plr := puller.NewPuller(ctxC, ctx.Vars().PDClient, n.credential, n.kvStorage,
		n.replicaInfo.StartTs, n.tableSpan(ctx), n.limitter, enableOldValue)
	n.wg.Go(func() error {
		ctx.Throw(errors.Trace(plr.Run(ctxC)))
		return nil
	})
	n.wg.Go(func() error {
		for {
			select {
			case <-ctxC.Done():
				return nil
			case rawKV := <-plr.Output():
				if rawKV == nil {
					continue
				}
				if rawKV.OpType == model.OpTypeResolved {
					metricTableResolvedTsGauge.Set(float64(oracle.ExtractPhysical(rawKV.CRTs)))
				}
				pEvent := model.NewPolymorphicEvent(rawKV)
				ctx.SendToNextNode(pipeline.PolymorphicEventMessage(pEvent))
			}
		}
	})
	n.cancel = cancel
	return nil
}

// Receive receives the message from the previous node
func (n *pullerNode) Receive(ctx pipeline.NodeContext) error {
	// just forward any messages to the next node
	ctx.SendToNextNode(ctx.Message())
	return nil
}

func (n *pullerNode) Destroy(ctx pipeline.NodeContext) error {
	tableResolvedTsGauge.DeleteLabelValues(n.changefeedID, ctx.Vars().CaptureAddr, n.tableName)
	n.cancel()
	return n.wg.Wait()
}
