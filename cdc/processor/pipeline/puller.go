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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/actor"
	"github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type pullerNode struct {
	tableName string // quoted schema and table, used in metircs only

	tableID     model.TableID
	replicaInfo *model.TableReplicaInfo
	cancel      context.CancelFunc
	wg          *errgroup.Group

	outputCh chan pipeline.Message
}

func newPullerNode(
	tableID model.TableID, replicaInfo *model.TableReplicaInfo, tableName string) pipeline.Node {
	return &pullerNode{
		tableID:     tableID,
		replicaInfo: replicaInfo,
		tableName:   tableName,
		outputCh:    make(chan pipeline.Message, 64),
	}
}

func (n *pullerNode) tableSpan(config *config.ReplicaConfig) []regionspan.Span {
	// start table puller
	spans := make([]regionspan.Span, 0, 4)
	spans = append(spans, regionspan.GetTableSpan(n.tableID))

	if config.Cyclic.IsEnabled() && n.replicaInfo.MarkTableID != 0 {
		spans = append(spans, regionspan.GetTableSpan(n.replicaInfo.MarkTableID))
	}
	return spans
}

func (n *pullerNode) Init(ctx pipeline.NodeContext) error {
	return n.Start(ctx, false, new(errgroup.Group), ctx.ChangefeedVars(), ctx.GlobalVars())
}

func (n *pullerNode) Start(ctx context.Context, isActor bool, wg *errgroup.Group, info *cdcContext.ChangefeedVars, vars *cdcContext.GlobalVars) error {
	n.wg = wg
	metricTableResolvedTsGauge := tableResolvedTsGauge.WithLabelValues(info.ID, vars.CaptureInfo.AdvertiseAddr, n.tableName)
	ctxC, cancel := context.WithCancel(ctx)
	ctxC = util.PutTableInfoInCtx(ctxC, n.tableID, n.tableName)
	// NOTICE: always pull the old value internally
	// See also: TODO(hi-rustin): add issue link here.
	plr := puller.NewPuller(ctxC, vars.PDClient, vars.GrpcPool, vars.KVStorage,
		n.replicaInfo.StartTs, n.tableSpan(info.Info.Config), true)
	n.wg.Go(func() error {
		err := plr.Run(ctxC)
		if err != nil {
			log.Error("puller stopped", zap.Error(err))
		}
		if isActor {
			_ = defaultRouter.SendB(ctxC, actor.ID(n.tableID), message.StopMessage())
		}
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
				msg := pipeline.PolymorphicEventMessage(pEvent)
				if isActor {
					n.outputCh <- msg
					_ = defaultRouter.Send(actor.ID(n.tableID), message.TickMessage())
				} else {
					ctx.(pipeline.NodeContext).SendToNextNode(msg)
				}
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
	tableResolvedTsGauge.DeleteLabelValues(ctx.ChangefeedVars().ID, ctx.GlobalVars().CaptureInfo.AdvertiseAddr, n.tableName)
	n.cancel()
	return n.wg.Wait()
}
