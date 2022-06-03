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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/pipeline"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"golang.org/x/sync/errgroup"
)

type pullerNode struct {
	tableName string // quoted schema and table, used in metircs only

	tableID     model.TableID
	replicaInfo *model.TableReplicaInfo
	changefeed  model.ChangeFeedID
	cancel      context.CancelFunc
	wg          *errgroup.Group

	upstream *upstream.Upstream
}

func newPullerNode(
	tableID model.TableID, replicaInfo *model.TableReplicaInfo,
	tableName string,
	changefeed model.ChangeFeedID,
	upstream *upstream.Upstream,
) *pullerNode {
	return &pullerNode{
		tableID:     tableID,
		replicaInfo: replicaInfo,
		tableName:   tableName,
		changefeed:  changefeed,
		upstream:    upstream,
	}
}

func (n *pullerNode) tableSpan(ctx cdcContext.Context) []regionspan.Span {
	// start table puller
	config := ctx.ChangefeedVars().Info.Config
	spans := make([]regionspan.Span, 0, 4)
	spans = append(spans, regionspan.GetTableSpan(n.tableID))

	if config.Cyclic.IsEnabled() && n.replicaInfo.MarkTableID != 0 {
		spans = append(spans, regionspan.GetTableSpan(n.replicaInfo.MarkTableID))
	}
	return spans
}

func (n *pullerNode) Init(ctx pipeline.NodeContext) error {
	return n.start(ctx,
		new(errgroup.Group), false, nil)
}

func (n *pullerNode) start(ctx pipeline.NodeContext,
	wg *errgroup.Group, isActorMode bool, sorter *sorterNode,
) error {
	n.wg = wg
	ctxC, cancel := context.WithCancel(ctx)
	ctxC = contextutil.PutTableInfoInCtx(ctxC, n.tableID, n.tableName)
	ctxC = contextutil.PutCaptureAddrInCtx(ctxC, ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
	ctxC = contextutil.PutChangefeedIDInCtx(ctxC, ctx.ChangefeedVars().ID)
	ctxC = contextutil.PutRoleInCtx(ctxC, util.RoleProcessor)
	kvCfg := config.GetGlobalServerConfig().KVClient
	// NOTICE: always pull the old value internally
	// See also: https://github.com/pingcap/tiflow/issues/2301.
	plr := puller.NewPuller(
		ctxC,
		n.upstream.PDClient,
		n.upstream.GrpcPool,
		n.upstream.RegionCache,
		n.upstream.KVStorage,
		n.upstream.PDClock,
		n.changefeed,
		n.replicaInfo.StartTs,
		n.tableSpan(ctx),
		kvCfg,
	)
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
				pEvent := model.NewPolymorphicEvent(rawKV)
				if isActorMode {
					sorter.handleRawEvent(ctx, pEvent)
				} else {
					ctx.SendToNextNode(pmessage.PolymorphicEventMessage(pEvent))
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
	n.cancel()
	return n.wg.Wait()
}
