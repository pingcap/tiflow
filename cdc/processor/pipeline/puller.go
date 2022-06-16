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
}

func newPullerNode(
	tableID model.TableID, replicaInfo *model.TableReplicaInfo,
	tableName string,
	changefeed model.ChangeFeedID,
) *pullerNode {
	return &pullerNode{
		tableID:     tableID,
		replicaInfo: replicaInfo,
		tableName:   tableName,
		changefeed:  changefeed,
	}
}

func (n *pullerNode) tableSpan(ctx cdcContext.Context) []regionspan.Span {
	// start table puller
	spans := make([]regionspan.Span, 0, 4)
	spans = append(spans, regionspan.GetTableSpan(n.tableID))
	return spans
}

func (n *pullerNode) start(ctx pipeline.NodeContext,
	up *upstream.Upstream, wg *errgroup.Group,
	sorter *sorterNode,
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
		up.PDClient,
		up.GrpcPool,
		up.RegionCache,
		up.KVStorage,
		up.PDClock,
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
				sorter.handleRawEvent(ctx, pEvent)
			}
		}
	})
	n.cancel = cancel
	return nil
}
