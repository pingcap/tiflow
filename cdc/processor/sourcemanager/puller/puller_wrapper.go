// Copyright 2022 PingCAP, Inc.
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

package puller

import (
	"context"
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/pkg/config"
	cdccontext "github.com/pingcap/tiflow/pkg/context"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
)

// Wrapper is a wrapper of puller used by source manager.
type Wrapper struct {
	changefeed model.ChangeFeedID
	span       tablepb.Span
	tableName  string // quoted schema and table, used in metircs only
	p          puller.Puller
	startTs    model.Ts
	// cancel is used to cancel the puller when remove or close the table.
	cancel context.CancelFunc
	// wg is used to wait the puller to exit.
	wg      sync.WaitGroup
	bdrMode bool
}

// NewPullerWrapper creates a new puller wrapper.
func NewPullerWrapper(
	changefeed model.ChangeFeedID,
	span tablepb.Span,
	tableName string,
	startTs model.Ts,
	bdrMode bool,
) *Wrapper {
	return &Wrapper{
		changefeed: changefeed,
		span:       span,
		tableName:  tableName,
		startTs:    startTs,
		bdrMode:    bdrMode,
	}
}

// Start the puller wrapper.
// We use cdc context to put capture info and role into context.
func (n *Wrapper) Start(
	ctx cdccontext.Context,
	up *upstream.Upstream,
	eventSortEngine engine.SortEngine,
	errChan chan<- error,
) {
	failpoint.Inject("ProcessorAddTableError", func() {
		errChan <- cerrors.New("processor add table injected error")
	})
	ctxC, cancel := context.WithCancel(ctx)
	ctxC = contextutil.PutCaptureAddrInCtx(ctxC, ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
	ctxC = contextutil.PutRoleInCtx(ctxC, util.RoleProcessor)
	kvCfg := config.GetGlobalServerConfig().KVClient
	// NOTICE: always pull the old value internally
	// See also: https://github.com/pingcap/tiflow/issues/2301.
	n.p = puller.New(
		ctxC,
		up.PDClient,
		up.GrpcPool,
		up.RegionCache,
		up.KVStorage,
		up.PDClock,
		n.startTs,
		[]tablepb.Span{n.span},
		kvCfg,
		n.changefeed,
		n.span.TableID,
		n.tableName,
		n.bdrMode,
		false,
	)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		err := n.p.Run(ctxC)
		if err != nil && !cerrors.Is(err, context.Canceled) {
			errChan <- err
		}
	}()
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for {
			select {
			case <-ctxC.Done():
				return
			case rawKV := <-n.p.Output():
				if rawKV == nil {
					continue
				}
				pEvent := model.NewPolymorphicEvent(rawKV)
				if err := eventSortEngine.Add(n.span, pEvent); err != nil {
					errChan <- err
				}
			}
		}
	}()
	n.cancel = cancel
}

// GetStats returns the puller stats.
func (n *Wrapper) GetStats() puller.Stats {
	return n.p.Stats()
}

// Close the puller wrapper.
func (n *Wrapper) Close() {
	n.cancel()
	n.wg.Wait()
}
