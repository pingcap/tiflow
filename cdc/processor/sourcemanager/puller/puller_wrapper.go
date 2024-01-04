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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/pkg/config"
	cdccontext "github.com/pingcap/tiflow/pkg/context"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// Wrapper is a wrapper of puller used by source manager.
type Wrapper struct {
	changefeed model.ChangeFeedID
	tableID    model.TableID
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
	tableID model.TableID,
	tableName string,
	startTs model.Ts,
	bdrMode bool,
) *Wrapper {
	return &Wrapper{
		changefeed: changefeed,
		tableID:    tableID,
		tableName:  tableName,
		startTs:    startTs,
		bdrMode:    bdrMode,
	}
}

// tableSpan returns the table span with the table ID.
func (n *Wrapper) tableSpan() []regionspan.Span {
	// start table puller
	spans := make([]regionspan.Span, 0, 4)
	spans = append(spans, regionspan.GetTableSpan(n.tableID))
	return spans
}

// Start the puller wrapper.
// We use cdc context to put capture info and role into context.
func (n *Wrapper) Start(
	ctx cdccontext.Context,
	up *upstream.Upstream,
	eventSortEngine engine.SortEngine,
	errChan chan<- error,
	enableTableMonitor bool,
) {
	failpoint.Inject("ProcessorAddTableError", func() {
		errChan <- cerrors.New("processor add table injected error")
	})
	ctxC, cancel := context.WithCancel(ctx)
	ctxC = contextutil.PutCaptureAddrInCtx(ctxC, ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
	ctxC = contextutil.PutRoleInCtx(ctxC, util.RoleProcessor)
	serverConfig := config.GetGlobalServerConfig()
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
		n.tableSpan(),
		serverConfig,
		n.changefeed,
		n.tableID,
		n.tableName,
		n.bdrMode,
		enableTableMonitor,
	)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		err := n.p.Run(ctxC)
		if err != nil && !cerrors.Is(err, context.Canceled) {
			select {
			case errChan <- err:
				// Do not block sending error, because the err channel
				// might be full and no goroutine receives.
			default:
				log.Warn("puller fail to send error",
					zap.String("namespace", n.changefeed.Namespace),
					zap.String("changefeed", n.changefeed.ID),
					zap.String("table", n.tableName),
					zap.Error(err))
			}
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
				eventSortEngine.Add(n.tableID, pEvent)
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
