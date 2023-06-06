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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/pkg/config"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
	"golang.org/x/sync/errgroup"
)

// Wrapper is a wrapper of puller used by source manager.
type Wrapper interface {
	// Start the puller and send internal errors into `errChan`.
	Start(
		ctx context.Context,
		up *upstream.Upstream,
		eventSortEngine engine.SortEngine,
		errChan chan<- error,
		multiplexingCli *kv.SharedClient,
	)
	GetStats() puller.Stats
	Close()
}

// WrapperImpl is a wrapper of puller used by source manager.
type WrapperImpl struct {
	changefeed model.ChangeFeedID
	span       tablepb.Span
	tableName  string // quoted schema and table, used in metircs only
	p          puller.Puller
	startTs    model.Ts
	bdrMode    bool

	// cancel is used to cancel the puller when remove or close the table.
	cancel context.CancelFunc
	// eg is used to wait the puller to exit.
	eg *errgroup.Group
}

// NewPullerWrapper creates a new puller wrapper.
func NewPullerWrapper(
	changefeed model.ChangeFeedID,
	span tablepb.Span,
	tableName string,
	startTs model.Ts,
	bdrMode bool,
) Wrapper {
	return &WrapperImpl{
		changefeed: changefeed,
		span:       span,
		tableName:  tableName,
		startTs:    startTs,
		bdrMode:    bdrMode,
	}
}

// Start the puller wrapper.
// We use cdc context to put capture info and role into context.
func (n *WrapperImpl) Start(
	ctx context.Context,
	up *upstream.Upstream,
	eventSortEngine engine.SortEngine,
	errChan chan<- error,
	kvCli *kv.SharedClient,
) {
	ctx, n.cancel = context.WithCancel(ctx)
	errorHandler := func(err error) {
		select {
		case <-ctx.Done():
		case errChan <- err:
		}
	}

	failpoint.Inject("ProcessorAddTableError", func() {
		errorHandler(cerrors.New("processor add table injected error"))
	})

	if kvCli == nil {
		n.p = puller.New(
			ctx,
			up.PDClient,
			up.GrpcPool,
			up.RegionCache,
			up.KVStorage,
			up.PDClock,
			n.startTs,
			[]tablepb.Span{n.span},
			config.GetGlobalServerConfig().KVClient,
			n.changefeed,
			n.span.TableID,
			n.tableName,
			n.bdrMode,
			false,
		)
	} else {
		n.p = puller.NewMultiplexing(
			n.changefeed,
			n.span.TableID,
			n.tableName,
			n.startTs,
			[]tablepb.Span{n.span},
			kvCli,
			make(chan model.RegionFeedEvent, 128), // TODO(qupeng): channel size.
			false,
		)
	}

	// Use errgroup to ensure all sub goroutines can exit without calling Close.
	n.eg, ctx = errgroup.WithContext(ctx)
	n.eg.Go(func() error {
		err := n.p.Run(ctx)
		errorHandler(err)
		return err
	})
	n.eg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case rawKV := <-n.p.Output():
				if rawKV == nil {
					continue
				}
				pEvent := model.NewPolymorphicEvent(rawKV)
				eventSortEngine.Add(n.span, pEvent)
			}
		}
	})
}

// GetStats returns the puller stats.
func (n *WrapperImpl) GetStats() puller.Stats {
	return n.p.Stats()
}

// Close the puller wrapper.
func (n *WrapperImpl) Close() {
	if n.cancel == nil {
		return
	}
	n.cancel()
	n.cancel = nil
	// The returned error can be ignored because the table is in removing.
	_ = n.eg.Wait()
}
