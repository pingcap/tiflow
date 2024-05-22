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
	)
	GetStats() puller.Stats
	Close()
}

// ShouldSplitKVEntry checks whether the raw kv entry should be splitted.
type ShouldSplitKVEntry func(raw *model.RawKVEntry) bool

// SplitUpdateKVEntry splits the raw kv entry into a delete entry and an insert entry.
type SplitUpdateKVEntry func(raw *model.RawKVEntry) (*model.RawKVEntry, *model.RawKVEntry, error)

// WrapperImpl is a wrapper of puller used by source manager.
type WrapperImpl struct {
	changefeed model.ChangeFeedID
	span       tablepb.Span
	tableName  string // quoted schema and table, used in metircs only
	p          puller.Puller
	startTs    model.Ts
	bdrMode    bool

	shouldSplitKVEntry ShouldSplitKVEntry
	splitUpdateKVEntry SplitUpdateKVEntry

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
	shouldSplitKVEntry ShouldSplitKVEntry,
	splitUpdateKVEntry SplitUpdateKVEntry,
) Wrapper {
	return &WrapperImpl{
		changefeed:         changefeed,
		span:               span,
		tableName:          tableName,
		startTs:            startTs,
		bdrMode:            bdrMode,
		shouldSplitKVEntry: shouldSplitKVEntry,
		splitUpdateKVEntry: splitUpdateKVEntry,
	}
}

// Start the puller wrapper.
// We use cdc context to put capture info and role into context.
func (n *WrapperImpl) Start(
	ctx context.Context,
	up *upstream.Upstream,
	eventSortEngine engine.SortEngine,
	errChan chan<- error,
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

	// NOTICE: always pull the old value internally
	// See also: https://github.com/pingcap/tiflow/issues/2301.
	n.p = puller.New(
		ctx,
		up.PDClient,
		up.GrpcPool,
		up.RegionCache,
		up.KVStorage,
		up.PDClock,
		n.startTs,
		[]tablepb.Span{n.span},
		config.GetGlobalServerConfig(),
		n.changefeed,
		n.span.TableID,
		n.tableName,
		n.bdrMode,
	)

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
				if n.shouldSplitKVEntry(rawKV) {
					deleteKVEntry, insertKVEntry, err := n.splitUpdateKVEntry(rawKV)
					if err != nil {
						return err
					}
					deleteEvent := model.NewPolymorphicEvent(deleteKVEntry)
					insertEvent := model.NewPolymorphicEvent(insertKVEntry)
					eventSortEngine.Add(n.span, deleteEvent, insertEvent)
				} else {
					pEvent := model.NewPolymorphicEvent(rawKV)
					eventSortEngine.Add(n.span, pEvent)
				}
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
