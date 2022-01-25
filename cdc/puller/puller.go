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

package puller

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller/frontier"
	"github.com/pingcap/tiflow/pkg/pdtime"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/txnutil"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// DDLPullerTableName is the fake table name for ddl puller.
const DDLPullerTableName = "DDL_PULLER"

const (
	defaultPullerEventChanSize  = 128
	defaultPullerOutputChanSize = 128
)

// Puller pull data from tikv and push changes into a buffer.
type Puller interface {
	// Run the puller, continually fetch event from TiKV and add event into buffer.
	Run(ctx context.Context) error
	GetResolvedTs() uint64
	Output() <-chan *model.RawKVEntry
	IsInitialized() bool
}

type pullerImpl struct {
	kvCli          kv.CDCKVClient
	kvStorage      tikv.Storage
	checkpointTs   uint64
	spans          []regionspan.ComparableSpan
	outputCh       chan *model.RawKVEntry
	tsTracker      frontier.Frontier
	resolvedTs     uint64
	initialized    int64
	enableOldValue bool
}

// NewPuller create a new Puller fetch event start from checkpointTs
// and put into buf.
func NewPuller(
	ctx context.Context,
	pdCli pd.Client,
	grpcPool kv.GrpcPool,
	regionCache *tikv.RegionCache,
	kvStorage tidbkv.Storage,
	pdClock pdtime.Clock,
	changefeed string,
	checkpointTs uint64,
	spans []regionspan.Span,
	enableOldValue bool,
) Puller {
	tikvStorage, ok := kvStorage.(tikv.Storage)
	if !ok {
		log.Panic("can't create puller for non-tikv storage")
	}
	comparableSpans := make([]regionspan.ComparableSpan, len(spans))
	for i := range spans {
		comparableSpans[i] = regionspan.ToComparableSpan(spans[i])
	}
	// To make puller level resolved ts initialization distinguishable, we set
	// the initial ts for frontier to 0. Once the puller level resolved ts
	// initialized, the ts should advance to a non-zero value.
	tsTracker := frontier.NewFrontier(0, comparableSpans...)
	kvCli := kv.NewCDCKVClient(ctx, pdCli, tikvStorage, grpcPool, regionCache, pdClock, changefeed)
	p := &pullerImpl{
		kvCli:          kvCli,
		kvStorage:      tikvStorage,
		checkpointTs:   checkpointTs,
		spans:          comparableSpans,
		outputCh:       make(chan *model.RawKVEntry, defaultPullerOutputChanSize),
		tsTracker:      tsTracker,
		resolvedTs:     checkpointTs,
		initialized:    0,
		enableOldValue: enableOldValue,
	}
	return p
}

// Run the puller, continually fetch event from TiKV and add event into buffer
func (p *pullerImpl) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	checkpointTs := p.checkpointTs
	eventCh := make(chan model.RegionFeedEvent, defaultPullerEventChanSize)

	lockresolver := txnutil.NewLockerResolver(p.kvStorage)
	for _, span := range p.spans {
		span := span

		g.Go(func() error {
			return p.kvCli.EventFeed(ctx, span, checkpointTs, p.enableOldValue, lockresolver, p, eventCh)
		})
	}

	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	tableID, _ := util.TableIDFromCtx(ctx)
	metricOutputChanSize := outputChanSizeHistogram.WithLabelValues(captureAddr, changefeedID)
	metricEventChanSize := eventChanSizeHistogram.WithLabelValues(captureAddr, changefeedID)
	metricPullerResolvedTs := pullerResolvedTsGauge.WithLabelValues(captureAddr, changefeedID)
	metricTxnCollectCounterKv := txnCollectCounter.WithLabelValues(captureAddr, changefeedID, "kv")
	metricTxnCollectCounterResolved := txnCollectCounter.WithLabelValues(captureAddr, changefeedID, "resolved")
	defer func() {
		outputChanSizeHistogram.DeleteLabelValues(captureAddr, changefeedID)
		eventChanSizeHistogram.DeleteLabelValues(captureAddr, changefeedID)
		memBufferSizeGauge.DeleteLabelValues(captureAddr, changefeedID)
		pullerResolvedTsGauge.DeleteLabelValues(captureAddr, changefeedID)
		kvEventCounter.DeleteLabelValues(captureAddr, changefeedID, "kv")
		kvEventCounter.DeleteLabelValues(captureAddr, changefeedID, "resolved")
		txnCollectCounter.DeleteLabelValues(captureAddr, changefeedID, "kv")
		txnCollectCounter.DeleteLabelValues(captureAddr, changefeedID, "resolved")
	}()

	lastResolvedTs := p.checkpointTs
	g.Go(func() error {
		metricsTicker := time.NewTicker(15 * time.Second)
		defer metricsTicker.Stop()
		output := func(raw *model.RawKVEntry) error {
			// even after https://github.com/pingcap/tiflow/pull/2038, kv client
			// could still miss region change notification, which leads to resolved
			// ts update missing in puller, however resolved ts fallback here can
			// be ignored since no late data is received and the guarantee of
			// resolved ts is not broken.
			if raw.CRTs < p.resolvedTs || (raw.CRTs == p.resolvedTs && raw.OpType != model.OpTypeResolved) {
				log.Warn("The CRTs is fallen back in puller",
					zap.Reflect("row", raw),
					zap.Uint64("CRTs", raw.CRTs),
					zap.Uint64("resolvedTs", p.resolvedTs),
					zap.Int64("tableID", tableID))
				return nil
			}
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case p.outputCh <- raw:
			}
			return nil
		}

		start := time.Now()
		initialized := false
		for {
			var e model.RegionFeedEvent
			select {
			case <-metricsTicker.C:
				metricEventChanSize.Observe(float64(len(eventCh)))
				metricOutputChanSize.Observe(float64(len(p.outputCh)))
				metricPullerResolvedTs.Set(float64(oracle.ExtractPhysical(atomic.LoadUint64(&p.resolvedTs))))
				continue
			case e = <-eventCh:
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			}
			if e.Val != nil {
				metricTxnCollectCounterKv.Inc()
				if err := output(e.Val); err != nil {
					return errors.Trace(err)
				}
			} else if e.Resolved != nil {
				metricTxnCollectCounterResolved.Inc()
				if !regionspan.IsSubSpan(e.Resolved.Span, p.spans...) {
					log.Panic("the resolved span is not in the total span",
						zap.Reflect("resolved", e.Resolved),
						zap.Int64("tableID", tableID),
						zap.Reflect("spans", p.spans),
					)
				}
				// Forward is called in a single thread
				p.tsTracker.Forward(e.Resolved.Span, e.Resolved.ResolvedTs)
				resolvedTs := p.tsTracker.Frontier()
				if resolvedTs > 0 && !initialized {
					// Advancing to a non-zero value means the puller level
					// resolved ts is initialized.
					atomic.StoreInt64(&p.initialized, 1)
					initialized = true

					spans := make([]string, 0, len(p.spans))
					for i := range p.spans {
						spans = append(spans, p.spans[i].String())
					}
					log.Info("puller is initialized",
						zap.Duration("duration", time.Since(start)),
						zap.String("changefeed", changefeedID),
						zap.Int64("tableID", tableID),
						zap.Strings("spans", spans),
						zap.Uint64("resolvedTs", resolvedTs))
				}
				if !initialized || resolvedTs == lastResolvedTs {
					continue
				}
				lastResolvedTs = resolvedTs
				err := output(&model.RawKVEntry{CRTs: resolvedTs, OpType: model.OpTypeResolved, RegionID: e.RegionID})
				if err != nil {
					return errors.Trace(err)
				}
				atomic.StoreUint64(&p.resolvedTs, resolvedTs)
			}
		}
	})
	return g.Wait()
}

func (p *pullerImpl) GetResolvedTs() uint64 {
	return atomic.LoadUint64(&p.resolvedTs)
}

func (p *pullerImpl) Output() <-chan *model.RawKVEntry {
	return p.outputCh
}

func (p *pullerImpl) IsInitialized() bool {
	return atomic.LoadInt64(&p.initialized) > 0
}
