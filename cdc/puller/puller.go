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
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller/frontier"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/txnutil"
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
	kvCli        kv.CDCKVClient
	kvStorage    tikv.Storage
	checkpointTs uint64
	spans        []regionspan.ComparableSpan
	outputCh     chan *model.RawKVEntry
	tsTracker    frontier.Frontier
	resolvedTs   uint64
	initialized  int64
}

// NewPuller create a new Puller fetch event start from checkpointTs
// and put into buf.
func NewPuller(
	ctx context.Context,
	pdCli pd.Client,
	grpcPool kv.GrpcPool,
	regionCache *tikv.RegionCache,
	kvStorage tidbkv.Storage,
	pdClock pdutil.Clock,
	changefeed model.ChangeFeedID,
	checkpointTs uint64,
	spans []regionspan.Span,
	cfg *config.KVClientConfig,
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
	pullerType := "dml"
	if len(spans) > 1 {
		pullerType = "ddl"
	}
	metricMissedRegionCollectCounter := missedRegionCollectCounter.
		WithLabelValues(changefeed.Namespace, changefeed.ID, pullerType)
	tsTracker := frontier.NewFrontier(0, metricMissedRegionCollectCounter, comparableSpans...)
	kvCli := kv.NewCDCKVClient(
		ctx, pdCli, tikvStorage, grpcPool, regionCache, pdClock, changefeed, cfg)
	p := &pullerImpl{
		kvCli:        kvCli,
		kvStorage:    tikvStorage,
		checkpointTs: checkpointTs,
		spans:        comparableSpans,
		outputCh:     make(chan *model.RawKVEntry, defaultPullerOutputChanSize),
		tsTracker:    tsTracker,
		resolvedTs:   checkpointTs,
		initialized:  0,
	}
	return p
}

// Run the puller, continually fetch event from TiKV and add event into buffer
func (p *pullerImpl) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	checkpointTs := p.checkpointTs
	eventCh := make(chan model.RegionFeedEvent, defaultPullerEventChanSize)

	lockResolver := txnutil.NewLockerResolver(p.kvStorage,
		contextutil.ChangefeedIDFromCtx(ctx), contextutil.RoleFromCtx(ctx))
	for _, span := range p.spans {
		span := span

		g.Go(func() error {
			return p.kvCli.EventFeed(ctx, span, checkpointTs, lockResolver, p, eventCh)
		})
	}

	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	tableID, _ := contextutil.TableIDFromCtx(ctx)
	metricOutputChanSize := outputChanSizeHistogram.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID)
	metricEventChanSize := eventChanSizeHistogram.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID)
	metricPullerResolvedTs := pullerResolvedTsGauge.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID)
	metricTxnCollectCounterKv := txnCollectCounter.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID, "kv")
	metricTxnCollectCounterResolved := txnCollectCounter.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID, "resolved")
	defer func() {
		outputChanSizeHistogram.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID)
		eventChanSizeHistogram.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID)
		memBufferSizeGauge.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID)
		pullerResolvedTsGauge.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID)
		kvEventCounter.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID, "kv")
		kvEventCounter.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID, "resolved")
		txnCollectCounter.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID, "kv")
		txnCollectCounter.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID, "resolved")
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
					zap.String("namespace", changefeedID.Namespace),
					zap.String("changefeed", changefeedID.ID),
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
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case <-metricsTicker.C:
				metricEventChanSize.Observe(float64(len(eventCh)))
				metricOutputChanSize.Observe(float64(len(p.outputCh)))
				metricPullerResolvedTs.Set(float64(oracle.ExtractPhysical(atomic.LoadUint64(&p.resolvedTs))))
				continue
			case e = <-eventCh:
			}

			if e.Val != nil {
				metricTxnCollectCounterKv.Inc()
				if err := output(e.Val); err != nil {
					return errors.Trace(err)
				}
				continue
			}

			if e.Resolved != nil {
				metricTxnCollectCounterResolved.Add(float64(len(e.Resolved.Spans)))
				for _, resolvedSpan := range e.Resolved.Spans {
					if !regionspan.IsSubSpan(resolvedSpan.Span, p.spans...) {
						log.Panic("the resolved span is not in the total span",
							zap.String("namespace", changefeedID.Namespace),
							zap.String("changefeed", changefeedID.ID),
							zap.Reflect("resolved", e.Resolved),
							zap.Int64("tableID", tableID),
							zap.Reflect("spans", p.spans),
						)
					}
					// Forward is called in a single thread
					p.tsTracker.Forward(resolvedSpan.Region, resolvedSpan.Span, e.Resolved.ResolvedTs)
				}
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
						zap.String("namespace", changefeedID.Namespace),
						zap.String("changefeed", changefeedID.ID),
						zap.Duration("duration", time.Since(start)),
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
