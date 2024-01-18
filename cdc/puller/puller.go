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
	"math"
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

const (
	defaultPullerEventChanSize  = 128
	defaultPullerOutputChanSize = 128
)

// Stats of a puller.
type Stats struct {
	RegionCount         uint64
	CheckpointTsIngress model.Ts
	ResolvedTsIngress   model.Ts
	CheckpointTsEgress  model.Ts
	ResolvedTsEgress    model.Ts
}

// Puller pull data from tikv and push changes into a buffer.
type Puller interface {
	// Run the puller, continually fetch event from TiKV and add event into buffer.
	Run(ctx context.Context) error
	GetResolvedTs() uint64
	Output() <-chan *model.RawKVEntry
	Stats() Stats
}

type pullerImpl struct {
	kvCli     kv.CDCKVClient
	kvStorage tikv.Storage
	spans     []regionspan.ComparableSpan
	outputCh  chan *model.RawKVEntry
	tsTracker frontier.Frontier
	// The commit ts of the latest raw kv event that puller has sent.
	checkpointTs uint64
	// The latest resolved ts that puller has sent.
	resolvedTs uint64

	changefeed model.ChangeFeedID
	tableID    model.TableID
	tableName  string

	cfg                   *config.ServerConfig
	lastForwardTime       time.Time
	lastForwardResolvedTs uint64
	// startResolvedTs is the resolvedTs when puller is initialized
	startResolvedTs uint64
}

// New create a new Puller fetch event start from checkpointTs and put into buf.
func New(ctx context.Context,
	pdCli pd.Client,
	grpcPool kv.GrpcPool,
	regionCache *tikv.RegionCache,
	kvStorage tidbkv.Storage,
	pdClock pdutil.Clock,
	checkpointTs uint64,
	spans []regionspan.Span,
	cfg *config.ServerConfig,
	changefeed model.ChangeFeedID,
	tableID model.TableID,
	tableName string,
	filterLoop bool,
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
		ctx, pdCli, grpcPool, regionCache, pdClock, cfg, changefeed, tableID, tableName, filterLoop)
	p := &pullerImpl{
		kvCli:        kvCli,
		kvStorage:    tikvStorage,
		checkpointTs: checkpointTs,
		spans:        comparableSpans,
		outputCh:     make(chan *model.RawKVEntry, defaultPullerOutputChanSize),
		tsTracker:    tsTracker,
		resolvedTs:   checkpointTs,
		changefeed:   changefeed,
		tableID:      tableID,
		tableName:    tableName,
		cfg:          cfg,

		startResolvedTs: checkpointTs,
	}
	return p
}

// Run the puller, continually fetch event from TiKV and add event into buffer
func (p *pullerImpl) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	checkpointTs := p.checkpointTs
	eventCh := make(chan model.RegionFeedEvent, defaultPullerEventChanSize)

	lockResolver := txnutil.NewLockerResolver(p.kvStorage,
		p.changefeed, contextutil.RoleFromCtx(ctx))
	for _, span := range p.spans {
		span := span

		g.Go(func() error {
			return p.kvCli.EventFeed(ctx, span, checkpointTs, lockResolver, eventCh)
		})
	}

	metricOutputChanSize := outputChanSizeHistogram.
		WithLabelValues(p.changefeed.Namespace, p.changefeed.ID)
	metricEventChanSize := eventChanSizeHistogram.
		WithLabelValues(p.changefeed.Namespace, p.changefeed.ID)
	metricPullerResolvedTs := pullerResolvedTsGauge.
		WithLabelValues(p.changefeed.Namespace, p.changefeed.ID)
	metricTxnCollectCounterKv := txnCollectCounter.
		WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "kv")
	metricTxnCollectCounterResolved := txnCollectCounter.
		WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved")
	defer func() {
		outputChanSizeHistogram.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID)
		eventChanSizeHistogram.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID)
		memBufferSizeGauge.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID)
		pullerResolvedTsGauge.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID)
		txnCollectCounter.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID, "kv")
		txnCollectCounter.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved")
	}()

	lastResolvedTs := p.checkpointTs
	lastAdvancedTime := time.Now()
	lastLogSlowRangeTime := time.Now()
	g.Go(func() error {
		metricsTicker := time.NewTicker(15 * time.Second)
		defer metricsTicker.Stop()
		stuckDetectorTicker := time.NewTicker(1 * time.Minute)
		defer stuckDetectorTicker.Stop()
		output := func(raw *model.RawKVEntry) error {
			// even after https://github.com/pingcap/tiflow/pull/2038, kv client
			// could still miss region change notification, which leads to resolved
			// ts update missing in puller, however resolved ts fallback here can
			// be ignored since no late data is received and the guarantee of
			// resolved ts is not broken.
			if raw.CRTs < p.resolvedTs || (raw.CRTs == p.resolvedTs && raw.OpType != model.OpTypeResolved) {
				log.Warn("The CRTs is fallen back in puller",
					zap.String("namespace", p.changefeed.Namespace),
					zap.String("changefeed", p.changefeed.ID),
					zap.Int64("tableID", p.tableID),
					zap.String("tableName", p.tableName),
					zap.Uint64("CRTs", raw.CRTs),
					zap.Uint64("resolvedTs", p.resolvedTs),
					zap.Any("row", raw))
				return nil
			}
			commitTs := raw.CRTs
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case p.outputCh <- raw:
				if atomic.LoadUint64(&p.checkpointTs) < commitTs {
					atomic.StoreUint64(&p.checkpointTs, commitTs)
				}
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
			case <-stuckDetectorTicker.C:
				if err := p.detectResolvedTsStuck(); err != nil {
					return errors.Trace(err)
				}
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
							zap.String("namespace", p.changefeed.Namespace),
							zap.String("changefeed", p.changefeed.ID),
							zap.Int64("tableID", p.tableID),
							zap.String("tableName", p.tableName),
							zap.Any("resolved", e.Resolved),
							zap.Any("spans", p.spans),
						)
					}
					// Forward is called in a single thread
					p.tsTracker.Forward(resolvedSpan.Region, resolvedSpan.Span, e.Resolved.ResolvedTs)
				}
				resolvedTs := p.tsTracker.Frontier()
				if resolvedTs > 0 && !initialized {
					initialized = true

					spans := make([]string, 0, len(p.spans))
					for i := range p.spans {
						spans = append(spans, p.spans[i].String())
					}
					log.Info("puller is initialized",
						zap.String("namespace", p.changefeed.Namespace),
						zap.String("changefeed", p.changefeed.ID),
						zap.Int64("tableID", p.tableID),
						zap.String("tableName", p.tableName),
						zap.Uint64("resolvedTs", resolvedTs),
						zap.Duration("duration", time.Since(start)),
						zap.Strings("spans", spans))
				}
				if !initialized {
					continue
				}
				if resolvedTs <= lastResolvedTs {
					if time.Since(lastAdvancedTime) > 30*time.Second && time.Since(lastLogSlowRangeTime) > 30*time.Second {
						var slowestTs uint64 = math.MaxUint64
						slowestRange := regionspan.ComparableSpan{}
						rangeFilled := true
						p.tsTracker.Entries(func(key []byte, ts uint64) {
							if ts < slowestTs {
								slowestTs = ts
								slowestRange.Start = key
								rangeFilled = false
							} else if !rangeFilled {
								slowestRange.End = key
								rangeFilled = true
							}
						})
						log.Info("table puller has been stucked",
							zap.String("namespace", p.changefeed.Namespace),
							zap.String("changefeed", p.changefeed.ID),
							zap.Int64("tableID", p.tableID),
							zap.String("tableName", p.tableName),
							zap.Uint64("resolvedTs", resolvedTs),
							zap.Uint64("slowestRangeTs", slowestTs),
							zap.Stringer("range", &slowestRange))
						lastLogSlowRangeTime = time.Now()
					}
					continue
				}
				lastResolvedTs = resolvedTs
				lastAdvancedTime = time.Now()
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

func (p *pullerImpl) detectResolvedTsStuck() error {
	if p.cfg.Debug.Puller.EnableResolvedTsStuckDetection {
		resolvedTs := p.tsTracker.Frontier()
		// check if the resolvedTs is advancing,
		// If the resolvedTs in Frontier is less than startResolvedTs, it means that the incremental scan has
		// not complete yet. We need to make no decision in this scenario.
		if resolvedTs <= p.startResolvedTs {
			return nil
		}
		if resolvedTs == p.lastForwardResolvedTs {
			log.Warn("ResolvedTs stuck detected in puller",
				zap.String("namespace", p.changefeed.Namespace),
				zap.String("changefeed", p.changefeed.ID),
				zap.Int64("tableID", p.tableID),
				zap.String("tableName", p.tableName),
				zap.Uint64("lastResolvedTs", p.lastForwardResolvedTs),
				zap.Uint64("resolvedTs", resolvedTs))
			if time.Since(p.lastForwardTime) > time.Duration(p.cfg.Debug.Puller.ResolvedTsStuckInterval) {
				// throw an error to cause changefeed restart
				return errors.New("resolved ts stuck")
			}
		} else {
			p.lastForwardTime = time.Now()
			p.lastForwardResolvedTs = resolvedTs
		}
	}
	return nil
}

func (p *pullerImpl) Output() <-chan *model.RawKVEntry {
	return p.outputCh
}

func (p *pullerImpl) Stats() Stats {
	return Stats{
		RegionCount:         p.kvCli.RegionCount(),
		ResolvedTsIngress:   p.kvCli.ResolvedTs(),
		CheckpointTsIngress: p.kvCli.CommitTs(),
		ResolvedTsEgress:    atomic.LoadUint64(&p.resolvedTs),
		CheckpointTsEgress:  atomic.LoadUint64(&p.checkpointTs),
	}
}
