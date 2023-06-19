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
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller/frontier"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/txnutil"
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
	Output() <-chan *model.RawKVEntry
	Stats() Stats
}

type pullerImpl struct {
	kvCli     kv.CDCKVClient
	kvStorage tikv.Storage
	spans     []tablepb.Span
	outputCh  chan *model.RawKVEntry
	tsTracker frontier.Frontier
	// The commit ts of the latest raw kv event that puller has sent.
	checkpointTs uint64
	// The latest resolved ts that puller has sent.
	resolvedTs uint64

	changefeed model.ChangeFeedID
	tableID    model.TableID
	tableName  string
}

// New create a new Puller fetch event start from checkpointTs and put into buf.
func New(ctx context.Context,
	pdCli pd.Client,
	grpcPool kv.GrpcPool,
	regionCache *tikv.RegionCache,
	kvStorage tidbkv.Storage,
	pdClock pdutil.Clock,
	checkpointTs uint64,
	spans []tablepb.Span,
	cfg *config.KVClientConfig,
	changefeed model.ChangeFeedID,
	tableID model.TableID,
	tableName string,
	filterLoop bool,
) Puller {
	tikvStorage, ok := kvStorage.(tikv.Storage)
	if !ok {
		log.Panic("can't create puller for non-tikv storage")
	}

	// To make puller level resolved ts initialization distinguishable, we set
	// the initial ts for frontier to 0. Once the puller level resolved ts
	// initialized, the ts should advance to a non-zero value.
	tsTracker := frontier.NewFrontier(0, spans...)
	kvCli := kv.NewCDCKVClient(
		ctx, pdCli, grpcPool, regionCache, pdClock, cfg, changefeed, tableID, tableName, filterLoop)
	p := &pullerImpl{
		kvCli:        kvCli,
		kvStorage:    tikvStorage,
		checkpointTs: checkpointTs,
		spans:        spans,
		outputCh:     make(chan *model.RawKVEntry, defaultPullerOutputChanSize),
		tsTracker:    tsTracker,
		resolvedTs:   checkpointTs,
		changefeed:   changefeed,
		tableID:      tableID,
		tableName:    tableName,
	}
	return p
}

// Run the puller, continually fetch event from TiKV and add event into buffer
func (p *pullerImpl) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	checkpointTs := p.checkpointTs
	eventCh := make(chan model.RegionFeedEvent, defaultPullerEventChanSize)

	lockResolver := txnutil.NewLockerResolver(p.kvStorage,
		p.changefeed)
	for _, span := range p.spans {
		span := span

		g.Go(func() error {
			return p.kvCli.EventFeed(ctx, span, checkpointTs, lockResolver, eventCh)
		})
	}

	metricPullerEventCounterKv := PullerEventCounter.
		WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "kv")
	metricPullerEventCounterResolved := PullerEventCounter.
		WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved")

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
			case e = <-eventCh:
			}

			if e.Val != nil {
				metricPullerEventCounterKv.Inc()
				if err := output(e.Val); err != nil {
					return errors.Trace(err)
				}
				continue
			}

			if e.Resolved != nil {
				metricPullerEventCounterResolved.Add(float64(len(e.Resolved.Spans)))
				for _, resolvedSpan := range e.Resolved.Spans {
					if !spanz.IsSubSpan(resolvedSpan.Span, p.spans...) {
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
