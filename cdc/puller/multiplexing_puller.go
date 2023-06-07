// Copyright 2023 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller/frontier"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type multiplexingPullerImpl struct {
	changefeed model.ChangeFeedID
	tableID    model.TableID
	tableName  string
	startTs    model.Ts
	spans      []tablepb.Span
	pullerType string

	client    *kv.SharedClient
	tsTracker frontier.Frontier
	inputCh   chan model.RegionFeedEvent
	outputCh  chan *model.RawKVEntry

	// Only accessed in `Run`.
	resolvedTs model.Ts
}

func NewMultiplexing(
	changefeed model.ChangeFeedID,
	tableID model.TableID,
	tableName string,
	startTs model.Ts,
	spans []tablepb.Span,
	client *kv.SharedClient,
	inputCh chan model.RegionFeedEvent,
	isDDLPuller bool,
) *multiplexingPullerImpl {
	pullerType := "dml"
	if isDDLPuller {
		pullerType = "ddl"
	}

	metricMissedRegionCollectCounter := missedRegionCollectCounter.
		WithLabelValues(changefeed.Namespace, changefeed.ID, pullerType)
	tsTracker := frontier.NewFrontier(0, metricMissedRegionCollectCounter, spans...)

	return &multiplexingPullerImpl{
		changefeed: changefeed,
		tableID:    tableID,
		tableName:  tableName,
		startTs:    startTs,
		spans:      spans,
		pullerType: pullerType,
		client:     client,
		inputCh:    inputCh,
		tsTracker:  tsTracker,
		outputCh:   make(chan *model.RawKVEntry, defaultPullerOutputChanSize),
		resolvedTs: startTs,
	}
}

func (p *multiplexingPullerImpl) Run(ctx context.Context) error {
	for _, span := range p.spans {
		if err := p.client.Subscribe(span, p.startTs, p.inputCh); err != nil {
			return errors.Trace(err)
		}
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

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		metricsTicker := time.NewTicker(15 * time.Second)
		defer metricsTicker.Stop()
		output := func(raw *model.RawKVEntry) error {
			if raw.CRTs < p.resolvedTs || (raw.CRTs == p.resolvedTs && raw.OpType != model.OpTypeResolved) {
				log.Warn("The CRTs is fallen back in puller",
					zap.String("namespace", p.changefeed.Namespace),
					zap.String("changefeed", p.changefeed.ID),
					zap.Int64("tableID", p.tableID),
					zap.String("tableName", p.tableName),
					zap.Uint64("CRTs", raw.CRTs),
					zap.Uint64("resolvedTs", p.resolvedTs))
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case p.outputCh <- raw:
			}
			return nil
		}

		initialized := false
		for {
			var e model.RegionFeedEvent
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-metricsTicker.C:
				metricEventChanSize.Observe(float64(len(p.inputCh)))
				metricOutputChanSize.Observe(float64(len(p.outputCh)))
				metricPullerResolvedTs.Set(float64(oracle.ExtractPhysical(p.resolvedTs)))
				continue
			case e = <-p.inputCh:
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
					if !spanz.IsSubSpan(resolvedSpan.Span, p.spans...) {
						log.Panic("the resolved span is not in the total span",
							zap.String("namespace", p.changefeed.Namespace),
							zap.String("changefeed", p.changefeed.ID),
							zap.Int64("tableID", p.tableID),
							zap.String("tableName", p.tableName),
							zap.Any("resolved", e.Resolved),
							zap.Any("spans", p.spans))
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
						zap.Uint64("resolvedTs", resolvedTs))
				}
				if resolvedTs > p.resolvedTs {
					p.resolvedTs = resolvedTs
					err := output(&model.RawKVEntry{CRTs: resolvedTs, OpType: model.OpTypeResolved, RegionID: e.RegionID})
					if err != nil {
						return errors.Trace(err)
					}
					log.Info("QPP puller advanced",
						zap.String("namespace", p.changefeed.Namespace),
						zap.String("changefeed", p.changefeed.ID),
						zap.Int64("tableID", p.tableID),
						zap.String("tableName", p.tableName),
						zap.Uint64("resolvedTs", resolvedTs))
				}
			}
		}
	})

	return g.Wait()
}

func (p *multiplexingPullerImpl) Output() <-chan *model.RawKVEntry {
	return p.outputCh
}

func (p *multiplexingPullerImpl) Stats() Stats {
	return Stats{}
}
