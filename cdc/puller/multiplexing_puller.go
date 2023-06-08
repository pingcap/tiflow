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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller/frontier"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	resolveLockTickInterval time.Duration = 5 * time.Second
	resolveLockInterval     time.Duration = 20 * time.Second

	multiplexingPullerEventChanSize = 1024
	resolvedSpanChanSize            = 128

	// TODO(qupeng): use a changefeed configuration instead.
	frontierConcurrent = 4
)

type tableProgress struct {
	changefeed model.ChangeFeedID
	pullerType string
	tableName  string
	spans      []tablepb.Span
	startTs    model.Ts

	initialized bool
	resolvedTs  model.Ts

	resolvedSpans chan *model.ResolvedSpans
	tsTracker     frontier.Frontier
	consume       func(context.Context, *model.RawKVEntry, []tablepb.Span) error

	scheduled atomic.Bool
}

// MultiplexingPuller works with `kv.SharedClient`. All tables share resources.
type MultiplexingPuller struct {
	changefeed model.ChangeFeedID

	client  *kv.SharedClient
	inputCh chan kv.MultiplexingEvent
	consume func(context.Context, *model.RawKVEntry, []tablepb.Span) error

	// NOTE: subscriptions can share one tableProgress if necessary.
	subscriptions struct {
		sync.RWMutex
		m map[kv.SubscriptionID]*tableProgress
	}

	advanceCh chan *tableProgress
}

// NewMultiplexingPuller creates a MultiplexingPuller. Outputs are handled by `consume`.
func NewMultiplexingPuller(
	changefeed model.ChangeFeedID,
	client *kv.SharedClient,
	consume func(context.Context, *model.RawKVEntry, []tablepb.Span) error,
) *MultiplexingPuller {
	x := &MultiplexingPuller{
		changefeed: changefeed,
		client:     client,
		inputCh:    make(chan kv.MultiplexingEvent, multiplexingPullerEventChanSize),
		consume:    consume,
		advanceCh:  make(chan *tableProgress, 128),
	}
	x.subscriptions.m = make(map[kv.SubscriptionID]*tableProgress)
	return x
}

// Subscribe some spans. They will share one same resolved timestamp progress.
func (p *MultiplexingPuller) Subscribe(
	pullerType string, tableName string,
	spans []tablepb.Span, startTs model.Ts,
) error {
	metricMissedRegionCollectCounter := missedRegionCollectCounter.
		WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, pullerType)
	tsTracker := frontier.NewFrontier(0, metricMissedRegionCollectCounter, spans...)

	progress := &tableProgress{
		changefeed: p.changefeed,
		pullerType: pullerType,
		tableName:  tableName,
		spans:      spans,
		startTs:    startTs,

		resolvedSpans: make(chan *model.ResolvedSpans, resolvedSpanChanSize),
		tsTracker:     tsTracker,
		consume:       p.consume,
	}
	for _, span := range spans {
		subID := p.client.AllocSubscriptionID()
		p.setProgress(subID, progress)
		if err := p.client.Subscribe(subID, span, startTs, p.inputCh); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Run the puller.
func (p *MultiplexingPuller) Run(ctx context.Context) (err error) {
	metricEventChanSize := eventChanSizeHistogram.WithLabelValues(p.changefeed.Namespace, p.changefeed.ID)
	metricCounterKv := txnCollectCounter.WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "kv")
	metricCounterResolved := txnCollectCounter.WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved")
	defer func() {
		eventChanSizeHistogram.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID)
		txnCollectCounter.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID, "kv")
		txnCollectCounter.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved")
	}()

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < frontierConcurrent; i++ {
		g.Go(func() error { return p.advanceSpans(ctx) })
	}

	g.Go(func() error {
		ticker := time.NewTicker(resolveLockInterval)
		defer ticker.Stop()
	LOOP:
		for {
			var e kv.MultiplexingEvent
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				metricEventChanSize.Observe(float64(len(p.inputCh)))
				goto LOOP
			case e = <-p.inputCh:
			}

			progress := p.getProgress(e.SubscriptionID)
			if progress == nil {
				continue
			}

			if e.Val != nil {
				metricCounterKv.Inc()
				err = p.consume(ctx, e.Val, progress.spans)
			} else if e.Resolved != nil {
				metricCounterResolved.Add(float64(len(e.Resolved.Spans)))
				select {
				case <-ctx.Done():
					err = ctx.Err()
				case progress.resolvedSpans <- e.Resolved:
					p.schedule(ctx, progress)
				}
			}
			if err != nil {
				return errors.Trace(err)
			}
		}
	})

	log.Info("MultiplexingPuller starts",
		zap.String("namespace", p.changefeed.Namespace),
		zap.String("changefeed", p.changefeed.ID),
		zap.Int("frontierConcurrent", frontierConcurrent))

	return g.Wait()
}

func (p *MultiplexingPuller) setProgress(subID kv.SubscriptionID, progress *tableProgress) {
	p.subscriptions.Lock()
	defer p.subscriptions.Unlock()
	p.subscriptions.m[subID] = progress
}

func (p *MultiplexingPuller) getProgress(subID kv.SubscriptionID) *tableProgress {
	p.subscriptions.RLock()
	defer p.subscriptions.RUnlock()
	return p.subscriptions.m[subID]
}

func (p *MultiplexingPuller) schedule(ctx context.Context, progress *tableProgress) {
	if progress.scheduled.CompareAndSwap(false, true) {
		select {
		case <-ctx.Done():
		case p.advanceCh <- progress:
		}
	}
}

func (p *MultiplexingPuller) advanceSpans(ctx context.Context) error {
	handleProgress := func(ctx context.Context, progress *tableProgress) error {
		defer func() {
			progress.scheduled.Store(false)
			if len(progress.resolvedSpans) > 0 {
				p.schedule(ctx, progress)
			}
		}()

		var span *model.ResolvedSpans
		for i := 0; i < 128; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case span = <-progress.resolvedSpans:
			default:
				return nil
			}

			if err := progress.handleResolvedSpan(ctx, span); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}

	var progress *tableProgress
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case progress = <-p.advanceCh:
			if err := handleProgress(ctx, progress); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (p *tableProgress) handleResolvedSpan(ctx context.Context, e *model.ResolvedSpans) (err error) {
	for _, resolvedSpan := range e.Spans {
		if !spanz.IsSubSpan(resolvedSpan.Span, p.spans...) {
			log.Panic("the resolved span is not in the table spans",
				zap.String("namespace", p.changefeed.Namespace),
				zap.String("changefeed", p.changefeed.ID),
				zap.String("tableName", p.tableName),
				zap.Any("spans", p.spans))
		}
		// Forward is called in a single thread
		p.tsTracker.Forward(resolvedSpan.Region, resolvedSpan.Span, e.ResolvedTs)
	}
	resolvedTs := p.tsTracker.Frontier()
	if resolvedTs > 0 && !p.initialized {
		p.initialized = true
		log.Info("table puller is initialized",
			zap.String("namespace", p.changefeed.Namespace),
			zap.String("changefeed", p.changefeed.ID),
			zap.String("tableName", p.tableName),
			zap.Uint64("resolvedTs", resolvedTs))
	}
	if resolvedTs > p.resolvedTs {
		p.resolvedTs = resolvedTs
		raw := &model.RawKVEntry{CRTs: resolvedTs, OpType: model.OpTypeResolved}
		err = p.consume(ctx, raw, p.spans)
	}
	return
}

func (p *MultiplexingPuller) Stats() Stats {
	return Stats{}
}
