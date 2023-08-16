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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	resolveLockFence        time.Duration = 20 * time.Second
	resolveLockTickInterval time.Duration = 10 * time.Second
)

type tableProgress struct {
	changefeed model.ChangeFeedID
	client     *kv.SharedClient
	spans      []tablepb.Span
	startTs    model.Ts
	tableName  string

	initialized          bool
	resolvedTsUpdated    time.Time
	resolvedTs           atomic.Uint64
	maxIngressResolvedTs atomic.Uint64

	resolvedSpans chan kv.MultiplexingEvent
	tsTracker     frontier.Frontier

	consume struct {
		sync.RWMutex
		removed bool
		f       func(context.Context, *model.RawKVEntry, []tablepb.Span) error
	}

	scheduled atomic.Bool
}

// MultiplexingPuller works with `kv.SharedClient`. All tables share resources.
type MultiplexingPuller struct {
	changefeed model.ChangeFeedID
	client     *kv.SharedClient
	consume    func(context.Context, *model.RawKVEntry, []tablepb.Span) error
	workers    int
	hasher     func(tablepb.Span, int) int
	frontiers  int

	// inputChs is used to collect events from client.
	inputChs []chan kv.MultiplexingEvent
	// advanceCh is used to handle resolved ts in frontier workers.
	advanceCh chan *tableProgress

	// NOTE: different subscriptions can share one tableProgress.
	subscriptions struct {
		sync.RWMutex
		m map[kv.SubscriptionID]*tableProgress
		n *spanz.HashMap[*tableProgress]
	}

	pullerEventCounterKv       prometheus.Counter
	pullerEventCounterResolved prometheus.Counter
	queueKvDuration            prometheus.Observer
	queueResolvedDuration      prometheus.Observer
}

// NewMultiplexingPuller creates a MultiplexingPuller. Outputs are handled by
// `consume`, which will be called in several sub-routines concurrently.
//
// `workers` specifies how many workers will be spawned to handle events.
// `frontiers` specifies how many workers will be spawned to handle resolved timestamps.
func NewMultiplexingPuller(
	changefeed model.ChangeFeedID,
	client *kv.SharedClient,
	consume func(context.Context, *model.RawKVEntry, []tablepb.Span) error,
	workers int,
	hasher func(tablepb.Span, int) int,
	frontiers int,
) *MultiplexingPuller {
	x := &MultiplexingPuller{
		changefeed: changefeed,
		client:     client,
		consume:    consume,
		workers:    workers,
		hasher:     hasher,
		frontiers:  frontiers,
		advanceCh:  make(chan *tableProgress, 128),
	}
	x.subscriptions.m = make(map[kv.SubscriptionID]*tableProgress)
	x.subscriptions.n = spanz.NewHashMap[*tableProgress]()

	x.inputChs = make([]chan kv.MultiplexingEvent, 0, workers)
	for i := 0; i < workers; i++ {
		x.inputChs = append(x.inputChs, make(chan kv.MultiplexingEvent, 1024))
	}
	return x
}

// Subscribe some spans. They will share one same resolved timestamp progress.
func (p *MultiplexingPuller) Subscribe(spans []tablepb.Span, startTs model.Ts, tableName string) {
	p.subscriptions.Lock()
	defer p.subscriptions.Unlock()
	p.subscribe(spans, startTs, tableName)
}

func (p *MultiplexingPuller) subscribe(spans []tablepb.Span, startTs model.Ts, tableName string) {
	progress := &tableProgress{
		changefeed: p.changefeed,
		client:     p.client,
		spans:      spans,
		startTs:    startTs,
		tableName:  tableName,

		resolvedSpans: make(chan kv.MultiplexingEvent, 16),
		tsTracker:     frontier.NewFrontier(0, spans...),
	}

	progress.consume.f = func(ctx context.Context, raw *model.RawKVEntry, spans []tablepb.Span) error {
		progress.consume.RLock()
		defer progress.consume.RUnlock()
		if !progress.consume.removed {
			return p.consume(ctx, raw, spans)
		}
		return nil
	}

	for _, span := range spans {
		subID := p.client.AllocSubscriptionID()
		p.subscriptions.m[subID] = progress
		p.subscriptions.n.ReplaceOrInsert(span, progress)

		slot := p.hasher(span, len(p.inputChs))
		if _, ok := p.client.Subscribe(subID, span, startTs, p.inputChs[slot]); !ok {
			log.Panic("redundant subscription",
				zap.String("namespace", p.changefeed.Namespace),
				zap.String("changefeed", p.changefeed.ID),
				zap.String("span", span.String()))
		}
	}
}

// Unsubscribe some spans, which must be subscribed in one call.
func (p *MultiplexingPuller) Unsubscribe(spans []tablepb.Span) {
	p.subscriptions.Lock()
	defer p.subscriptions.Unlock()
	p.unsubscribe(spans)
}

func (p *MultiplexingPuller) unsubscribe(spans []tablepb.Span) {
	for _, span := range spans {
		if subID, ok := p.client.Unsubscribe(span); ok {
			progress := p.subscriptions.m[subID]
			progress.consume.Lock()
			progress.consume.removed = true
			progress.consume.Unlock()

			delete(p.subscriptions.m, subID)
			p.subscriptions.n.Delete(span)
			continue
		}
		log.Panic("unexist unsubscription",
			zap.String("namespace", p.changefeed.Namespace),
			zap.String("changefeed", p.changefeed.ID),
			zap.String("span", span.String()))
	}
}

// Run the puller.
func (p *MultiplexingPuller) Run(ctx context.Context) (err error) {
	return p.run(ctx, true)
}

func (p *MultiplexingPuller) run(ctx context.Context, includeClient bool) error {
	p.pullerEventCounterKv = PullerEventCounter.WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "kv")
	p.pullerEventCounterResolved = PullerEventCounter.WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved")
	p.queueKvDuration = pullerQueueDuration.WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "kv")
	p.queueResolvedDuration = pullerQueueDuration.WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved")
	defer func() {
		PullerEventCounter.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID, "kv")
		PullerEventCounter.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved")
		pullerQueueDuration.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID, "kv")
		pullerQueueDuration.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved")
	}()

	g, ctx := errgroup.WithContext(ctx)
	if includeClient {
		g.Go(func() error { return p.client.Run(ctx) })
	}

	g.Go(func() error { return p.checkResolveLock(ctx) })

	for i := 0; i < p.frontiers; i++ {
		g.Go(func() error { return p.advanceSpans(ctx) })
	}
	for i := range p.inputChs {
		inputCh := p.inputChs[i]
		g.Go(func() error { return p.handleInputCh(ctx, inputCh) })
	}

	log.Info("MultiplexingPuller starts",
		zap.String("namespace", p.changefeed.Namespace),
		zap.String("changefeed", p.changefeed.ID),
		zap.Int("frontierConcurrent", p.frontiers))
	return g.Wait()
}

func (p *MultiplexingPuller) handleInputCh(ctx context.Context, inputCh <-chan kv.MultiplexingEvent) error {
	for {
		var e kv.MultiplexingEvent
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e = <-inputCh:
		}

		progress := p.getProgress(e.SubscriptionID)
		if progress == nil {
			continue
		}

		if e.Val != nil {
			p.queueKvDuration.Observe(float64(time.Since(e.Start).Milliseconds()))
			p.pullerEventCounterKv.Inc()
			if err := progress.consume.f(ctx, e.Val, progress.spans); err != nil {
				return errors.Trace(err)
			}
		} else if e.Resolved != nil {
			p.pullerEventCounterResolved.Add(float64(len(e.Resolved.Spans)))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case progress.resolvedSpans <- e:
				p.schedule(ctx, progress)
			}
		}
	}
}

func (p *MultiplexingPuller) getProgress(subID kv.SubscriptionID) *tableProgress {
	p.subscriptions.RLock()
	defer p.subscriptions.RUnlock()
	return p.subscriptions.m[subID]
}

func (p *MultiplexingPuller) getAllProgresses() map[*tableProgress]struct{} {
	p.subscriptions.RLock()
	defer p.subscriptions.RUnlock()
	hashset := make(map[*tableProgress]struct{}, len(p.subscriptions.m))
	for _, value := range p.subscriptions.m {
		hashset[value] = struct{}{}
	}
	return hashset
}

func (p *MultiplexingPuller) getProgressBySpan(span tablepb.Span) *tableProgress {
	p.subscriptions.RLock()
	defer p.subscriptions.RUnlock()
	return p.subscriptions.n.GetV(span)
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

		var event kv.MultiplexingEvent
		var spans *model.ResolvedSpans
		for i := 0; i < 128; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event = <-progress.resolvedSpans:
				spans = event.RegionFeedEvent.Resolved
			default:
				return nil
			}
			if spans == nil {
				// It means the event comes from `checkResolveLock`.
				progress.resolveLock()
				continue
			}
			p.queueResolvedDuration.Observe(float64(time.Since(event.Start).Milliseconds()))
			if err := progress.handleResolvedSpans(ctx, spans); err != nil {
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

func (p *MultiplexingPuller) checkResolveLock(ctx context.Context) error {
	resolveLockTicker := time.NewTicker(resolveLockTickInterval)
	defer resolveLockTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resolveLockTicker.C:
		}
		for progress := range p.getAllProgresses() {
			progress.resolvedSpans <- kv.MultiplexingEvent{}
		}
	}
}

func (p *tableProgress) handleResolvedSpans(ctx context.Context, e *model.ResolvedSpans) (err error) {
	for _, resolvedSpan := range e.Spans {
		if !spanz.IsSubSpan(resolvedSpan.Span, p.spans...) {
			log.Panic("the resolved span is not in the table spans",
				zap.String("namespace", p.changefeed.Namespace),
				zap.String("changefeed", p.changefeed.ID),
				zap.String("tableName", p.tableName),
				zap.Any("spans", p.spans))
		}
		p.tsTracker.Forward(resolvedSpan.Region, resolvedSpan.Span, e.ResolvedTs)
		if e.ResolvedTs > p.maxIngressResolvedTs.Load() {
			p.maxIngressResolvedTs.Store(e.ResolvedTs)
		}
	}
	resolvedTs := p.tsTracker.Frontier()

	if resolvedTs > 0 && !p.initialized {
		p.initialized = true
		log.Info("puller is initialized",
			zap.String("namespace", p.changefeed.Namespace),
			zap.String("changefeed", p.changefeed.ID),
			zap.String("tableName", p.tableName),
			zap.Uint64("resolvedTs", resolvedTs))
	}
	if resolvedTs > p.resolvedTs.Load() {
		p.resolvedTs.Store(resolvedTs)
		p.resolvedTsUpdated = time.Now()
		raw := &model.RawKVEntry{CRTs: resolvedTs, OpType: model.OpTypeResolved}
		err = p.consume.f(ctx, raw, p.spans)
	}

	return
}

func (p *tableProgress) resolveLock() {
	if !p.initialized || time.Since(p.resolvedTsUpdated) < resolveLockFence {
		return
	}
	resolvedTs := p.resolvedTs.Load()
	resolvedTime := oracle.GetTimeFromTS(resolvedTs)
	currentTime := p.client.GetPDClock().CurrentTime()
	if currentTime.Sub(resolvedTime) < resolveLockFence {
		return
	}

	maxVersion := oracle.GoTimeToTS(resolvedTime.Add(resolveLockFence))
	for _, span := range p.spans {
		p.client.ResolveLock(span, maxVersion)
	}
}

// Stats returns Stats.
func (p *MultiplexingPuller) Stats(span tablepb.Span) Stats {
	var progress *tableProgress
	if progress = p.getProgressBySpan(span); progress == nil {
		return Stats{}
	}
	return Stats{
		RegionCount:         p.client.RegionCount(span),
		ResolvedTsIngress:   progress.maxIngressResolvedTs.Load(),
		CheckpointTsIngress: progress.maxIngressResolvedTs.Load(),
		ResolvedTsEgress:    progress.resolvedTs.Load(),
		CheckpointTsEgress:  progress.resolvedTs.Load(),
	}
}
