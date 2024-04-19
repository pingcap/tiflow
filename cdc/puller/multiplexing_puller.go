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
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	resolveLockFence        time.Duration = 4 * time.Second
	resolveLockTickInterval time.Duration = 2 * time.Second

	// Suppose there are 50K tables, total size of `resolvedEventsCache`s will be
	// unsafe.SizeOf(kv.MultiplexingEvent) * 50K * 256 = 800M.
	tableResolvedTsBufferSize int = 256

	defaultPullerOutputChanSize = 128

	inputChSize                = 1024
	tableProgressAdvanceChSize = 128
)

type tableProgress struct {
	changefeed      model.ChangeFeedID
	client          *kv.SharedClient
	spans           []tablepb.Span
	subscriptionIDs []kv.SubscriptionID
	startTs         model.Ts
	tableName       string

	initialized          atomic.Bool
	resolvedTsUpdated    atomic.Int64
	resolvedTs           atomic.Uint64
	maxIngressResolvedTs atomic.Uint64

	resolvedEventsCache chan kv.MultiplexingEvent
	tsTracker           frontier.Frontier

	consume struct {
		// This lock is used to prevent the table progress from being
		// removed while consuming events.
		sync.RWMutex
		removed bool
		f       func(context.Context, *model.RawKVEntry, []tablepb.Span) error
	}

	scheduled atomic.Bool
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

	if resolvedTs > 0 && p.initialized.CompareAndSwap(false, true) {
		log.Info("puller is initialized",
			zap.String("namespace", p.changefeed.Namespace),
			zap.String("changefeed", p.changefeed.ID),
			zap.String("tableName", p.tableName),
			zap.Uint64("resolvedTs", resolvedTs))
	}
	if resolvedTs > p.resolvedTs.Load() {
		p.resolvedTs.Store(resolvedTs)
		p.resolvedTsUpdated.Store(time.Now().Unix())
		raw := &model.RawKVEntry{CRTs: resolvedTs, OpType: model.OpTypeResolved}
		err = p.consume.f(ctx, raw, p.spans)
	}

	return
}

func (p *tableProgress) resolveLock(currentTime time.Time) {
	resolvedTsUpdated := time.Unix(p.resolvedTsUpdated.Load(), 0)
	if !p.initialized.Load() || time.Since(resolvedTsUpdated) < resolveLockFence {
		return
	}
	resolvedTs := p.resolvedTs.Load()
	resolvedTime := oracle.GetTimeFromTS(resolvedTs)
	if currentTime.Sub(resolvedTime) < resolveLockFence {
		return
	}

	maxVersion := oracle.GoTimeToTS(resolvedTime.Add(resolveLockFence))
	for _, subID := range p.subscriptionIDs {
		p.client.ResolveLock(subID, maxVersion)
	}
}

type subscription struct {
	*tableProgress
	subID kv.SubscriptionID
}

// MultiplexingPuller works with `kv.SharedClient`. All tables share resources.
type MultiplexingPuller struct {
	changefeed model.ChangeFeedID
	client     *kv.SharedClient
	pdClock    pdutil.Clock
	consume    func(context.Context, *model.RawKVEntry, []tablepb.Span) error
	// inputChannelIndexer is used to determine which input channel to use for a given span.
	inputChannelIndexer func(span tablepb.Span, workerCount int) int

	// inputChs is used to collect events from client.
	inputChs []chan kv.MultiplexingEvent
	// tableProgressAdvanceCh is used to notify the tableAdvancer goroutine
	// to advance the progress of a table.
	tableProgressAdvanceCh chan *tableProgress

	// NOTE: A tableProgress can have multiple subscription, all of them share
	// the same tableProgress. So, we use two maps to store the relationshipxxx
	// between subscription and tableProgress.
	subscriptions struct {
		sync.RWMutex
		// m map subscriptionID -> tableProgress, used to cache and
		// get tableProgress by subscriptionID quickly.
		m map[kv.SubscriptionID]*tableProgress
		// n map span -> subscription, used to cache and get subscription by span quickly.
		n *spanz.HashMap[subscription]
	}

	resolvedTsAdvancerCount int

	CounterKv              prometheus.Counter
	CounterResolved        prometheus.Counter
	CounterResolvedDropped prometheus.Counter
	queueKvDuration        prometheus.Observer
	queueResolvedDuration  prometheus.Observer
}

// NewMultiplexingPuller creates a MultiplexingPuller.
// `workerCount` specifies how many workers will be spawned to handle events from kv client.
// `frontierCount` specifies how many workers will be spawned to handle resolvedTs event.
func NewMultiplexingPuller(
	changefeed model.ChangeFeedID,
	client *kv.SharedClient,
	pdClock pdutil.Clock,
	consume func(context.Context, *model.RawKVEntry, []tablepb.Span) error,
	workerCount int,
	inputChannelIndexer func(tablepb.Span, int) int,
	resolvedTsAdvancerCount int,
) *MultiplexingPuller {
	mpuller := &MultiplexingPuller{
		changefeed:              changefeed,
		client:                  client,
		pdClock:                 pdClock,
		consume:                 consume,
		inputChannelIndexer:     inputChannelIndexer,
		resolvedTsAdvancerCount: resolvedTsAdvancerCount,
		tableProgressAdvanceCh:  make(chan *tableProgress, tableProgressAdvanceChSize),
	}
	mpuller.subscriptions.m = make(map[kv.SubscriptionID]*tableProgress)
	mpuller.subscriptions.n = spanz.NewHashMap[subscription]()

	mpuller.inputChs = make([]chan kv.MultiplexingEvent, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		mpuller.inputChs = append(mpuller.inputChs, make(chan kv.MultiplexingEvent, inputChSize))
	}
	return mpuller
}

// Subscribe some spans. They will share one same resolved timestamp progress.
func (p *MultiplexingPuller) Subscribe(spans []tablepb.Span, startTs model.Ts, tableName string) {
	p.subscriptions.Lock()
	defer p.subscriptions.Unlock()
	p.subscribe(spans, startTs, tableName)
}

func (p *MultiplexingPuller) subscribe(
	spans []tablepb.Span,
	startTs model.Ts,
	tableName string,
) {
	for _, span := range spans {
		// Base on the current design, a MultiplexingPuller is only used for one changefeed.
		// So, one span can only be subscribed once.
		if _, exists := p.subscriptions.n.Get(span); exists {
			log.Panic("redundant subscription",
				zap.String("namespace", p.changefeed.Namespace),
				zap.String("changefeed", p.changefeed.ID),
				zap.String("span", span.String()))
		}
	}

	// Create a new table progress for the spans.
	progress := &tableProgress{
		changefeed:      p.changefeed,
		client:          p.client,
		spans:           spans,
		subscriptionIDs: make([]kv.SubscriptionID, len(spans)),
		startTs:         startTs,
		tableName:       tableName,

		resolvedEventsCache: make(chan kv.MultiplexingEvent, tableResolvedTsBufferSize),
		tsTracker:           frontier.NewFrontier(0, spans...),
	}

	progress.consume.f = func(
		ctx context.Context,
		raw *model.RawKVEntry,
		spans []tablepb.Span,
	) error {
		progress.consume.RLock()
		defer progress.consume.RUnlock()
		if !progress.consume.removed {
			return p.consume(ctx, raw, spans)
		}
		return nil
	}

	for i, span := range spans {
		subID := p.client.AllocSubscriptionID()
		progress.subscriptionIDs[i] = subID

		p.subscriptions.m[subID] = progress
		p.subscriptions.n.ReplaceOrInsert(span, subscription{progress, subID})

		slot := p.inputChannelIndexer(span, len(p.inputChs))
		p.client.Subscribe(subID, span, startTs, p.inputChs[slot])
	}

	progress.initialized.Store(false)
	progress.resolvedTsUpdated.Store(time.Now().Unix())
}

// Unsubscribe some spans, which must be subscribed in one call.
func (p *MultiplexingPuller) Unsubscribe(spans []tablepb.Span) {
	p.subscriptions.Lock()
	defer p.subscriptions.Unlock()
	p.unsubscribe(spans)
}

func (p *MultiplexingPuller) unsubscribe(spans []tablepb.Span) {
	var progress *tableProgress
	for _, span := range spans {
		if prog, exists := p.subscriptions.n.Get(span); exists {
			if prog.tableProgress != progress && progress != nil {
				log.Panic("unsubscribe spans not in one subscription",
					zap.String("namespace", p.changefeed.Namespace),
					zap.String("changefeed", p.changefeed.ID))
			}
			progress = prog.tableProgress
		} else {
			log.Panic("unexist unsubscription",
				zap.String("namespace", p.changefeed.Namespace),
				zap.String("changefeed", p.changefeed.ID),
				zap.String("span", span.String()))
		}
	}
	if len(progress.spans) != len(spans) {
		log.Panic("unsubscribe spans not same with subscription",
			zap.String("namespace", p.changefeed.Namespace),
			zap.String("changefeed", p.changefeed.ID))
	}

	progress.consume.Lock()
	progress.consume.removed = true
	progress.consume.Unlock()
	for i, span := range progress.spans {
		p.client.Unsubscribe(progress.subscriptionIDs[i])
		delete(p.subscriptions.m, progress.subscriptionIDs[i])
		p.subscriptions.n.Delete(span)
	}
}

// Run the puller.
func (p *MultiplexingPuller) Run(ctx context.Context) (err error) {
	return p.run(ctx, true)
}

func (p *MultiplexingPuller) run(ctx context.Context, includeClient bool) error {
	p.CounterKv = PullerEventCounter.WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "kv")
	p.CounterResolved = PullerEventCounter.WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved")
	p.CounterResolvedDropped = PullerEventCounter.WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved-dropped")
	p.queueKvDuration = pullerQueueDuration.WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "kv")
	p.queueResolvedDuration = pullerQueueDuration.WithLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved")
	defer func() {
		PullerEventCounter.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID, "kv")
		PullerEventCounter.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved")
		PullerEventCounter.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved-dropped")
		pullerQueueDuration.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID, "kv")
		pullerQueueDuration.DeleteLabelValues(p.changefeed.Namespace, p.changefeed.ID, "resolved")
		log.Info("MultiplexingPuller exits",
			zap.String("namespace", p.changefeed.Namespace),
			zap.String("changefeed", p.changefeed.ID))
	}()

	eg, ctx := errgroup.WithContext(ctx)

	// Only !includeClient in tests.
	if includeClient {
		eg.Go(func() error { return p.client.Run(ctx) })
	}

	// Start workers to handle events received from kv client.
	for i := range p.inputChs {
		inputCh := p.inputChs[i]
		eg.Go(func() error { return p.runEventHandler(ctx, inputCh) })
	}

	// Start workers to check and resolve stale locks.
	eg.Go(func() error { return p.runResolveLockChecker(ctx) })

	for i := 0; i < p.resolvedTsAdvancerCount; i++ {
		eg.Go(func() error { return p.runResolvedTsAdvancer(ctx) })
	}

	log.Info("MultiplexingPuller starts",
		zap.String("namespace", p.changefeed.Namespace),
		zap.String("changefeed", p.changefeed.ID),
		zap.Int("workerConcurrent", len(p.inputChs)),
		zap.Int("frontierConcurrent", p.resolvedTsAdvancerCount))
	return eg.Wait()
}

// runEventHandler consumes events from inputCh:
// 1. If the event is a kv event, consume by calling progress.consume.f.
// 2. If the event is a resolved event, send it to the resolvedEventsCache of the corresponding progress.
func (p *MultiplexingPuller) runEventHandler(ctx context.Context, inputCh <-chan kv.MultiplexingEvent) error {
	for {
		var e kv.MultiplexingEvent
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e = <-inputCh:
		}

		progress := p.getProgress(e.SubscriptionID)
		// There is a chance that some stale events are received after
		// the subscription is removed. We can just ignore them.
		if progress == nil {
			continue
		}

		if e.Val != nil {
			p.queueKvDuration.Observe(float64(time.Since(e.Start).Milliseconds()))
			p.CounterKv.Inc()
			if err := progress.consume.f(ctx, e.Val, progress.spans); err != nil {
				return errors.Trace(err)
			}
		} else if e.Resolved != nil {
			p.CounterResolved.Add(float64(len(e.Resolved.Spans)))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case progress.resolvedEventsCache <- e:
				p.schedule(ctx, progress)
			default:
				p.CounterResolvedDropped.Add(float64(len(e.Resolved.Spans)))
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

func (p *MultiplexingPuller) schedule(ctx context.Context, progress *tableProgress) {
	if progress.scheduled.CompareAndSwap(false, true) {
		select {
		case <-ctx.Done():
		case p.tableProgressAdvanceCh <- progress:
		}
	}
}

// runResolvedTsAdvancer receives tableProgress from tableProgressAdvanceCh
// and advances the resolvedTs of the tableProgress.
func (p *MultiplexingPuller) runResolvedTsAdvancer(ctx context.Context) error {
	advanceTableProgress := func(ctx context.Context, progress *tableProgress) error {
		defer func() {
			progress.scheduled.Store(false)
			// Schedule the progress again if there are still events in the cache.
			if len(progress.resolvedEventsCache) > 0 {
				p.schedule(ctx, progress)
			}
		}()

		var event kv.MultiplexingEvent
		var spans *model.ResolvedSpans
		for i := 0; i < 128; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event = <-progress.resolvedEventsCache:
				spans = event.RegionFeedEvent.Resolved
			default:
				return nil
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
		case progress = <-p.tableProgressAdvanceCh:
			if err := advanceTableProgress(ctx, progress); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (p *MultiplexingPuller) runResolveLockChecker(ctx context.Context) error {
	resolveLockTicker := time.NewTicker(resolveLockTickInterval)
	defer resolveLockTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resolveLockTicker.C:
		}
		currentTime := p.pdClock.CurrentTime()
		for progress := range p.getAllProgresses() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				progress.resolveLock(currentTime)
			}
		}
	}
}

// Stats of a puller.
type Stats struct {
	RegionCount         uint64
	CheckpointTsIngress model.Ts
	ResolvedTsIngress   model.Ts
	CheckpointTsEgress  model.Ts
	ResolvedTsEgress    model.Ts
}

// Stats returns Stats.
func (p *MultiplexingPuller) Stats(span tablepb.Span) Stats {
	p.subscriptions.RLock()
	progress := p.subscriptions.n.GetV(span)
	p.subscriptions.RUnlock()
	if progress.tableProgress == nil {
		return Stats{}
	}
	return Stats{
		RegionCount:         p.client.RegionCount(progress.subID),
		ResolvedTsIngress:   progress.maxIngressResolvedTs.Load(),
		CheckpointTsIngress: progress.maxIngressResolvedTs.Load(),
		ResolvedTsEgress:    progress.resolvedTs.Load(),
		CheckpointTsEgress:  progress.resolvedTs.Load(),
	}
}
