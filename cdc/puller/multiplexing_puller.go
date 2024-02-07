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
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	resolveLockFence        time.Duration = 20 * time.Second
	resolveLockTickInterval time.Duration = 10 * time.Second

	// Suppose there are 50K tables, total size of `resolvedEventsCache`s will be
	// unsafe.SizeOf(kv.MultiplexingEvent) * 50K * 256 = 800M.
	tableResolvedTsBufferSize int = 256
)

type tableProgress struct {
	changefeed model.ChangeFeedID
	client     *kv.SharedClient
	spans      []tablepb.Span
	subIDs     []kv.SubscriptionID
	startTs    model.Ts
	tableName  string

	initialized          atomic.Bool
	resolvedTsUpdated    atomic.Int64
	resolvedTs           atomic.Uint64
	maxIngressResolvedTs atomic.Uint64

	resolvedEventsCache chan kv.MultiplexingEvent
	tsTracker           frontier.Frontier

	consume struct {
		sync.RWMutex
		removed bool
		f       func(context.Context, *model.RawKVEntry, []tablepb.Span) error
	}

	scheduled atomic.Bool
}

type tableProgressWithSubID struct {
	*tableProgress
	subID kv.SubscriptionID
}

// MultiplexingPuller works with `kv.SharedClient`. All tables share resources.
type MultiplexingPuller struct {
	changefeed model.ChangeFeedID
	client     *kv.SharedClient
	consume    func(context.Context, *model.RawKVEntry, []tablepb.Span) error

	advanceInternalInMs uint

	// how many goroutines to handle table frontiers.
	frontiers int
	// inputChs is used to collect events from client.
	inputChs []chan kv.MultiplexingEvent
	// how to hash a table span to a slot in inputChs.
	hasher func(tablepb.Span, int) int
	// advanceCh is used to handle resolved ts in frontier workers.
	advanceCh chan *tableProgress

	// NOTE: different subscriptions can share one tableProgress.
	subscriptions struct {
		sync.RWMutex
		m map[kv.SubscriptionID]*tableProgress
		n *spanz.HashMap[tableProgressWithSubID]
	}

	CounterKv              prometheus.Counter
	CounterResolved        prometheus.Counter
	CounterResolvedDropped prometheus.Counter
	queueKvDuration        prometheus.Observer
	queueResolvedDuration  prometheus.Observer
}

// NewMultiplexingDMLPuller creates a MultiplexingPuller. Outputs are handled by
// `consume`, which will be called in several sub-routines concurrently.
//
// `workers` specifies how many workers will be spawned to handle events.
// `frontiers` specifies how many workers will be spawned to handle resolved timestamps.
func NewMultiplexingDMLPuller(
	changefeed model.ChangeFeedID,
	client *kv.SharedClient,
	consume func(context.Context, *model.RawKVEntry, []tablepb.Span) error,
	// TODO: remove follow parameters after cfg.KVClient.AdvanceIntervalInMs is stabled.
	workers int,
	hasher func(tablepb.Span, int) int,
) *MultiplexingPuller {
	x := &MultiplexingPuller{
		changefeed: changefeed,
		client:     client,
		consume:    consume,
	}
	x.subscriptions.m = make(map[kv.SubscriptionID]*tableProgress)
	x.subscriptions.n = spanz.NewHashMap[tableProgressWithSubID]()

	cfg := config.GetGlobalServerConfig()
	if cfg.KVClient.AdvanceIntervalInMs > 0 {
		x.advanceInternalInMs = cfg.KVClient.AdvanceIntervalInMs
		x.frontiers = 0
	} else {
		x.frontiers = int(cfg.KVClient.FrontierConcurrent)
		x.inputChs = make([]chan kv.MultiplexingEvent, 0, workers)
		for i := 0; i < workers; i++ {
			x.inputChs = append(x.inputChs, make(chan kv.MultiplexingEvent, 1024))
		}
		x.hasher = hasher
		x.advanceCh = make(chan *tableProgress, 128)
	}

	return x
}

// NewMultiplexingDDLPuller is like NewMultiplexingDMLPuller, with some simplifications
// for tiny input flow from DDL meta regions.
func NewMultiplexingDDLPuller(
	changefeed model.ChangeFeedID,
	client *kv.SharedClient,
	consume func(context.Context, *model.RawKVEntry, []tablepb.Span) error,
) *MultiplexingPuller {
	x := &MultiplexingPuller{
		changefeed: changefeed,
		client:     client,
		consume:    consume,
	}
	x.subscriptions.m = make(map[kv.SubscriptionID]*tableProgress)
	x.subscriptions.n = spanz.NewHashMap[tableProgressWithSubID]()

	cfg := config.GetGlobalServerConfig()
	if cfg.KVClient.AdvanceIntervalInMs > 0 {
		x.advanceInternalInMs = cfg.KVClient.AdvanceIntervalInMs
		x.frontiers = 0
	} else {
		x.frontiers = 1
		x.inputChs = make([]chan kv.MultiplexingEvent, 0, 1)
		x.inputChs = append(x.inputChs, make(chan kv.MultiplexingEvent, 1024))
		x.hasher = func(tablepb.Span, int) int { return 0 }
		x.advanceCh = make(chan *tableProgress, 128)
	}

	return x
}

// Subscribe some spans. They will share one same resolved timestamp progress.
func (p *MultiplexingPuller) Subscribe(spans []tablepb.Span, startTs model.Ts, tableName string) {
	p.subscriptions.Lock()
	defer p.subscriptions.Unlock()
	p.subscribe(spans, startTs, tableName)
}

func (p *MultiplexingPuller) subscribe(spans []tablepb.Span, startTs model.Ts, tableName string) []kv.SubscriptionID {
	for _, span := range spans {
		if _, exists := p.subscriptions.n.Get(span); exists {
			log.Panic("redundant subscription",
				zap.String("namespace", p.changefeed.Namespace),
				zap.String("changefeed", p.changefeed.ID),
				zap.String("span", span.String()))
		}
	}

	progress := &tableProgress{
		changefeed: p.changefeed,
		client:     p.client,
		spans:      spans,
		subIDs:     make([]kv.SubscriptionID, len(spans)),
		startTs:    startTs,
		tableName:  tableName,

		resolvedEventsCache: make(chan kv.MultiplexingEvent, tableResolvedTsBufferSize),
		tsTracker:           frontier.NewFrontier(0, spans...),
	}
	progress.initialized.Store(false)
	progress.resolvedTsUpdated.Store(time.Now().Unix())

	progress.consume.f = func(ctx context.Context, raw *model.RawKVEntry, spans []tablepb.Span) error {
		progress.consume.RLock()
		defer progress.consume.RUnlock()
		if !progress.consume.removed {
			return p.consume(ctx, raw, spans)
		}
		return nil
	}

	for i, span := range spans {
		subID := p.client.AllocSubscriptionID()
		progress.subIDs[i] = subID

		p.subscriptions.m[subID] = progress
		p.subscriptions.n.ReplaceOrInsert(span, tableProgressWithSubID{progress, subID})

		var eventHandler kv.EventHandler
		if p.advanceInternalInMs > 0 {
			eventHandler = func(ctx context.Context, event kv.MultiplexingEvent) error {
				progress := p.getProgress(event.SubscriptionID)
				if event.Val != nil {
					p.CounterKv.Inc()
					return progress.consume.f(ctx, event.Val, progress.spans)
				} else if event.Resolved != nil {
					// Table span ResolvedTs has been advanced in module kv-client.
					if event.Resolved.ResolvedTs > progress.resolvedTs.Load() {
						progress.initialized.Store(true)
						progress.resolvedTs.Store(event.Resolved.ResolvedTs)
						progress.resolvedTsUpdated.Store(time.Now().Unix())
						resolved := &model.RawKVEntry{CRTs: event.Resolved.ResolvedTs, OpType: model.OpTypeResolved}
						return progress.consume.f(ctx, resolved, progress.spans)
					}
				}
				return errors.New("unexpected event without Val or Resolved")
			}
		} else {
			slot := p.hasher(span, len(p.inputChs))
			eventHandler = func(ctx context.Context, event kv.MultiplexingEvent) error {
				select {
				case p.inputChs[slot] <- event:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}

		}
		p.client.Subscribe(subID, span, startTs, eventHandler)
	}
	return progress.subIDs
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
		p.client.Unsubscribe(progress.subIDs[i])
		delete(p.subscriptions.m, progress.subIDs[i])
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
		zap.Int("workerConcurrent", len(p.inputChs)),
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
		case p.advanceCh <- progress:
		}
	}
}

func (p *MultiplexingPuller) advanceSpans(ctx context.Context) error {
	handleProgress := func(ctx context.Context, progress *tableProgress) error {
		defer func() {
			progress.scheduled.Store(false)
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
		currentTime := p.client.GetPDClock().CurrentTime()
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
	for _, subID := range p.subIDs {
		p.client.ResolveLock(subID, maxVersion)
	}
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
