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

package codec

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultInputChanSize  = 128
	defaultMetricInterval = 15 * time.Second
)

// EncoderGroup manages a group of encoders
type EncoderGroup interface {
	// Run start the group
	Run(ctx context.Context) error
	// AddEvents add events into the group and encode them by one of the encoders in the group.
	// Note: The caller should make sure all events should belong to the same topic and partition.
	AddEvents(ctx context.Context, key model.TopicPartitionKey, events ...*dmlsink.RowChangeCallbackableEvent) error
	// Output returns a channel produce futures
	Output() <-chan *future
}

type encoderGroup struct {
	changefeedID model.ChangeFeedID

	builder RowEventEncoderBuilder
	// concurrency is the number of encoder pipelines to run
	concurrency int
	// inputCh is the input channel for each encoder pipeline
	inputCh []chan *future
	index   uint64

	outputCh        chan *future
	bootstrapWorker *bootstrapWorker
}

// NewEncoderGroup creates a new EncoderGroup instance
func NewEncoderGroup(
	cfg *config.SinkConfig,
	builder RowEventEncoderBuilder,
	changefeedID model.ChangeFeedID,
) *encoderGroup {
	concurrency := util.GetOrZero(cfg.EncoderConcurrency)
	if concurrency <= 0 {
		concurrency = config.DefaultEncoderGroupConcurrency
	}
	inputCh := make([]chan *future, concurrency)
	for i := 0; i < concurrency; i++ {
		inputCh[i] = make(chan *future, defaultInputChanSize)
	}
	outCh := make(chan *future, defaultInputChanSize*concurrency)

	var bootstrapWorker *bootstrapWorker
	if cfg.ShouldSendBootstrapMsg() {
		bootstrapWorker = newBootstrapWorker(
			changefeedID,
			outCh,
			builder.Build(),
			util.GetOrZero(cfg.SendBootstrapIntervalInSec),
			util.GetOrZero(cfg.SendBootstrapInMsgCount),
			util.GetOrZero(cfg.SendBootstrapToAllPartition),
			defaultMaxInactiveDuration,
		)
	}

	return &encoderGroup{
		changefeedID:    changefeedID,
		builder:         builder,
		concurrency:     concurrency,
		inputCh:         inputCh,
		index:           0,
		outputCh:        outCh,
		bootstrapWorker: bootstrapWorker,
	}
}

func (g *encoderGroup) Run(ctx context.Context) error {
	defer func() {
		g.cleanMetrics()
		log.Info("encoder group exited",
			zap.String("namespace", g.changefeedID.Namespace),
			zap.String("changefeed", g.changefeedID.ID))
	}()
	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < g.concurrency; i++ {
		idx := i
		eg.Go(func() error {
			return g.runEncoder(ctx, idx)
		})
	}
	eg.Go(func() error {
		return g.collectMetrics(ctx)
	})

	if g.bootstrapWorker != nil {
		eg.Go(func() error {
			return g.bootstrapWorker.run(ctx)
		})
	}

	return eg.Wait()
}

func (g *encoderGroup) collectMetrics(ctx context.Context) error {
	ticker := time.NewTicker(defaultMetricInterval)
	inputChSize := encoderGroupInputChanSizeGauge.WithLabelValues(g.changefeedID.Namespace, g.changefeedID.ID)
	outputChSize := encoderGroupOutputChanSizeGauge.WithLabelValues(g.changefeedID.Namespace, g.changefeedID.ID)
	defer func() {
		ticker.Stop()
		encoderGroupInputChanSizeGauge.DeleteLabelValues(g.changefeedID.Namespace, g.changefeedID.ID)
		encoderGroupOutputChanSizeGauge.DeleteLabelValues(g.changefeedID.Namespace, g.changefeedID.ID)
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			var total int
			for _, ch := range g.inputCh {
				total += len(ch)
			}
			inputChSize.Set(float64(total))
			outputChSize.Set(float64(len(g.outputCh)))
		}
	}
}

func (g *encoderGroup) runEncoder(ctx context.Context, idx int) error {
	encoder := g.builder.Build()
	inputCh := g.inputCh[idx]
	for {
		select {
		case <-ctx.Done():
			return nil
		case future := <-inputCh:
			for _, event := range future.events {
				err := encoder.AppendRowChangedEvent(ctx, future.Key.Topic, event.Event, event.Callback)
				if err != nil {
					return errors.Trace(err)
				}
			}
			future.Messages = encoder.Build()
			close(future.done)
		}
	}
}

func (g *encoderGroup) AddEvents(
	ctx context.Context,
	key model.TopicPartitionKey,
	events ...*dmlsink.RowChangeCallbackableEvent,
) error {
	// bootstrapWorker only not nil when the protocol is simple
	if g.bootstrapWorker != nil {
		err := g.bootstrapWorker.addEvent(ctx, key, events[0].Event)
		if err != nil {
			return errors.Trace(err)
		}
	}

	future := newFuture(key, events...)
	index := atomic.AddUint64(&g.index, 1) % uint64(g.concurrency)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case g.inputCh[index] <- future:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case g.outputCh <- future:
	}

	return nil
}

func (g *encoderGroup) Output() <-chan *future {
	return g.outputCh
}

func (g *encoderGroup) cleanMetrics() {
	g.builder.CleanMetrics()
	common.CleanMetrics(g.changefeedID)
}

// future is a wrapper of the result of encoding events
// It's used to notify the caller that the result is ready.
type future struct {
	Key      model.TopicPartitionKey
	events   []*dmlsink.RowChangeCallbackableEvent
	Messages []*common.Message
	done     chan struct{}
}

func newFuture(key model.TopicPartitionKey,
	events ...*dmlsink.RowChangeCallbackableEvent,
) *future {
	return &future{
		Key:    key,
		events: events,
		done:   make(chan struct{}),
	}
}

// Ready waits until the response is ready, should be called before consuming the future.
func (p *future) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
	}
	return nil
}
