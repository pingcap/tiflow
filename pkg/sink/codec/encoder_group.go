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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultEncoderGroupSize = 16
	defaultInputChanSize    = 256
	defaultMetricInterval   = 15 * time.Second
)

// EncoderGroup manages a group of encoders
type EncoderGroup interface {
	// Run start the group
	Run(ctx context.Context) error
	// AddEvents add events into the group, handled by one of the encoders
	// all input events should belong to the same topic and partition, this should be guaranteed by the caller
	AddEvents(ctx context.Context, topic string, partition int32,
		events ...*dmlsink.RowChangeCallbackableEvent) error
	// Output returns a channel produce futures
	Output() <-chan *future
}

type encoderGroup struct {
	changefeedID model.ChangeFeedID

	builder RowEventEncoderBuilder
	count   int
	inputCh []chan *future
	index   uint64

	outputCh chan *future
}

// NewEncoderGroup creates a new EncoderGroup instance
func NewEncoderGroup(builder RowEventEncoderBuilder,
	count int, changefeedID model.ChangeFeedID,
) *encoderGroup {
	if count <= 0 {
		count = defaultEncoderGroupSize
	}

	inputCh := make([]chan *future, count)
	for i := 0; i < count; i++ {
		inputCh[i] = make(chan *future, defaultInputChanSize)
	}

	return &encoderGroup{
		changefeedID: changefeedID,

		builder:  builder,
		count:    count,
		inputCh:  inputCh,
		index:    0,
		outputCh: make(chan *future, defaultInputChanSize*count),
	}
}

func (g *encoderGroup) Run(ctx context.Context) error {
	defer func() {
		encoderGroupInputChanSizeGauge.DeleteLabelValues(g.changefeedID.Namespace, g.changefeedID.ID)
		log.Info("encoder group exited",
			zap.String("namespace", g.changefeedID.Namespace),
			zap.String("changefeed", g.changefeedID.ID))
	}()
	eg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < g.count; i++ {
		idx := i
		eg.Go(func() error {
			return g.runEncoder(ctx, idx)
		})
	}
	return eg.Wait()
}

func (g *encoderGroup) runEncoder(ctx context.Context, idx int) error {
	encoder := g.builder.Build()
	inputCh := g.inputCh[idx]
	metric := encoderGroupInputChanSizeGauge.
		WithLabelValues(g.changefeedID.Namespace, g.changefeedID.ID, strconv.Itoa(idx))
	ticker := time.NewTicker(defaultMetricInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			metric.Set(float64(len(inputCh)))
		case future := <-inputCh:
			for _, event := range future.events {
				err := encoder.AppendRowChangedEvent(ctx, future.Topic, event.Event, event.Callback)
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
	topic string,
	partition int32,
	events ...*dmlsink.RowChangeCallbackableEvent,
) error {
	future := newFuture(topic, partition, events...)
	index := atomic.AddUint64(&g.index, 1) % uint64(g.count)
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

type future struct {
	Topic     string
	Partition int32
	events    []*dmlsink.RowChangeCallbackableEvent
	Messages  []*common.Message

	done chan struct{}
}

func newFuture(topic string, partition int32,
	events ...*dmlsink.RowChangeCallbackableEvent,
) *future {
	return &future{
		Topic:     topic,
		Partition: partition,
		events:    events,

		done: make(chan struct{}),
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
