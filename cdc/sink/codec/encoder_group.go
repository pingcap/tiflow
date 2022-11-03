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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"golang.org/x/sync/errgroup"
)

const (
	defaultEncoderGroupSize = 16
	defaultInputChannelSize = 256
	defaultMetricInterval   = 15 * time.Second
)

type EncoderGroup interface {
	Run(ctx context.Context) error
	AddEvent(ctx context.Context, topic string, partition int32, event *model.RowChangedEvent, callback func()) error
	Responses() <-chan *responsePromise
}

type encoderGroup struct {
	changefeedID model.ChangeFeedID

	builder EncoderBuilder
	count   int
	inputCh []chan *responsePromise
	index   uint64

	responses chan *responsePromise
}

func NewEncoderGroup(builder EncoderBuilder, number int, changefeedID model.ChangeFeedID) *encoderGroup {
	if number <= 0 {
		number = defaultEncoderGroupSize
	}

	inputCh := make([]chan *responsePromise, number)
	for i := 0; i < number; i++ {
		inputCh[i] = make(chan *responsePromise, defaultInputChannelSize)
	}

	return &encoderGroup{
		changefeedID: changefeedID,

		builder:   builder,
		count:     number,
		inputCh:   inputCh,
		index:     0,
		responses: make(chan *responsePromise, defaultInputChannelSize*number),
	}
}

func (g *encoderGroup) Run(ctx context.Context) error {
	defer func() {
		encoderGroupInputChanSizeGauge.DeleteLabelValues(g.changefeedID.Namespace, g.changefeedID.ID)
		close(g.responses)
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
	inputChan := g.inputCh[idx]
	metric := encoderGroupInputChanSizeGauge.
		WithLabelValues(g.changefeedID.Namespace, g.changefeedID.ID, strconv.Itoa(idx))
	ticker := time.NewTicker(defaultMetricInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			metric.Set(float64(len(inputChan)))
		case promise := <-inputChan:
			if err := encoder.AppendRowChangedEvent(ctx, promise.Topic, promise.event, promise.callback); err != nil {
				return err
			}
			promise.Messages = encoder.Build()
			promise.Done()
		}
	}
}

func (g *encoderGroup) AddEvent(ctx context.Context, topic string, partition int32, event *model.RowChangedEvent, callback func()) error {
	promise := newResponsePromise(topic, partition, event, callback)
	index := atomic.AddUint64(&g.index, 1) % uint64(g.count)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case g.inputCh[index] <- promise:
		g.responses <- promise
		return nil
	}
}

func (g *encoderGroup) Responses() <-chan *responsePromise {
	return g.responses
}

type responsePromise struct {
	Topic     string
	Partition int32
	event     *model.RowChangedEvent
	callback  func()

	Messages []*common.Message

	doneCh chan struct{}
}

func newResponsePromise(topic string, partition int32, event *model.RowChangedEvent, callback func()) *responsePromise {
	return &responsePromise{
		Topic:     topic,
		Partition: partition,
		event:     event,
		callback:  callback,

		doneCh: make(chan struct{}, 1),
	}
}

func (p *responsePromise) Done() {
	p.event = nil
	close(p.doneCh)
}

func (p *responsePromise) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.doneCh:
		return nil
	}
}
