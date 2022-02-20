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

package sink

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/producer"
)

const (
	flushBatchSize = 1024
	flushInterval  = 500 * time.Millisecond
)

type mqEvent struct {
	row        *model.RowChangedEvent
	resolvedTs model.Ts
	partition  int32
}

type flushWorker struct {
	msgChan  chan mqEvent
	encoder  codec.EventBatchEncoder
	producer producer.Producer
}

func newFlushWorker(encoder codec.EventBatchEncoder, producer producer.Producer) *flushWorker {
	w := &flushWorker{
		msgChan:  make(chan mqEvent),
		encoder:  encoder,
		producer: producer,
	}
	return w
}

func (w *flushWorker) batch(ctx context.Context) []mqEvent {
	var events []mqEvent
	select {
	case <-ctx.Done():
		// TODO: should return error?
		return events
	case msg := <-w.msgChan:
		events = append(events, msg)
	}

	tick := time.NewTicker(flushInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return events
		case msg := <-w.msgChan:
			if msg.resolvedTs != 0 {
				return events
			}

			if msg.row != nil {
				events = append(events, msg)
			}
			if len(events) >= flushBatchSize {
				return events
			}
		case <-tick.C:
			return events
		}
	}
}

func (w *flushWorker) group(events []mqEvent) map[int32][]mqEvent {
	paritionedEvents := make(map[int32][]mqEvent)
	for _, event := range events {
		if _, ok := paritionedEvents[event.partition]; !ok {
			paritionedEvents[event.partition] = make([]mqEvent, 0)
		}
		paritionedEvents[event.partition] = append(paritionedEvents[event.partition], event)
	}
	return paritionedEvents
}

func (w *flushWorker) flush(ctx context.Context, paritionedEvents map[int32][]mqEvent) error {
	for partition, events := range paritionedEvents {
		for _, event := range events {
			err := w.encoder.AppendRowChangedEvent(event.row)
			if err != nil {
				return err
			}
		}
		for _, message := range w.encoder.Build() {
			err := w.producer.AsyncSendMessage(ctx, message, partition)
			if err != nil {
				return err
			}
		}
		// TODO: reset not working for other protocols.
		w.encoder.Reset()
	}

	return nil
}

func (w *flushWorker) run(ctx context.Context) error {
	for {
		events := w.batch(ctx)
		paritionedEvents := w.group(events)
		err := w.flush(ctx, paritionedEvents)
		if err != nil {
			return err
		}
	}
}
