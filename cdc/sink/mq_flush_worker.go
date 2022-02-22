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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/producer"
)

const (
	// flushBatchSize is the batch size of the flush worker.
	flushBatchSize = 1024
	// flushInterval is the interval of the flush worker.
	flushInterval = 500 * time.Millisecond
)

// mqEvent is the event of the mq flush worker.
// It carries the partition information of the message,
// and it is also used as resolved ts messaging.
type mqEvent struct {
	row        *model.RowChangedEvent
	resolvedTs model.Ts
	partition  int32
}

// flushWorker is responsible for sending messages to the Kafka producer on a batch basis.
type flushWorker struct {
	msgChan  chan mqEvent
	encoder  codec.EventBatchEncoder
	producer producer.Producer
}

// newFlushWorker creates a new flush worker.
func newFlushWorker(encoder codec.EventBatchEncoder, producer producer.Producer) *flushWorker {
	w := &flushWorker{
		msgChan:  make(chan mqEvent),
		encoder:  encoder,
		producer: producer,
	}
	return w
}

// batch collects a batch of messages to be sent to the Kafka producer.
func (w *flushWorker) batch(ctx context.Context) ([]mqEvent, error) {
	var events []mqEvent
	// We need to receive at least one message or be interrupted,
	// otherwise it will lead to idling.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg := <-w.msgChan:
		events = append(events, msg)
	}

	tick := time.NewTicker(flushInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg := <-w.msgChan:
			// When the resolved ts is received,
			// we need to write the previous data to the producer as soon as possible.
			if msg.resolvedTs != 0 {
				return events, nil
			}

			if msg.row != nil {
				events = append(events, msg)
			}

			if len(events) >= flushBatchSize {
				return events, nil
			}
		case <-tick.C:
			return events, nil
		}
	}
}

// group is responsible for grouping messages by the partition.
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

// flush is responsible for sending messages to the Kafka producer.
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
	}

	return nil
}

// run starts a loop that keeps collecting, sorting and sending messages
// until it encounters an error or is interrupted.
func (w *flushWorker) run(ctx context.Context) error {
	for {
		events, err := w.batch(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		paritionedEvents := w.group(events)
		err = w.flush(ctx, paritionedEvents)
		if err != nil {
			return errors.Trace(err)
		}
	}
}
