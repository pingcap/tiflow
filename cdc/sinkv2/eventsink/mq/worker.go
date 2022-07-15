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

package mq

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	mqv1 "github.com/pingcap/tiflow/cdc/sink/mq"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/mq/producer"
	"github.com/pingcap/tiflow/pkg/chann"
	"go.uber.org/zap"
)

// mqEvent is the event of the mq worker.
// It carries the partition information of the message.
type mqEvent struct {
	key mqv1.TopicPartitionKey
	row *eventsink.RowChangeCallbackableEvent
}

// worker will send messages to the Kafka producer on a batch basis.
type worker struct {
	// msgChan caches the messages to be sent.
	msgChan *chann.Chann[mqEvent]
	ticker  *time.Ticker

	encoder  codec.EventBatchEncoder
	producer producer.Producer
}

// newWorker creates a new flush worker.
func newWorker(encoder codec.EventBatchEncoder,
	producer producer.Producer,
) *worker {
	w := &worker{
		msgChan:  chann.New[mqEvent](),
		ticker:   time.NewTicker(mqv1.FlushInterval),
		encoder:  encoder,
		producer: producer,
	}

	return w
}

// run starts a loop that keeps collecting, sorting and sending messages
// until it encounters an error or is interrupted.
func (w *worker) run(ctx context.Context) (retErr error) {
	defer func() {
		w.ticker.Stop()
		// TODO: log changefeed ID here
		log.Info("flushWorker exited", zap.Error(retErr))
	}()
	eventsBuf := make([]mqEvent, mqv1.FlushBatchSize)
	for {
		endIndex, err := w.batch(ctx, eventsBuf)
		if err != nil {
			return errors.Trace(err)
		}
		if endIndex == 0 {
			continue
		}
		msgs := eventsBuf[:endIndex]
		partitionedRows := w.group(msgs)
		err = w.asyncSend(ctx, partitionedRows)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

// batch collects a batch of messages to be sent to the Kafka producer.
func (w *worker) batch(
	ctx context.Context, events []mqEvent,
) (int, error) {
	index := 0
	max := len(events)
	// We need to receive at least one message or be interrupted,
	// otherwise it will lead to idling.
	select {
	case <-ctx.Done():
		return index, ctx.Err()
	case msg, ok := <-w.msgChan.Out():
		if !ok {
			log.Warn("MQ sink flush worker channel closed")
			return index, nil
		}
		if msg.row != nil {
			events[index] = msg
			index++
		}
	}

	// Start a new tick to flush the batch.
	w.ticker.Reset(mqv1.FlushInterval)
	for {
		select {
		case <-ctx.Done():
			return index, ctx.Err()
		case msg, ok := <-w.msgChan.Out():
			if !ok {
				log.Warn("MQ sink flush worker channel closed")
				return index, nil
			}

			if msg.row != nil {
				events[index] = msg
				index++
			}

			if index >= max {
				return index, nil
			}
		case <-w.ticker.C:
			return index, nil
		}
	}
}

// group is responsible for grouping messages by the partition.
func (w *worker) group(
	events []mqEvent,
) map[mqv1.TopicPartitionKey][]*eventsink.RowChangeCallbackableEvent {
	partitionedRows := make(map[mqv1.TopicPartitionKey][]*eventsink.RowChangeCallbackableEvent)
	for _, event := range events {
		if _, ok := partitionedRows[event.key]; !ok {
			partitionedRows[event.key] = make([]*eventsink.RowChangeCallbackableEvent, 0)
		}
		partitionedRows[event.key] = append(partitionedRows[event.key], event.row)
	}
	return partitionedRows
}

// asyncSend is responsible for sending messages to the Kafka producer.
func (w *worker) asyncSend(
	ctx context.Context,
	partitionedRows map[mqv1.TopicPartitionKey][]*eventsink.RowChangeCallbackableEvent,
) error {
	for key, events := range partitionedRows {
		for _, event := range events {
			err := w.encoder.AppendRowChangedEvent(ctx, key.Topic, event.Event, event.Callback)
			if err != nil {
				return err
			}
		}

		thisBatchSize := 0
		for _, message := range w.encoder.Build() {
			err := w.producer.AsyncSendMessage(ctx, key.Topic, key.Partition, message)
			if err != nil {
				return err
			}
			thisBatchSize += message.GetRowsCount()
		}
		log.Debug("MQSink flush worker flushed", zap.Int("thisBatchSize", thisBatchSize))
	}

	return nil
}

func (w *worker) close() {
	w.msgChan.Close()
	// We must finish consuming the data here,
	// otherwise it will cause the channel to not close properly.
	for range w.msgChan.Out() {
		// Do nothing. We do not care about the data.
	}
	// We need to close it asynchronously.
	// Otherwise, we might get stuck with it in an unhealthy state of kafka.
	go func() {
		err := w.producer.Close()
		if err != nil {
			log.Error("failed to close Kafka producer", zap.Error(err))
		}
	}()
}
