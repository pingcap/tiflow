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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/mq/producer"
	"github.com/pingcap/tiflow/pkg/chann"
	"go.uber.org/zap"
)

const (
	// FlushBatchSize is the batch size of the flush worker.
	FlushBatchSize = 2048
	// FlushInterval is the interval of the flush worker.
	FlushInterval = 500 * time.Millisecond
)

// TopicPartitionKey contains the topic and partition key of the message.
type TopicPartitionKey struct {
	Topic     string
	Partition int32
}

type flushEvent struct {
	resolvedTs model.ResolvedTs
	flushed    chan<- struct{}
}

// mqEvent is the event of the mq flush worker.
// It carries the partition information of the message,
// and it is also used to flush all events.
type mqEvent struct {
	key   TopicPartitionKey
	row   *model.RowChangedEvent
	flush *flushEvent
}

// flushWorker is responsible for sending messages to the Kafka producer on a batch basis.
type flushWorker struct {
	msgChan *chann.DrainableChann[mqEvent]
	ticker  *time.Ticker
	// needsFlush is used to indicate whether the flush worker needs to flush the messages.
	// It is also used to notify that the flush has completed.
	needsFlush chan<- struct{}

	encoder    codec.EventBatchEncoder
	producer   producer.Producer
	statistics *metrics.Statistics
}

// newFlushWorker creates a new flush worker.
func newFlushWorker(
	encoder codec.EventBatchEncoder,
	producer producer.Producer,
	statistics *metrics.Statistics,
) *flushWorker {
	w := &flushWorker{
		msgChan:    chann.NewDrainableChann[mqEvent](),
		ticker:     time.NewTicker(FlushInterval),
		encoder:    encoder,
		producer:   producer,
		statistics: statistics,
	}
	return w
}

// batch collects a batch of messages to be sent to the Kafka producer.
func (w *flushWorker) batch(
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
		// When the flush event is received,
		// we need to write the previous data to the producer as soon as possible.
		if msg.flush != nil {
			w.needsFlush = msg.flush.flushed
			return index, nil
		}

		if msg.row != nil {
			events[index] = msg
			index++
		}
	}

	// Start a new tick to flush the batch.
	w.ticker.Reset(FlushInterval)
	for {
		select {
		case <-ctx.Done():
			return index, ctx.Err()
		case msg, ok := <-w.msgChan.Out():
			if !ok {
				log.Warn("MQ sink flush worker channel closed")
				return index, nil
			}
			// When the flush event is received,
			// we need to write the previous data to the producer as soon as possible.
			if msg.flush != nil {
				w.needsFlush = msg.flush.flushed
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
func (w *flushWorker) group(events []mqEvent) map[TopicPartitionKey][]*model.RowChangedEvent {
	partitionedRows := make(map[TopicPartitionKey][]*model.RowChangedEvent)
	for _, event := range events {
		if _, ok := partitionedRows[event.key]; !ok {
			partitionedRows[event.key] = make([]*model.RowChangedEvent, 0)
		}
		partitionedRows[event.key] = append(partitionedRows[event.key], event.row)
	}
	return partitionedRows
}

// asyncSend is responsible for sending messages to the Kafka producer.
func (w *flushWorker) asyncSend(
	ctx context.Context,
	partitionedRows map[TopicPartitionKey][]*model.RowChangedEvent,
) error {
	for key, events := range partitionedRows {
		for _, event := range events {
			err := w.encoder.AppendRowChangedEvent(ctx, key.Topic, event, nil)
			if err != nil {
				return err
			}
		}

		err := w.statistics.RecordBatchExecution(func() (int, error) {
			thisBatchSize := 0
			for _, message := range w.encoder.Build() {
				err := w.producer.AsyncSendMessage(ctx, key.Topic, key.Partition, message)
				if err != nil {
					return 0, err
				}
				thisBatchSize += message.GetRowsCount()
			}
			return thisBatchSize, nil
		})
		if err != nil {
			return err
		}
		w.statistics.ObserveRows(events...)
	}

	// Wait for all messages to ack.
	if w.needsFlush != nil {
		if err := w.flushAndNotify(ctx); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// run starts a loop that keeps collecting, sorting and sending messages
// until it encounters an error or is interrupted.
func (w *flushWorker) run(ctx context.Context) (retErr error) {
	defer func() {
		w.ticker.Stop()
		// TODO: log changefeed ID here
		log.Info("flushWorker exited", zap.Error(retErr))
	}()
	eventsBuf := make([]mqEvent, FlushBatchSize)
	for {
		endIndex, err := w.batch(ctx, eventsBuf)
		if err != nil {
			return errors.Trace(err)
		}
		if endIndex == 0 {
			if w.needsFlush != nil {
				// NOTICE: We still need to do a flush here.
				// This is because there may be some rows that
				// were sent that have not been confirmed yet.
				if err := w.flushAndNotify(ctx); err != nil {
					return errors.Trace(err)
				}
			}
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

// addEvent is used to add one event to the flushWorker.
func (w *flushWorker) addEvent(ctx context.Context, event mqEvent) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case w.msgChan.In() <- event:
		return nil
	}
}

// flushAndNotify is used to flush all events
// and notify the mqSink that all events has been flushed.
func (w *flushWorker) flushAndNotify(ctx context.Context) error {
	start := time.Now()
	err := w.producer.Flush(ctx)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.needsFlush <- struct{}{}:
		close(w.needsFlush)
		// NOTICE: Do not forget to reset the needsFlush.
		w.needsFlush = nil
		log.Debug("flush worker flushed", zap.Duration("duration", time.Since(start)))
	}

	return nil
}

func (w *flushWorker) close() {
	w.msgChan.CloseAndDrain()
}
