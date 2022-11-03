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
	mqv1 "github.com/pingcap/tiflow/cdc/sink/mq"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics/mq"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink/state"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// flushBatchSize is the batch size of the flush worker.
	flushBatchSize = 2048
	// flushInterval is the interval of the flush worker.
	// We should not set it too big, otherwise it will cause we wait too long to send the message.
	flushInterval = 15 * time.Millisecond
)

// mqEvent is the event of the mq worker.
// It carries the topic and partition information of the message.
type mqEvent struct {
	key      mqv1.TopicPartitionKey
	rowEvent *eventsink.RowChangeCallbackableEvent
}

// worker will send messages to the DML producer on a batch basis.
type worker struct {
	// changeFeedID indicates this sink belongs to which processor(changefeed).
	changeFeedID model.ChangeFeedID
	// protocol indicates the protocol used by this sink.
	protocol config.Protocol
	// msgChan caches the messages to be sent.
	// It is an unbounded channel.
	msgChan *chann.Chann[mqEvent]
	// ticker used to force flush the messages when the interval is reached.
	ticker *time.Ticker
	// encoder is used to encode the messages.
	encoder codec.EventBatchEncoder

	// producer is used to send the messages to the Kafka broker.
	producer dmlproducer.DMLProducer
	// metricMQWorkerFlushDuration is the metric of the flush duration.
	// We record the flush duration for each batch.
	metricMQWorkerFlushDuration prometheus.Observer
	// statistics is used to record DML metrics.
	statistics *metrics.Statistics

	encoderGroup codec.EncoderGroup
}

// newWorker creates a new flush worker.
func newWorker(
	id model.ChangeFeedID,
	protocol config.Protocol,
	builder codec.EncoderBuilder,
	producer dmlproducer.DMLProducer,
	statistics *metrics.Statistics,
) *worker {
	w := &worker{
		changeFeedID:                id,
		protocol:                    protocol,
		msgChan:                     chann.New[mqEvent](),
		ticker:                      time.NewTicker(flushInterval),
		encoder:                     builder.Build(),
		encoderGroup:                codec.NewEncoderGroup(builder, 16, id),
		producer:                    producer,
		metricMQWorkerFlushDuration: mq.WorkerFlushDuration.WithLabelValues(id.Namespace, id.ID),
		statistics:                  statistics,
	}

	return w
}

// run starts a loop that keeps collecting, sorting and sending messages
// until it encounters an error or is interrupted.
func (w *worker) run(ctx context.Context) (retErr error) {
	defer func() {
		w.ticker.Stop()
		log.Info("MQ sink worker exited", zap.Error(retErr),
			zap.String("namespace", w.changeFeedID.Namespace),
			zap.String("changefeed", w.changeFeedID.ID),
			zap.String("protocol", w.protocol.String()),
		)
	}()

	if w.protocol.IsBatchEncode() {
		return w.batchEncodeRun(ctx)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return w.encoderGroup.Run(ctx)
	})
	g.Go(func() error {
		return w.nonBatchEncodeRun(ctx)
	})
	g.Go(func() error {
		return w.sendMessages(ctx)
	})
	return g.Wait()
}

// Directly send the message to the producer.
func (w *worker) nonBatchEncodeRun(ctx context.Context) error {
	log.Info("MQ sink non batch worker started",
		zap.String("namespace", w.changeFeedID.Namespace),
		zap.String("changefeed", w.changeFeedID.ID),
		zap.String("protocol", w.protocol.String()),
	)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event, ok := <-w.msgChan.Out():
			if !ok {
				log.Warn("MQ sink flush worker channel closed",
					zap.String("namespace", w.changeFeedID.Namespace),
					zap.String("changefeed", w.changeFeedID.ID))
				return nil
			}
			if event.rowEvent.GetTableSinkState() != state.TableSinkSinking {
				event.rowEvent.Callback()
				log.Debug("Skip event of stopped table",
					zap.String("namespace", w.changeFeedID.Namespace),
					zap.String("changefeed", w.changeFeedID.ID),
					zap.Any("event", event))
				continue
			}
			if err := w.encoderGroup.AddEvent(ctx, event.key.Topic, event.key.Partition, event.rowEvent.Event, event.rowEvent.Callback); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// Collect messages and send them to the producer in batches.
func (w *worker) batchEncodeRun(ctx context.Context) (retErr error) {
	log.Info("MQ sink batch worker started",
		zap.String("namespace", w.changeFeedID.Namespace),
		zap.String("changefeed", w.changeFeedID.ID),
		zap.String("protocol", w.protocol.String()),
	)
	// Fixed size of the batch.
	eventsBuf := make([]mqEvent, flushBatchSize)
	for {
		start := time.Now()
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
		duration := time.Since(start)
		w.metricMQWorkerFlushDuration.Observe(duration.Seconds())
	}
}

// batch collects a batch of messages to be sent to the DML producer.
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
		if msg.rowEvent != nil {
			events[index] = msg
			index++
		}
	}

	// Start a new tick to flush the batch.
	w.ticker.Reset(flushInterval)
	for {
		select {
		case <-ctx.Done():
			return index, ctx.Err()
		case msg, ok := <-w.msgChan.Out():
			if !ok {
				log.Warn("MQ sink flush worker channel closed")
				return index, nil
			}

			if msg.rowEvent != nil {
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
		partitionedRows[event.key] = append(partitionedRows[event.key], event.rowEvent)
	}
	return partitionedRows
}

// asyncSend is responsible for sending messages to the DML producer.
func (w *worker) asyncSend(
	ctx context.Context,
	partitionedRows map[mqv1.TopicPartitionKey][]*eventsink.RowChangeCallbackableEvent,
) error {
	for key, events := range partitionedRows {
		rowsCount := 0
		for _, event := range events {
			// Skip this event when the table is stopping.
			if event.GetTableSinkState() != state.TableSinkSinking {
				event.Callback()
				log.Debug("Skip event of stopped table", zap.Any("event", event))
				continue
			}
			err := w.encoder.AppendRowChangedEvent(ctx, key.Topic, event.Event, event.Callback)
			if err != nil {
				return err
			}
			rowsCount++
			w.statistics.ObserveRows(event.Event)
		}

		for _, message := range w.encoder.Build() {
			err := w.statistics.RecordBatchExecution(func() (int, error) {
				err := w.producer.AsyncSendMessage(ctx, key.Topic, key.Partition, message)
				if err != nil {
					return 0, err
				}
				return message.GetRowsCount(), nil
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *worker) sendMessages(ctx context.Context) error {
	responses := w.encoderGroup.Responses()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case promise, ok := <-responses:
			if !ok {
				log.Warn("MQ sink encode response promise channel closed",
					zap.String("namespace", w.changeFeedID.Namespace),
					zap.String("changefeed", w.changeFeedID.ID))
				return nil
			}
			if err := promise.Wait(ctx); err != nil {
				return errors.Trace(err)
			}
			for _, message := range promise.Messages {
				if err := w.statistics.RecordBatchExecution(func() (int, error) {
					if err := w.producer.AsyncSendMessage(ctx, promise.Topic, promise.Partition, message); err != nil {
						return 0, err
					}
					return message.GetRowsCount(), nil
				}); err != nil {
					return err
				}
			}
		}
	}
}

func (w *worker) close() {
	w.msgChan.Close()
	// We must finish consuming the data here,
	// otherwise it will cause the channel to not close properly.
	for range w.msgChan.Out() {
		// Do nothing. We do not care about the data.
	}
	w.producer.Close()
}
