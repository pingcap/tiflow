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
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
	"github.com/pingcap/tiflow/cdc/sink/mq/producer"
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
	flushInterval = 15 * time.Millisecond
)

type topicPartitionKey struct {
	topic     string
	partition int32
}

type flushEvent struct {
	resolvedTs model.ResolvedTs
	flushed    chan<- struct{}
}

// mqEvent is the event of the mq flush worker.
// It carries the partition information of the message,
// and it is also used to flush all events.
type mqEvent struct {
	key   topicPartitionKey
	row   *model.RowChangedEvent
	flush *flushEvent
}

// flushWorker is responsible for sending messages to the Kafka producer on a batch basis.
type flushWorker struct {
	id model.ChangeFeedID

	msgChan *chann.Chann[mqEvent]
	ticker  *time.Ticker
	// needsFlush is used to indicate whether the flush worker needs to flush the messages.
	// It is also used to notify that the flush has completed.
	needsFlush chan<- struct{}

	protocol config.Protocol

	encoderGroup codec.EncoderGroup
	producer     producer.Producer
	statistics   *metrics.Statistics

	// metricMQWorkerBatchSize tracks each batch's size.
	metricMQWorkerBatchSize prometheus.Observer
	// metricMQWorkerBatchDuration tracks the time duration cost on batch messages.
	metricMQWorkerBatchDuration prometheus.Observer
}

// newFlushWorker creates a new flush worker.
func newFlushWorker(
	builder codec.EncoderBuilder,
	producer producer.Producer,
	statistics *metrics.Statistics,
	protocol config.Protocol,
	encoderConcurrency int,
	changefeed model.ChangeFeedID,
) *flushWorker {
	w := &flushWorker{
		id:                          changefeed,
		msgChan:                     chann.New[mqEvent](),
		ticker:                      time.NewTicker(flushInterval),
		protocol:                    protocol,
		encoderGroup:                codec.NewEncoderGroup(builder, encoderConcurrency, changefeed),
		producer:                    producer,
		statistics:                  statistics,
		metricMQWorkerBatchSize:     workerBatchSize.WithLabelValues(changefeed.Namespace, changefeed.ID),
		metricMQWorkerBatchDuration: workerBatchDuration.WithLabelValues(changefeed.Namespace, changefeed.ID),
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
func (w *flushWorker) group(events []mqEvent) map[topicPartitionKey][]*model.RowChangedEvent {
	partitionedRows := make(map[topicPartitionKey][]*model.RowChangedEvent)
	for _, event := range events {
		if _, ok := partitionedRows[event.key]; !ok {
			partitionedRows[event.key] = make([]*model.RowChangedEvent, 0)
		}
		partitionedRows[event.key] = append(partitionedRows[event.key], event.row)
	}
	return partitionedRows
}

// run starts a loop that keeps collecting, sorting and sending messages
// until it encounters an error or is interrupted.
func (w *flushWorker) run(ctx context.Context) (retErr error) {
	defer func() {
		w.ticker.Stop()
		// TODO: log changefeed ID here
		log.Info("flushWorker exited", zap.Error(retErr))
	}()

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return w.encoderGroup.Run(ctx)
	})
	g.Go(func() error {
		if w.protocol.IsBatchEncode() {
			return w.batchEncodeRun(ctx)
		}
		return w.nonBatchEncodeRun(ctx)
	})
	g.Go(func() error {
		return w.sendMessages(ctx)
	})
	return g.Wait()
}

// nonBatchEncodeRun add events to the encoder group immediately.
func (w *flushWorker) nonBatchEncodeRun(ctx context.Context) error {
	log.Info("MQ sink non batch worker started",
		zap.String("namespace", w.id.Namespace),
		zap.String("changefeed", w.id.ID),
		zap.String("protocol", w.protocol.String()),
	)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event, ok := <-w.msgChan.Out():
			if !ok {
				log.Warn("MQ sink flush worker channel closed",
					zap.String("namespace", w.id.Namespace),
					zap.String("changefeed", w.id.ID))
				return nil
			}

			if event.row != nil {
				if err := w.encoderGroup.AddEvents(ctx, event.key.topic, event.key.partition, event.row); err != nil {
					return errors.Trace(err)
				}
				w.statistics.ObserveRows(event.row)
				continue
			}

			if event.flush != nil {
				if err := w.encoderGroup.AddFlush(ctx, event.flush.flushed); err != nil {
					return errors.Trace(err)
				}
			}

		}
	}
}

// batchEncodeRun collect messages into batch and add them to the encoder group.
func (w *flushWorker) batchEncodeRun(ctx context.Context) (retErr error) {
	log.Info("MQ sink batch worker started",
		zap.String("namespace", w.id.Namespace),
		zap.String("changefeed", w.id.ID),
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

		// when the endIndex is 0, it indicates the first message is a flush message.
		if endIndex == 0 {
			if w.needsFlush != nil {
				// NOTICE: We still need to do a flush here.
				// This is because there may be some rows that
				// were sent that have not been confirmed yet.
				if err := w.encoderGroup.AddFlush(ctx, w.needsFlush); err != nil {
					return errors.Trace(err)
				}
				w.needsFlush = nil
			}
			continue
		}

		w.metricMQWorkerBatchSize.Observe(float64(endIndex))
		w.metricMQWorkerBatchDuration.Observe(time.Since(start).Seconds())
		msgs := eventsBuf[:endIndex]

		partitionedRows := w.group(msgs)
		for key, events := range partitionedRows {
			if err := w.encoderGroup.AddEvents(ctx, key.topic, key.partition, events...); err != nil {
				return errors.Trace(err)
			}
			w.statistics.ObserveRows(events...)
		}

		if w.needsFlush != nil {
			if err := w.encoderGroup.AddFlush(ctx, w.needsFlush); err != nil {
				return errors.Trace(err)
			}
			w.needsFlush = nil
		}
	}
}

func (w *flushWorker) sendMessages(ctx context.Context) error {
	inputCh := w.encoderGroup.Output()
	ticker := time.NewTicker(15 * time.Second)
	metric := codec.EncoderGroupOutputChanSizeGauge.
		WithLabelValues(w.id.Namespace, w.id.ID)
	defer func() {
		ticker.Stop()
		codec.EncoderGroupOutputChanSizeGauge.
			DeleteLabelValues(w.id.Namespace, w.id.ID)
	}()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			metric.Set(float64(len(inputCh)))
		case future, ok := <-inputCh:
			if !ok {
				log.Warn("MQ sink encode output channel closed",
					zap.String("namespace", w.id.Namespace),
					zap.String("changefeed", w.id.ID))
				return nil
			}
			if err := future.Ready(ctx); err != nil {
				return errors.Trace(err)
			}

			for _, message := range future.Messages {
				if err := w.statistics.RecordBatchExecution(func() (int, error) {
					if err := w.producer.AsyncSendMessage(ctx, future.Topic, future.Partition, message); err != nil {
						return 0, err
					}
					return message.GetRowsCount(), nil
				}); err != nil {
					return err
				}
			}

			// When the flush event is received,
			// we need to write the previous data to the producer as soon as possible.
			if future.Flush != nil {
				if err := w.flushAndNotify(ctx, future.Flush); err != nil {
					return errors.Trace(err)
				}
			}
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
func (w *flushWorker) flushAndNotify(ctx context.Context, flush chan<- struct{}) error {
	start := time.Now()
	err := w.producer.Flush(ctx)
	if err != nil {
		return err
	}

	close(flush)
	log.Debug("flush worker flushed", zap.Duration("duration", time.Since(start)))

	return nil
}

func (w *flushWorker) close() {
	w.msgChan.Close()
	// We must finish consuming the data here,
	// otherwise it will cause the channel to not close properly.
	for range w.msgChan.Out() {
		// Do nothing. We do not care about the data.
	}
	workerBatchSize.DeleteLabelValues(w.id.Namespace, w.id.ID)
	workerBatchDuration.DeleteLabelValues(w.id.Namespace, w.id.ID)
}
