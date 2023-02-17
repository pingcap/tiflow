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
	msgChan *chann.DrainableChann[mqEvent]
	// ticker used to force flush the messages when the interval is reached.
	ticker *time.Ticker

	encoderGroup codec.EncoderGroup

	// producer is used to send the messages to the Kafka broker.
	producer dmlproducer.DMLProducer

	// metricMQWorkerSendMessageDuration tracks the time duration cost on send messages.
	metricMQWorkerSendMessageDuration prometheus.Observer
	// metricMQWorkerBatchSize tracks each batch's size.
	metricMQWorkerBatchSize prometheus.Observer
	// metricMQWorkerBatchDuration tracks the time duration cost on batch messages.
	metricMQWorkerBatchDuration prometheus.Observer
	// statistics is used to record DML metrics.
	statistics *metrics.Statistics
}

// newWorker creates a new flush worker.
func newWorker(
	id model.ChangeFeedID,
	protocol config.Protocol,
	builder codec.EncoderBuilder,
	encoderConcurrency int,
	producer dmlproducer.DMLProducer,
	statistics *metrics.Statistics,
) *worker {
	w := &worker{
		changeFeedID:                      id,
		protocol:                          protocol,
		msgChan:                           chann.NewDrainableChann[mqEvent](),
		ticker:                            time.NewTicker(flushInterval),
		encoderGroup:                      codec.NewEncoderGroup(builder, encoderConcurrency, id),
		producer:                          producer,
		metricMQWorkerSendMessageDuration: mq.WorkerSendMessageDuration.WithLabelValues(id.Namespace, id.ID),
		metricMQWorkerBatchSize:           mq.WorkerBatchSize.WithLabelValues(id.Namespace, id.ID),
		metricMQWorkerBatchDuration:       mq.WorkerBatchDuration.WithLabelValues(id.Namespace, id.ID),
		statistics:                        statistics,
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
			if err := w.encoderGroup.AddEvents(ctx, event.key.Topic, event.key.Partition, event.rowEvent); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// batchEncodeRun collect messages into batch and add them to the encoder group.
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

		w.metricMQWorkerBatchSize.Observe(float64(endIndex))
		w.metricMQWorkerBatchDuration.Observe(time.Since(start).Seconds())
		msgs := eventsBuf[:endIndex]
		partitionedRows := w.group(msgs)
		for key, events := range partitionedRows {
			if err := w.encoderGroup.AddEvents(ctx, key.Topic, key.Partition, events...); err != nil {
				return errors.Trace(err)
			}
		}
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
			w.statistics.ObserveRows(msg.rowEvent.Event)
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
				w.statistics.ObserveRows(msg.rowEvent.Event)
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
		// Skip this event when the table is stopping.
		if event.rowEvent.GetTableSinkState() != state.TableSinkSinking {
			event.rowEvent.Callback()
			log.Debug("Skip event of stopped table", zap.Any("event", event.rowEvent))
			continue
		}
		if _, ok := partitionedRows[event.key]; !ok {
			partitionedRows[event.key] = make([]*eventsink.RowChangeCallbackableEvent, 0)
		}
		partitionedRows[event.key] = append(partitionedRows[event.key], event.rowEvent)
	}
	return partitionedRows
}

func (w *worker) sendMessages(ctx context.Context) error {
	inputCh := w.encoderGroup.Output()
	ticker := time.NewTicker(15 * time.Second)
	metric := codec.EncoderGroupOutputChanSizeGauge.
		WithLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	defer func() {
		ticker.Stop()
		codec.EncoderGroupOutputChanSizeGauge.
			DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
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
					zap.String("namespace", w.changeFeedID.Namespace),
					zap.String("changefeed", w.changeFeedID.ID))
				return nil
			}
			if err := future.Ready(ctx); err != nil {
				return errors.Trace(err)
			}
			for _, message := range future.Messages {
				start := time.Now()
				if err := w.statistics.RecordBatchExecution(func() (int, error) {
					if err := w.producer.AsyncSendMessage(ctx, future.Topic, future.Partition, message); err != nil {
						return 0, err
					}
					return message.GetRowsCount(), nil
				}); err != nil {
					return err
				}
				w.metricMQWorkerSendMessageDuration.Observe(time.Since(start).Seconds())
			}
		}
	}
}

func (w *worker) close() {
	w.msgChan.CloseAndDrain()
	w.producer.Close()

	mq.WorkerSendMessageDuration.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	mq.WorkerBatchSize.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	mq.WorkerBatchDuration.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
}
