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
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/metrics/mq"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// batchSize is the maximum size of the number of messages in a batch.
	batchSize = 2048
	// batchInterval is the interval of the worker to collect a batch of messages.
	// It shouldn't be too large, otherwise it will lead to a high latency.
	batchInterval = 15 * time.Millisecond
)

// mqEvent is the event of the mq worker.
// It carries the topic and partition information of the message.
type mqEvent struct {
	key      model.TopicPartitionKey
	rowEvent *dmlsink.RowChangeCallbackableEvent
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
	// ticker used to force flush the batched messages when the interval is reached.
	ticker *time.Ticker

	encoderGroup codec.EncoderGroup

	// producer is used to send the messages to the Kafka broker.
	producer dmlproducer.DMLProducer
	// statistics is used to record DML metrics.
	statistics *metrics.Statistics
}

// newWorker creates a new flush worker.
func newWorker(
	id model.ChangeFeedID,
	protocol config.Protocol,
	producer dmlproducer.DMLProducer,
	encoderGroup codec.EncoderGroup,
) *worker {
	w := &worker{
		changeFeedID: id,
		protocol:     protocol,
		msgChan:      chann.NewAutoDrainChann[mqEvent](),
		ticker:       time.NewTicker(batchInterval),
		encoderGroup: encoderGroup,
		producer:     producer,
		statistics:   metrics.NewStatistics(id, sink.RowSink),
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
			if err := w.encoderGroup.AddEvents(
				ctx,
				event.key,
				event.rowEvent); err != nil {
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

	metricBatchDuration := mq.WorkerBatchDuration.WithLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	metricBatchSize := mq.WorkerBatchSize.WithLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	defer func() {
		mq.WorkerBatchDuration.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
		mq.WorkerBatchSize.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	}()

	msgsBuf := make([]mqEvent, batchSize)
	for {
		start := time.Now()
		msgCount, err := w.batch(ctx, msgsBuf, batchInterval)
		if err != nil {
			return errors.Trace(err)
		}
		if msgCount == 0 {
			continue
		}
		metricBatchSize.Observe(float64(msgCount))
		metricBatchDuration.Observe(time.Since(start).Seconds())

		msgs := msgsBuf[:msgCount]
		// Group messages by its TopicPartitionKey before adding them to the encoder group.
		groupedMsgs := w.group(msgs)
		for key, msg := range groupedMsgs {
			if err := w.encoderGroup.AddEvents(ctx, key, msg...); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// batch collects a batch of messages from w.msgChan into buffer.
// It returns the number of messages collected.
// Note: It will block until at least one message is received.
func (w *worker) batch(
	ctx context.Context, buffer []mqEvent, flushInterval time.Duration,
) (int, error) {
	msgCount := 0
	maxBatchSize := len(buffer)
	// We need to receive at least one message or be interrupted,
	// otherwise it will lead to idling.
	select {
	case <-ctx.Done():
		return msgCount, ctx.Err()
	case msg, ok := <-w.msgChan.Out():
		if !ok {
			log.Warn("MQ sink flush worker channel closed")
			return msgCount, nil
		}
		if msg.rowEvent != nil {
			w.statistics.ObserveRows(msg.rowEvent.Event)
			buffer[msgCount] = msg
			msgCount++
		}
	}

	// Reset the ticker to start a new batching.
	// We need to stop batching when the interval is reached.
	w.ticker.Reset(flushInterval)
	for {
		select {
		case <-ctx.Done():
			return msgCount, ctx.Err()
		case msg, ok := <-w.msgChan.Out():
			if !ok {
				log.Warn("MQ sink flush worker channel closed")
				return msgCount, nil
			}

			if msg.rowEvent != nil {
				w.statistics.ObserveRows(msg.rowEvent.Event)
				buffer[msgCount] = msg
				msgCount++
			}

			if msgCount >= maxBatchSize {
				return msgCount, nil
			}
		case <-w.ticker.C:
			return msgCount, nil
		}
	}
}

// group groups messages by its key.
func (w *worker) group(
	msgs []mqEvent,
) map[model.TopicPartitionKey][]*dmlsink.RowChangeCallbackableEvent {
	groupedMsgs := make(map[model.TopicPartitionKey][]*dmlsink.RowChangeCallbackableEvent)
	for _, msg := range msgs {
		// Skip this event when the table is stopping.
		if msg.rowEvent.GetTableSinkState() != state.TableSinkSinking {
			msg.rowEvent.Callback()
			log.Debug("Skip event of stopped table", zap.Any("event", msg.rowEvent))
			continue
		}
		if _, ok := groupedMsgs[msg.key]; !ok {
			groupedMsgs[msg.key] = make([]*dmlsink.RowChangeCallbackableEvent, 0)
		}
		groupedMsgs[msg.key] = append(groupedMsgs[msg.key], msg.rowEvent)
	}
	return groupedMsgs
}

func (w *worker) sendMessages(ctx context.Context) error {
	metricSendMessageDuration := mq.WorkerSendMessageDuration.WithLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	defer mq.WorkerSendMessageDuration.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)

	var err error
	outCh := w.encoderGroup.Output()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case future, ok := <-outCh:
			if !ok {
				log.Warn("MQ sink encoder's output channel closed",
					zap.String("namespace", w.changeFeedID.Namespace),
					zap.String("changefeed", w.changeFeedID.ID))
				return nil
			}
			if err = future.Ready(ctx); err != nil {
				return errors.Trace(err)
			}
			for _, message := range future.Messages {
				start := time.Now()
				if err = w.statistics.RecordBatchExecution(func() (int, int64, error) {
					message.SetPartitionKey(future.Key.PartitionKey)
					if err := w.producer.AsyncSendMessage(
						ctx,
						future.Key.Topic,
						future.Key.Partition,
						message); err != nil {
						return 0, 0, err
					}
					return message.GetRowsCount(), int64(message.Length()), nil
				}); err != nil {
					return err
				}
				metricSendMessageDuration.Observe(time.Since(start).Seconds())
			}
		}
	}
}

func (w *worker) close() {
	w.msgChan.CloseAndDrain()
	w.producer.Close()
	w.statistics.Close()
	mq.WorkerSendMessageDuration.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	mq.WorkerBatchSize.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
	mq.WorkerBatchDuration.DeleteLabelValues(w.changeFeedID.Namespace, w.changeFeedID.ID)
}
