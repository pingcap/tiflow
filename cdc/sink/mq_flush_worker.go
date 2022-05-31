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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/producer"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const (
	// flushBatchSize is the batch size of the flush worker.
	flushBatchSize = 2048
	// flushInterval is the interval of the flush worker.
	flushInterval = 500 * time.Millisecond
)

<<<<<<< HEAD:cdc/sink/mq_flush_worker.go
=======
type topicPartitionKey struct {
	topic     string
	partition int32
}

type flushEvent struct {
	resolvedTs model.ResolvedTs
	flushed    chan<- struct{}
}

>>>>>>> dd41f0f1b (sink/mq(ticdc): flush waits for all events to be acked (#5657)):cdc/sink/mq/mq_flush_worker.go
// mqEvent is the event of the mq flush worker.
// It carries the partition information of the message,
// and it is also used to flush all events.
type mqEvent struct {
<<<<<<< HEAD:cdc/sink/mq_flush_worker.go
	row        *model.RowChangedEvent
	resolvedTs model.Ts
	partition  int32
=======
	key   topicPartitionKey
	row   *model.RowChangedEvent
	flush *flushEvent
>>>>>>> dd41f0f1b (sink/mq(ticdc): flush waits for all events to be acked (#5657)):cdc/sink/mq/mq_flush_worker.go
}

// flushWorker is responsible for sending messages to the Kafka producer on a batch basis.
type flushWorker struct {
	msgChan chan mqEvent
	ticker  *time.Ticker
	// needsFlush is used to indicate whether the flush worker needs to flush the messages.
	// It is also used to notify that the flush has completed.
	needsFlush chan<- struct{}

	// errCh is used to store one error if `run` exits unexpectedly.
	// After sending an error to errCh, errCh must be closed so that
	// subsequent callers to addEvent will always know that the flushWorker
	// is NOT running normally.
	errCh chan error

	encoder    codec.EventBatchEncoder
	producer   producer.Producer
	statistics *Statistics
}

// newFlushWorker creates a new flush worker.
func newFlushWorker(encoder codec.EventBatchEncoder, producer producer.Producer, statistics *Statistics) *flushWorker {
	w := &flushWorker{
		msgChan: make(chan mqEvent),
		ticker:  time.NewTicker(flushInterval),
		// errCh must be a buffered channel, or otherwise sending error to it will
		// almost certainly go to the default branch, making errCh useless.
		errCh:      make(chan error, 1),
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
	case msg := <-w.msgChan:
		// When the flush event is received,
		// we need to write the previous data to the producer as soon as possible.
<<<<<<< HEAD:cdc/sink/mq_flush_worker.go
		if msg.resolvedTs != 0 {
			w.needSyncFlush = true
=======
		if msg.flush != nil {
			w.needsFlush = msg.flush.flushed
>>>>>>> dd41f0f1b (sink/mq(ticdc): flush waits for all events to be acked (#5657)):cdc/sink/mq/mq_flush_worker.go
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
		case msg := <-w.msgChan:
<<<<<<< HEAD:cdc/sink/mq_flush_worker.go
			if msg.resolvedTs != 0 {
				w.needSyncFlush = true
=======
			// When the flush event is received,
			// we need to write the previous data to the producer as soon as possible.
			if msg.flush != nil {
				w.needsFlush = msg.flush.flushed
>>>>>>> dd41f0f1b (sink/mq(ticdc): flush waits for all events to be acked (#5657)):cdc/sink/mq/mq_flush_worker.go
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
func (w *flushWorker) group(events []mqEvent) map[int32][]*model.RowChangedEvent {
	paritionedRows := make(map[int32][]*model.RowChangedEvent)
	for _, event := range events {
		if _, ok := paritionedRows[event.partition]; !ok {
			paritionedRows[event.partition] = make([]*model.RowChangedEvent, 0)
		}
		paritionedRows[event.partition] = append(paritionedRows[event.partition], event.row)
	}
	return paritionedRows
}

// asyncSend is responsible for sending messages to the Kafka producer.
func (w *flushWorker) asyncSend(
	ctx context.Context,
	paritionedRows map[int32][]*model.RowChangedEvent,
) error {
	for partition, events := range paritionedRows {
		for _, event := range events {
			err := w.encoder.AppendRowChangedEvent(event)
			if err != nil {
				return err
			}
		}

		err := w.statistics.RecordBatchExecution(func() (int, error) {
			thisBatchSize := 0
			for _, message := range w.encoder.Build() {
				err := w.producer.AsyncSendMessage(ctx, message, partition)
				if err != nil {
					return 0, err
				}
				thisBatchSize += message.GetRowsCount()
			}
			log.Debug("MQSink flush worker flushed", zap.Int("thisBatchSize", thisBatchSize))
			return thisBatchSize, nil
		})
		if err != nil {
			return err
		}
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
		// Note that errCh is sent to exactly once.
		w.errCh <- retErr
		close(w.errCh)
		// TODO: log changefeed ID here
		log.Info("flushWorker exited", zap.Error(retErr))
	}()
	defer w.ticker.Stop()
	eventsBuf := make([]mqEvent, flushBatchSize)
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
		paritionedRows := w.group(msgs)
		err = w.asyncSend(ctx, paritionedRows)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

// addEvent is used to add one event to the flushWorker.
// It will return an ErrMQWorkerClosed if the flushWorker has exited.
func (w *flushWorker) addEvent(ctx context.Context, event mqEvent) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err, ok := <-w.errCh:
		if !ok {
			return cerror.ErrMQWorkerClosed.GenWithStackByArgs()
		}
		return err
	case w.msgChan <- event:
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
