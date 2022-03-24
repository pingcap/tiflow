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
	msgChan       chan mqEvent
	ticker        *time.Ticker
	needSyncFlush bool

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
		// When the resolved ts is received,
		// we need to write the previous data to the producer as soon as possible.
		if msg.resolvedTs != 0 {
			w.needSyncFlush = true
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
			if msg.resolvedTs != 0 {
				w.needSyncFlush = true
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

	if w.needSyncFlush {
		start := time.Now()
		err := w.producer.Flush(ctx)
		if err != nil {
			return err
		}
		w.needSyncFlush = false
		log.Debug("flush worker flushed", zap.Duration("duration", time.Since(start)))
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
