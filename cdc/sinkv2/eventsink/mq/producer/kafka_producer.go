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

package producer

import (
	"context"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

var _ Producer = (*kafkaProducer)(nil)

// messageMetaData is used to store the callback function for the message.
type messageMetaData struct {
	callback eventsink.CallbackFunc
}

// kafkaProducer is used to send messages to kafka.
type kafkaProducer struct {
	// id indicates which processor (changefeed) this sink belongs to.
	id model.ChangeFeedID
	// role indicates what this sink is used for.
	role util.Role
	// We hold the client to make close operation faster.
	// Please see the comment of Close().
	client sarama.Client
	// asyncProducer is used to send messages to kafka asynchronously.
	asyncProducer sarama.AsyncProducer
	// closedMu is used to protect `closed`.
	// We need to ensure that closed producers are never written to.
	closedMu sync.RWMutex
	// closed is used to indicate whether the producer is closed.
	// We also use it to guard against double closes.
	closed bool
	// closedChan is used to notify the run loop to exit.
	closedChan chan struct{}
	// failpointCh is used to inject failpoints to the run loop.
	// Only used in test.
	failpointCh chan error
}

func (k *kafkaProducer) AsyncSendMessage(
	ctx context.Context, topic string,
	partition int32, message *codec.MQMessage,
) error {
	// We have to hold the lock to avoid writing to a closed producer.
	// Close may be blocked for a long time.
	k.closedMu.RLock()
	defer k.closedMu.RUnlock()

	// If the producer is closed, we should skip the message.
	if k.closed {
		// We return the context error directly rather than any other error.
		// Because if producer already closed, it means the context maybe is canceling or canceled.
		// So we shouldn't return other errors to the caller to mess up the shutdown process.
		return ctx.Err()
	}
	failpoint.Inject("KafkaSinkAsyncSendError", func() {
		// simulate sending message to input channel successfully but flushing
		// message to Kafka meets error
		log.Info("failpoint error injected", zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID), zap.Any("role", k.role))
		k.failpointCh <- errors.New("kafka sink injected error")
		failpoint.Return(nil)
	})

	failpoint.Inject("SinkFlushDMLPanic", func() {
		time.Sleep(time.Second)
		log.Panic("SinkFlushDMLPanic",
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID), zap.Any("role", k.role))
	})

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Key:       sarama.StringEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Metadata:  messageMetaData{callback: message.Callback},
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case k.asyncProducer.Input() <- msg:
	}
	return nil
}

func (k *kafkaProducer) Close() {
	// We have to hold the lock to synchronize closing with writing.
	k.closedMu.Lock()
	defer k.closedMu.Unlock()
	// If the producer has already been closed, we should skip this close operation.
	if k.closed {
		// We need to guard against double closed the clients,
		// which could lead to panic.
		log.Warn("kafka producer already closed",
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID),
			zap.Any("role", k.role))
		return
	}
	close(k.failpointCh)
	// Notify the run loop to exit.
	close(k.closedChan)
	k.closed = true
	// We need to close it asynchronously.
	// Otherwise, we might get stuck with an unhealthy state of kafka.
	go func() {
		// `client` is mainly used by `asyncProducer` to fetch metadata and perform other related
		// operations. When we close the `kafkaSaramaProducer`, there is no need for TiCDC to make sure
		// that all buffered messages are flushed.
		// Consider the situation where the broker is irresponsive. If the client were not
		// closed, `asyncProducer.Close()` would waste a mount of time to try flush all messages.
		// To prevent the scenario mentioned above, close the client first.
		start := time.Now()
		if err := k.client.Close(); err != nil {
			log.Error("close sarama client with error", zap.Error(err),
				zap.Duration("duration", time.Since(start)),
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID), zap.Any("role", k.role))
		} else {
			log.Info("sarama client closed", zap.Duration("duration", time.Since(start)),
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID), zap.Any("role", k.role))
		}

		start = time.Now()
		err := k.asyncProducer.Close()
		if err != nil {
			log.Error("close async client with error", zap.Error(err),
				zap.Duration("duration", time.Since(start)),
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID),
				zap.Any("role", k.role))
		} else {
			log.Info("async client closed", zap.Duration("duration", time.Since(start)),
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID), zap.Any("role", k.role))
		}
	}()
}

func (k *kafkaProducer) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-k.closedChan:
			return nil
		case err := <-k.failpointCh:
			log.Warn("receive from failpoint chan", zap.Error(err),
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID), zap.Any("role", k.role))
			return err
		case ack := <-k.asyncProducer.Successes():
			if ack != nil {
				callback := ack.Metadata.(messageMetaData).callback
				if callback != nil {
					callback()
				}
			}
		case err := <-k.asyncProducer.Errors():
			// We should not wrap a nil pointer if the pointer
			// is of a subtype of `error` because Go would store the type info
			// and the resulted `error` variable would not be nil,
			// which will cause the pkg/error library to malfunction.
			// See: https://go.dev/doc/faq#nil_error
			if err == nil {
				return nil
			}
			return cerror.WrapError(cerror.ErrKafkaAsyncSendMessage, err)
		}
	}
}

// NewKafkaProducer creates a new kafka producer.
func NewKafkaProducer(
	ctx context.Context,
	client sarama.Client,
	errCh chan error,
) (Producer, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	role := contextutil.RoleFromCtx(ctx)
	log.Info("Starting kafka sarama producer ...",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID), zap.Any("role", role))

	asyncProducer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	k := &kafkaProducer{
		client:        client,
		asyncProducer: asyncProducer,
		id:            changefeedID,
		role:          role,
		closed:        false,
		closedChan:    make(chan struct{}),
		failpointCh:   make(chan error, 1),
	}

	go func() {
		if err := k.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("error channel is full", zap.Error(err),
					zap.String("namespace", k.id.Namespace),
					zap.String("changefeed", k.id.ID), zap.Any("role", role))
			}
		}
	}()

	return k, nil
}
