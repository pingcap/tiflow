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
	// id indicates this sink belongs to which processor(changefeed).
	id model.ChangeFeedID
	// role indicates this sink used for what.
	role util.Role
	// We hold the client to make close operation faster.
	// Please see the comment of Close().
	client sarama.Client
	// asyncProducer is used to send messages to kafka asynchronously.
	asyncProducer sarama.AsyncProducer
	// closingMu is used to protect `closing`.
	// We need to ensure that never write to closed producers.
	closingMu sync.RWMutex
	// closing is used to indicate whether the producer is closing.
	// We also use it to guard against double closing.
	closing bool
	// closedChan is used to notify the run loop to exit.
	closedChan chan struct{}
}

func (k *kafkaProducer) AsyncSendMessage(
	ctx context.Context, topic string,
	partition int32, message *codec.MQMessage,
) error {
	// We have to hold the lock to prevent write to closed producer.
	// Close may be blocked for a long time.
	k.closingMu.RLock()
	defer k.closingMu.RUnlock()

	// If the producer is closed, we should skip the message.
	if k.closing {
		// TODO: refine this error.
		return ctx.Err()
	}
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

func (k *kafkaProducer) Close() error {
	// We have to hold the lock to prevent write to closed producer.
	k.closingMu.Lock()
	defer k.closingMu.Unlock()
	// If the producer was already closed, we should skip the close operation.
	if k.closing {
		// We need to guard against double closing the clients,
		// which could lead to panic.
		log.Warn("kafka producer already closed",
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID),
			zap.Any("role", k.role))
		return nil
	}
	k.closing = true
	// Notify the run loop to exit.
	close(k.closedChan)
	// `client` is mainly used by `asyncProducer` to fetch metadata and other related
	// operations. When we close the `kafkaSaramaProducer`, TiCDC no need to make sure
	// that buffered messages flushed.
	// Consider the situation that the broker does not respond, If the client is not
	// closed, `asyncProducer.Close()` would waste a mount of time to try flush all messages.
	// To prevent the scenario mentioned above, close client first.
	start := time.Now()
	if err := k.client.Close(); err != nil {
		log.Error("close sarama client with error", zap.Error(err),
			zap.Duration("duration", time.Since(start)),
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID), zap.Any("role", k.role))
		return err
	}
	log.Info("sarama client closed", zap.Duration("duration", time.Since(start)),
		zap.String("namespace", k.id.Namespace),
		zap.String("changefeed", k.id.ID), zap.Any("role", k.role))

	start = time.Now()
	err := k.asyncProducer.Close()
	if err != nil {
		log.Error("close async client with error", zap.Error(err),
			zap.Duration("duration", time.Since(start)),
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID),
			zap.Any("role", k.role))
		return err
	}
	log.Info("async client closed", zap.Duration("duration", time.Since(start)),
		zap.String("namespace", k.id.Namespace),
		zap.String("changefeed", k.id.ID), zap.Any("role", k.role))

	return nil
}

func (k *kafkaProducer) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-k.closedChan:
			return nil
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
			if err == nil {
				return nil
			}
			return cerror.WrapError(cerror.ErrKafkaAsyncSendMessage, err)
		}
	}
}

// NewKafkaProducer creates a new kafka producer.
// TODO: add logs
func NewKafkaProducer(
	ctx context.Context,
	client sarama.Client,
	errCh chan error,
) (Producer, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	role := contextutil.RoleFromCtx(ctx)

	asyncProducer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	k := &kafkaProducer{
		client:        client,
		asyncProducer: asyncProducer,
		id:            changefeedID,
		role:          role,
		closing:       false,
		closedChan:    make(chan struct{}),
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
