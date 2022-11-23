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

package dmlproducer

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
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	collector "github.com/pingcap/tiflow/cdc/sinkv2/metrics/mq/kafka"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

var _ DMLProducer = (*kafkaDMLProducer)(nil)

// messageMetaData is used to store the callback function for the message.
type messageMetaData struct {
	callback eventsink.CallbackFunc
}

// kafkaDMLProducer is used to send messages to kafka.
type kafkaDMLProducer struct {
	// id indicates which processor (changefeed) this sink belongs to.
	id model.ChangeFeedID
	// We hold the client to make close operation faster.
	// Please see the comment of Close().
	client sarama.Client
	// asyncProducer is used to send messages to kafka asynchronously.
	asyncProducer sarama.AsyncProducer
	// collector is used to report metrics.
	collector *collector.Collector
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

// NewKafkaDMLProducer creates a new kafka producer.
func NewKafkaDMLProducer(
	ctx context.Context,
	client sarama.Client,
	adminClient pkafka.ClusterAdminClient,
	errCh chan error,
) (DMLProducer, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	log.Info("Starting kafka DML producer ...",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID))

	asyncProducer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		// Close the client to prevent the goroutine leak.
		// Because it may be a long time to close the client,
		// so close it asynchronously.
		go func() {
			if err := client.Close(); err != nil {
				log.Error("Close sarama client with error in kafka "+
					"DML producer", zap.Error(err),
					zap.String("namespace", changefeedID.Namespace),
					zap.String("changefeed", changefeedID.ID))
			}
			if err := adminClient.Close(); err != nil {
				log.Error("Close sarama admin client with error in kafka "+
					"DML producer", zap.Error(err),
					zap.String("namespace", changefeedID.Namespace),
					zap.String("changefeed", changefeedID.ID))
			}
		}()
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	collector := collector.New(changefeedID, util.RoleProcessor,
		adminClient, client.Config().MetricRegistry)

	k := &kafkaDMLProducer{
		id:            changefeedID,
		client:        client,
		asyncProducer: asyncProducer,
		collector:     collector,
		closed:        false,
		closedChan:    make(chan struct{}),
		failpointCh:   make(chan error, 1),
	}

	// Start collecting metrics.
	go k.collector.Run(ctx)

	go func() {
		if err := k.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
				log.Error("Kafka DML producer run error",
					zap.String("namespace", k.id.Namespace),
					zap.String("changefeed", k.id.ID),
					zap.Error(err))
			default:
				log.Error("Error channel is full in kafka DML producer",
					zap.String("namespace", k.id.Namespace),
					zap.String("changefeed", k.id.ID),
					zap.Error(err))
			}
		}
	}()

	return k, nil
}

func (k *kafkaDMLProducer) AsyncSendMessage(
	ctx context.Context, topic string,
	partition int32, message *common.Message,
) error {
	// We have to hold the lock to avoid writing to a closed producer.
	// Close may be blocked for a long time.
	k.closedMu.RLock()
	defer k.closedMu.RUnlock()

	// If the producer is closed, we should skip the message and return an error.
	if k.closed {
		return cerror.ErrKafkaProducerClosed.GenWithStackByArgs()
	}
	failpoint.Inject("KafkaSinkAsyncSendError", func() {
		// simulate sending message to input channel successfully but flushing
		// message to Kafka meets error
		log.Info("KafkaSinkAsyncSendError error injected", zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID))
		k.failpointCh <- errors.New("kafka sink injected error")
		failpoint.Return(nil)
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
		return errors.Trace(ctx.Err())
	case k.asyncProducer.Input() <- msg:
	}
	return nil
}

func (k *kafkaDMLProducer) Close() {
	// We have to hold the lock to synchronize closing with writing.
	k.closedMu.Lock()
	defer k.closedMu.Unlock()
	// If the producer has already been closed, we should skip this close operation.
	if k.closed {
		// We need to guard against double closing the clients,
		// which could lead to panic.
		log.Warn("Kafka DML producer already closed",
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID))
		return
	}
	close(k.failpointCh)
	// Notify the run loop to exit.
	close(k.closedChan)
	k.closed = true
	// We need to close it asynchronously. Otherwise, we might get stuck
	// with an unhealthy(i.e. Network jitter, isolation) state of Kafka.
	// Safety:
	// * If the kafka cluster is running well, it will be closed as soon as possible.
	//   Also, we cancel all table pipelines before closed, so it's safe.
	// * If there is a problem with the kafka cluster, it will shut down the client first,
	//   which means no more data will be sent because the connection to the broker is dropped.
	//   Also, we cancel all table pipelines before closed, so it's safe.
	// * For Kafka Sink, duplicate data is acceptable.
	// * There is a risk of goroutine leakage, but it is acceptable and our main
	//   goal is not to get stuck with the processor tick.
	go func() {
		// `client` is mainly used by `asyncProducer` to fetch metadata and perform other related
		// operations. When we close the `kafkaSaramaProducer`,
		// there is no need for TiCDC to make sure that all buffered messages are flushed.
		// Consider the situation where the broker is irresponsive. If the client were not
		// closed, `asyncProducer.Close()` would waste a mount of time to try flush all messages.
		// To prevent the scenario mentioned above, close the client first.
		start := time.Now()
		if err := k.client.Close(); err != nil {
			log.Error("Close sarama client with error in kafka "+
				"DML producer", zap.Error(err),
				zap.Duration("duration", time.Since(start)),
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID))
		} else {
			log.Info("Sarama client closed in kafka "+
				"DML producer", zap.Duration("duration", time.Since(start)),
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID))
		}

		start = time.Now()
		err := k.asyncProducer.Close()
		if err != nil {
			log.Error("Close async client with error in kafka "+
				"DML producer", zap.Error(err),
				zap.Duration("duration", time.Since(start)),
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID))
		} else {
			log.Info("Async client closed in kafka "+
				"DML producer", zap.Duration("duration", time.Since(start)),
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID))
		}
		// Finally, close the metric collector.
		k.collector.Close()
	}()
}

func (k *kafkaDMLProducer) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-k.closedChan:
			return nil
		case err := <-k.failpointCh:
			log.Warn("Receive from failpoint chan in kafka "+
				"DML producer",
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID),
				zap.Error(err))
			return errors.Trace(err)
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
