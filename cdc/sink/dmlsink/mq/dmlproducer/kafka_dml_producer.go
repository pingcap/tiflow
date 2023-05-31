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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

var _ DMLProducer = (*kafkaDMLProducer)(nil)

// kafkaDMLProducer is used to send messages to kafka.
type kafkaDMLProducer struct {
	// id indicates which processor (changefeed) this sink belongs to.
	id model.ChangeFeedID
	// asyncProducer is used to send messages to kafka asynchronously.
	asyncProducer kafka.AsyncProducer
	// metricsCollector is used to report metrics.
	metricsCollector kafka.MetricsCollector
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

	cancel context.CancelFunc
}

// NewKafkaDMLProducer creates a new kafka producer.
func NewKafkaDMLProducer(
	ctx context.Context,
	factory kafka.Factory,
	adminClient kafka.ClusterAdminClient,
	errCh chan error,
) (DMLProducer, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	log.Info("Starting kafka DML producer ...",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID))

	closeCh := make(chan struct{})
	failpointCh := make(chan error, 1)
	asyncProducer, err := factory.AsyncProducer(ctx, closeCh, failpointCh)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	metricsCollector := factory.MetricsCollector(
		util.RoleProcessor,
		adminClient)

	ctx, cancel := context.WithCancel(ctx)
	k := &kafkaDMLProducer{
		id:               changefeedID,
		asyncProducer:    asyncProducer,
		metricsCollector: metricsCollector,
		closed:           false,
		closedChan:       closeCh,
		failpointCh:      failpointCh,
		cancel:           cancel,
	}

	// Start collecting metrics.
	go k.metricsCollector.Run(ctx)

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
	return k.asyncProducer.AsyncSend(ctx, topic, partition,
		message.Key, message.Value, message.Callback)
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
	if k.cancel != nil {
		k.cancel()
	}

	close(k.failpointCh)
	// Notify the run loop to exit.
	close(k.closedChan)
	k.closed = true

	k.asyncProducer.Close()
}

func (k *kafkaDMLProducer) run(ctx context.Context) error {
	return k.asyncProducer.AsyncRunCallback(ctx)
}
