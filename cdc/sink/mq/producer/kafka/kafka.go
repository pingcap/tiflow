// Copyright 2020 PingCAP, Inc.
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

package kafka

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

const (
	kafkaProducerRunning = 0
	kafkaProducerClosing = 1
)

type producer struct {
	// clientLock is used to protect concurrent access of asyncProducer and syncProducer.
	// Since we don't close these two clients (which have an input chan) from the
	// sender routine, data race or send on closed chan could happen.
	clientLock    sync.RWMutex
	asyncProducer kafka.AsyncProducer
	syncProducer  kafka.SyncProducer

	// producersReleased records whether asyncProducer and syncProducer have been closed properly
	producersReleased bool

	// It is used to count the number of messages sent out and messages received when flushing data.
	mu struct {
		sync.Mutex
		inflight  int64
		flushDone chan struct{}
	}

	failpointCh chan error

	closeCh chan struct{}
	// atomic flag indicating whether the producer is closing
	closing kafkaProducerClosingFlag

	role util.Role
	id   model.ChangeFeedID
}

type kafkaProducerClosingFlag = int32

// AsyncSendMessage asynchronously sends a message to kafka.
// Notice: this method is not thread-safe.
// Do not try to call AsyncSendMessage and Flush functions in different threads,
// otherwise Flush will not work as expected. It may never finish or flush the wrong message.
// Because inflight will be modified by mistake.
func (k *producer) AsyncSendMessage(
	ctx context.Context, topic string, partition int32, message *common.Message,
) error {
	k.clientLock.RLock()
	defer k.clientLock.RUnlock()

	// Checks whether the producer is closing.
	// The atomic flag must be checked under `clientLock.RLock()`
	if atomic.LoadInt32(&k.closing) == kafkaProducerClosing {
		return nil
	}

	failpoint.Inject("KafkaSinkAsyncSendError", func() {
		// simulate sending message to input channel successfully but flushing
		// message to Kafka meets error
		log.Info("failpoint error injected", zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID), zap.Any("role", k.role))
		k.failpointCh <- errors.New("kafka sink injected error")
		failpoint.Return(nil)
	})

	k.mu.Lock()
	k.mu.inflight++
	log.Debug("emitting inflight messages to kafka", zap.Int64("inflight", k.mu.inflight))
	k.mu.Unlock()

	return k.asyncProducer.AsyncSend(ctx, topic, partition,
		message.Key, message.Value, func() {
			k.mu.Lock()
			k.mu.inflight--
			if k.mu.inflight == 0 && k.mu.flushDone != nil {
				k.mu.flushDone <- struct{}{}
				k.mu.flushDone = nil
			}
			k.mu.Unlock()
		})
}

func (k *producer) SyncBroadcastMessage(
	ctx context.Context, topic string, partitionsNum int32, message *common.Message,
) error {
	k.clientLock.RLock()
	defer k.clientLock.RUnlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-k.closeCh:
		return nil
	default:
		err := k.syncProducer.SendMessages(ctx, topic,
			partitionsNum, message.Key, message.Value)
		return cerror.WrapError(cerror.ErrKafkaSendMessage, err)
	}
}

// Flush waits for all the messages in the async producer to be sent to Kafka.
// Notice: this method is not thread-safe.
// Do not try to call AsyncSendMessage and Flush functions in different threads,
// otherwise Flush will not work as expected. It may never finish or flush the wrong message.
// Because inflight will be modified by mistake.
func (k *producer) Flush(ctx context.Context) error {
	done := make(chan struct{}, 1)

	k.mu.Lock()
	inflight := k.mu.inflight
	immediateFlush := inflight == 0
	if !immediateFlush {
		k.mu.flushDone = done
	}
	k.mu.Unlock()

	if immediateFlush {
		return nil
	}

	log.Debug("flush waiting for inflight messages", zap.Int64("inflight", inflight))
	select {
	case <-k.closeCh:
		return cerror.ErrKafkaFlushUnfinished.GenWithStackByArgs()
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// stop closes the closeCh to signal other routines to exit
// It SHOULD NOT be called under `clientLock`.
func (k *producer) stop() {
	if atomic.SwapInt32(&k.closing, kafkaProducerClosing) == kafkaProducerClosing {
		return
	}
	log.Info("kafka producer closing...", zap.String("namespace", k.id.Namespace),
		zap.String("changefeed", k.id.ID), zap.Any("role", k.role))
	close(k.closeCh)
}

// Close closes the sync and async clients.
func (k *producer) Close() error {
	log.Info("stop the kafka producer", zap.String("namespace", k.id.Namespace),
		zap.String("changefeed", k.id.ID), zap.Any("role", k.role))
	k.stop()

	k.clientLock.Lock()
	defer k.clientLock.Unlock()

	if k.producersReleased {
		// We need to guard against double closing the clients,
		// which could lead to panic.
		log.Warn("kafka producer already released",
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID),
			zap.Any("role", k.role))
		return nil
	}
	k.producersReleased = true

	start := time.Now()
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
	start = time.Now()
	err = k.syncProducer.Close()
	if err != nil {
		log.Error("close sync client with error", zap.Error(err),
			zap.Duration("duration", time.Since(start)),
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID), zap.Any("role", k.role))
	} else {
		log.Info("sync client closed", zap.Duration("duration", time.Since(start)),
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID), zap.Any("role", k.role))
	}
	return nil
}

func (k *producer) run(ctx context.Context) error {
	defer func() {
		log.Info("stop the kafka producer",
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID), zap.Any("role", k.role))
		k.stop()
	}()

	return k.asyncProducer.AsyncRunCallback(ctx)
}

// NewFactoryImpl specifies the build method for the  client.
var NewFactoryImpl kafka.FactoryCreator = kafka.NewSaramaFactory

// NewProducer creates a kafka producer
func NewProducer(
	ctx context.Context,
	factory kafka.Factory,
	admin kafka.ClusterAdminClient,
	options *kafka.Options,
	errCh chan error,
	changefeedID model.ChangeFeedID,
) (*producer, error) {
	role := contextutil.RoleFromCtx(ctx)
	log.Info("Starting kafka sarama producer ...", zap.Any("options", options),
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID), zap.Any("role", role))

	closeCh := make(chan struct{})
	failpointCh := make(chan error, 1)
	asyncProducer, err := factory.AsyncProducer(changefeedID, closeCh, failpointCh)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	syncProducer, err := factory.SyncProducer()
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	runMetricsMonitor(ctx, factory.MetricRegistry(), changefeedID, role, admin)

	k := &producer{
		asyncProducer: asyncProducer,
		syncProducer:  syncProducer,
		closeCh:       closeCh,
		failpointCh:   failpointCh,
		closing:       kafkaProducerRunning,

		id:   changefeedID,
		role: role,
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
