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

package ddlproducer

import (
	"context"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	collector "github.com/pingcap/tiflow/cdc/sinkv2/metrics/mq/kafka"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// Assert DDLEventSink implementation
var _ DDLProducer = (*kafkaDDLProducer)(nil)

// kafkaDDLProducer is used to send messages to kafka synchronously.
type kafkaDDLProducer struct {
	// id indicates this sink belongs to which processor(changefeed).
	id model.ChangeFeedID
	// We hold the client to make close operation faster.
	// Please see the comment of Close().
	client sarama.Client
	// collector is used to report metrics.
	collector *collector.Collector
	// asyncProducer is used to send messages to kafka synchronously.
	syncProducer sarama.SyncProducer
	// closedMu is used to protect `closed`.
	// We need to ensure that closed producers are never written to.
	closedMu sync.RWMutex
	// closed is used to indicate whether the producer is closed.
	// We also use it to guard against double closes.
	closed bool
}

// NewKafkaDDLProducer creates a new kafka producer for replicating DDL.
func NewKafkaDDLProducer(ctx context.Context, client sarama.Client,
	adminClient pkafka.ClusterAdminClient,
) (DDLProducer, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)

	syncProducer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		// Close the client to prevent the goroutine leak.
		// Because it may be a long time to close the client,
		// so close it asynchronously.
		go func() {
			err := client.Close()
			if err != nil {
				log.Error("Close sarama client with error in kafka "+
					"DDL producer", zap.Error(err),
					zap.String("namespace", changefeedID.Namespace),
					zap.String("changefeed", changefeedID.ID))
			}
			if err := adminClient.Close(); err != nil {
				log.Error("Close sarama admin client with error in kafka "+
					"DDL producer", zap.Error(err),
					zap.String("namespace", changefeedID.Namespace),
					zap.String("changefeed", changefeedID.ID))
			}
		}()
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}
	collector := collector.New(changefeedID, util.RoleOwner,
		adminClient, client.Config().MetricRegistry)

	p := &kafkaDDLProducer{
		id:           changefeedID,
		client:       client,
		collector:    collector,
		syncProducer: syncProducer,
		closed:       false,
	}

	// Start collecting metrics.
	go p.collector.Run(ctx)

	return p, nil
}

func (k *kafkaDDLProducer) SyncBroadcastMessage(ctx context.Context, topic string,
	totalPartitionsNum int32, message *common.Message,
) error {
	k.closedMu.RLock()
	defer k.closedMu.RUnlock()

	if k.closed {
		return cerror.ErrKafkaProducerClosed.GenWithStackByArgs()
	}

	msgs := make([]*sarama.ProducerMessage, totalPartitionsNum)
	for i := 0; i < int(totalPartitionsNum); i++ {
		msgs[i] = &sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.ByteEncoder(message.Key),
			Value:     sarama.ByteEncoder(message.Value),
			Partition: int32(i),
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		err := k.syncProducer.SendMessages(msgs)
		return cerror.WrapError(cerror.ErrKafkaSendMessage, err)
	}
}

func (k *kafkaDDLProducer) SyncSendMessage(ctx context.Context, topic string,
	partitionNum int32, message *common.Message,
) error {
	k.closedMu.RLock()
	defer k.closedMu.RUnlock()

	if k.closed {
		return cerror.ErrKafkaProducerClosed.GenWithStackByArgs()
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Partition: partitionNum,
	}
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
		_, _, err := k.syncProducer.SendMessage(msg)
		return cerror.WrapError(cerror.ErrKafkaSendMessage, err)
	}
}

func (k *kafkaDDLProducer) Close() {
	// We have to hold the lock to prevent write to closed producer.
	k.closedMu.Lock()
	defer k.closedMu.Unlock()
	// If the producer was already closed, we should skip the close operation.
	if k.closed {
		// We need to guard against double closed the clients,
		// which could lead to panic.
		log.Warn("Kafka DDL producer already closed",
			zap.String("namespace", k.id.Namespace),
			zap.String("changefeed", k.id.ID))
		return
	}
	k.closed = true
	// We need to close it asynchronously. Otherwise, we might get stuck
	// with an unhealthy(i.e. Network jitter, isolation) state of Kafka.
	// Client has a background thread to fetch and update the metadata.
	// If we close the client synchronously, we might get stuck.
	// Safety:
	// * If the kafka cluster is running well, it will be closed as soon as possible.
	// * If there is a problem with the kafka cluster,
	//   no data will be lost because this is a synchronous client.
	// * There is a risk of goroutine leakage, but it is acceptable and our main
	//   goal is not to get stuck with the owner tick.
	go func() {
		start := time.Now()
		if err := k.client.Close(); err != nil {
			log.Error("Close sarama client with error in kafka "+
				"DDL producer", zap.Error(err),
				zap.Duration("duration", time.Since(start)),
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID))
		} else {
			log.Info("Sarama client closed in kafka "+
				"DDL producer", zap.Duration("duration", time.Since(start)),
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID))
		}

		start = time.Now()
		err := k.syncProducer.Close()
		if err != nil {
			log.Error("Close sync client with error in kafka "+
				"DDL producer", zap.Error(err),
				zap.Duration("duration", time.Since(start)),
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID))
		} else {
			log.Info("Sync client closed in kafka "+
				"DDL producer", zap.Duration("duration", time.Since(start)),
				zap.String("namespace", k.id.Namespace),
				zap.String("changefeed", k.id.ID))
		}

		// Finally, close the metric collector.
		k.collector.Close()
	}()
}
