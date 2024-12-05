// Copyright 2023 PingCAP, Inc.
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
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// Factory is used to produce all kafka components.
type Factory interface {
	// AdminClient return a kafka cluster admin client
	AdminClient(ctx context.Context) (ClusterAdminClient, error)
	// SyncProducer creates a sync producer to writer message to kafka
	SyncProducer(ctx context.Context) (SyncProducer, error)
	// AsyncProducer creates an async producer to writer message to kafka
	AsyncProducer(ctx context.Context, failpointCh chan error) (AsyncProducer, error)
	// MetricsCollector returns the kafka metrics collector
	MetricsCollector(role util.Role, adminClient ClusterAdminClient) MetricsCollector
}

// FactoryCreator defines the type of factory creator.
type FactoryCreator func(*Options, model.ChangeFeedID) (Factory, error)

// SyncProducer is the kafka sync producer
type SyncProducer interface {
	// SendMessage produces a given message, and returns only when it either has
	// succeeded or failed to produce. It will return the partition and the offset
	// of the produced message, or an error if the message failed to produce.
	SendMessage(ctx context.Context,
		topic string, partitionNum int32,
		message *common.Message) error

	// SendMessages produces a given set of messages, and returns only when all
	// messages in the set have either succeeded or failed. Note that messages
	// can succeed and fail individually; if some succeed and some fail,
	// SendMessages will return an error.
	SendMessages(ctx context.Context, topic string, partitionNum int32, message *common.Message) error

	// Close shuts down the producer; you must call this function before a producer
	// object passes out of scope, as it may otherwise leak memory.
	// You must call this before calling Close on the underlying client.
	Close()
}

// AsyncProducer is the kafka async producer
type AsyncProducer interface {
	// Close shuts down the producer and waits for any buffered messages to be
	// flushed. You must call this function before a producer object passes out of
	// scope, as it may otherwise leak memory. You must call this before process
	// shutting down, or you may lose messages. You must call this before calling
	// Close on the underlying client.
	Close()

	// AsyncSend is the input channel for the user to write messages to that they
	// wish to send.
	AsyncSend(ctx context.Context, topic string, partition int32, message *common.Message) error

	// AsyncRunCallback process the messages that has sent to kafka,
	// and run tha attached callback. the caller should call this
	// method in a background goroutine
	AsyncRunCallback(ctx context.Context) error
}

type saramaSyncProducer struct {
	id       model.ChangeFeedID
	client   sarama.Client
	producer sarama.SyncProducer
}

func (p *saramaSyncProducer) SendMessage(
	_ context.Context,
	topic string, partitionNum int32,
	message *common.Message,
) error {
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Partition: partitionNum,
	})
	return err
}

func (p *saramaSyncProducer) SendMessages(ctx context.Context, topic string, partitionNum int32, message *common.Message) error {
	msgs := make([]*sarama.ProducerMessage, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		msgs[i] = &sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.ByteEncoder(message.Key),
			Value:     sarama.ByteEncoder(message.Value),
			Partition: int32(i),
		}
	}
	return p.producer.SendMessages(msgs)
}

func (p *saramaSyncProducer) Close() {
	go func() {
		// We need to close it asynchronously. Otherwise, we might get stuck
		// with an unhealthy(i.e. Network jitter, isolation) state of Kafka.
		// Factory has a background thread to fetch and update the metadata.
		// If we close the client synchronously, we might get stuck.
		// Safety:
		// * If the kafka cluster is running well, it will be closed as soon as possible.
		// * If there is a problem with the kafka cluster,
		//   no data will be lost because this is a synchronous client.
		// * There is a risk of goroutine leakage, but it is acceptable and our main
		//   goal is not to get stuck with the owner tick.
		start := time.Now()
		if err := p.client.Close(); err != nil {
			log.Warn("Close Kafka DDL client with error",
				zap.String("namespace", p.id.Namespace),
				zap.String("changefeed", p.id.ID),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
		} else {
			log.Info("Kafka DDL client closed",
				zap.String("namespace", p.id.Namespace),
				zap.String("changefeed", p.id.ID),
				zap.Duration("duration", time.Since(start)))
		}
		start = time.Now()
		err := p.producer.Close()
		if err != nil {
			log.Error("Close Kafka DDL producer with error",
				zap.String("namespace", p.id.Namespace),
				zap.String("changefeed", p.id.ID),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
		} else {
			log.Info("Kafka DDL producer closed",
				zap.String("namespace", p.id.Namespace),
				zap.String("changefeed", p.id.ID),
				zap.Duration("duration", time.Since(start)))
		}
	}()
}

type saramaAsyncProducer struct {
	client       sarama.Client
	producer     sarama.AsyncProducer
	changefeedID model.ChangeFeedID
	failpointCh  chan error
}

func (p *saramaAsyncProducer) Close() {
	go func() {
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

		// `client` is mainly used by `asyncProducer` to fetch metadata and perform other related
		// operations. When we close the `kafkaSaramaProducer`,
		// there is no need for TiCDC to make sure that all buffered messages are flushed.
		// Consider the situation where the broker is irresponsive. If the client were not
		// closed, `asyncProducer.Close()` would waste a mount of time to try flush all messages.
		// To prevent the scenario mentioned above, close the client first.
		start := time.Now()
		if err := p.client.Close(); err != nil {
			log.Warn("Close kafka async producer client error",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
		} else {
			log.Info("Close kafka async producer client success",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Duration("duration", time.Since(start)))
		}

		start = time.Now()
		if err := p.producer.Close(); err != nil {
			log.Warn("Close kafka async producer error",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
		} else {
			log.Info("Close kafka async producer success",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Duration("duration", time.Since(start)))
		}
	}()
}

type item struct {
	tableID  string
	commitTs string
	offset   int64
}

func (p *saramaAsyncProducer) AsyncRunCallback(
	ctx context.Context,
) error {
	memo := make(map[int32]item)
	for {
		select {
		case <-ctx.Done():
			log.Info("async producer exit since context is done",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID))
			return errors.Trace(ctx.Err())
		case err := <-p.failpointCh:
			log.Warn("Receive from failpoint chan in kafka DML producer",
				zap.String("namespace", p.changefeedID.Namespace),
				zap.String("changefeed", p.changefeedID.ID),
				zap.Error(err))
			return errors.Trace(err)
		case ack := <-p.producer.Successes():
			if ack != nil {
				callback := ack.Metadata.(func())
				if callback != nil {
					callback()
				}
				schema := string(ack.Headers[0].Value)
				table := string(ack.Headers[1].Value)
				tableID := string(ack.Headers[2].Value)
				commitTs := string(ack.Headers[3].Value)
				previous := memo[ack.Partition]
				log.Info("async producer receive ack",
					zap.Int32("partition", ack.Partition),
					zap.Int64("oldOffset", previous.offset), zap.String("oldCommitTs", previous.commitTs), zap.String("oldTableID", previous.tableID),
					zap.Int64("newOffset", ack.Offset), zap.String("commitTs", commitTs), zap.String("tableID", tableID), zap.String("schema", schema), zap.String("table", table))
				if ack.Offset < previous.offset {
					log.Warn("kafka async producer receive an out-of-order message",
						zap.Int32("partition", ack.Partition),
						zap.Int64("oldOffset", previous.offset), zap.String("oldCommitTs", previous.commitTs), zap.String("oldTableID", previous.tableID),
						zap.Int64("newOffset", ack.Offset), zap.String("commitTs", commitTs), zap.String("tableID", tableID), zap.String("schema", schema), zap.String("table", table))
				}
				memo[ack.Partition] = item{
					tableID:  tableID,
					commitTs: commitTs,
					offset:   ack.Offset,
				}
			}
		case err := <-p.producer.Errors():
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

// AsyncSend is the input channel for the user to write messages to that they
// wish to send.
func (p *saramaAsyncProducer) AsyncSend(ctx context.Context, topic string, partition int32, message *common.Message) error {
	headers := []sarama.RecordHeader{
		{[]byte("Schema"), []byte(message.GetSchema())},
		{[]byte("Table"), []byte(message.GetTable())},
		{[]byte("TableID"), []byte(strconv.FormatUint(uint64(message.TableID), 10))},
		{[]byte("CommitTs"), []byte(strconv.FormatUint(message.Ts, 10))},
	}
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Headers:   headers,
		Key:       sarama.StringEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Metadata:  message.Callback,
	}
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case p.producer.Input() <- msg:
	}
	log.Info("async producer send message", zap.String("schema", message.GetSchema()), zap.String("table", message.GetTable()),
		zap.Int64("tableID", message.TableID), zap.Int32("partition", partition), zap.Uint64("commitTs", message.Ts))

	return nil
}
