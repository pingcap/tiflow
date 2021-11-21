// Copyright 2021 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/sink/codec"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type yakProducer struct {
	topic string
	partitionNum int32

	// clientLock is used to protect concurrent access of syncWriter and asyncWriter.
	// Since we don't close these two clients (which have an input chan) from the
	// sender routine, data race or send on closed chan could happen.
	clientLock  sync.RWMutex
	syncWriter *kafka.Writer
	asyncWriter *kafka.Writer

	partitionOffset []struct {
		flushed uint64
		sent    uint64
	}

	flushedNotifier *notify.Notifier
	flushedReceiver *notify.Receiver

	failpointCh chan error

	closeCh chan struct{}
	// atomic flag indicating whether the producer is closing
	closing kafkaProducerClosingFlag
	// released records whether asyncWriter and syncWriter have been closed properly
	released bool
}

func newWriter(async bool) *kafka.Writer {
	return &kafka.Writer{
		Addr:         nil,
		Topic:        "",
		Balancer:     nil,
		MaxAttempts:  0,
		BatchSize:    0,
		BatchBytes:   0,
		BatchTimeout: 0,
		ReadTimeout:  0,
		WriteTimeout: 0,
		RequiredAcks: 0,
		Async:        async,
		Completion:   nil,
		Compression:  0,
		Logger:       nil,
		ErrorLogger:  nil,
		Transport:    nil,
	}
}

type topicMetadata struct {
	maxMessageBytes int
	partitionNum int32
}

func fetchTopicMetadata(ctx context.Context, client kafka.Client, topic string) (*topicMetadata, error) {
	resp, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{
		Resources:            []kafka.DescribeConfigRequestResource{{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: topic,
			ConfigNames: []string{"max.message.bytes", "partitions"},
		}},
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := new(topicMetadata)
	for _, item := range resp.Resources {
		if item.ResourceType == int8(kafka.ResourceTypeTopic) && item.ResourceName == "" {
			for _, entry := range item.ConfigEntries {
				if entry.ConfigName == "max.message.bytes" {
					a, err := strconv.Atoi(entry.ConfigValue)
					if err != nil {
						return nil, errors.Trace(err)
					}
					result.maxMessageBytes = a
				}
				if entry.ConfigName == "partitions" {
					a, err := strconv.Atoi(entry.ConfigValue)
					if err != nil {
						return nil, errors.Trace(err)
					}
					result.partitionNum = int32(a)
				}
			}
		}
	}
	return result, nil
}

func (m *topicMetadata) fetchBrokerMessageMaxBytes(ctx context.Context, client kafka.Client, endPoint string) error {
	if m.maxMessageBytes != 0 {
		return nil
	}

	conn, err := kafka.DialContext(ctx, "tcp", endPoint)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Warn("close conn failed", zap.Error(err))
		}
	}()

	broker, err := conn.Controller()
	if err != nil {
		return errors.Trace(err)
	}

	target := "message.max.bytes"
	resp, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{
		Resources: []kafka.DescribeConfigRequestResource{
			{
				ResourceType: kafka.ResourceTypeBroker,
				ResourceName: strconv.Itoa(broker.ID),
				ConfigNames: []string{target},
			},
		},
	})
	if err != nil {
		return errors.Trace(err)
	}

	if len(resp.Resources) == 0 || resp.Resources[0].ResourceName != target {
		return errors.Trace(err)
	}

	a, err := strconv.Atoi(resp.Resources[0].ConfigEntries[0].ConfigValue)
	if err != nil {
		return errors.Trace(err)
	}
	m.maxMessageBytes = a
	return nil
}

func foreplay(ctx context.Context, topic string, protocol codec.Protocol, config *Config) error {
	client := kafka.Client{
		Addr:      kafka.TCP(config.BrokerEndpoints[0]),
		Timeout:   10 * time.Second,
	}

	topicMeta, err := fetchTopicMetadata(ctx, client, topic)
	if err != nil {
		return errors.Trace(err)
	}
	// once we have found the topic, no matter `auto-create-topic`, make sure user input parameters are valid.
	if topicMeta.partitionNum != 0 {
		// make sure that topic's `max.message.bytes` is not less than given `max-message-bytes`
		// else the producer will send message that too large to make topic reject, then changefeed would error.
		// only the default `open protocol` and `craft protocol` use `max-message-bytes`, so check this for them.
		if protocol == codec.ProtocolDefault || protocol == codec.ProtocolCraft {
			if topicMeta.maxMessageBytes < config.MaxMessageBytes {
				return cerror.ErrKafkaInvalidConfig.GenWithStack(
					"topic already exist, and topic's max.message.bytes(%d) less than max-message-bytes(%d)."+
						"Please make sure `max-message-bytes` not greater than topic `max.message.bytes`",
					topicMeta.maxMessageBytes, config.MaxMessageBytes)
			}
		}

		// no need to create the topic, but we would have to log user if they found enter wrong topic name later
		if config.AutoCreate {
			log.Warn("topic already exist, TiCDC will not create the topic",
				zap.String("topic", topic), zap.Any("detail", topicMeta))
		}

		if err := config.adjustPartitionNum(topicMeta.partitionNum); err != nil {
			return errors.Trace(err)
		}

		return nil
	}

	if !config.AutoCreate {
		return cerror.ErrKafkaInvalidConfig.GenWithStack("`auto-create-topic` is false, and topic not found")
	}

	// when try to create the topic, we don't know how to set the `max.message.bytes` for the topic.
	// Kafka would create the topic with broker's `message.max.bytes`,
	// we have to make sure it's not greater than `max-message-bytes` for the default open protocol & craft protocol.
	if protocol == codec.ProtocolDefault || protocol == codec.ProtocolCraft {
		if err := topicMeta.fetchBrokerMessageMaxBytes(ctx, client, config.BrokerEndpoints[0]); err != nil {
			log.Warn("TiCDC cannot find `message.max.bytes` from broker's configuration")
			return errors.Trace(err)
		}

		if topicMeta.maxMessageBytes < config.MaxMessageBytes {
			return cerror.ErrKafkaInvalidConfig.GenWithStack(
				"broker's message.max.bytes(%d) less than max-message-bytes(%d)"+
					"Please make sure `max-message-bytes` not greater than broker's `message.max.bytes`",
				topicMeta.maxMessageBytes, config.MaxMessageBytes)
		}
	}

	// topic not created yet, and user does not specify the `partition-num` in the sink uri.
	if config.PartitionNum == 0 {
		config.PartitionNum = defaultPartitionNum
		log.Warn("partition-num is not set, use the default partition count",
			zap.String("topic", topic), zap.Int32("partitions", config.PartitionNum))
	}

	createTopicResp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics:       []kafka.TopicConfig{
			{
				Topic:             topic,
				NumPartitions:     int(config.PartitionNum),
				ReplicationFactor: int(config.ReplicationFactor),
			},
		},
		ValidateOnly: false,
	})
	if err != nil {
		return errors.Trace(err)
	}
	for _, err := range createTopicResp.Errors {
		if err != nil {
			return errors.Trace(err)
		}
	}

	log.Info("TiCDC create the topic",
		zap.Int32("partition-num", config.PartitionNum),
		zap.Int16("replication-factor", config.ReplicationFactor))

	return nil
}

func NewYakProducer(ctx context.Context, topic string, protocol codec.Protocol, config *Config, errCh chan error) (*yakProducer, error) {
	if err := foreplay(ctx, topic, protocol, config); err != nil {
		return nil, errors.Trace(err)
	}

	notifier := new(notify.Notifier)
	flushedReceiver, err := notifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, errors.Trace(err)
	}

	p := &yakProducer{
		topic: topic,
		partitionNum: config.PartitionNum,

		syncWriter: newWriter(false),
		asyncWriter: newWriter(true),

		partitionOffset: make([]struct {
			flushed uint64
			sent    uint64
		}, config.PartitionNum),
		flushedNotifier: notifier,
		flushedReceiver: flushedReceiver,
		closeCh:         make(chan struct{}),
		failpointCh:     make(chan error, 1),
		closing:         kafkaProducerRunning,
	}

	p.asyncWriter.Completion = func(messages []kafka.Message, err error) {
		if err != nil {
			errCh <- err
			return
		}
		for _, msg := range messages {
			if uint64(msg.Offset) > atomic.LoadUint64(&p.partitionOffset[msg.Partition].flushed) {
				atomic.StoreUint64(&p.partitionOffset[msg.Partition].flushed, uint64(msg.Offset))
			}
		}
	}

	go func() {
		if err := p.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("error channel is full", zap.Error(err))
			}
		}
	}()

	return p, nil
}

func (p *yakProducer) run(ctx context.Context) error {
	defer func() {
		p.flushedReceiver.Stop()
		p.stop()
	}()

	for {
		select {
		case <- ctx.Done():
			return ctx.Err()
		case <- p.closeCh:
			return nil
		case err := <- p.failpointCh:
			log.Warn("receive from failpoint chan", zap.Error(err))
			return err
		// todo: fetch producer commit information from the writer in a proper way.
		// update the flushed offset, also handle commit error.
		default:
			var (
				partition int
				flushedOffset uint64
			)
			atomic.StoreUint64(&p.partitionOffset[partition].flushed, flushedOffset)
			p.flushedNotifier.Notify()
		}
	}
}

func (p *yakProducer) AsyncSendMessage(ctx context.Context, message *codec.MQMessage, partition int32) error {
	p.clientLock.RLock()
	defer p.clientLock.RUnlock()

	// Checks whether the producer is closing.
	// The atomic flag must be checked under `clientLock.RLock()`
	if atomic.LoadInt32(&p.closing) == kafkaProducerClosing {
		return nil
	}

	failpoint.Inject("KafkaSinkAsyncSendError", func() {
		// simulate sending message to input channel successfully but flushing
		// message to Kafka meets error
		log.Info("failpoint error injected")
		p.failpointCh <- errors.New("kafka sink injected error")
		failpoint.Return(nil)
	})

	failpoint.Inject("SinkFlushDMLPanic", func() {
		time.Sleep(time.Second)
		log.Panic("SinkFlushDMLPanic")
	})

	atomic.AddUint64(&p.partitionOffset[partition].sent, 1)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closeCh:
		return nil
	default:
	}
	if err := p.asyncWriter.WriteMessages(ctx, kafka.Message{
		Topic:         p.topic,
		Partition: int(partition),
		Offset:        0,
		HighWaterMark: 0,
		Key:           message.Key,
		Value:         message.Value,
	}); err != nil {
		return cerror.WrapError(cerror.ErrKafkaAsyncSendMessage, err)
	}
	return nil
}

func (p *yakProducer) SyncBroadcastMessage(ctx context.Context, message *codec.MQMessage) error {
	p.clientLock.RLock()
	defer p.clientLock.Unlock()

	messages := make([]kafka.Message, p.partitionNum)
	for i := 0; i < int(p.partitionNum); i++ {
		messages[i] = kafka.Message{
			Topic:         p.topic,
			Partition:     i,
			Key:           message.Key,
			Value:         message.Value,
		}
	}

	select {
	case <- ctx.Done():
		return ctx.Err()
	case <- p.closeCh:
		return nil
	default:
	}
	err := p.syncWriter.WriteMessages(ctx, messages...)
	if err != nil {
		return cerror.WrapError(cerror.ErrKafkaSendMessage, err)
	}
	return nil
}

func (p *yakProducer) Flush(ctx context.Context) error {
	targetOffest := make([]uint64, p.partitionNum)
	for i := 0; i < len(p.partitionOffset); i++ {
		targetOffest[i] = atomic.LoadUint64(&p.partitionOffset[i].sent)
	}

	allFlushed := true
	for i, target := range targetOffest {
		// there is still some messages sent to kafka producer, but does not flushed to brokers yet.
		if target > atomic.LoadUint64(&p.partitionOffset[i].flushed) {
			allFlushed = false
			break
		}
	}

	if allFlushed {
		return nil
	}

flushLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.closeCh:
			if p.allMessagesFlushed() {
				return nil
			}
			return cerror.ErrKafkaFlushUnfinished.GenWithStackByArgs()
		case <-p.flushedReceiver.C:
			if !p.allMessagesFlushed() {
				continue flushLoop
			}
			return nil
		}
	}
}

func(p *yakProducer) allMessagesFlushed() bool {
	targetOffset := make([]uint64, p.partitionNum)
	for i := 0; i < int(p.partitionNum); i++ {
		targetOffset[i] = atomic.LoadUint64(&p.partitionOffset[i].sent)
	}

	for i, target := range targetOffset {
		// there is still some messages sent to kafka producer, but does not flushed to brokers yet.
		if target > atomic.LoadUint64(&p.partitionOffset[i].flushed) {
			return false
		}
	}

	return true
}

func (p *yakProducer) GetPartitionNum() int32 {
	return p.partitionNum
}

// stop closes the closeCh to signal other routines to exit
// It SHOULD NOT be called under `clientLock`.
func (p *yakProducer) stop() {
	if atomic.SwapInt32(&p.closing, kafkaProducerClosing) == kafkaProducerClosing {
		return
	}
	close(p.closeCh)
}

func (p *yakProducer) Close() error {
	p.stop()

	p.clientLock.Lock()
	defer p.clientLock.Unlock()

	if p.released {
		// We need to guard against double closing the clients,
		// which could lead to panic.
		return nil
	}
	p.released = true

	// In fact close sarama sync client doesn't return any error.
	// But close async client returns error if error channel is not empty, we
	// don't populate this error to the upper caller, just add a log here.
	if err := p.syncWriter.Close();err != nil {
		log.Error("close sync client with error", zap.Error(err))
	}
	if err := p.asyncWriter.Close(); err != nil {
		log.Error("close async client with error", zap.Error(err))
	}
	return nil
}
