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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/kafka"
	"github.com/pingcap/tiflow/pkg/notify"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

const defaultPartitionNum = 3

type kafkaSaramaProducer struct {
	// clientLock is used to protect concurrent access of asyncProducer and syncProducer.
	// Since we don't close these two clients (which have an input chan) from the
	// sender routine, data race or send on closed chan could happen.
	clientLock    sync.RWMutex
	client        sarama.Client
	asyncProducer sarama.AsyncProducer
	syncProducer  sarama.SyncProducer

	// producersReleased records whether asyncProducer and syncProducer have been closed properly
	producersReleased bool
	topic             string
	partitionNum      int32

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

	role util.Role
	id   model.ChangeFeedID
}

type kafkaProducerClosingFlag = int32

const (
	kafkaProducerRunning = 0
	kafkaProducerClosing = 1
)

func (k *kafkaSaramaProducer) SendMessage(ctx context.Context, message *codec.MQMessage, partition int32) error {
	k.clientLock.RLock()
	defer k.clientLock.RUnlock()

	// Checks whether the producer is closing.
	// The atomic flag must be checked under `clientLock.RLock()`
	if atomic.LoadInt32(&k.closing) == kafkaProducerClosing {
		return nil
	}

	msg := &sarama.ProducerMessage{
		Topic:     k.topic,
		Key:       sarama.ByteEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Partition: partition,
	}
	msg.Metadata = atomic.AddUint64(&k.partitionOffset[partition].sent, 1)

	failpoint.Inject("KafkaSinkAsyncSendError", func() {
		// simulate sending message to input channel successfully but flushing
		// message to Kafka meets error
		log.Info("failpoint error injected", zap.String("changefeed", k.id), zap.Any("role", k.role))
		k.failpointCh <- errors.New("kafka sink injected error")
		failpoint.Return(nil)
	})

	failpoint.Inject("SinkFlushDMLPanic", func() {
		time.Sleep(time.Second)
		log.Panic("SinkFlushDMLPanic",
			zap.String("changefeed", k.id), zap.Any("role", k.role))
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-k.closeCh:
		return nil
	case k.asyncProducer.Input() <- msg:
	}
	return nil
}

func (k *kafkaSaramaProducer) SyncBroadcastMessage(ctx context.Context, message *codec.MQMessage) error {
	k.clientLock.RLock()
	defer k.clientLock.RUnlock()
	msgs := make([]*sarama.ProducerMessage, k.partitionNum)
	for i := 0; i < int(k.partitionNum); i++ {
		msgs[i] = &sarama.ProducerMessage{
			Topic:     k.topic,
			Key:       sarama.ByteEncoder(message.Key),
			Value:     sarama.ByteEncoder(message.Value),
			Partition: int32(i),
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-k.closeCh:
		return nil
	default:
		err := k.syncProducer.SendMessages(msgs)
		return cerror.WrapError(cerror.ErrKafkaSendMessage, err)
	}
}

func (k *kafkaSaramaProducer) Flush(ctx context.Context) error {
	targetOffsets := make([]uint64, k.partitionNum)
	for i := 0; i < len(k.partitionOffset); i++ {
		targetOffsets[i] = atomic.LoadUint64(&k.partitionOffset[i].sent)
	}

	noEventsToFLush := true
	for i, target := range targetOffsets {
		if target > atomic.LoadUint64(&k.partitionOffset[i].flushed) {
			noEventsToFLush = false
			break
		}
	}
	if noEventsToFLush {
		// no events to flush
		return nil
	}

	// checkAllPartitionFlushed checks whether data in each partition is flushed
	checkAllPartitionFlushed := func() bool {
		for i, target := range targetOffsets {
			if target > atomic.LoadUint64(&k.partitionOffset[i].flushed) {
				return false
			}
		}
		return true
	}

flushLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-k.closeCh:
			if checkAllPartitionFlushed() {
				return nil
			}
			return cerror.ErrKafkaFlushUnfinished.GenWithStackByArgs()
		case <-k.flushedReceiver.C:
			if !checkAllPartitionFlushed() {
				continue flushLoop
			}
			return nil
		}
	}
}

func (k *kafkaSaramaProducer) GetPartitionNum() int32 {
	return k.partitionNum
}

// stop closes the closeCh to signal other routines to exit
// It SHOULD NOT be called under `clientLock`.
func (k *kafkaSaramaProducer) stop() {
	if atomic.SwapInt32(&k.closing, kafkaProducerClosing) == kafkaProducerClosing {
		return
	}
	log.Info("kafka producer closing...", zap.String("changefeed", k.id), zap.Any("role", k.role))
	close(k.closeCh)
}

// Close closes the sync and async clients.
func (k *kafkaSaramaProducer) Close() error {
	log.Info("stop the kafka producer", zap.String("changefeed", k.id), zap.Any("role", k.role))
	k.stop()

	k.clientLock.Lock()
	defer k.clientLock.Unlock()

	if k.producersReleased {
		// We need to guard against double closing the clients,
		// which could lead to panic.
		return nil
	}
	k.producersReleased = true

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
			zap.String("changefeed", k.id), zap.Any("role", k.role))
	} else {
		log.Info("sarama client closed", zap.Duration("duration", time.Since(start)),
			zap.String("changefeed", k.id), zap.Any("role", k.role))
	}

	start = time.Now()
	err := k.asyncProducer.Close()
	if err != nil {
		log.Error("close async client with error", zap.Error(err),
			zap.Duration("duration", time.Since(start)),
			zap.String("changefeed", k.id), zap.Any("role", k.role))
	} else {
		log.Info("async client closed", zap.Duration("duration", time.Since(start)),
			zap.String("changefeed", k.id), zap.Any("role", k.role))
	}
	start = time.Now()
	err = k.syncProducer.Close()
	if err != nil {
		log.Error("close sync client with error", zap.Error(err),
			zap.Duration("duration", time.Since(start)),
			zap.String("changefeed", k.id), zap.Any("role", k.role))
	} else {
		log.Info("sync client closed", zap.Duration("duration", time.Since(start)),
			zap.String("changefeed", k.id), zap.Any("role", k.role))
	}
	return nil
}

func (k *kafkaSaramaProducer) run(ctx context.Context) error {
	defer func() {
		k.flushedReceiver.Stop()
		log.Info("stop the kafka producer",
			zap.String("changefeed", k.id), zap.Any("role", k.role))
		k.stop()
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-k.closeCh:
			return nil
		case err := <-k.failpointCh:
			log.Warn("receive from failpoint chan", zap.Error(err),
				zap.String("changefeed", k.id), zap.Any("role", k.role))
			return err
		case msg := <-k.asyncProducer.Successes():
			if msg == nil || msg.Metadata == nil {
				continue
			}
			flushedOffset := msg.Metadata.(uint64)
			atomic.StoreUint64(&k.partitionOffset[msg.Partition].flushed, flushedOffset)
			k.flushedNotifier.Notify()
		case err := <-k.asyncProducer.Errors():
			// We should not wrap a nil pointer if the pointer is of a subtype of `error`
			// because Go would store the type info and the resulted `error` variable would not be nil,
			// which will cause the pkg/error library to malfunction.
			if err == nil {
				return nil
			}
			return cerror.WrapError(cerror.ErrKafkaAsyncSendMessage, err)
		}
	}
}

var (
	newSaramaConfigImpl                                 = newSaramaConfig
	NewAdminClientImpl  kafka.ClusterAdminClientCreator = kafka.NewSaramaAdminClient
)

// NewKafkaSaramaProducer creates a kafka sarama producer
func NewKafkaSaramaProducer(ctx context.Context, topic string, config *Config, opts map[string]string, errCh chan error) (*kafkaSaramaProducer, error) {
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	role := util.RoleFromCtx(ctx)
	log.Info("Starting kafka sarama producer ...", zap.Any("config", config),
		zap.String("changefeed", changefeedID), zap.Any("role", role))

	cfg, err := newSaramaConfigImpl(ctx, config)
	if err != nil {
		return nil, err
	}

	admin, err := NewAdminClientImpl(config.BrokerEndpoints, cfg)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Warn("close kafka cluster admin failed", zap.Error(err),
				zap.String("changefeed", changefeedID), zap.Any("role", role))
		}
	}()

	if err := validateAndCreateTopic(admin, topic, config, cfg, opts); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	client, err := sarama.NewClient(config.BrokerEndpoints, cfg)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	asyncProducer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	syncProducer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	notifier := new(notify.Notifier)
	flushedReceiver, err := notifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, err
	}
	k := &kafkaSaramaProducer{
		client:        client,
		asyncProducer: asyncProducer,
		syncProducer:  syncProducer,
		topic:         topic,
		partitionNum:  config.PartitionNum,
		partitionOffset: make([]struct {
			flushed uint64
			sent    uint64
		}, config.PartitionNum),
		flushedNotifier: notifier,
		flushedReceiver: flushedReceiver,
		closeCh:         make(chan struct{}),
		failpointCh:     make(chan error, 1),
		closing:         kafkaProducerRunning,

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
					zap.String("changefeed", k.id), zap.Any("role", role))
			}
		}
	}()
	return k, nil
}

var (
	validClientID     = regexp.MustCompile(`\A[A-Za-z0-9._-]+\z`)
	commonInvalidChar = regexp.MustCompile(`[\?:,"]`)
)

func kafkaClientID(role, captureAddr, changefeedID, configuredClientID string) (clientID string, err error) {
	if configuredClientID != "" {
		clientID = configuredClientID
	} else {
		clientID = fmt.Sprintf("TiCDC_sarama_producer_%s_%s_%s", role, captureAddr, changefeedID)
		clientID = commonInvalidChar.ReplaceAllString(clientID, "_")
	}
	if !validClientID.MatchString(clientID) {
		return "", cerror.ErrKafkaInvalidClientID.GenWithStackByArgs(clientID)
	}
	return
}

func validateAndCreateTopic(admin kafka.ClusterAdminClient, topic string, config *Config, saramaConfig *sarama.Config,
	opts map[string]string) error {
	topics, err := admin.ListTopics()
	if err != nil {
		return cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	info, created := topics[topic]
	// once we have found the topic, no matter `auto-create-topic`, make sure user input parameters are valid.
	if created {
		// make sure that topic's `max.message.bytes` is not less than given `max-message-bytes`
		// else the producer will send message that too large to make topic reject, then changefeed would error.
		// only the default `open protocol` and `craft protocol` use `max-message-bytes`, so check this for them.
		topicMaxMessageBytes, err := getTopicMaxMessageBytes(admin, info)
		if err != nil {
			return cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
		}
		if topicMaxMessageBytes < config.MaxMessageBytes {
			log.Warn("topic's `max.message.bytes` less than the `max-message-bytes`,"+
				"use topic's `max.message.bytes` to initialize the Kafka producer",
				zap.Int("max.message.bytes", topicMaxMessageBytes),
				zap.Int("max-message-bytes", config.MaxMessageBytes))
			saramaConfig.Producer.MaxMessageBytes = topicMaxMessageBytes
		}

		// no need to create the topic, but we would have to log user if they found enter wrong topic name later
		if config.AutoCreate {
			log.Warn("topic already exist, TiCDC will not create the topic",
				zap.String("topic", topic), zap.Any("detail", info))
		}

		if err := config.setPartitionNum(info.NumPartitions); err != nil {
			return errors.Trace(err)
		}

		return nil
	}

	if !config.AutoCreate {
		return cerror.ErrKafkaInvalidConfig.GenWithStack("`auto-create-topic` is false, and topic not found")
	}

	// when try to create the topic, we don't know how to set the `max.message.bytes` for the topic.
	// Kafka would create the topic with broker's `message.max.bytes`,
	// we have to make sure it's not greater than `max-message-bytes` for the default open protocol.
	brokerMessageMaxBytes, err := getBrokerMessageMaxBytes(admin)
	if err != nil {
		log.Warn("TiCDC cannot find `message.max.bytes` from broker's configuration")
		return errors.Trace(err)
	}

	if brokerMessageMaxBytes < config.MaxMessageBytes {
		log.Warn("broker's `message.max.bytes` less than the `max-message-bytes`,"+
			"use broker's `message.max.bytes` to initialize the Kafka producer",
			zap.Int("message.max.bytes", brokerMessageMaxBytes),
			zap.Int("max-message-bytes", config.MaxMessageBytes))
		saramaConfig.Producer.MaxMessageBytes = brokerMessageMaxBytes
	}

	// topic not created yet, and user does not specify the `partition-num` in the sink uri.
	if config.PartitionNum == 0 {
		config.PartitionNum = defaultPartitionNum
		log.Warn("partition-num is not set, use the default partition count",
			zap.String("topic", topic), zap.Int32("partitions", config.PartitionNum))
	}

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     config.PartitionNum,
		ReplicationFactor: config.ReplicationFactor,
	}, false)
	// TODO identify the cause of "Topic with this name already exists"
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	log.Info("TiCDC create the topic",
		zap.Int32("partition-num", config.PartitionNum),
		zap.Int16("replication-factor", config.ReplicationFactor))

	return nil
}

func init() {
	sarama.MaxRequestSize = 1024 * 1024 * 1024 // 1GB
}

func getBrokerMessageMaxBytes(admin sarama.ClusterAdmin) (int, error) {
	target := "message.max.bytes"
	_, controllerID, err := admin.DescribeCluster()
	if err != nil {
		return 0, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	configEntries, err := admin.DescribeConfig(sarama.ConfigResource{
		Type:        sarama.BrokerResource,
		Name:        strconv.Itoa(int(controllerID)),
		ConfigNames: []string{target},
	})
	if err != nil {
		return 0, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	if len(configEntries) == 0 || configEntries[0].Name != target {
		return 0, cerror.ErrKafkaNewSaramaProducer.GenWithStack(
			"since cannot find the `message.max.bytes` from the broker's configuration, " +
				"ticdc decline to create the topic and changefeed to prevent potential error")
	}

	result, err := strconv.Atoi(configEntries[0].Value)
	if err != nil {
		return 0, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	return result, nil
}

func getTopicMaxMessageBytes(admin sarama.ClusterAdmin, info sarama.TopicDetail) (int, error) {
	if a, ok := info.ConfigEntries["max.message.bytes"]; ok {
		result, err := strconv.Atoi(*a)
		if err != nil {
			return 0, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
		}
		return result, nil
	}

	return getBrokerMessageMaxBytes(admin)
}
