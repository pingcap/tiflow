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
	"net/url"
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
	"github.com/pingcap/ticdc/cdc/sink/codec"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

const defaultPartitionNum = 3

// Config stores user specified Kafka producer configuration
type Config struct {
	BrokerEndpoints []string

	TopicName string

	PartitionNum int32
	// User should make sure that `replication-factor` not greater than the number of kafka brokers.
	ReplicationFactor int16

	Version         string
	MaxMessageBytes int
	Compression     string
	ClientID        string
	Credential      *security.Credential
	SaslScram       *security.SaslScram
	// control whether to create topic
	AutoCreate bool
}

// NewConfig returns a default Kafka configuration
func NewConfig() *Config {
	return &Config{
		Version: "2.4.0",
		// MaxMessageBytes will be used to initialize producer, we set the default value (1M) identical to kafka broker.
		MaxMessageBytes:   1 * 1024 * 1024,
		ReplicationFactor: 1,
		Compression:       "none",
		Credential:        &security.Credential{},
		SaslScram:         &security.SaslScram{},
		AutoCreate:        true,
	}
}

// Initialize the kafka configuration
func (c *Config) Initialize(sinkURI *url.URL, replicaConfig *config.ReplicaConfig, opts map[string]string) error {
	c.BrokerEndpoints = strings.Split(sinkURI.Host, ",")
	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	if topic == "" {
		return cerror.ErrKafkaInvalidConfig.GenWithStack("topic name not found")
	}
	c.TopicName = topic

	params := sinkURI.Query()
	s := params.Get("partition-num")
	if s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.PartitionNum = int32(a)
		if c.PartitionNum <= 0 {
			return cerror.ErrKafkaInvalidPartitionNum.GenWithStackByArgs(c.PartitionNum)
		}
	}

	s = params.Get("replication-factor")
	if s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.ReplicationFactor = int16(a)
	}

	s = params.Get("kafka-version")
	if s != "" {
		c.Version = s
	}

	s = params.Get("max-message-bytes")
	if s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.MaxMessageBytes = a
		opts["max-message-bytes"] = s
	}

	s = params.Get("max-batch-size")
	if s != "" {
		opts["max-batch-size"] = s
	}

	s = params.Get("compression")
	if s != "" {
		c.Compression = s
	}

	c.ClientID = params.Get("kafka-client-id")

	s = params.Get("ca")
	if s != "" {
		c.Credential.CAPath = s
	}

	s = params.Get("cert")
	if s != "" {
		c.Credential.CertPath = s
	}

	s = params.Get("key")
	if s != "" {
		c.Credential.KeyPath = s
	}

	s = params.Get("sasl-user")
	if s != "" {
		c.SaslScram.SaslUser = s
	}

	s = params.Get("sasl-password")
	if s != "" {
		c.SaslScram.SaslPassword = s
	}

	s = params.Get("sasl-mechanism")
	if s != "" {
		c.SaslScram.SaslMechanism = s
	}

	s = params.Get("auto-create-topic")
	if s != "" {
		autoCreate, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		c.AutoCreate = autoCreate
	}

	s = params.Get("protocol")
	if s != "" {
		replicaConfig.Sink.Protocol = s
	}

	s = params.Get("enable-tidb-extension")
	if s != "" {
		_, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		if replicaConfig.Sink.Protocol != "canal-json" {
			return cerror.WrapError(cerror.ErrKafkaInvalidConfig, errors.New("enable-tidb-extension only support canal-json"))
		}
		opts["enable-tidb-extension"] = s
	}

	return nil
}

type kafkaSaramaProducer struct {
	// clientLock is used to protect concurrent access of asyncClient and syncClient.
	// Since we don't close these two clients (which have an input chan) from the
	// sender routine, data race or send on closed chan could happen.
	clientLock  sync.RWMutex
	asyncClient sarama.AsyncProducer
	syncClient  sarama.SyncProducer
	// producersReleased records whether asyncClient and syncClient have been closed properly
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
}

type kafkaProducerClosingFlag = int32

const (
	kafkaProducerRunning = 0
	kafkaProducerClosing = 1
)

func (k *kafkaSaramaProducer) AsyncSendMessage(ctx context.Context, message *codec.MQMessage, partition int32) error {
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
		log.Info("failpoint error injected")
		k.failpointCh <- errors.New("kafka sink injected error")
		failpoint.Return(nil)
	})

	failpoint.Inject("SinkFlushDMLPanic", func() {
		time.Sleep(time.Second)
		log.Panic("SinkFlushDMLPanic")
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-k.closeCh:
		return nil
	case k.asyncClient.Input() <- msg:
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
		err := k.syncClient.SendMessages(msgs)
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
	close(k.closeCh)
}

// Close closes the sync and async clients.
func (k *kafkaSaramaProducer) Close() error {
	k.stop()

	k.clientLock.Lock()
	defer k.clientLock.Unlock()

	if k.producersReleased {
		// We need to guard against double closing the clients,
		// which could lead to panic.
		return nil
	}
	k.producersReleased = true
	// In fact close sarama sync client doesn't return any error.
	// But close async client returns error if error channel is not empty, we
	// don't populate this error to the upper caller, just add a log here.
	err1 := k.syncClient.Close()
	err2 := k.asyncClient.Close()
	if err1 != nil {
		log.Error("close sync client with error", zap.Error(err1))
	}
	if err2 != nil {
		log.Error("close async client with error", zap.Error(err2))
	}
	return nil
}

func (k *kafkaSaramaProducer) run(ctx context.Context) error {
	defer func() {
		k.flushedReceiver.Stop()
		k.stop()
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-k.closeCh:
			return nil
		case err := <-k.failpointCh:
			log.Warn("receive from failpoint chan", zap.Error(err))
			return err
		case msg := <-k.asyncClient.Successes():
			if msg == nil || msg.Metadata == nil {
				continue
			}
			flushedOffset := msg.Metadata.(uint64)
			atomic.StoreUint64(&k.partitionOffset[msg.Partition].flushed, flushedOffset)
			k.flushedNotifier.Notify()
		case err := <-k.asyncClient.Errors():
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

func topicPreProcess(config *Config, saramaConfig *sarama.Config) error {
	failpoint.Inject("workaround4Test", func() {
		failpoint.Return(nil)
	})
	admin, err := sarama.NewClusterAdmin(config.BrokerEndpoints, saramaConfig)
	if err != nil {
		return err
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Warn("close kafka cluster admin failed", zap.Error(err))
		}
	}()

	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}

	var realPartitionCount int32 = 0
	info, ok := topics[config.TopicName]
	if ok {
		realPartitionCount = info.NumPartitions
	}

	// if user specify cdc to create the topic, we must make sure that topic number and replication-factor is given
	if config.AutoCreate {
		// if the specified topic name already exist, we should make sure that topic's `max.message.bytes` is not
		// less than given `max-message-size`
		if ok {
			if a, ok := info.ConfigEntries["max.message.bytes"]; ok {
				topicMaxMessageBytes, err := strconv.Atoi(*a)
				if err != nil {
					return err
				}
				if topicMaxMessageBytes < config.MaxMessageBytes {
					return cerror.ErrKafkaInvalidConfig.GenWithStack(
						"topic already exist, and max.message.size(%d) less than max-message-size(%d)",
						topicMaxMessageBytes, config.MaxMessageBytes)
				}
			}

			config.PartitionNum = realPartitionCount
			log.Warn("topic already exist", zap.String("topic", config.TopicName))
			return nil
		}
		// this could happen if user does not specify the `partition-num` in the sink uri.
		if config.PartitionNum == 0 {
			config.PartitionNum = defaultPartitionNum
			log.Warn("partition-num is not set, use the default partition count",
				zap.String("topic", config.TopicName), zap.Int32("partitions", config.PartitionNum))
		}
		err := admin.CreateTopic(config.TopicName, &sarama.TopicDetail{
			NumPartitions:     config.PartitionNum,
			ReplicationFactor: config.ReplicationFactor,
		}, false)
		// TODO identify the cause of "Topic with this name already exists"
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			return err
		}
		return nil
	}

	// if `auto-create-topic` is disabled by user, we would assume topic should have already created
	if !ok {
		return cerror.ErrKafkaInvalidConfig.GenWithStack("auto-create-topic is false, and topic not found")
	}

	if config.PartitionNum == 0 {
		config.PartitionNum = realPartitionCount
		return nil
	}

	if config.PartitionNum < realPartitionCount {
		log.Warn("partition count assigned in sink-uri is less than that of topic, some partitions would not have messages to be dispatched",
			zap.Int32("sink-uri partitions", config.PartitionNum), zap.Int32("topic partitions", realPartitionCount))
		return nil
	}

	// for topic already exist, we just make sure that the user specified partition count is not greater than the real
	// partition count, since we would dispatch messages to different partitions, this could prevent potential message loss.
	if config.PartitionNum > realPartitionCount {
		return cerror.ErrKafkaInvalidPartitionNum.GenWithStack(
			"partition number(%d) assigned in sink-uri is more than that of topic(%d)", config.PartitionNum, realPartitionCount)
	}

	return nil
}

var newSaramaConfigImpl = newSaramaConfig

// NewKafkaSaramaProducer creates a kafka sarama producer
func NewKafkaSaramaProducer(ctx context.Context, config *Config, errCh chan error) (*kafkaSaramaProducer, error) {
	log.Info("Starting kafka sarama producer ...", zap.Reflect("config", config))
	cfg, err := newSaramaConfigImpl(ctx, config)
	if err != nil {
		return nil, err
	}

	if err := topicPreProcess(config, cfg); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	asyncClient, err := sarama.NewAsyncProducer(config.BrokerEndpoints, cfg)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}
	syncClient, err := sarama.NewSyncProducer(config.BrokerEndpoints, cfg)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	notifier := new(notify.Notifier)
	flushedReceiver, err := notifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, err
	}
	k := &kafkaSaramaProducer{
		asyncClient:  asyncClient,
		syncClient:   syncClient,
		topic:        config.TopicName,
		partitionNum: config.PartitionNum,
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
	go func() {
		if err := k.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("error channel is full", zap.Error(err))
			}
		}
	}()
	return k, nil
}

func init() {
	sarama.MaxRequestSize = 1024 * 1024 * 1024 // 1GB
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

// NewSaramaConfig return the default config and set the according version and metrics
func newSaramaConfig(ctx context.Context, c *Config) (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(c.Version)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidVersion, err)
	}
	var role string
	if util.IsOwnerFromCtx(ctx) {
		role = "owner"
	} else {
		role = "processor"
	}
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)

	config.ClientID, err = kafkaClientID(role, captureAddr, changefeedID, c.ClientID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	config.Version = version
	// See: https://kafka.apache.org/documentation/#replication
	// When one of the brokers in a Kafka cluster is down, the partition leaders in this broker is broken,
	// Kafka will election a new partition leader and replication logs, this process will last from a few seconds to a few minutes.
	// Kafka cluster will not provide a writing service in this process.
	// Time out in one minute(120 * 500ms).
	config.Metadata.Retry.Max = 120
	config.Metadata.Retry.Backoff = 500 * time.Millisecond

	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = c.MaxMessageBytes
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	// Time out in five minutes(600 * 500ms).
	config.Producer.Retry.Max = 600
	config.Producer.Retry.Backoff = 500 * time.Millisecond

	switch strings.ToLower(strings.TrimSpace(c.Compression)) {
	case "none":
		config.Producer.Compression = sarama.CompressionNone
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	default:
		log.Warn("Unsupported compression algorithm", zap.String("compression", c.Compression))
		config.Producer.Compression = sarama.CompressionNone
	}

	// Time out in one minute(120 * 500ms).
	config.Admin.Retry.Max = 120
	config.Admin.Retry.Backoff = 500 * time.Millisecond
	config.Admin.Timeout = 20 * time.Second

	if c.Credential != nil && len(c.Credential.CAPath) != 0 {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config, err = c.Credential.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if c.SaslScram != nil && len(c.SaslScram.SaslUser) != 0 {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = c.SaslScram.SaslUser
		config.Net.SASL.Password = c.SaslScram.SaslPassword
		config.Net.SASL.Mechanism = sarama.SASLMechanism(c.SaslScram.SaslMechanism)
		if strings.EqualFold(c.SaslScram.SaslMechanism, "SCRAM-SHA-256") {
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &security.XDGSCRAMClient{HashGeneratorFcn: security.SHA256} }
		} else if strings.EqualFold(c.SaslScram.SaslMechanism, "SCRAM-SHA-512") {
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &security.XDGSCRAMClient{HashGeneratorFcn: security.SHA512} }
		} else {
			return nil, errors.New("Unsupported sasl-mechanism, should be SCRAM-SHA-256 or SCRAM-SHA-512")
		}
	}

	return config, err
}
