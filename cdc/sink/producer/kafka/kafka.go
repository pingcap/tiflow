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
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/notify"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

const defaultPartitionNum = 3

// Config stores user specified Kafka producer configuration
type Config struct {
	BrokerEndpoints []string
	PartitionNum    int32

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
		MaxMessageBytes:   config.DefaultMaxMessageBytes,
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
	params := sinkURI.Query()
	s := params.Get("partition-num")
	if s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		if a <= 0 {
			return cerror.ErrKafkaInvalidPartitionNum.GenWithStackByArgs(a)
		}
		c.PartitionNum = int32(a)
	}

	s = sinkURI.Query().Get("replication-factor")
	if s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.ReplicationFactor = int16(a)
	}

	s = sinkURI.Query().Get("kafka-version")
	if s != "" {
		c.Version = s
	}

	s = sinkURI.Query().Get("max-message-bytes")
	if s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.MaxMessageBytes = a
		opts["max-message-bytes"] = s
	}

	s = sinkURI.Query().Get("max-batch-size")
	if s != "" {
		opts["max-batch-size"] = s
	}

	s = sinkURI.Query().Get("compression")
	if s != "" {
		c.Compression = s
	}

	c.ClientID = sinkURI.Query().Get("kafka-client-id")

	s = sinkURI.Query().Get("protocol")
	if s != "" {
		replicaConfig.Sink.Protocol = s
	}

	s = sinkURI.Query().Get("ca")
	if s != "" {
		c.Credential.CAPath = s
	}

	s = sinkURI.Query().Get("cert")
	if s != "" {
		c.Credential.CertPath = s
	}

	s = sinkURI.Query().Get("key")
	if s != "" {
		c.Credential.KeyPath = s
	}

	s = sinkURI.Query().Get("sasl-user")
	if s != "" {
		c.SaslScram.SaslUser = s
	}

	s = sinkURI.Query().Get("sasl-password")
	if s != "" {
		c.SaslScram.SaslPassword = s
	}

	s = sinkURI.Query().Get("sasl-mechanism")
	if s != "" {
		c.SaslScram.SaslMechanism = s
	}

	s = sinkURI.Query().Get("auto-create-topic")
	if s != "" {
		autoCreate, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		c.AutoCreate = autoCreate
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
	log.Info("kafka producer closing...")
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
	start := time.Now()
	err := k.asyncClient.Close()
	if err != nil {
		log.Error("close async client with error", zap.Error(err), zap.Duration("duration", time.Since(start)))
	} else {
		log.Info("async client closed", zap.Duration("duration", time.Since(start)))
	}
	start = time.Now()
	err = k.syncClient.Close()
	if err != nil {
		log.Error("close sync client with error", zap.Error(err), zap.Duration("duration", time.Since(start)))
	} else {
		log.Info("sync client closed", zap.Duration("duration", time.Since(start)))
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

func topicPreProcess(topic string, config *Config, saramaConfig *sarama.Config) error {
	// FIXME: find a way to remove this failpoint for workload the unit test
	failpoint.Inject("SkipTopicAutoCreate", func() {
		failpoint.Return(nil)
	})
	admin, err := sarama.NewClusterAdmin(config.BrokerEndpoints, saramaConfig)
	if err != nil {
		return cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Warn("close kafka cluster admin failed", zap.Error(err))
		}
	}()

	topics, err := admin.ListTopics()
	if err != nil {
		return cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	info, created := topics[topic]
	// once we have found the topic, no matter `auto-create-topic`, make sure user input parameters are valid.
	if created {
		// make sure that producer's `MaxMessageBytes` smaller than topic's `max.message.bytes`
		topicMaxMessageBytes, err := getTopicMaxMessageBytes(admin, info)
		if err != nil {
			return cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
		}

		if topicMaxMessageBytes < config.MaxMessageBytes {
			log.Warn("topic's `max.message.bytes` less than the user set `max-message-bytes`,"+
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

		if err := config.adjustPartitionNum(info.NumPartitions); err != nil {
			return errors.Trace(err)
		}

		return nil
	}

	if !config.AutoCreate {
		return cerror.ErrKafkaInvalidConfig.GenWithStack("`auto-create-topic` is false, and topic not found")
	}

	brokerMessageMaxBytes, err := getBrokerMessageMaxBytes(admin)
	if err != nil {
		log.Warn("TiCDC cannot find `message.max.bytes` from broker's configuration")
		return errors.Trace(err)
	}

	// when create the topic, `max.message.bytes` is decided by the broker,
	// it would use broker's `message.max.bytes` to set topic's `max.message.bytes`.
	// TiCDC need to make sure that the producer's `MaxMessageBytes` won't larger than
	// broker's `message.max.bytes`.
	if brokerMessageMaxBytes < config.MaxMessageBytes {
		log.Warn("broker's `message.max.bytes` less than the user set `max-message-bytes`,"+
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

var newSaramaConfigImpl = newSaramaConfig

// NewKafkaSaramaProducer creates a kafka sarama producer
func NewKafkaSaramaProducer(ctx context.Context, topic string, config *Config, opts map[string]string, errCh chan error) (*kafkaSaramaProducer, error) {
	log.Info("Starting kafka sarama producer ...", zap.Reflect("config", config))
	cfg, err := newSaramaConfigImpl(ctx, config)
	if err != nil {
		return nil, err
	}

	if err := topicPreProcess(topic, config, cfg); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}
	opts["max-message-bytes"] = strconv.Itoa(cfg.Producer.MaxMessageBytes)

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
		topic:        topic,
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
	// When one of the brokers in a Kafka cluster is down, the partition leaders
	// in this broker is broken, Kafka will election a new partition leader and
	// replication logs, this process will last from a few seconds to a few minutes.
	// Kafka cluster will not provide a writing service in this process.
	// Time out in one minute.
	config.Metadata.Retry.Max = 120
	config.Metadata.Retry.Backoff = 500 * time.Millisecond
	// If it is not set, this means a metadata request against an unreachable
	// cluster (all brokers are unreachable or unresponsive) can take up to
	// `Net.[Dial|Read]Timeout * BrokerCount * (Metadata.Retry.Max + 1) +
	// Metadata.Retry.Backoff * Metadata.Retry.Max`
	// to fail.
	// See: https://github.com/Shopify/sarama/issues/765
	// and https://github.com/pingcap/tiflow/issues/3352.
	config.Metadata.Timeout = 1 * time.Minute

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

// adjust the partition-num by the topic's partition count
func (c *Config) adjustPartitionNum(realPartitionCount int32) error {
	// user does not specify the `partition-num` in the sink-uri
	if c.PartitionNum == 0 {
		c.PartitionNum = realPartitionCount
		return nil
	}

	if c.PartitionNum < realPartitionCount {
		log.Warn("number of partition specified in sink-uri is less than that of the actual topic. "+
			"Some partitions will not have messages dispatched to",
			zap.Int32("sink-uri partitions", c.PartitionNum),
			zap.Int32("topic partitions", realPartitionCount))
		return nil
	}

	// Make sure that the user-specified `partition-num` is not greater than
	// the real partition count, since messages would be dispatched to different
	// partitions, this could prevent potential correctness problems.
	if c.PartitionNum > realPartitionCount {
		return cerror.ErrKafkaInvalidPartitionNum.GenWithStack(
			"the number of partition (%d) specified in sink-uri is more than that of actual topic (%d)",
			c.PartitionNum, realPartitionCount)
	}
	return nil
}
