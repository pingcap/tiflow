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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

// Config stores the Kafka configuration
type Config struct {
	PartitionNum      int32
	ReplicationFactor int16

	Version         string
	MaxMessageBytes int
	Compression     string
	ClientID        string
	Credential      *security.Credential
	// TODO support SASL authentication
}

// NewKafkaConfig returns a default Kafka configuration
func NewKafkaConfig() Config {
	return Config{
		Version:           "2.4.0",
		MaxMessageBytes:   512 * 1024 * 1024, // 512M
		ReplicationFactor: 1,
		Compression:       "none",
		Credential:        &security.Credential{},
	}
}

type kafkaSaramaProducer struct {
	// clientLock is used to protect concurrent access of asyncClient and syncClient.
	// Since we don't close these two clients (which have a input chan) from the
	// sender routine, data race or send on closed chan could happen.
	clientLock   sync.RWMutex
	asyncClient  sarama.AsyncProducer
	syncClient   sarama.SyncProducer
	topic        string
	partitionNum int32

	partitionOffset []struct {
		flushed uint64
		sent    uint64
	}
	flushedNotifier *notify.Notifier
	flushedReceiver *notify.Receiver

	failpointCh chan error

	closeCh chan struct{}
	closed  int32
}

func (k *kafkaSaramaProducer) SendMessage(ctx context.Context, key []byte, value []byte, partition int32) error {
	k.clientLock.RLock()
	defer k.clientLock.RUnlock()
	msg := &sarama.ProducerMessage{
		Topic:     k.topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(value),
		Partition: partition,
	}
	msg.Metadata = atomic.AddUint64(&k.partitionOffset[partition].sent, 1)

	failpoint.Inject("KafkaSinkAsyncSendError", func() {
		// simulate sending message to intput channel successfully but flushing
		// message to Kafka meets error
		log.Info("failpoint error injected")
		k.failpointCh <- errors.New("kafka sink injected error")
		failpoint.Return(nil)
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-k.closeCh:
		return nil
	default:
		k.asyncClient.Input() <- msg
	}
	return nil
}

func (k *kafkaSaramaProducer) SyncBroadcastMessage(ctx context.Context, key []byte, value []byte) error {
	k.clientLock.RLock()
	defer k.clientLock.RUnlock()
	msgs := make([]*sarama.ProducerMessage, k.partitionNum)
	for i := 0; i < int(k.partitionNum); i++ {
		msgs[i] = &sarama.ProducerMessage{
			Topic:     k.topic,
			Key:       sarama.ByteEncoder(key),
			Value:     sarama.ByteEncoder(value),
			Partition: int32(i),
		}
	}
	select {
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
			return cerror.ErrKafkaFlushUnfished.GenWithStackByArgs()
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
func (k *kafkaSaramaProducer) stop() {
	k.clientLock.Lock()
	defer k.clientLock.Unlock()
	select {
	case <-k.closeCh:
		return
	default:
		close(k.closeCh)
	}
}

// Close implements the Producer interface
func (k *kafkaSaramaProducer) Close() error {
	k.stop()
	k.clientLock.Lock()
	defer k.clientLock.Unlock()
	// close sarama client multiple times will cause panic
	if atomic.LoadInt32(&k.closed) == 1 {
		return nil
	}
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
	atomic.StoreInt32(&k.closed, 1)
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
			return cerror.WrapError(cerror.ErrKafkaAsyncSendMessage, err)
		}
	}
}

// NewKafkaSaramaProducer creates a kafka sarama producer
func NewKafkaSaramaProducer(ctx context.Context, address string, topic string, config Config, errCh chan error) (*kafkaSaramaProducer, error) {
	log.Info("Starting kafka sarama producer ...", zap.Reflect("config", config))
	cfg, err := newSaramaConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	if config.PartitionNum < 0 {
		return nil, cerror.ErrKafkaInvalidPartitionNum.GenWithStackByArgs(config.PartitionNum)
	}
	asyncClient, err := sarama.NewAsyncProducer(strings.Split(address, ","), cfg)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}
	syncClient, err := sarama.NewSyncProducer(strings.Split(address, ","), cfg)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	// get partition number or create topic automatically
	admin, err := sarama.NewClusterAdmin(strings.Split(address, ","), cfg)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}
	topics, err := admin.ListTopics()
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}
	partitionNum := config.PartitionNum
	topicDetail, exist := topics[topic]
	if exist {
		log.Info("get partition number of topic", zap.String("topic", topic), zap.Int32("partition_num", topicDetail.NumPartitions))
		if partitionNum == 0 {
			partitionNum = topicDetail.NumPartitions
		} else if partitionNum < topicDetail.NumPartitions {
			log.Warn("partition number assigned in sink-uri is less than that of topic", zap.Int32("topic partition num", topicDetail.NumPartitions))
		} else if partitionNum > topicDetail.NumPartitions {
			return nil, cerror.ErrKafkaInvalidPartitionNum.GenWithStack(
				"partition number(%d) assigned in sink-uri is more than that of topic(%d)", partitionNum, topicDetail.NumPartitions)
		}
	} else {
		if partitionNum == 0 {
			partitionNum = 4
			log.Warn("topic not found and partition number is not specified, using default partition number", zap.String("topic", topic), zap.Int32("partition_num", partitionNum))
		}
		log.Info("create a topic", zap.String("topic", topic), zap.Int32("partition_num", partitionNum), zap.Int16("replication_factor", config.ReplicationFactor))
		err := admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     partitionNum,
			ReplicationFactor: config.ReplicationFactor,
		}, false)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
		}
	}

	err = admin.Close()
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}
	notifier := new(notify.Notifier)
	k := &kafkaSaramaProducer{
		asyncClient:  asyncClient,
		syncClient:   syncClient,
		topic:        topic,
		partitionNum: partitionNum,
		partitionOffset: make([]struct {
			flushed uint64
			sent    uint64
		}, partitionNum),
		flushedNotifier: notifier,
		flushedReceiver: notifier.NewReceiver(50 * time.Millisecond),
		closeCh:         make(chan struct{}),
		failpointCh:     make(chan error, 1),
	}
	go func() {
		if err := k.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			}
		}
	}()
	return k, nil
}

func init() {
	sarama.MaxRequestSize = 1024 * 1024 * 1024 // 1GB
}

var (
	validClienID      *regexp.Regexp = regexp.MustCompile(`\A[A-Za-z0-9._-]+\z`)
	commonInvalidChar *regexp.Regexp = regexp.MustCompile(`[\?:,"]`)
)

func kafkaClientID(role, captureAddr, changefeedID, configuredClientID string) (clientID string, err error) {
	if configuredClientID != "" {
		clientID = configuredClientID
	} else {
		clientID = fmt.Sprintf("TiCDC_sarama_producer_%s_%s_%s", role, captureAddr, changefeedID)
		clientID = commonInvalidChar.ReplaceAllString(clientID, "_")
	}
	if !validClienID.MatchString(clientID) {
		return "", cerror.ErrKafkaInvalidClientID.GenWithStackByArgs(clientID)
	}
	return
}

// NewSaramaConfig return the default config and set the according version and metrics
func newSaramaConfig(ctx context.Context, c Config) (*sarama.Config, error) {
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
	config.Metadata.Retry.Max = 20
	config.Metadata.Retry.Backoff = 500 * time.Millisecond

	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = c.MaxMessageBytes
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll

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

	config.Producer.Retry.Max = 20
	config.Producer.Retry.Backoff = 500 * time.Millisecond

	config.Admin.Retry.Max = 10000
	config.Admin.Retry.Backoff = 500 * time.Millisecond
	config.Admin.Timeout = 20 * time.Second

	if c.Credential != nil && len(c.Credential.CAPath) != 0 {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config, err = c.Credential.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return config, err
}
