package mqProducer

import (
	"context"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pingcap/ticdc/pkg/util"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// KafkaConfig stores the Kafka configuration
type KafkaConfig struct {
	PartitionNum      int32
	ReplicationFactor int16

	Version         string
	MaxMessageBytes int
}

// DefaultKafkaConfig is the default Kafka configuration
var DefaultKafkaConfig = KafkaConfig{
	Version:           "2.4.0",
	MaxMessageBytes:   1 << 26, // 64M
	ReplicationFactor: 1,
}

type kafkaSaramaProducer struct {
	asyncClient  sarama.AsyncProducer
	topic        string
	partitionNum int32
	currentIndex uint64

	partitionMaxSentIndex    []uint64
	partitionMaxSucceedIndex []uint64
}

func (k *kafkaSaramaProducer) Run(ctx context.Context) error {
	if util.IsOwnerFromCtx(ctx) {
		log.Info("run kafkaSaramaProducer")
	}
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case msg := <-k.asyncClient.Successes():
			log.Info("successes", zap.Reflect("msg", msg))
			cb := msg.Metadata.(func(error))
			cb(nil)
		case err := <-k.asyncClient.Errors():
			log.Info("error", zap.Reflect("err", err))
			cb := err.Msg.Metadata.(func(error))
			cb(err.Err)
		}
	}
}

func (k *kafkaSaramaProducer) SendMessage(ctx context.Context, key []byte, value []byte, partition int32, callback func(err error)) (uint64, error) {
	if util.IsOwnerFromCtx(ctx) {
		log.Info("send message", zap.ByteString("key", key), zap.ByteString("value", value), zap.Int32("partition", partition))
	}
	index := atomic.AddUint64(&k.currentIndex, 1)
	atomic.StoreUint64(&k.partitionMaxSentIndex[partition], index)

	cb := func(err error) {
		atomic.StoreUint64(&k.partitionMaxSucceedIndex[partition], index)
		if callback != nil {
			callback(err)
		}
	}
	select {
	case <-ctx.Done():
		return 0, errors.Trace(ctx.Err())
	case k.asyncClient.Input() <- &sarama.ProducerMessage{
		Topic:     k.topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(value),
		Partition: partition,
		Metadata:  cb,
	}:
	}
	if util.IsOwnerFromCtx(ctx) {
		log.Info("finish sent", zap.Uint64("index", index))
	}
	return index, nil
}

func (k *kafkaSaramaProducer) BroadcastMessage(ctx context.Context, key []byte, value []byte, callback func(err error)) (uint64, error) {
	var lastIndex uint64
	var err error
	for i := int32(0); i < k.partitionNum; i++ {
		lastIndex, err = k.SendMessage(ctx, key, value, i, callback)
	}
	return lastIndex, err
}

func (k *kafkaSaramaProducer) SyncBroadcastMessage(ctx context.Context, key []byte, value []byte) error {
	wg, cctx := errgroup.WithContext(ctx)
	for i := int32(0); i < k.partitionNum; i++ {
		wg.Go(func() error {
			var err1, err2 error
			done := make(chan struct{})
			_, err1 = k.SendMessage(cctx, key, value, i, func(err error) {
				err2 = err
				log.Info("done")
				close(done)
			})
			if err1 != nil {
				return err1
			}
			<-done
			log.Info("done2")
			return err2
		})
	}
	return wg.Wait()
}

func (k *kafkaSaramaProducer) MaxSuccessesIndex() uint64 {
	maxSentIndex := uint64(0)
	minSucceededIndex := uint64(math.MaxUint64)
	for i := 0; i < int(k.partitionNum); i++ {
		succeedIndex := atomic.LoadUint64(&k.partitionMaxSucceedIndex[i])
		sentIndex := atomic.LoadUint64(&k.partitionMaxSentIndex[i])
		if maxSentIndex < sentIndex {
			maxSentIndex = sentIndex
		}
		// if succeedIndex is equal to sentIndex, it means that all of the msgs are sent in this partition,
		if minSucceededIndex > succeedIndex && succeedIndex != sentIndex {
			minSucceededIndex = succeedIndex
		}
		log.Info("find index", zap.Uint64("success", succeedIndex), zap.Uint64("sent", sentIndex), zap.Int("partition", i))
	}
	if minSucceededIndex == uint64(math.MaxUint64) {
		minSucceededIndex = maxSentIndex
	}
	log.Info("get max success index", zap.Uint64("maxIndex", minSucceededIndex))
	return minSucceededIndex
}

// NewKafkaSaramaProducer creates a kafka sarama producer
func NewKafkaSaramaProducer(address string, topic string, config KafkaConfig) (*kafkaSaramaProducer, error) {
	cfg, err := newSaramaConfig(config)
	if err != nil {
		return nil, err
	}
	asyncClient, err := sarama.NewAsyncProducer(strings.Split(address, ","), cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// get partition number or create topic automatically
	admin, err := sarama.NewClusterAdmin(strings.Split(address, ","), cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	topics, err := admin.ListTopics()
	if err != nil {
		return nil, errors.Trace(err)
	}
	partitionNum := config.PartitionNum
	topicDetail, exist := topics[topic]
	if exist {
		log.Info("get partition number of topic", zap.String("topic", topic), zap.Int32("partition_num", topicDetail.NumPartitions))
		if partitionNum == 0 {
			partitionNum = topicDetail.NumPartitions
		} else if partitionNum < topicDetail.NumPartitions {
			log.Warn("partition number assigned in sink-uri is less than that of topic")
		} else if partitionNum > topicDetail.NumPartitions {
			return nil, errors.Errorf("partition number(%d) assigned in sink-uri is more than that of topic(%d)", partitionNum, topicDetail.NumPartitions)
		}
	} else {
		if partitionNum == 0 {
			partitionNum = 4
			log.Warn("topic not found and partition number is not specified, using default partition number", zap.String("topic", topic), zap.Int32("partition_num", partitionNum))
		}
		err := admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     partitionNum,
			ReplicationFactor: config.ReplicationFactor,
		}, false)
		log.Info("create a topic", zap.String("topic", topic), zap.Int32("partition_num", partitionNum), zap.Int16("replication_factor", config.ReplicationFactor))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	err = admin.Close()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &kafkaSaramaProducer{
		asyncClient:              asyncClient,
		topic:                    topic,
		partitionNum:             partitionNum,
		partitionMaxSucceedIndex: make([]uint64, partitionNum),
		partitionMaxSentIndex:    make([]uint64, partitionNum),
	}, nil
}

// NewSaramaConfig return the default config and set the according version and metrics
func newSaramaConfig(c KafkaConfig) (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(c.Version)
	if err != nil {
		return nil, errors.Trace(err)
	}

	config.ClientID = "ticdc_kafka_sarama_producer"
	config.Version = version
	log.Debug("kafka consumer", zap.Stringer("version", version))

	config.Producer.Flush.MaxMessages = c.MaxMessageBytes
	config.Metadata.Retry.Max = 10000
	config.Metadata.Retry.Backoff = 500 * time.Millisecond

	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = 1 << 30
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	config.Producer.Retry.Max = 10000
	config.Producer.Retry.Backoff = 500 * time.Millisecond
	return config, err
}

func (k *kafkaSaramaProducer) GetPartitionNum() int32 {
	return k.partitionNum
}

func (k *kafkaSaramaProducer) Close() error {
	return k.asyncClient.Close()
}
